package io.ilyamor.ks.snapshot

import io.ilyamor.ks.snapshot.tools.{Archiver, CheckPointCreator, UploadS3ClientForStore}
import io.ilyamor.ks.utils.EitherOps.EitherOps
import org.apache.commons.compress.archivers.ArchiveEntry
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.commons.io.FileUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl
import org.apache.kafka.streams.processor.{StateStore, StateStoreContext}
import org.apache.kafka.streams.state.internals.StateStoreToS3.SnapshotStoreListeners.{SnapshotStoreListener, TppStore}
import org.apache.kafka.streams.state.internals.{AbstractRocksDBSegmentedBytesStore, OffsetCheckpoint, Segment}
import org.apache.logging.log4j.scala.Logging
import org.rocksdb.RocksDB
import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.services.s3.model.GetObjectResponse

import java.io.{File, FileOutputStream}
import java.lang
import java.lang.System.currentTimeMillis
import java.nio.file.{Files, Path}
import scala.concurrent.Future
import scala.jdk.CollectionConverters.{MapHasAsScala, MutableMapHasAsJava}
import scala.util.{Random, Try}


case class Snapshoter[S <: Segment, Store <: AbstractRocksDBSegmentedBytesStore[S]](
                                                                                     s3ClientForStore: UploadS3ClientForStore,
                                                                                     snapshotStoreListener: SnapshotStoreListener.type,
                                                                                     context: ProcessorContextImpl,
                                                                                     storeName: String,
                                                                                     underlyingStore: Store,
                                                                                     segmentFetcher: Store => List[RocksDB]) extends Logging {

  def initFromSnapshot() = {
    Fetcher.initFromSnapshot()
  }

  def flushSnapshot(snapshotFrequency:Int) = {
    Flusher.flushSnapshot(snapshotFrequency)
  }

  private object Fetcher {

    def initFromSnapshot() = {
      val topic = context.changelogFor(storeName)
      val partition = context.taskId.partition()
      val tp = new TopicPartition(topic, partition)
      if (!Option(snapshotStoreListener.taskStore.get(TppStore(tp, storeName))).getOrElse(false))
        getSnapshotStore(context)
    }

    private def getSnapshotStore(context: StateStoreContext) = {
      val localCheckPointFile = getLocalCheckpointFile(context).toOption
      val remoteCheckPoint = fetchRemoteCheckPointFile(context).toOption
      if (shouldFetchStateStoreFromSnapshot(localCheckPointFile, remoteCheckPoint)) {
        fetchAndWriteLocally(context, remoteCheckPoint) ->
          overrideLocalCheckPointFile(context, localCheckPointFile, remoteCheckPoint, ".checkpoint") -> {
          val localPosition = overLocalPositionFile(context)
          val remotePosition = getPositionFileFromDownloadedStore(context)
          overrideLocalCheckPointFile(context, localPosition.toOption, remotePosition.toOption, s"${storeName}.position")
        }
      }
    }

    private def getPositionFileFromDownloadedStore(context: StateStoreContext): Either[IllegalArgumentException, OffsetCheckpoint] = {
      val positionFile = s"${context.stateDir()}/$storeName/$storeName.position"
      val file = new File(positionFile)
      if (file.exists())
        Right(new OffsetCheckpoint(file))
      else {
        Left(new IllegalArgumentException("Checkpoint file not found"))
      }
    }
  }

  private def overLocalPositionFile(context: StateStoreContext) = {
    val checkpoint = ".position"
    val stateDir = context.stateDir()
    val checkpointFile = s"${stateDir.toString}/$checkpoint"
    val file = new File(checkpointFile)
    if (file.exists())
      Right(new OffsetCheckpoint(file))
    else {
      Left(new IllegalArgumentException("Checkpoint file not found"))
    }
  }

  private def overrideLocalCheckPointFile(context: StateStoreContext, localCheckPointFile: Option[OffsetCheckpoint], remoteCheckPoint: Option[OffsetCheckpoint], checkpointSuffix: String) = {
    val res = (localCheckPointFile, remoteCheckPoint) match {
      case (Some(local), Some(remote)) =>
        logger.info("Overriding local checkpoint file with existing one")
        val localOffsets = local.read().asScala
        val remoteOffsets = remote.read().asScala
        Right((localOffsets ++ remoteOffsets).asJava)

      case (None, Some(remote)) =>
        logger.info("Overriding local checkpoint file doesn't exist with existing one,using remote")
        Right(remote.read())

      case _ =>
        logger.error("Error while overriding local checkpoint file")
        Left(new IllegalArgumentException("Error while overriding local checkpoint file"))
    }
    res.flatMap { newOffsets =>

      val checkpointPath = context.stateDir().toString + "/" + checkpointSuffix
      logger.info(s"Writing new offsets to local checkpoint file: $checkpointPath with new offset $newOffsets")
      Try {
        new OffsetCheckpoint(new File(checkpointPath)).write(newOffsets)
      }.toEither
        .tapError(e => logger.error(s"Error while overriding local checkpoint file: $e", e))
    }
  }


  private def fetchAndWriteLocally(context: StateStoreContext, remoteCheckPoint: Option[OffsetCheckpoint]): Either[Throwable, Unit] = {
    remoteCheckPoint match {
      case Some(localCheckPointFile) =>
        localCheckPointFile.read().asScala.values.headOption match {
          case Some(offset) =>

            s3ClientForStore.getStateStores(context.taskId.toString, storeName, context.applicationId(), offset.toString)
              .tapError(e => logger.error(s"Error while fetching remote state store: $e", e))
              .tapError(e => throw e)
              .map((response: ResponseInputStream[GetObjectResponse]) => {
                val destDir = s"${context.stateDir.getAbsolutePath}"
                extractAndDecompress(destDir, response)
                ()
              })

          case None =>
            logger.error("remote checkpoint file is offsets is empty")
            Left(new IllegalArgumentException("remote checkpoint file is offsets is empty"))

        }
      case None =>
        logger.error("Local checkpoint file is empty")
        Left(new IllegalArgumentException("remote-- checkpoint file is empty"))
    }
  }

  private def shouldFetchStateStoreFromSnapshot(localCheckPointFile: Option[OffsetCheckpoint], remoteCheckPoint: Option[OffsetCheckpoint]) = {
    (localCheckPointFile, remoteCheckPoint) match {
      case (_, None) => {
        logger.info("Remote checkpoint file not found, not fetching remote state store")
        false
      }
      case (None, _) => {
        logger.info("Local checkpoint file not found, fetching remote state store")
        true
      }

      case (Some(local), Some(remote)) => isOffsetBiggerThanMin(local, remote)
    }
  }

  private def isOffsetBiggerThanMin(local: OffsetCheckpoint, remote: OffsetCheckpoint) = {
    val remoteOffsets = remote.read().asScala
    if (remoteOffsets.size != 1) {
      logger.warn(s"Remote checkpoint has more than one offset: $remoteOffsets")
      false
    }
    else {
      remoteOffsets.exists({
        case (storeName, remoteOffset) =>
          val localOffset = local.read().asScala.get(storeName)
          localOffset match {
            case Some(offset) => {
              val should = offset + OFFSETTHRESHOLD < remoteOffset
              logger.info(s"Remote offset is bigger than local offset by more than $offset -$remoteOffset")
              should
            }
            case None => true
          }
      })
    }
  }

  private def getLocalCheckpointFile(context: StateStoreContext): Either[IllegalArgumentException, OffsetCheckpoint] = {
    val checkpoint = ".checkpoint"
    val stateDir = context.stateDir()
    val checkpointFile = stateDir.toString + "/" + checkpoint
    val file = new File(checkpointFile)
    if (file.exists())
      Right(new OffsetCheckpoint(file))
    else {
      Left(new IllegalArgumentException("Checkpoint file not found"))
    }
  }

  private def fetchRemoteCheckPointFile(context: StateStoreContext): Either[Throwable, OffsetCheckpoint] = {
    s3ClientForStore.getCheckpointFile(context.taskId.toString, storeName, context.applicationId())
      .tapError(e => logger.error(s"Error while fetching remote checkpoint: $e"))
  }

  val OFFSETTHRESHOLD: Int = 10000

  private def extractAndDecompress(destDir: String, response: ResponseInputStream[GetObjectResponse]) = {
    try {
      val gzipInputStream = new GzipCompressorInputStream(response)
      val tarInputStream = new TarArchiveInputStream(gzipInputStream)
      try {
        var entry: ArchiveEntry = null
        do {
          entry = tarInputStream.getNextEntry
          if (entry != null) {
            val destPath = new File(destDir, entry.getName)
            if (entry.isDirectory) destPath.mkdirs // Create directories
            else {
              // Ensure parent directories exist
              destPath.getParentFile.mkdirs
              // Write file content
              try {
                val outputStream = new FileOutputStream(destPath)
                try {
                  val buffer = new Array[Byte](1024)
                  var bytesRead = 0
                  do {
                    bytesRead = tarInputStream.read(buffer)
                    if (bytesRead != -1)
                      outputStream.write(buffer, 0, bytesRead)
                  } while (bytesRead != -1)

                } finally if (outputStream != null) outputStream.close()
              }
            }
          }
        }
        while ((entry != null))
      } finally {
        if (response != null) response.close()
        if (gzipInputStream != null) gzipInputStream.close()
        if (tarInputStream != null) tarInputStream.close()
      }
    }
  }


  private object Flusher {

    def copyStorePathToTempDir(storePath: String): Path = {
      val storeDir = new File(storePath)
      val newTempDir = Files.createTempDirectory(s"${storeDir.getName}").toAbsolutePath
      val newTempDirFile = Files.createDirectory(Path.of(newTempDir.toAbsolutePath+ "/" + storeName))
      FileUtils.copyDirectory(storeDir, newTempDirFile.toFile)
      newTempDirFile
    }

    def flushSnapshot(snapshotFrequency:Int): Unit = {
      val stateDir = context.stateDir()
      val topic = context.changelogFor(storeName)
      val partition = context.taskId.partition()
      val tp = new TopicPartition(topic, partition)


      val tppStore = TppStore(tp, storeName)
      if (!snapshotStoreListener.taskStore.getOrDefault(tppStore, false)
        && !snapshotStoreListener.workingFlush.getOrDefault(tppStore, false)
        && !snapshotStoreListener.standby.getOrDefault(tppStore, false)) {
        val sourceTopic = Option(Try(context.topic()).toOption).flatten
        val offset = Option(context.recordCollector())
          .flatMap(collector => Option(collector.offsets().get(tp)))
        if (offset.isDefined && sourceTopic.isDefined && Random.nextInt(snapshotFrequency) == 0) {

          val time = currentTimeMillis()
          val segments = segmentFetcher(underlyingStore)

          segments.foreach(_.pauseBackgroundWork())
          println("pausing took: " + (currentTimeMillis() - time))
          val storePath = s"${stateDir.getAbsolutePath}/$storeName"
          //copying storePath to tempDir

          val tempDir = Files.createTempDirectory(s"$partition-$storeName")
          val path = copyStorePathToTempDir(storePath)
          val time2 = currentTimeMillis()

          segments.foreach(_.continueBackgroundWork())
          println("continue took: " + (currentTimeMillis() - time2))

          Future {
            snapshotStoreListener.workingFlush.put(tppStore, true)
            logger.info(s"starting to snapshot for task ${context.taskId()} store: " + storeName + " with offset: " + offset)

            val stateStore: StateStore = context.stateManager().getStore(storeName)
            val positions: Map[TopicPartition, lang.Long] = stateStore.getPosition.getPartitionPositions(context.topic())
              .asScala.map(tp => (new TopicPartition(sourceTopic.get, tp._1), tp._2)).toMap

            val files = for {
              positionFile <- CheckPointCreator.create(tempDir.toFile, s"$storeName.position", positions).write()
              archivedFile <- Archiver(tempDir.toFile, offset.get, new File(s"${path.toAbsolutePath}"), positionFile).archive()
              checkpointFile <- CheckPointCreator(tempDir.toFile, tp, offset.get).write()
              uploadResultQuarto <- s3ClientForStore.uploadStateStore(archivedFile, checkpointFile)
            } yield uploadResultQuarto
            files
              .tap(
                e => logger.error(s"Error while uploading state store: $e", e),
                files => logger.info(s"Successfully uploaded state store: $files"))
            snapshotStoreListener.workingFlush.put(tppStore, false)
          }(scala.concurrent.ExecutionContext.global)
        }

      }
    }

    println()
  }

}