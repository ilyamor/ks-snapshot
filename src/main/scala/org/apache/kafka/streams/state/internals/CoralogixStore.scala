package org.apache.kafka.streams.state.internals

import io.confluent.examples.streams.SnapshotStoreListener.{SnapshotStoreListener, TppStore}
import org.apache.commons.compress.archivers.ArchiveEntry
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl
import org.apache.kafka.streams.processor.{StateStore, StateStoreContext}
import org.apache.kafka.streams.state.WindowStore
import org.apache.logging.log4j.scala.Logging
import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{GetObjectRequest, GetObjectResponse}
import tools.EitherOps.EitherOps

import java.io.{File, FileOutputStream}
import scala.jdk.CollectionConverters.{mapAsScalaMapConverter, mutableMapAsJavaMapConverter}
import scala.util.{Try, Using}

object CoralogixStore extends Logging {


  case class SnapshotFile()

  class S3ClientWrapper(bucketName: String) {

    val region = Region.EU_NORTH_1
    val s3: S3Client = S3Client.builder
      .region(region)
      .build


    val CHECKPOINT = ".checkpoint"
    val state = "state.tar.gz"

    def upload(): Unit = {
      println("uploading to s3")
    }

    def getCheckpointFile(context: StateStoreContext, partition: String, storeName: String, applicationId: String): Either[Throwable, OffsetCheckpoint] = {
      val rootPath = s"$applicationId/$partition/$storeName"
      val checkpointPath = s"$rootPath/$CHECKPOINT"
      Try {
        val res: ResponseInputStream[GetObjectResponse] = s3.getObject(GetObjectRequest.builder().bucket(bucketName).key(checkpointPath).build())
        val tempFile = new File("checkpoint")

        Using.resource(new FileOutputStream(tempFile)) {
          fos =>
            res.transferTo(fos)
            tempFile
        }
      }.map(new OffsetCheckpoint(_))
        .toEither


    }

    def getStateStores(partition: String, storeName: String, applicationId: String): Either[Throwable, ResponseInputStream[GetObjectResponse]] = {
      val rootPath = s"$applicationId/$partition/$storeName"
      //      val version = getLatestVersion(partition, applicationId)
      val stateFileCompressed = s"$rootPath/$state"

      Try {
        s3.getObject(GetObjectRequest.builder().bucket(bucketName).key(stateFileCompressed).build())
        //check if exists and read
      }.toEither


    }

    private def getLatestVersion(partition: String, applicationId: String): String = {
      ""
    }
  }

  class CoralogixStore(bytesStore: SegmentedBytesStore, retainDuplicates: Boolean, windowSize: Long, snapshotStoreListener: SnapshotStoreListener) extends RocksDBWindowStore(bytesStore, retainDuplicates, windowSize) {

    var context: StateStoreContext = _
    var root: StateStore = _

    val coreLogicS3Client = new S3ClientWrapper("cx-snapshot-test")


    override def flush(): Unit = {
      super.flush()
      //      val newContext = context.asInstanceOf[ProcessorContextImpl]
      //
      //      val stateDir = newContext.stateDir()
      //      val storeName = name();
      //      val topic = newContext.changelogFor(storeName)
      //      val partition = newContext.taskId.partition()
      //      val tp = new TopicPartition(topic, partition)
      //      val offset = newContext.recordCollector().offsets().get(tp)
      //
      //      if (offset != null) {
      //        val storePath = s"${stateDir.getAbsolutePath}/$storeName"
      //        val tempDir = Files.createTempDirectory(s"$partition-$storeName")
      //
      //        val files = for {
      //          a <- Archiver(tempDir.toFile, offset: Long, new File(storePath)).archive()
      //          c <- CheckPointCreator(tempDir.toFile, offset).create()
      //          u <- S3ClientForStore(
      //            bucket = "cx-snapshot-test",
      //            prefix = "snapshots",
      //            region = Region.EU_NORTH_1,
      //            storeName = storeName,
      //            partition = partition,
      //            offset = offset
      //
      //          ).uploadStateStore(a, c)
      //        } yield (a, c)

      println()

    }

    override def init(context: StateStoreContext, root: StateStore): Unit = {
      this.context = context
      this.root = root
        val newContext = context.asInstanceOf[ProcessorContextImpl]
        val storeName = name();
        val topic = newContext.changelogFor(storeName)
        val partition = newContext.taskId.partition()
        val tp = new TopicPartition(topic, partition)
         if(!snapshotStoreListener.taskStore.getIfPresent(TppStore(tp, storeName)).getOrElse(false))
            getSnapshotStore(context)

      super.init(context, root)


    }

    override def close(): Unit = {
      val newContext = context.asInstanceOf[ProcessorContextImpl]
      super.close()
    }

    private def getSnapshotStore(context: StateStoreContext) = {
      val localCheckPointFile = getLocalCheckpointFile(context)
      val remoteCheckPoint = fetchRemoteCheckPointFile(context)
      if (shouldFetchStateStoreFromSnapshot(localCheckPointFile, remoteCheckPoint)) {
        fetchAndWriteLocally(context) ->
          overrideLocalCheckPointFile(context, localCheckPointFile, remoteCheckPoint)
      }

    }

    private def overrideLocalCheckPointFile(context: StateStoreContext, localCheckPointFile: Either[IllegalArgumentException, OffsetCheckpoint], remoteCheckPoint: Either[Throwable, OffsetCheckpoint]) = {
      val res = (localCheckPointFile, remoteCheckPoint) match {
        case (Right(local), Right(remote)) =>
          logger.info("Overriding local checkpoint file with existing one")

          val localOffsets = local.read().asScala
          val remoteOffsets = remote.read().asScala
          Right((localOffsets ++ remoteOffsets).asJava)
        case (Left(_), Right(remote)) =>
          logger.info("Overriding local checkpoint file doesn't exist with existing one,using remote")

          Right(remote.read())
        case _ => {
          logger.error("Error while overriding local checkpoint file")
          Left(new IllegalArgumentException("Error while overriding local checkpoint file"))

        }
      }
      res.flatMap { newOffsets =>

        val checkpointPath = context.stateDir().toString + "/" + ".checkpoint"
        logger.info(s"Writing new offsets to local checkpoint file: $checkpointPath with new offset $newOffsets")
        Try {
          new OffsetCheckpoint(new File(checkpointPath)).write(newOffsets)
        }.toEither
          .tapError(e => logger.error(s"Error while overriding local checkpoint file: $e", e))
      }
    }


    private def fetchAndWriteLocally(context: StateStoreContext): Either[Throwable, Unit] = {
      coreLogicS3Client.getStateStores(context.taskId.toString, name(), context.applicationId())
        .tapError(e => logger.error(s"Error while fetching remote state store: $e", e))
        .tapError(e => throw e)
        .map((response: ResponseInputStream[GetObjectResponse]) => {
          val destDir = s"${context.stateDir.getAbsolutePath}/${name()}"
          extractAndDecompress(destDir, response)

        })
    }

    private def shouldFetchStateStoreFromSnapshot(localCheckPointFile: Either[IllegalArgumentException, OffsetCheckpoint], remoteCheckPoint: Either[Throwable, OffsetCheckpoint]) = {
      (localCheckPointFile, remoteCheckPoint) match {
        case (Left(_), _) => {
          logger.info("Local checkpoint file not found, fetching remote state store")
          true
        }
        case (_, Left(_)) => {
          logger.info("Remote checkpoint file not found, not fetching remote state store")
          false
        }
        case (Right(local), Right(remote)) => isOffsetBiggerThanMin(local, remote)
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


    def getLocalCheckpointFile(context: StateStoreContext): Either[IllegalArgumentException, OffsetCheckpoint] = {
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
      coreLogicS3Client.getCheckpointFile(context, context.taskId.toString, name(), context.applicationId())
        .tapError(e => logger.error(s"Error while fetching remote checkpoint: $e"))
        .tapError(e => throw e)


    }

    private def appanedRemoteCheckPointToLocal(localCheckPoint: String, remoteCheckpoint: String) = {
      val local = Try(new OffsetCheckpoint(new File(localCheckPoint)))
      val remote = Try(new OffsetCheckpoint(new File(remoteCheckpoint)))


      localCheckPoint.split("\n").lastOption.getOrElse("") + remoteCheckpoint
    }

    private def shouldFetchRemoteDb(localCheckPoint: String, remoteCheckpoint: String) = {
      println("local: " + localCheckPoint)
      println("remote: " + remoteCheckpoint)
      Right(true)

    }


  }

  private var OFFSETTHRESHOLD = 10000


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

  class WindowsCoralogixSupplier(name: String,
                                 retentionPeriod: Long,
                                 segmentInterval: Long,
                                 windowSize: Long,
                                 retainDuplicates: Boolean,
                                 returnTimestampedStore: Boolean,
                                 snapshotStoreListener: SnapshotStoreListener
                                ) extends RocksDbWindowBytesStoreSupplier(name, retentionPeriod, segmentInterval, windowSize, retainDuplicates, returnTimestampedStore) {


    private val windowStoreType = if (returnTimestampedStore) RocksDbWindowBytesStoreSupplier.WindowStoreTypes.TIMESTAMPED_WINDOW_STORE else RocksDbWindowBytesStoreSupplier.WindowStoreTypes.DEFAULT_WINDOW_STORE

    override def get(): WindowStore[Bytes, Array[Byte]] = {
      windowStoreType match {
        case RocksDbWindowBytesStoreSupplier.WindowStoreTypes.DEFAULT_WINDOW_STORE =>
          new CoralogixStore(new RocksDBSegmentedBytesStore(name, metricsScope, retentionPeriod, segmentInterval, new WindowKeySchema), retainDuplicates, windowSize, snapshotStoreListener)

        case _ =>
          throw new IllegalArgumentException("invalid window store type: " + windowStoreType)
      }
    }
  }


}