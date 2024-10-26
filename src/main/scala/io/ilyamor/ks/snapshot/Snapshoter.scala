package io.ilyamor.ks.snapshot

import io.ilyamor.ks.snapshot.tools.{Archiver, CheckPointCreator, StorageUploader}
import io.ilyamor.ks.utils.ConcurrentMapOps.ConcurrentMapOps
import io.ilyamor.ks.utils.EitherOps.EitherOps
import org.apache.commons.compress.archivers.ArchiveEntry
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.commons.io.FileUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Sensor
import org.apache.kafka.common.metrics.Sensor.RecordingLevel
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl
import org.apache.kafka.streams.processor.{StateStore, StateStoreContext}
import org.apache.kafka.streams.state.internals.StateStoreToS3.S3StateStoreConfig
import org.apache.kafka.streams.state.internals.StateStoreToS3.S3StateStoreConfig.{STATE_OFFSET_THRESHOLD, STATE_SNAPSHOT_FREQUENCY_SECONDS}
import org.apache.kafka.streams.state.internals.StateStoreToS3.SnapshotStoreListeners.SnapshotStoreListener.FlushingState
import org.apache.kafka.streams.state.internals.StateStoreToS3.SnapshotStoreListeners.{SnapshotStoreListener, TppStore}
import org.apache.kafka.streams.state.internals.{AbstractRocksDBSegmentedBytesStore, OffsetCheckpoint, Segment}
import org.apache.logging.log4j.scala.Logging
import org.rocksdb.RocksDB

import java.io.{File, FileOutputStream, InputStream}
import java.lang
import java.lang.System.currentTimeMillis
import java.nio.file.{Files, Path}
import scala.concurrent.Future
import scala.jdk.CollectionConverters.{MapHasAsScala, MutableMapHasAsJava}
import scala.util.Try

case class Snapshoter[S <: Segment, Store <: AbstractRocksDBSegmentedBytesStore[S]](
                                                                                     snapshotStoreListener: SnapshotStoreListener.type,
                                                                                     storageClientForStore: StorageUploader,
                                                                                     context: ProcessorContextImpl,
                                                                                     storeName: String,
                                                                                     underlyingStore: Store,
                                                                                     segmentFetcher: Store => List[RocksDB],
                                                                                     config: S3StateStoreConfig
                                                                                   ) extends Logging {

  private lazy val OFFSET_THRESHOLD_RESTORE_FROM_S3: Int = config.getInt(STATE_OFFSET_THRESHOLD)
  private lazy val SNAPSHOT_FREQUENCY_MS = config.getLong(STATE_SNAPSHOT_FREQUENCY_SECONDS) * 1000;

  private lazy val downloadAndExtractSensor: Sensor = context.metrics().addLatencyRateTotalSensor("s3_state_store", "init", "all", RecordingLevel.INFO, "storeName", storeName)
  private lazy val downloadCheckpointSensor: Sensor = context.metrics().addLatencyRateTotalSensor("s3_state_store", "init", "download_checkpoint", RecordingLevel.INFO, "storeName", storeName)
  private lazy val downloadStateSensor: Sensor = context.metrics().addLatencyRateTotalSensor("s3_state_store", "init", "download_state", RecordingLevel.INFO, "storeName", storeName)
  private lazy val downloadStateErrorSensor: Sensor = context.metrics().addRateTotalSensor("s3_state_store", "init", "download_state_error", RecordingLevel.INFO, "storeName", storeName)
  private lazy val extractStateSensor: Sensor = context.metrics().addLatencyRateTotalSensor("s3_state_store", "init", "extract_state", RecordingLevel.INFO, "storeName", storeName)
  private lazy val extractStateErrorSensor: Sensor = context.metrics().addRateTotalSensor("s3_state_store", "init", "extract_state_error", RecordingLevel.INFO, "storeName", storeName)

  private lazy val pauseRocksDBBackgroundSensor: Sensor = context.metrics().addLatencyRateTotalSensor("s3_state_store", "flush", "pause_rocksdb_background", RecordingLevel.INFO, "storeName", storeName)
  private lazy val copyRocksDBToTempSensor: Sensor = context.metrics().addLatencyRateTotalSensor("s3_state_store", "flush", "copy_rocksdb_to_temp", RecordingLevel.INFO, "storeName", storeName)
  private lazy val archiveSensor: Sensor = context.metrics().addLatencyRateTotalSensor("s3_state_store", "flush", "archive_store", RecordingLevel.INFO, "storeName", storeName)
  private lazy val archiveErrorSensor: Sensor = context.metrics().addRateTotalSensor("s3_state_store", "flush", "archive_store_error", RecordingLevel.INFO, "storeName", storeName)
  private lazy val uploadStoreSensor: Sensor = context.metrics().addLatencyRateTotalSensor("s3_state_store", "flush", "upload_state", RecordingLevel.INFO, "storeName", storeName)
  private lazy val uploadStoreErrorSensor: Sensor = context.metrics().addRateTotalSensor("s3_state_store", "flush", "upload_state_error", RecordingLevel.INFO, "storeName", storeName)
  private lazy val flushStoreSensor: Sensor = context.metrics().addLatencyRateTotalSensor("s3_state_store", "flush", "all", RecordingLevel.INFO, "storeName", storeName)

  def initFromSnapshot() = {
    Fetcher.initFromSnapshot()
  }

  def flushSnapshot() = {
    Flusher.flushSnapshot()

  }

  private object Fetcher {

    def initFromSnapshot(): Unit = {
      val topic = context.changelogFor(storeName)
      val partition = context.taskId.partition()
      val tp = new TopicPartition(topic, partition)
      if (!Option(snapshotStoreListener.taskStore.get(TppStore(tp, storeName))).getOrElse(false))
        getSnapshotStore(context)
    }

    private def getSnapshotStore(context: StateStoreContext): Unit = {
      val localCheckPointFile = getLocalCheckpointFile(context).toOption
      val remoteCheckPoint = fetchRemoteCheckPointFile(context).toOption
      if (shouldFetchStateStoreFromSnapshot(localCheckPointFile, remoteCheckPoint)) {
        withLatencyMetrics(downloadAndExtractSensor, () =>
          fetchAndWriteLocally(context, remoteCheckPoint) ->
            overrideLocalCheckPointFile(context, localCheckPointFile, remoteCheckPoint, ".checkpoint") -> {
            val localPosition = overLocalPositionFile(context)
            val remotePosition = getPositionFileFromDownloadedStore(context)
            overrideLocalCheckPointFile(context, localPosition.toOption, remotePosition.toOption, s"${storeName}.position")
          }
        )
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

  private def withLatencyMetrics[V](sensor: Sensor, f: () => V): V = {
    val start = currentTimeMillis()
    val res = f()
    val end = currentTimeMillis()
    sensor.record(end - start, end)
    res
  }

  private def fetchAndWriteLocally(context: StateStoreContext, remoteCheckPoint: Option[OffsetCheckpoint]): Either[Throwable, Unit] = {
    remoteCheckPoint match {
      case Some(localCheckPointFile) =>

        localCheckPointFile.read().asScala.values.headOption match {
          case Some(offset) =>
            withLatencyMetrics(downloadStateSensor, () =>
              storageClientForStore.getStateStores(context.taskId.toString, storeName, context.applicationId(), offset.toString)
            )
              .tapError(e => {
                downloadStateErrorSensor.record()
                logger.error(s"Error while fetching remote state store: $e", e)
              })
              .tapError(e => throw e)
              .map((response: InputStream) => {
                val destDir = s"${context.stateDir.getAbsolutePath}"
                withLatencyMetrics(extractStateSensor, () => extractAndDecompress(destDir, response))
                  .tapError(_ => extractStateErrorSensor.record())
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
              val should = offset + OFFSET_THRESHOLD_RESTORE_FROM_S3 < remoteOffset
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
    withLatencyMetrics(downloadCheckpointSensor,
      () => storageClientForStore.getCheckpointFile(context.taskId.toString, storeName, context.applicationId())
    ).tapError(e => logger.error(s"Error while fetching remote checkpoint: $e"))
  }

  private def extractAndDecompress(destDir: String, response: InputStream): Either[Throwable, Unit] = {
    Try {
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
        while (entry != null)
      } finally {
        if (response != null) response.close()
        if (gzipInputStream != null) gzipInputStream.close()
        if (tarInputStream != null) tarInputStream.close()
      }
      ()
    }.toEither
  }

  private object Flusher {

    def copyStorePathToTempDir(storePath: String): Path = {
      val storeDir = new File(storePath)
      val newTempDir = Files.createTempDirectory(s"${storeDir.getName}").toAbsolutePath
      val newTempDirFile = Files.createDirectory(Path.of(newTempDir.toAbsolutePath + "/" + storeName))
      FileUtils.copyDirectory(storeDir, newTempDirFile.toFile)
      newTempDirFile
    }

    def flushSnapshot(): Unit = {
      val stateDir = context.stateDir()
      val topic = context.changelogFor(storeName)
      val partition = context.taskId.partition()
      val tp = new TopicPartition(topic, partition)

      val tppStore = TppStore(tp, storeName)
      if (!snapshotStoreListener.taskStore.getOrDefault(tppStore, false)
        && snapshotStoreListener.workingFlush.availableForWork(tppStore, System.currentTimeMillis(), SNAPSHOT_FREQUENCY_MS)
        && !snapshotStoreListener.standby.getOrDefault(tppStore, false)) {
        val sourceTopic = Option(Try(context.topic()).toOption).flatten
        val offset = Option(context.recordCollector())
          .flatMap(collector => Option(collector.offsets().get(tp)))
        if (offset.isDefined && sourceTopic.isDefined) {

          val (tempDir, path) = withLatencyMetrics(pauseRocksDBBackgroundSensor, () => {
            val segments = segmentFetcher(underlyingStore)
            segments.foreach(_.pauseBackgroundWork())
            val storePath = s"${stateDir.getAbsolutePath}/$storeName"
            //copying storePath to tempDir
            val paths = withLatencyMetrics(copyRocksDBToTempSensor, () => {
              val tempDir = Files.createTempDirectory(s"$partition-$storeName")
              val path = copyStorePathToTempDir(storePath)
              (tempDir, path)
            })
            segments.foreach(_.continueBackgroundWork())
            paths
          })

          val stateStore: StateStore = context.stateManager().getStore(storeName)
          val stateStorePositions = stateStore.getPosition
          if (!stateStorePositions.isEmpty) {
            Future {
              logger.debug(s"starting to snapshot for task ${context.taskId()} store: " + storeName + " with offset: " + offset)
              val positions: Map[TopicPartition, lang.Long] = stateStorePositions.getPartitionPositions(context.topic())
                .asScala.map(tp => (new TopicPartition(sourceTopic.get, tp._1), tp._2)).toMap
              snapshotStoreListener.workingFlush.put(tppStore, FlushingState(inWork = true, currentTimeMillis()))
              withLatencyMetrics(flushStoreSensor, () => {
                val files = for {
                  positionFile <- CheckPointCreator.create(tempDir.toFile, s"$storeName.position", positions).write()
                  archivedFile <- withLatencyMetrics(archiveSensor,
                    () => Archiver(tempDir.toFile, offset.get, new File(s"${path.toAbsolutePath}"), positionFile).archive()
                  ).tapError(_ => archiveErrorSensor.record())
                  checkpointFile <- CheckPointCreator(tempDir.toFile, tp, offset.get).write()
                  uploadResultQuarto <- withLatencyMetrics(uploadStoreSensor, () => storageClientForStore.uploadStateStore(archivedFile, checkpointFile))
                    .tapError(_ => uploadStoreErrorSensor.record())
                } yield uploadResultQuarto
                files
                  .tap(
                    e =>
                      logger.error(s"Error while uploading state store: $e", e),
                    files => logger.debug(s"Successfully uploaded state store: $files"))
              })
              snapshotStoreListener.workingFlush.put(tppStore, FlushingState(inWork = false, System.currentTimeMillis()))
            }(scala.concurrent.ExecutionContext.global)
          }
        }
      }

    }
  }
}

