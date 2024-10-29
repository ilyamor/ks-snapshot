package io.ilyamor.ks.snapshot

import io.ilyamor.ks.snapshot.Snapshoter.SnapshotMetrics
import io.ilyamor.ks.snapshot.tools.{Archiver, CheckPointCreator, StorageUploader}
import io.ilyamor.ks.utils.ConcurrentMapOps.ConcurrentMapOps
import io.ilyamor.ks.utils.EitherOps.EitherOps
import io.ilyamor.ks.utils.MetricUtils.MetricsTimer
import org.apache.commons.compress.archivers.ArchiveEntry
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.commons.io.FileUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Sensor
import org.apache.kafka.common.metrics.Sensor.RecordingLevel
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl
import org.apache.kafka.streams.processor.{StateStore, StateStoreContext}
import org.apache.kafka.streams.state.internals.StateStoreToS3.S3StateStoreConfig.{STATE_OFFSET_THRESHOLD, STATE_SNAPSHOT_FREQUENCY_SECONDS}
import org.apache.kafka.streams.state.internals.StateStoreToS3.SnapshotStoreListeners.SnapshotStoreListener.FlushingState
import org.apache.kafka.streams.state.internals.StateStoreToS3.SnapshotStoreListeners.TppStore
import org.apache.kafka.streams.state.internals.StateStoreToS3.{S3StateStoreConfig, SnapshotStoreListeners}
import org.apache.kafka.streams.state.internals.{AbstractRocksDBSegmentedBytesStore, OffsetCheckpoint, Segment}
import org.apache.logging.log4j.scala.Logging
import org.rocksdb.RocksDB

import java.io.{File, FileOutputStream, InputStream}
import java.lang
import java.lang.System.currentTimeMillis
import java.nio.file.{Files, Path}
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.Future
import scala.jdk.CollectionConverters.{MapHasAsScala, MutableMapHasAsJava}
import scala.util.Try
class Snapshoter[S <: Segment, Store <: AbstractRocksDBSegmentedBytesStore[S]](
  snapshotStoreListener: SnapshotStoreListeners.SnapshotStoreListener.type,
  storageClientForStore: StorageUploader,
  context: ProcessorContextImpl,
  storeName: String,
  underlyingStore: Store,
  segmentFetcher: Store => List[RocksDB],
  config: S3StateStoreConfig,
  metrics: SnapshotMetrics,
  implicit val ec: scala.concurrent.ExecutionContext
) extends Logging {

  private lazy val SNAPSHOT_FREQUENCY_MS = config.getLong(STATE_SNAPSHOT_FREQUENCY_SECONDS) * 1000
  private lazy val OFFSET_THRESHOLD_RESTORE_FROM_S3: Int = config.getInt(STATE_OFFSET_THRESHOLD)

  def initFromSnapshot(): Unit =
    Fetcher.initFromSnapshot()

  def flushSnapshot(): Unit =
    Flusher.flushSnapshot()

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
        (fetchAndWriteLocally(context, remoteCheckPoint) ->
          overrideLocalCheckPointFile(
            context,
            localCheckPointFile,
            remoteCheckPoint,
            ".checkpoint"
          ) -> {
          val localPosition = overLocalPositionFile(context)
          val remotePosition = getPositionFileFromDownloadedStore(context)
          overrideLocalCheckPointFile(
            context,
            localPosition.toOption,
            remotePosition.toOption,
            s"$storeName.position"
          )
        }).time(metrics.downloadAndExtractSensor)

      }
    }

    private def getPositionFileFromDownloadedStore(
      context: StateStoreContext
    ): Either[IllegalArgumentException, OffsetCheckpoint] = {
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

  private def overrideLocalCheckPointFile(
    context: StateStoreContext,
    localCheckPointFile: Option[OffsetCheckpoint],
    remoteCheckPoint: Option[OffsetCheckpoint],
    checkpointSuffix: String
  ) = {
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
      logger.info(
        s"Writing new offsets to local checkpoint file: $checkpointPath with new offset $newOffsets"
      )
      Try {
        new OffsetCheckpoint(new File(checkpointPath)).write(newOffsets)
      }.toEither
        .tapError(e => logger.error(s"Error while overriding local checkpoint file: $e", e))
    }
  }

  private def fetchAndWriteLocally(
    context: StateStoreContext,
    remoteCheckPoint: Option[OffsetCheckpoint]
  ): Either[Throwable, Unit] =
    remoteCheckPoint match {
      case Some(localCheckPointFile) =>
        localCheckPointFile.read().asScala.values.headOption match {
          case Some(offset) =>
            storageClientForStore
              .getStateStores(
                context.taskId.toString,
                storeName,
                context.applicationId(),
                offset.toString
              )
              .time(metrics.downloadStateSensor)
              .tapError { e =>
                metrics.downloadStateErrorSensor.record()
                logger.error(s"Error while fetching remote state store: $e", e)
              }
              .tapError(e => throw e)
              .map { (response: InputStream) =>
                val destDir = s"${context.stateDir.getAbsolutePath}"
                extractAndDecompress(destDir, response)
                  .time(metrics.extractStateSensor)
                  .tapError(_ => metrics.extractStateErrorSensor.record())
              }

          case None =>
            logger.error("remote checkpoint file is offsets is empty")
            Left(new IllegalArgumentException("remote checkpoint file is offsets is empty"))

        }
      case None =>
        logger.error("Local checkpoint file is empty")
        Left(new IllegalArgumentException("remote-- checkpoint file is empty"))
    }

  private def shouldFetchStateStoreFromSnapshot(
    localCheckPointFile: Option[OffsetCheckpoint],
    remoteCheckPoint: Option[OffsetCheckpoint]
  ) =
    (localCheckPointFile, remoteCheckPoint) match {
      case (_, None) =>
        logger.info("Remote checkpoint file not found, not fetching remote state store")
        false
      case (None, _) =>
        logger.info("Local checkpoint file not found, fetching remote state store")
        true

      case (Some(local), Some(remote)) => isOffsetBiggerThanMin(local, remote)
    }

  private def isOffsetBiggerThanMin(local: OffsetCheckpoint, remote: OffsetCheckpoint) = {
    val remoteOffsets = remote.read().asScala
    if (remoteOffsets.size != 1) {
      logger.warn(s"Remote checkpoint has more than one offset: $remoteOffsets")
      false
    } else {
      remoteOffsets.exists({
        case (storeName, remoteOffset) =>
          val localOffset = local.read().asScala.get(storeName)
          localOffset match {
            case Some(offset) =>
              val should = offset + OFFSET_THRESHOLD_RESTORE_FROM_S3 < remoteOffset
              logger.info(
                s"Remote offset is bigger than local offset by more than $offset -$remoteOffset"
              )
              should
            case None => true
          }
      })
    }
  }

  private def getLocalCheckpointFile(
    context: StateStoreContext
  ): Either[IllegalArgumentException, OffsetCheckpoint] = {
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

  private def fetchRemoteCheckPointFile(
    context: StateStoreContext
  ): Either[Throwable, OffsetCheckpoint] =
    storageClientForStore
      .getCheckpointFile(
        context.taskId.toString,
        storeName,
        context.applicationId()
      )
      .tapError(e => logger.error(s"Error while fetching remote checkpoint: $e"))
      .time(metrics.downloadCheckpointSensor)

  private def extractAndDecompress(
    destDir: String,
    response: InputStream
  ): Either[Throwable, Unit] =
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
        } while (entry != null)
      } finally {
        if (response != null) response.close()
        if (gzipInputStream != null) gzipInputStream.close()
        if (tarInputStream != null) tarInputStream.close()
      }
      ()
    }.toEither

  private object Flusher {

    def copyStorePathToTempDir(storePath: String): Path = {
      val storeDir = new File(storePath)
      val newTempDir = Files.createTempDirectory(s"${storeDir.getName}").toAbsolutePath
      val newTempDirFile =
        Files.createDirectory(Path.of(newTempDir.toAbsolutePath + "/" + storeName))
      FileUtils.copyDirectory(storeDir, newTempDirFile.toFile)
      newTempDirFile
    }

    def flushSnapshot(): Unit = {
      val stateDir = context.stateDir()
      val topic = context.changelogFor(storeName)
      val partition = context.taskId.partition()
      val tp = new TopicPartition(topic, partition)

      val tppStore = TppStore(tp, storeName)

      def createCheckpoint(tempDir: Path, positions: Map[TopicPartition, lang.Long]) =
        CheckPointCreator
          .create(tempDir.toFile, s"$storeName.position", positions)
          .write()

      if (!snapshotStoreListener.taskStore.getOrDefault(tppStore, false)
          && snapshotStoreListener.workingFlush.availableForWork(
            tppStore,
            System.currentTimeMillis(),
            SNAPSHOT_FREQUENCY_MS
          )
          && !snapshotStoreListener.standby.getOrDefault(tppStore, false)) {
        val sourceTopic = Option(Try(context.topic()).toOption).flatten
        val offset = Option(context.recordCollector())
          .flatMap(collector => Option(collector.offsets().get(tp)))
        if (offset.isDefined && sourceTopic.isDefined) {

          val segments = segmentFetcher(underlyingStore)
          segments.foreach(_.pauseBackgroundWork()).time(metrics.pauseRocksDBBackgroundSensor)
          val (tempDir, path) = copyStateToTempDir(stateDir, partition)
          segments.foreach(_.continueBackgroundWork()).time(metrics.pauseRocksDBBackgroundSensor)

          val stateStore: StateStore = context.stateManager().getStore(storeName)
          val stateStorePositions = stateStore.getPosition
          if (!stateStorePositions.isEmpty) {
            Future {
              logger.debug(
                s"starting to snapshot for task ${context.taskId()} store: " + storeName + " with offset: " + offset
              )
              val positions = stateStorePositions
                .getPartitionPositions(context.topic())
                .asScala
                .map(tp => (new TopicPartition(sourceTopic.get, tp._1), tp._2))
                .toMap

              snapshotStoreListener.workingFlush
                .put(tppStore, FlushingState(inWork = true, currentTimeMillis()))

              (for {
                positionFile       <- createCheckpoint(tempDir, positions)
                archivedFile       <- compressStateStore(offset, tempDir, path, positionFile)
                checkpointFile     <- CheckPointCreator(tempDir.toFile, tp, offset.get).write()
                uploadResultQuarto <- uploadToRemoteStorage(archivedFile, checkpointFile)
              } yield uploadResultQuarto)
                .tap(
                  e => logger.error(s"Error while uploading state store: $e", e),
                  files => logger.debug(s"Successfully uploaded state store: $files")
                )
                .time(metrics.flushStoreSensor)

              snapshotStoreListener.workingFlush
                .put(tppStore, FlushingState(inWork = false, System.currentTimeMillis()))

            }(ec)
          }
        }
      }

    }

    private def copyStateToTempDir(stateDir: File, partition: Int) = {
      val storePath = s"${stateDir.getAbsolutePath}/$storeName"
      //copying storePath to tempDir
      val tempDir = Files.createTempDirectory(s"$partition-$storeName")
      val path = copyStorePathToTempDir(storePath).time(metrics.copyRocksDBToTempSensor)
      (tempDir, path)
    }

    private def uploadToRemoteStorage(archivedFile: File, checkpointFile: File) =
      storageClientForStore
        .uploadStateStore(archivedFile, checkpointFile)
        .time(metrics.uploadStoreSensor)
        .tapError(_ => metrics.uploadStoreErrorSensor.record())

    private def compressStateStore(
      offset: Option[lang.Long],
      tempDir: Path,
      path: Path,
      positionFile: File
    ) =
      Archiver(
        tempDir.toFile,
        offset.get,
        new File(s"${path.toAbsolutePath}"),
        positionFile
      ).archive()
        .time(metrics.archiveSensor)
        .tapError(_ => metrics.archiveErrorSensor.record())
  }
}
object Snapshoter {
  import java.util.concurrent.{Executors, ThreadPoolExecutor}
  import scala.concurrent.ExecutionContext

  private val threadPool: ThreadPoolExecutor =
    Executors.newFixedThreadPool(3).asInstanceOf[ThreadPoolExecutor]
  implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(threadPool)
  //create ExecutionContext with 3 threads
  private val isClosed = new AtomicBoolean(false)
  Runtime.getRuntime.addShutdownHook(new Thread(() => shutdownHookThread()))
  private def shutdownHookThread(): Unit =
    if (isClosed.compareAndSet(false, true)) {
      threadPool.shutdown()
    }
  def apply[S <: Segment, Store <: AbstractRocksDBSegmentedBytesStore[S]](
    snapshotStoreListener: SnapshotStoreListeners.SnapshotStoreListener.type,
    storageClientForStore: StorageUploader,
    context: ProcessorContextImpl,
    storeName: String,
    underlyingStore: Store,
    segmentFetcher: Store => List[RocksDB],
    config: S3StateStoreConfig
  ) = {
    val metrics = SnapshotMetrics.registerMetrics(context, context.taskId.toString, storeName)
    val store: Snapshoter[S, Store] = new Snapshoter(
      snapshotStoreListener,
      storageClientForStore,
      context,
      storeName,
      underlyingStore,
      segmentFetcher,
      config,
      metrics,
      ec
    )
    store
  }

  object SnapshotMetrics {

    private val PREFIX = "s3-state-snapshot"
    private val OPERATION_GROUP_INIT = "init"
    private val OPERATION_GROUP_FLUSH = "flush"

    private def addTags(taskId: String, storeName: String) =
      List(List("storeName", storeName), List("taskId", taskId))

    def registerMetrics(
      context: StateStoreContext,
      taskId: String,
      storeName: String
    ): SnapshotMetrics = {
      val tagsMap: List[String] = addTags(taskId, storeName).flatten

      val downloadAndExtractSensor: Sensor = context
        .metrics()
        .addLatencyRateTotalSensor(
          PREFIX,
          OPERATION_GROUP_INIT,
          "all",
          RecordingLevel.INFO,
          tagsMap: _*
        )

      val downloadCheckpointSensor: Sensor = context
        .metrics()
        .addLatencyRateTotalSensor(
          PREFIX,
          OPERATION_GROUP_INIT,
          "download_checkpoint",
          RecordingLevel.INFO,
          tagsMap: _*
        )
      val downloadStateSensor: Sensor = context
        .metrics()
        .addLatencyRateTotalSensor(
          PREFIX,
          OPERATION_GROUP_INIT,
          "download_state",
          RecordingLevel.INFO,
          tagsMap: _*
        )
      val downloadStateErrorSensor: Sensor = context
        .metrics()
        .addRateTotalSensor(
          PREFIX,
          OPERATION_GROUP_INIT,
          "download_state_error",
          RecordingLevel.INFO,
          tagsMap: _*
        )
      val extractStateSensor: Sensor = context
        .metrics()
        .addLatencyRateTotalSensor(
          PREFIX,
          OPERATION_GROUP_INIT,
          "extract_state",
          RecordingLevel.INFO,
          tagsMap: _*
        )
      val extractStateErrorSensor: Sensor = context
        .metrics()
        .addRateTotalSensor(
          PREFIX,
          OPERATION_GROUP_INIT,
          "extract_state_error",
          RecordingLevel.INFO,
          tagsMap: _*
        )

      val pauseRocksDBBackgroundSensor: Sensor = context
        .metrics()
        .addLatencyRateTotalSensor(
          PREFIX,
          OPERATION_GROUP_FLUSH,
          "pause_rocksdb_background",
          RecordingLevel.INFO,
          tagsMap: _*
        )
      val continueRocksDBBackgroundSensor = context
        .metrics()
        .addLatencyRateTotalSensor(
          PREFIX,
          OPERATION_GROUP_FLUSH,
          "continue_rocksdb_background",
          RecordingLevel.INFO,
          tagsMap: _*
        )
      val copyRocksDBToTempSensor: Sensor = context
        .metrics()
        .addLatencyRateTotalSensor(
          PREFIX,
          OPERATION_GROUP_FLUSH,
          "copy_rocksdb_to_temp",
          RecordingLevel.INFO,
          tagsMap: _*
        )
      val archiveSensor: Sensor = context
        .metrics()
        .addLatencyRateTotalSensor(
          PREFIX,
          OPERATION_GROUP_FLUSH,
          "archive_store",
          RecordingLevel.INFO,
          tagsMap: _*
        )
      val archiveErrorSensor: Sensor = context
        .metrics()
        .addRateTotalSensor(
          PREFIX,
          OPERATION_GROUP_FLUSH,
          "archive_store_error",
          RecordingLevel.INFO,
          tagsMap: _*
        )
      val uploadStoreSensor: Sensor = context
        .metrics()
        .addLatencyRateTotalSensor(
          PREFIX,
          OPERATION_GROUP_FLUSH,
          "upload_state",
          RecordingLevel.INFO,
          tagsMap: _*
        )
      val uploadStoreErrorSensor: Sensor = context
        .metrics()
        .addRateTotalSensor(
          PREFIX,
          OPERATION_GROUP_FLUSH,
          "upload_state_error",
          RecordingLevel.INFO,
          tagsMap: _*
        )
      val flushStoreSensor: Sensor = context
        .metrics()
        .addLatencyRateTotalSensor(
          PREFIX,
          OPERATION_GROUP_FLUSH,
          "all",
          RecordingLevel.INFO,
          tagsMap: _*
        )
      SnapshotMetrics(
        flushStoreSensor,
        uploadStoreErrorSensor,
        uploadStoreSensor,
        archiveErrorSensor,
        archiveSensor,
        copyRocksDBToTempSensor,
        pauseRocksDBBackgroundSensor,
        continueRocksDBBackgroundSensor,
        extractStateErrorSensor,
        extractStateSensor,
        downloadStateErrorSensor,
        downloadStateSensor,
        downloadCheckpointSensor,
        downloadAndExtractSensor
      )
    }

  }
  case class SnapshotMetrics(
    flushStoreSensor: Sensor,
    uploadStoreErrorSensor: Sensor,
    uploadStoreSensor: Sensor,
    archiveErrorSensor: Sensor,
    archiveSensor: Sensor,
    copyRocksDBToTempSensor: Sensor,
    pauseRocksDBBackgroundSensor: Sensor,
    continueRocksDBBackgroundSensor: Sensor,
    extractStateErrorSensor: Sensor,
    extractStateSensor: Sensor,
    downloadStateErrorSensor: Sensor,
    downloadStateSensor: Sensor,
    downloadCheckpointSensor: Sensor,
    downloadAndExtractSensor: Sensor
  )
}
