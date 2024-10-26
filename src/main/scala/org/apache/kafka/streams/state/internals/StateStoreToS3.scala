package org.apache.kafka.streams.state.internals

import io.ilyamor.ks.snapshot.Snapshoter
import io.ilyamor.ks.snapshot.tools.{
  StorageUploader,
  StorageUploaderJava,
  UploadS3ClientForStore,
  UploaderUtils
}
import io.ilyamor.ks.utils.EitherOps.EitherOps
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigDef.Range.atLeast
import org.apache.kafka.common.config.ConfigDef.{ Importance, Type }
import org.apache.kafka.common.config.{ AbstractConfig, ConfigDef, ConfigException }
import org.apache.kafka.common.utils.{ Bytes, Utils }
import org.apache.kafka.streams.processor._
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl
import org.apache.kafka.streams.state.WindowStore
import org.apache.kafka.streams.state.internals.StateStoreToS3.S3StateStoreConfig.STATE_STORAGE_UPLOADER
import org.apache.logging.log4j.scala.Logging
import org.rocksdb.RocksDB

import java.io.{ File, InputStream }
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.Try

object StateStoreToS3 extends Logging {

  object SnapshotStoreListeners {
    case class TppStore(topicPartition: TopicPartition, storeName: String)

    case object SnapshotStoreListener extends StateRestoreListener with StandbyUpdateListener {

      private var isEnabled = false

      def enable(): Unit =
        isEnabled = true

      override def onUpdateStart(
        topicPartition: TopicPartition,
        storeName: String,
        startingOffset: Long
      ): Unit = {
        standby.put(TppStore(topicPartition, storeName), true)
        onRestoreStart(topicPartition, storeName, startingOffset, 0)
      }

      override def onBatchLoaded(
        topicPartition: TopicPartition,
        storeName: String,
        taskId: TaskId,
        batchEndOffset: Long,
        batchSize: Long,
        currentEndOffset: Long
      ): Unit =
        onBatchRestored(topicPartition, storeName, batchEndOffset, batchSize)

      override def onUpdateSuspended(
        topicPartition: TopicPartition,
        storeName: String,
        storeOffset: Long,
        currentEndOffset: Long,
        reason: StandbyUpdateListener.SuspendReason
      ): Unit = {
        standby.put(TppStore(topicPartition, storeName), false)
        onRestoreSuspended(topicPartition, storeName, currentEndOffset)
      }

      val taskStore: ConcurrentHashMap[TppStore, Boolean] =
        new ConcurrentHashMap[TppStore, Boolean]()
      val standby: ConcurrentHashMap[TppStore, Boolean] = new ConcurrentHashMap[TppStore, Boolean]()

      val workingFlush: ConcurrentHashMap[TppStore, FlushingState] =
        new ConcurrentHashMap[TppStore, FlushingState]()

      case class FlushingState(inWork: Boolean, lastFlush: Long)

      override def onRestoreStart(
        topicPartition: TopicPartition,
        storeName: String,
        startingOffset: Long,
        endingOffset: Long
      ): Unit = {
        println(
          Thread.currentThread() + "before restore topic" + topicPartition + " store " + storeName
        )
        taskStore.put(TppStore(topicPartition, storeName), true)
      }

      override def onBatchRestored(
        topicPartition: TopicPartition,
        storeName: String,
        batchEndOffset: Long,
        numRestored: Long
      ): Unit = {}

      override def onRestoreEnd(
        topicPartition: TopicPartition,
        storeName: String,
        totalRestored: Long
      ): Unit = {
        taskStore.put(TppStore(topicPartition, storeName), false)

        println(
          Thread.currentThread() + "after restore topic" + topicPartition + " store " + storeName
        )

      }

      override def onRestoreSuspended(
        topicPartition: TopicPartition,
        storeName: String,
        totalRestored: Long
      ): Unit = {
        if (totalRestored <= 0)
          taskStore.put(TppStore(topicPartition, storeName), true)
        else {
          taskStore.put(TppStore(topicPartition, storeName), false)
        }
        super.onRestoreSuspended(topicPartition, storeName, totalRestored)
      }
    }
  }

  class WindowedSnapshotSupplier(
    name: String,
    retentionPeriod: Long,
    segmentInterval: Long,
    windowSize: Long,
    retainDuplicates: Boolean,
    returnTimestampedStore: Boolean,
    props: Properties
  ) extends RocksDbWindowBytesStoreSupplier(
        name,
        retentionPeriod,
        segmentInterval,
        windowSize,
        retainDuplicates,
        returnTimestampedStore
      ) {

    private val windowStoreType =
      if (returnTimestampedStore)
        RocksDbWindowBytesStoreSupplier.WindowStoreTypes.TIMESTAMPED_WINDOW_STORE
      else RocksDbWindowBytesStoreSupplier.WindowStoreTypes.DEFAULT_WINDOW_STORE

    override def get(): WindowStore[Bytes, Array[Byte]] =
      windowStoreType match {
        case RocksDbWindowBytesStoreSupplier.WindowStoreTypes.DEFAULT_WINDOW_STORE =>
          new S3StateSegmentedStateStore[RocksDBSegmentedBytesStore, KeyValueSegment](
            new RocksDBSegmentedBytesStore(
              name,
              metricsScope,
              retentionPeriod,
              segmentInterval,
              new WindowKeySchema
            ),
            retainDuplicates,
            windowSize,
            props, { store: RocksDBSegmentedBytesStore =>
              store.getSegments.asScala.map(_.db).toList
            }
          )
        case RocksDbWindowBytesStoreSupplier.WindowStoreTypes.TIMESTAMPED_WINDOW_STORE =>
          new S3StateSegmentedStateStoreTimeStamped[
            RocksDBTimestampedSegmentedBytesStore,
            TimestampedSegment
          ](
            new RocksDBTimestampedSegmentedBytesStore(
              name,
              metricsScope,
              retentionPeriod,
              segmentInterval,
              new WindowKeySchema
            ),
            retainDuplicates,
            windowSize,
            props, { store: RocksDBTimestampedSegmentedBytesStore =>
              store.getSegments.asScala.map(_.db).toList
            }
          )
      }
  }

  private case class StorageUploaderWrapperToJava(storageUploader: StorageUploaderJava)
      extends StorageUploader {

    override def getCheckpointFile(
      partition: String,
      storeName: String,
      applicationId: String
    ): Either[Throwable, OffsetCheckpoint] =
      Try(storageUploader.getCheckpointFile(partition, storeName, applicationId)).toEither

    override def getStateStores(
      partition: String,
      storeName: String,
      applicationId: String,
      offset: String
    ): Either[Throwable, InputStream] =
      Try(storageUploader.getStateStores(partition, storeName, applicationId, offset)).toEither

    override def uploadStateStore(
      archiveFile: File,
      checkPoint: File
    ): Either[Throwable, (String, String, Long)] =
      Try(storageUploader.uploadStateStore(archiveFile, checkPoint))
        .map(tr => (tr.getPathToArchive, tr.getPathToCheckpoint, tr.getTimeOfUploading))
        .toEither

    override def configure(params: Properties, storeName: String): StorageUploader =
      StorageUploaderWrapperToJava(storageUploader.configure(params, storeName))
  }

  class S3StateSegmentedStateStore[T <: AbstractRocksDBSegmentedBytesStore[S], S <: Segment](
    wrapped: SegmentedBytesStore,
    retainDuplicates: Boolean,
    windowSize: Long,
    props: Properties,
    segmentFetcher: T => List[RocksDB]
  ) extends RocksDBWindowStore(wrapped, retainDuplicates, windowSize) with Logging {

    var context: StateStoreContext = _
    var snapshoter: Snapshoter[S, T] = _

    override def init(context: StateStoreContext, root: StateStore): Unit = {

      val prefixKey = s"${context.applicationId()}/${context.taskId()}/${name()}"

      val config = S3StateStoreConfig(props)

      val storageClientUploader =
        if (UploaderUtils.isJavaStorageUploaderInstance(config.getClass(STATE_STORAGE_UPLOADER))) {
          StorageUploaderWrapperToJava(
            Utils.newInstance(config.getClass(STATE_STORAGE_UPLOADER), classOf[StorageUploaderJava])
          ).configure(props, prefixKey)
        } else {
          Utils
            .newInstance(config.getClass(STATE_STORAGE_UPLOADER), classOf[StorageUploader])
            .configure(props, prefixKey)
        }

      val underlyingStore = this.wrapped.asInstanceOf[T]
      this.snapshoter = Snapshoter(
        snapshotStoreListener = SnapshotStoreListeners.SnapshotStoreListener,
        storageClientForStore = storageClientUploader,
        context = context.asInstanceOf[ProcessorContextImpl],
        storeName = name(),
        underlyingStore = underlyingStore,
        segmentFetcher = segmentFetcher,
        config = config
      )
      snapshoter.initFromSnapshot()
      Try {
        super.init(context, root)
      }.toEither.tapError(e => logger.error(s"Error while initializing store: ${e.getMessage}"))
    }

    override def flush(): Unit = {
      super.flush()
      snapshoter.flushSnapshot()
    }

    override def close(): Unit =
      super.close()
  }

  class S3StateSegmentedStateStoreTimeStamped[T <: AbstractRocksDBSegmentedBytesStore[S], S <: Segment](
    wrapped: SegmentedBytesStore,
    retainDuplicates: Boolean,
    windowSize: Long,
    props: Properties,
    segmentFetcher: T => List[RocksDB]
  ) extends RocksDBTimestampedWindowStore(wrapped, retainDuplicates, windowSize) with Logging {

    var context: StateStoreContext = _
    var snapshoter: Snapshoter[S, T] = _

    override def init(context: StateStoreContext, root: StateStore): Unit = {

      val prefixKey = s"${context.applicationId()}/${context.taskId()}/${name()}"

      val config: S3StateStoreConfig = S3StateStoreConfig(props)

      val storageClientUploader: StorageUploader =
        if (UploaderUtils.isJavaStorageUploaderInstance(config.getClass(STATE_STORAGE_UPLOADER))) {
          StorageUploaderWrapperToJava(
            Utils.newInstance(config.getClass(STATE_STORAGE_UPLOADER), classOf[StorageUploaderJava])
          ).configure(props, prefixKey)
        } else {
          Utils
            .newInstance(config.getClass(STATE_STORAGE_UPLOADER), classOf[StorageUploader])
            .configure(props, prefixKey)
        }

      val underlyingStore = this.wrapped.asInstanceOf[T]
      this.snapshoter = Snapshoter(
        snapshotStoreListener = SnapshotStoreListeners.SnapshotStoreListener,
        storageClientForStore = storageClientUploader,
        context = context.asInstanceOf[ProcessorContextImpl],
        storeName = name(),
        underlyingStore = underlyingStore,
        segmentFetcher = segmentFetcher,
        config = config
      )
      snapshoter.initFromSnapshot()
      Try {
        super.init(context, root)
      }.toEither.tapError(e => logger.error(s"Error while initializing store: ${e.getMessage}"))
    }

    override def flush(): Unit = {
      super.flush()
      snapshoter.flushSnapshot()
    }

    override def close(): Unit =
      super.close()
  }

  object S3StateStoreConfig {

    def STATE_ENABLED = "state.s3.enabled"

    def STATE_BUCKET = "state.s3.bucket.name"

    def STATE_KEY_PREFIX = "state.s3.key.prefix"

    def STATE_REGION = "state.s3.region"

    def STATE_S3_ENDPOINT = "state.s3.endpoint"

    def STATE_SNAPSHOT_FREQUENCY_SECONDS = "state.s3.snapshot.frequency.seconds"

    def STATE_OFFSET_THRESHOLD = "state.s3.offset.threshold"

    def STATE_STORAGE_UPLOADER = "state.s3.storage.uploader"

    private def CONFIG =
      new ConfigDef()
        .define(STATE_ENABLED, Type.BOOLEAN, false, Importance.MEDIUM, "")
        .define(
          STATE_BUCKET,
          Type.STRING,
          Importance.MEDIUM,
          "Defines S3 bucket to use to store state store. Required."
        )
        .define(
          STATE_KEY_PREFIX,
          Type.STRING,
          "",
          Importance.LOW,
          "Defines some s3 bucket key prefix to store state store. Optional."
        )
        .define(
          STATE_REGION,
          Type.STRING,
          Importance.MEDIUM,
          "Defines s3 region to use where to store state store. Required."
        )
        .define(
          STATE_S3_ENDPOINT,
          Type.STRING,
          "",
          Importance.LOW,
          "Defines custom S3 endpoint (like MinIO). Optional."
        )
        .define(
          STATE_SNAPSHOT_FREQUENCY_SECONDS,
          Type.LONG,
          60L,
          atLeast(1),
          Importance.MEDIUM,
          "Defines what is the frequency to flush state store in seconds. Default 60 seconds. Optional."
        )
        .define(
          STATE_OFFSET_THRESHOLD,
          Type.INT,
          10000,
          atLeast(100),
          Importance.LOW,
          "Defines what is the threshold to restore data from s3. If the value is less, than restoring from kafka. Should be grater than 100. Default 1000 Optional."
        )
        .define(
          STATE_STORAGE_UPLOADER,
          Type.CLASS,
          classOf[UploadS3ClientForStore],
          (name: String, value: Any) =>
            value match {
              case c: Class[Any] if c.isInstanceOf[Class[StorageUploader]] =>
              case _ =>
                throw new ConfigException(
                  s"Value $value for config $name should be subclass of ${StorageUploader.getClass.getName}"
                )
            },
          Importance.LOW,
          "Defines class to use for upload data to storage. Should implements StorageUploader trait and should have empty constructor"
        )

    def apply(props: Properties): S3StateStoreConfig =
      new S3StateStoreConfig(CONFIG, props.asInstanceOf[java.util.Map[Any, Any]])
  }

  class S3StateStoreConfig private (definition: ConfigDef, originals: java.util.Map[Any, Any])
      extends AbstractConfig(definition, originals) {}
}
