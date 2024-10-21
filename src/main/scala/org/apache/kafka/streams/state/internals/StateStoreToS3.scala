package org.apache.kafka.streams.state.internals

import io.ilyamor.ks.snapshot.Snapshoter
import io.ilyamor.ks.snapshot.tools.UploadS3ClientForStore
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.processor._
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl
import org.apache.kafka.streams.state.WindowStore
import org.apache.logging.log4j.scala.Logging
import org.rocksdb.RocksDB
import io.ilyamor.ks.utils.EitherOps.EitherOps
import org.apache.kafka.streams.state.internals.StateStoreToS3.S3StateStoreConfig.STATE_SNAPSHOT_FREQUENCY

import java.util.Properties
import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.Try

object StateStoreToS3 extends Logging {

  object SnapshotStoreListeners {
    case class TppStore(topicPartition: TopicPartition, storeName: String)

    case object SnapshotStoreListener extends StateRestoreListener with StandbyUpdateListener {

      private var isEnabled = false;

      def enable(): Unit = {
        isEnabled = true
      }

      override def onUpdateStart(topicPartition: TopicPartition, storeName: String, startingOffset: Long): Unit = {
        standby.put(TppStore(topicPartition, storeName), true)
        onRestoreStart(topicPartition, storeName, startingOffset, 0)
      }

      override def onBatchLoaded(topicPartition: TopicPartition, storeName: String, taskId: TaskId, batchEndOffset: Long, batchSize: Long, currentEndOffset: Long): Unit = {
        onBatchRestored(topicPartition, storeName, batchEndOffset, batchSize)
      }

      override def onUpdateSuspended(topicPartition: TopicPartition, storeName: String, storeOffset: Long, currentEndOffset: Long, reason: StandbyUpdateListener.SuspendReason): Unit = {
        standby.put(TppStore(topicPartition, storeName), false)
        onRestoreSuspended(topicPartition, storeName, currentEndOffset)
      }

      val taskStore: ConcurrentHashMap[TppStore, Boolean] = new ConcurrentHashMap[TppStore, Boolean]()
      val standby: ConcurrentHashMap[TppStore, Boolean] = new ConcurrentHashMap[TppStore, Boolean]()

      val workingFlush: ConcurrentHashMap[TppStore, Boolean] = new ConcurrentHashMap[TppStore, Boolean]()

      override def onRestoreStart(topicPartition: TopicPartition, storeName: String, startingOffset: Long, endingOffset: Long): Unit = {
        println(Thread.currentThread() + "before restore topic" + topicPartition + " store " + storeName)
        taskStore.put(TppStore(topicPartition, storeName), true)
      }

      override def onBatchRestored(topicPartition: TopicPartition, storeName: String, batchEndOffset: Long, numRestored: Long): Unit = {

        println(Thread.currentThread() + "on ")
      }

      override def onRestoreEnd(topicPartition: TopicPartition, storeName: String, totalRestored: Long): Unit = {
        taskStore.put(TppStore(topicPartition, storeName), false)

        println(Thread.currentThread() + "on end")

      }

      override def onRestoreSuspended(topicPartition: TopicPartition, storeName: String, totalRestored: Long): Unit = {
        if (totalRestored <= 0)
          taskStore.put(TppStore(topicPartition, storeName), true)
        else {
          taskStore.put(TppStore(topicPartition, storeName), false)
        }
        super.onRestoreSuspended(topicPartition, storeName, totalRestored)
      }
    }
  }

  class WindowedSnapshotSupplier(name: String,
                                 retentionPeriod: Long,
                                 segmentInterval: Long,
                                 windowSize: Long,
                                 retainDuplicates: Boolean,
                                 returnTimestampedStore: Boolean,
                                 streamProps: S3StateStoreConfig
      ) extends RocksDbWindowBytesStoreSupplier(name, retentionPeriod, segmentInterval, windowSize, retainDuplicates, returnTimestampedStore) {

    private val windowStoreType = if (returnTimestampedStore) RocksDbWindowBytesStoreSupplier.WindowStoreTypes.TIMESTAMPED_WINDOW_STORE else RocksDbWindowBytesStoreSupplier.WindowStoreTypes.DEFAULT_WINDOW_STORE

    override def get(): WindowStore[Bytes, Array[Byte]] = {
      windowStoreType match {
        case RocksDbWindowBytesStoreSupplier.WindowStoreTypes.DEFAULT_WINDOW_STORE =>
          new S3StateSegmentedStateStore[RocksDBSegmentedBytesStore, KeyValueSegment](
            new RocksDBSegmentedBytesStore(name, metricsScope, retentionPeriod, segmentInterval, new WindowKeySchema),
            retainDuplicates, windowSize, streamProps, { store: RocksDBSegmentedBytesStore => store.getSegments.asScala.map(_.db).toList }
          )
        case RocksDbWindowBytesStoreSupplier.WindowStoreTypes.TIMESTAMPED_WINDOW_STORE =>
          new S3StateSegmentedStateStore[RocksDBTimestampedSegmentedBytesStore, TimestampedSegment](
            new RocksDBTimestampedSegmentedBytesStore(name, metricsScope, retentionPeriod, segmentInterval, new WindowKeySchema),
            retainDuplicates, windowSize, streamProps, { store: RocksDBTimestampedSegmentedBytesStore => store.getSegments.asScala.map(_.db).toList }
          )
      }
    }
  }

  class S3StateSegmentedStateStore[T <: AbstractRocksDBSegmentedBytesStore[S], S <: Segment]
              (wrapped: SegmentedBytesStore, retainDuplicates: Boolean, windowSize: Long, config: S3StateStoreConfig, segmentFetcher: T => List[RocksDB])
      extends RocksDBWindowStore(wrapped, retainDuplicates, windowSize) with Logging {

    var snapshotFrequency:Int = _
    var context: StateStoreContext = _
    var snapshoter: Snapshoter[S, T] = _
    val snapshotStoreListener: SnapshotStoreListeners.SnapshotStoreListener.type = SnapshotStoreListeners.SnapshotStoreListener

    override def init(context: StateStoreContext, root: StateStore): Unit = {
      this.context = context
      // this.root = root

      this.snapshotFrequency = Option(config.getString(STATE_SNAPSHOT_FREQUENCY)).getOrElse("20").toInt
      val s3ClientWrapper = UploadS3ClientForStore(
        config, s"${context.applicationId()}/${context.taskId()}/${name()}"
      )
      val underlyingStore = this.wrapped.asInstanceOf[T]
      this.snapshoter = Snapshoter(
        snapshotStoreListener = snapshotStoreListener,
        s3ClientForStore = s3ClientWrapper,
        context = context.asInstanceOf[ProcessorContextImpl],
        storeName = name(),
        underlyingStore = underlyingStore,
        segmentFetcher = segmentFetcher
      )
      snapshoter.initFromSnapshot()
      Try {
        super.init(context, root)
      }.toEither.tapError { e =>
        logger.error(s"Error while initializing store: ${e.getMessage}")
      }
    }

    override def flush(): Unit = {
      super.flush()
      snapshoter.flushSnapshot(snapshotFrequency)
    }

    override def close(): Unit = {
      super.close()
    }
  }

  object S3StateStoreConfig {

    //def STATE_ENABLED = "state.s3.enabled"
    def STATE_BUCKET = "state.s3.bucket.name"
    def STATE_KEY_PREFIX = "state.s3.key.prefix"
    def STATE_REGION = "state.s3.region"
    def STATE_S3_ENDPOINT = "state.s3.endpoint"
    def STATE_SNAPSHOT_FREQUENCY = "state.s3.snapshot.frequency"

    private def CONFIG = new ConfigDef()
      //.define(STATE_ENABLED, Type.BOOLEAN, false, Importance.MEDIUM, "")
      .define(STATE_BUCKET, Type.STRING, "", Importance.MEDIUM, "")
      .define(STATE_KEY_PREFIX, Type.STRING, "", Importance.LOW, "")
      .define(STATE_REGION, Type.STRING, "", Importance.MEDIUM, "")
      .define(STATE_S3_ENDPOINT, Type.STRING, "", Importance.LOW, "")
      .define(STATE_S3_ENDPOINT, Type.STRING, "", Importance.LOW, "")


    def apply(props: Properties): S3StateStoreConfig = {
      new S3StateStoreConfig(CONFIG, props.asInstanceOf[java.util.Map[Any, Any]])
    }
  }

  class S3StateStoreConfig private (definition: ConfigDef, originals: java.util.Map[Any, Any])
    extends AbstractConfig(definition, originals) {}
}