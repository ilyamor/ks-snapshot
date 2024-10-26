package org.apache.kafka.streams

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.StoreFactory.S3StateBuilder
import org.apache.kafka.streams.kstream.implicitConversion
import org.apache.kafka.streams.processor.StateStore
import org.apache.kafka.streams.scala.kstream.Materialized
import org.apache.kafka.streams.state.internals.StateStoreToS3.SnapshotStoreListeners.SnapshotStoreListener

import java.util.Properties

object StoreFactory {

  implicit class KStreamOps(stream: KafkaStreams) {
    def enableS3Snapshot(): Unit = {
      stream.setGlobalStateRestoreListener(SnapshotStoreListener)
      stream.setStandbyUpdateListener(SnapshotStoreListener)
      SnapshotStoreListener.enable()

    }
  }

  case class S3StateBuilder(props: Properties) {
    def windowStoreToSnapshotStore[K, V, S <: StateStore](
      keySerde: Serde[K],
      valueSerde: Serde[V]
    ): Materialized[K, V, S] =
      implicitConversion.windowStoreToSnapshotStore(keySerde, valueSerde, props)

    /**
      * just enables S3Snapshot
      *
      * @param stream to enable S3Snapshot on
      */
    def enable(stream: KafkaStreams): KafkaStreams = {
      StoreFactory.KStreamOps(stream).enableS3Snapshot()
      stream
    }
  }

}

// for easier use for non-scala (a.k.a java/kotlin) code
object S3StateBuilder {

  /**
    * <pre>
    * <code>
    * StreamBuilder streamBuilder = ...;
    * S3StateBuilder builder = S3StateBuilder.builder(props);
    * Materialized<K, V, S> m1 = builder.windowStoreToSnapshotStore(keySerde, valueSerde);
    * Materialized<K, V, S> m2 = builder.windowStoreToSnapshotStore(keySerde, valueSerde);
    * // you should use building windows and/or aggregations with methods receiving Materialized,
    * // else state to s3 snapshoter won't be used
    * streamBuilder.windowBy(...).count(m1).toStream(....);
    * streamBuilder.windowBy(...).sum(m2).toStream(....);
    * KafkaStream streams = new KafkaStream(...);
    * builder.enable(streams).start();
    * </code>
    * </pre>
    *
    * @param props kafka props included "s3.state...." properties
    * @return S3StateBuilder
    */
  def builder(props: Properties): S3StateBuilder =
    new S3StateBuilder(props)
}
