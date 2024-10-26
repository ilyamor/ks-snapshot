package org.apache.kafka.streams.kstream

import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.streams.processor.StateStore
import org.apache.kafka.streams.scala.kstream.Materialized
import org.apache.kafka.streams.state._
import org.apache.kafka.streams.state.internals.RocksDbIndexedTimeOrderedWindowBytesStoreSupplier
import org.apache.kafka.streams.state.internals.StateStoreToS3.{S3StateStoreConfig, WindowedSnapshotSupplier}

import java.util.Properties

object implicitConversion {

  implicit def windowStoreToSnapshotStore[K, V, S <: StateStore](implicit
                                                                 keySerde: Serde[K],
                                                                 valueSerde: Serde[V],
                                                                 props: Properties
                                                                ): Materialized[K, V, S] = {
    val materialized = Materialized.`with`[K, V, S](keySerde, valueSerde)
    if (S3StateStoreConfig(props).getBoolean(S3StateStoreConfig.STATE_ENABLED)) {
      if (materialized.dslStoreSuppliers == null)
        materialized.dslStoreSuppliers = Utils.newInstance(classOf[BuiltInDslStoreSuppliers.RocksDBDslStoreSuppliers], classOf[DslStoreSuppliers])
      materialized.dslStoreSuppliers = new SnapshotStoreSupplier(materialized.dslStoreSuppliers, props)
      println("replacing windowStoreToSnapshotStore")
    }
    materialized
  }

  implicit class SnapshotMaterialized[K, V, S <: StateStore](materialized: Materialized[K, V, S]) {
    def withSnapshotEnabled(implicit props: Properties): Materialized[K, V, S] = {
      if (S3StateStoreConfig(props).getBoolean(S3StateStoreConfig.STATE_ENABLED)) {
        if (materialized.dslStoreSuppliers == null)
          materialized.dslStoreSuppliers = Utils.newInstance(classOf[BuiltInDslStoreSuppliers.RocksDBDslStoreSuppliers], classOf[DslStoreSuppliers])
        materialized.dslStoreSuppliers = new SnapshotStoreSupplier(materialized.dslStoreSuppliers, props)
      }
      materialized
    }
  }

  class SnapshotStoreSupplier(innerSupplier: DslStoreSuppliers, props: Properties) extends DslStoreSuppliers() {
    override def keyValueStore(params: DslKeyValueParams): KeyValueBytesStoreSupplier = {
      innerSupplier.keyValueStore(params)
    }

    override def windowStore(params: DslWindowParams): WindowBytesStoreSupplier = {
      val innerStore = innerSupplier.windowStore(params)
      if (params.emitStrategy.`type` eq EmitStrategy.StrategyType.ON_WINDOW_CLOSE) return RocksDbIndexedTimeOrderedWindowBytesStoreSupplier.create(params.name, params.retentionPeriod, params.windowSize, params.retainDuplicates, params.isSlidingWindow)
      new WindowedSnapshotSupplier(innerStore.name(), innerStore.retentionPeriod(), innerStore.segmentIntervalMs(), innerStore.windowSize(), innerStore.retainDuplicates(), params.isTimestamped, props)
    }

    override def sessionStore(params: DslSessionParams): SessionBytesStoreSupplier = {
      innerSupplier.sessionStore(params)
    }
  }
}