package snapshot

import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.state.internals.CoralogixStore.SnapshotStoreListeners.SnapshotStoreListener


object StoreFactory {

  implicit class KStreamOps(stream: KafkaStreams) {
    def enableS3Snapshot(): Unit = {
      stream.setGlobalStateRestoreListener(SnapshotStoreListener)
      stream.setStandbyUpdateListener(SnapshotStoreListener)
      SnapshotStoreListener.enable()
    }
  }

}