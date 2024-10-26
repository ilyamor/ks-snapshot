package io.ilyamor.ks.utils

import org.apache.kafka.streams.state.internals.StateStoreToS3.SnapshotStoreListeners.SnapshotStoreListener.FlushingState
import org.apache.kafka.streams.state.internals.StateStoreToS3.SnapshotStoreListeners.TppStore

import java.util.concurrent.ConcurrentMap

object ConcurrentMapOps {
  implicit class ConcurrentMapOps(concurrentMap: ConcurrentMap[TppStore, FlushingState]) {
    def getOps(key: TppStore): Option[FlushingState] = {
      val value = concurrentMap.get(key)
      if (value == null) None else Option(value)
    }

    def availableForWork(key: TppStore, now: Long, frequencyMs: Long): Boolean =
      getOps(key) match {
        case None                                                                => true
        case Some(state) if state.inWork || now - state.lastFlush <= frequencyMs => false
        case _                                                                   => true
      }
  }

}
