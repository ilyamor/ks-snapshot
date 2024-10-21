package io.ilyamor.ks.snapshot.tools

import org.apache.kafka.streams.state.internals.OffsetCheckpoint

import java.io.{File, InputStream}
import java.util.Properties

trait StorageUploader {
  def getCheckpointFile(partition: String, storeName: String, applicationId: String): Either[Throwable, OffsetCheckpoint]
  def getStateStores(partition: String, storeName: String, applicationId: String, offset: String): Either[Throwable, InputStream]
  def uploadStateStore(archiveFile: File, checkPoint: File): Either[Throwable, (String, String, Long)]
  def configure(params: Properties, storeName: String): StorageUploader
}

object StorageUploader {}