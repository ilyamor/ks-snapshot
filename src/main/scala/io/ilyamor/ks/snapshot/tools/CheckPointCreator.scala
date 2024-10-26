package io.ilyamor.ks.snapshot.tools

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.streams.state.internals.OffsetCheckpoint

import java.io.File
import java.nio.file.Path
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.util.Try

case class CheckPointCreator private (
  pathToVersion: Path,
  tp: Map[TopicPartition, java.lang.Long]
) {

  def write(): Either[Throwable, File] =
    Try {
      val file = pathToVersion.toFile
      new OffsetCheckpoint(file).write(tp.asJava)
      file
    }.toEither
}

object CheckPointCreator {
  def apply(dir: File, tp: TopicPartition, offset: Long): CheckPointCreator =
    create(dir, ".checkpoint", Map((tp, offset)))

  def create(
    dir: File,
    name: String,
    tp: Map[TopicPartition, java.lang.Long]
  ): CheckPointCreator = {
    val pathToVersion: Path = new File(s"${dir.getAbsolutePath}/$name").toPath
    pathToVersion.toFile.getParentFile.mkdirs()
    CheckPointCreator(pathToVersion, tp)
  }
}
