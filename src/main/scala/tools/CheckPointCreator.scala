package tools

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.streams.state.internals.OffsetCheckpoint

import java.io.File
import java.nio.file.Path
import scala.jdk.CollectionConverters.mapAsJavaMapConverter
import scala.util.Try

case class CheckPointCreator private(pathToVersion: Path, tp: TopicPartition, offset: Long) {

  def create(): Either[Throwable, File] = {
    Try {
      val file = pathToVersion.toFile
      new OffsetCheckpoint(file)
        .write(Map((tp, java.lang.Long.valueOf(offset.toString))).asJava)
      file
    }.toEither
  }
}

object CheckPointCreator {
  def apply(dir: File, tp: TopicPartition, offset: Long): CheckPointCreator = {
    val pathToVersion: Path = new File(s"${dir.getAbsolutePath}/.checkpoint").toPath

    pathToVersion.toFile.getParentFile.mkdirs()
    CheckPointCreator(pathToVersion, tp, offset)
  }
}