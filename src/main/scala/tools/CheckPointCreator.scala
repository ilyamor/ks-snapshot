package tools

import java.io.File
import java.nio.file.{Files, Path, StandardOpenOption}
import scala.util.Try

case class CheckPointCreator private (pathToVersion: Path, offset: Long) {

  def create(pathToVersion: Path, offset: Long): Either[Throwable, File] = {
    Try {
      val writer = Files.newBufferedWriter(pathToVersion, StandardOpenOption.CREATE, StandardOpenOption.APPEND)
      writer.append(s"$offset\n").close()
      pathToVersion.toFile
    }.toEither
  }
}

object CheckPointCreator {
  def apply(dir: File, offset: Long): CheckPointCreator = {
    val pathToVersion: Path = new File(s"${dir.getAbsolutePath}/.checkpoint").toPath
    pathToVersion.toFile.getParentFile.mkdirs()
    CheckPointCreator(pathToVersion, offset)
  }
}