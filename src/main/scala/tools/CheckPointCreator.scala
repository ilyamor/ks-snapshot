package tools

import java.io.File
import java.nio.file.{Files, Path, StandardOpenOption}
import scala.util.Try

case class CheckPointCreator private (pathToVersion: Path, offset: Long) {
  def create(): Either[Throwable, File] = {
    Try(createVersion(pathToVersion, offset)).toEither
  }

  private def createVersion(pathToVersion: Path, offset: Long): File = {
    val writer = Files.newBufferedWriter(pathToVersion, StandardOpenOption.CREATE, StandardOpenOption.APPEND)
    writer.append(s"$offset\n").close()
    pathToVersion.toFile
  }
}

object CheckPointCreator {
  def apply(dir: File, offset: Long): CheckPointCreator = {
    val pathToVersion: Path = new File(s"${dir.getAbsolutePath}/.checkpoint").toPath
    pathToVersion.toFile.getParentFile.mkdirs()
    CheckPointCreator(pathToVersion, offset)
  }
}