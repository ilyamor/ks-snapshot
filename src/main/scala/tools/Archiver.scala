package tools

import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveOutputStream}
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream
import org.apache.commons.io.IOUtils

import java.io.{BufferedOutputStream, File, FileInputStream, FileOutputStream}
import java.nio.file.Files
import scala.util.Try

case class Archiver private (outputFile: File, sourceDir: File) {

  def archive(): Either[Throwable, File] = {
    Try(start()).toEither
  }

  private def start(): File = {
    try {
      outputFile.getParentFile.mkdirs()
      outputFile.createNewFile()
      val fos = new FileOutputStream(outputFile)
      val bos = new BufferedOutputStream(fos)
      val gzos = new GzipCompressorOutputStream(bos)
      val tarOs = new TarArchiveOutputStream(gzos)
      try
        // Recursively add files to the tar archive
        addFilesToTarGz(tarOs, sourceDir, "")
      catch {
        case e => e.printStackTrace()
        case _ => println("OOOps")
      }
      finally {
        if (fos != null) Try(fos).map(_.close)
        if (bos != null) Try(bos).map(_.close)
        if (gzos != null) Try(gzos).map(_.close)
        if (tarOs != null) Try(tarOs).map(_.close)
      }
    }
    outputFile
  }

  private def addFilesToTarGz(tarOs: TarArchiveOutputStream, file: File, parentDir: String): Unit = {
    val entryName = parentDir + file.getName
    if (file.isFile) {
      val fis = new FileInputStream(file)
      val tarEntry = new TarArchiveEntry(file, entryName)
      tarOs.putArchiveEntry(tarEntry)
      try {
        println(s"Archiving file ${file.getAbsolutePath}")
        IOUtils.copy(fis, tarOs)
      } finally {
        fis.close()
        tarOs.closeArchiveEntry
      }
    } else if (file.isDirectory) {
      println(s"Starting dir ${file.getAbsolutePath}/")
      Files.list(file.toPath).forEach(childFile => addFilesToTarGz(tarOs, childFile.toFile, entryName + "/"))
    }
  }
}

object Archiver {
  def apply(tempDir: File, offset: Long, sourceDir: File): Archiver = {
    val outputFile = new File(s"${tempDir.getAbsolutePath}/$offset.tzr.gz")
    new Archiver(outputFile, sourceDir)
  }
}
