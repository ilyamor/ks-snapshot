package tools

import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveOutputStream}
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream
import org.apache.commons.io.IOUtils

import java.io._
import java.nio.file.Files
import scala.util.Try

case class Archiver(outputFile: File, sourceDir: File) {

  def archive(): Either[Throwable, File] = {
    Try {
      outputFile.getParentFile.mkdirs()
      outputFile.createNewFile()
      val fos = new FileOutputStream(outputFile)
      val bos = new BufferedOutputStream(fos)
      val gzos = new GzipCompressorOutputStream(bos)
      val tarOs = new TarArchiveOutputStream(gzos)
      try
        // Recursively add files to the tar archive
        addFilesToTarGz(tarOs, sourceDir, "")
      finally {
        silentClose(fos, bos, gzos, tarOs)
      }
    }.toEither.flatMap(_ => {
      val hasBytes = new FileInputStream(outputFile).available()
      if (hasBytes > 0)
        Right(outputFile)
      else
        Left(new Exception("Empty state"+ outputFile.getAbsolutePath))
    })
  }

  private def silentClose(in: OutputStream*): Unit = {
    in.foreach(cl => Try({
      cl.flush();
      cl.close();
    }))
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
        tarOs.closeArchiveEntry()
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
