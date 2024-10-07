package tools

import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{CompleteMultipartUploadRequest, CompletedMultipartUpload, CompletedPart, CreateMultipartUploadRequest, CreateMultipartUploadResponse, GetUrlRequest, PutObjectRequest, UploadPartCopyRequest, UploadPartRequest}

import java.io.{File, RandomAccessFile}
import java.nio.ByteBuffer
import java.util
import scala.util.Try

case class S3ClientForStore private (client: S3Client, bucket: String, key: String) {
  def uploadStateStore(archiveFile: File, checkPoint: File): Long = {
    val res = for {
      f <- uploadArchive(archiveFile)
      u <- uploadCheckpoint(checkPoint)
    } yield (f, u)

    System.currentTimeMillis()
  }

  private def uploadCheckpoint(archiveFile: File): Either[Throwable, String] = {
    null
  }

  private def uploadArchive(archiveFile: File): Either[Throwable, String] = {
    Try({
      val createRequest = CreateMultipartUploadRequest.builder.bucket(bucket).key(key).build
      val createResponse: CreateMultipartUploadResponse = client.createMultipartUpload(createRequest)
      val uploadId = createResponse.uploadId
      val completedParts = prepareMultipart(archiveFile, uploadId)

      val completedUpload = CompletedMultipartUpload.builder.parts(completedParts).build
      val completeRequest = CompleteMultipartUploadRequest.builder.bucket(bucket).key(key).uploadId(uploadId).multipartUpload(completedUpload).build
      val completeResponse = client.completeMultipartUpload(completeRequest)

      // completeResponse.checksumSHA256() to check sum

      client.utilities().getUrl(GetUrlRequest.builder()
        .bucket(bucket).key(key).build()).toExternalForm
    }).toEither
  }

  private def prepareMultipart(f: File, uploadId: String): util.ArrayList[CompletedPart] = {

    val completedParts = new java.util.ArrayList[CompletedPart]()
    var partNumber = 1
    val buffer = ByteBuffer.allocate(5 * 1024 * 1024) // Set your preferred part size (5 MB in this example)
    var file: RandomAccessFile = null;
    try {
      file = new RandomAccessFile(f, "r")
      val fileSize = file.length
      var position = 0
      while (position < fileSize) {
        file.seek(position)
        val bytesRead = file.getChannel.read(buffer)
        buffer.flip
        val uploadPartRequest = UploadPartRequest.builder.bucket(bucket).key(key).uploadId(uploadId).partNumber(partNumber).contentLength(bytesRead.toLong).build
        val response = client.uploadPart(uploadPartRequest, RequestBody.fromByteBuffer(buffer))
        completedParts.add(CompletedPart.builder.partNumber(partNumber).eTag(response.eTag).build)
        buffer.clear
        position += bytesRead
        partNumber += 1
      }
    } finally if (file != null) file.close()
    completedParts
  }
}

object S3ClientForStore {
  def apply(bucket: String, prefix: String, region: Region, storeName: String, partition: Int, offset: Long): S3ClientForStore = {
    val client: S3Client = S3Client.builder.region(region).build
    new S3ClientForStore(client, bucket, s"$prefix/$storeName/$partition/$offset")
  }
}
