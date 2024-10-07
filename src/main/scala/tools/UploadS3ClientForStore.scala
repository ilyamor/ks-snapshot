package tools

import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, AwsCredentials, AwsCredentialsProvider}
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.endpoints.{Endpoint, EndpointProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.endpoints.{S3EndpointParams, S3EndpointProvider}
import software.amazon.awssdk.services.s3.model.{CompleteMultipartUploadRequest, CompletedMultipartUpload, CompletedPart, CreateMultipartUploadRequest, CreateMultipartUploadResponse, GetUrlRequest, PutObjectRequest, UploadPartCopyRequest, UploadPartRequest}

import java.io.{File, RandomAccessFile}
import java.net.URI
import java.nio.ByteBuffer
import java.util
import java.util.concurrent.CompletableFuture
import scala.util.Try

case class UploadS3ClientForStore private(client: S3Client, bucket: String, basePathS3: String) {
  def uploadStateStore(archiveFile: File, checkPoint: File): Either[Throwable, (String, String, Long)] = {
    for {
      f <- uploadArchive(archiveFile)
      u <- uploadCheckpoint(checkPoint)
    } yield (f, u, System.currentTimeMillis())
  }

  private def uploadCheckpoint(checkPointFile: File): Either[Throwable, String] = {
    Try({
      val checkpointKey = s"$basePathS3/${checkPointFile.getName}"
      val putRequest = PutObjectRequest.builder().bucket(bucket).key(checkpointKey).build()
      client.putObject(putRequest, checkPointFile.toPath)
      client.utilities().getUrl(GetUrlRequest.builder()
        .bucket(bucket).key(checkpointKey).build()).toExternalForm
    }).toEither
  }

  private def uploadArchive(archiveFile: File): Either[Throwable, String] = {
    val archiveKey = s"$basePathS3/${archiveFile.getName}"
    Try({
      val createRequest = CreateMultipartUploadRequest.builder.bucket(bucket).key(archiveKey).build
      val createResponse: CreateMultipartUploadResponse = client.createMultipartUpload(createRequest)
      val uploadId = createResponse.uploadId
      val completedParts = prepareMultipart(archiveFile, uploadId)

      val completedUpload = CompletedMultipartUpload.builder.parts(completedParts).build
      val completeRequest = CompleteMultipartUploadRequest.builder.bucket(bucket).key(archiveKey).uploadId(uploadId).multipartUpload(completedUpload).build
      client.completeMultipartUpload(completeRequest)

      client.utilities().getUrl(GetUrlRequest.builder()
        .bucket(bucket).key(archiveKey).build()).toExternalForm
    }).toEither
  }

  private def prepareMultipart(archiveFile: File, uploadId: String): util.ArrayList[CompletedPart] = {
    val archiveKey = s"$basePathS3/${archiveFile.getName}"
    val completedParts = new java.util.ArrayList[CompletedPart]()
    var partNumber = 1
    val buffer = ByteBuffer.allocate(5 * 1024 * 1024) // Set your preferred part size (5 MB in this example)
    var file: RandomAccessFile = null;
    try {
      file = new RandomAccessFile(archiveFile, "r")
      val fileSize = file.length
      var position = 0
      while (position < fileSize) {
        file.seek(position)
        val bytesRead = file.getChannel.read(buffer)
        buffer.flip
        val uploadPartRequest = UploadPartRequest.builder.bucket(bucket).key(archiveKey).uploadId(uploadId).partNumber(partNumber).contentLength(bytesRead.toLong).build
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

object UploadS3ClientForStore {
  def apply(bucket: String, prefix: String, region: Region, storeName: String, partition: Int): UploadS3ClientForStore = {
    val client: S3Client = S3Client.builder
      /*.endpointOverride(new URI("http://localhost:9000"))
      .endpointProvider(new S3EndpointProvider {
        override def resolveEndpoint(endpointParams: S3EndpointParams): CompletableFuture[Endpoint] = {
          CompletableFuture.completedFuture(Endpoint.builder()
            .url(URI.create("http://localhost:9000/" + endpointParams.bucket()))
            .build());
        }
      })
      .credentialsProvider(() => AwsBasicCredentials.create("test", "testtest"))*/
      .region(region).build
    new UploadS3ClientForStore(client, bucket, buildPath(prefix, storeName, partition))
  }

  // when we want custom configured S3Client
  def apply(client: S3Client, bucket: String, prefix: String, storeName: String, partition: Int): UploadS3ClientForStore = {
    new UploadS3ClientForStore(client, bucket, buildPath(prefix, storeName, partition))
  }

  private def buildPath(parts: Any*): String = {
    parts
      .map(an => an.toString)
      .map(str => removeEndSlash(str))
      // we want to avoid paths with //, so if we have empty string, let's replace with '_',
      // so path we'll look like 'value1/value2/_/value4'
      .map(str => if (str.isBlank) "_" else str)
      .mkString("/")
  }

  private def removeEndSlash(str: String): String = {
    val stripped = str.strip()
    if (stripped.endsWith("/")) stripped.substring(0, stripped.length - 1) else stripped
  }
}
