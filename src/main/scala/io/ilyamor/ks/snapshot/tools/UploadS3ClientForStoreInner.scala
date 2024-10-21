package io.ilyamor.ks.snapshot.tools

import org.apache.kafka.streams.state.internals.OffsetCheckpoint
import org.apache.kafka.streams.state.internals.StateStoreToS3.S3StateStoreConfig
import org.apache.kafka.streams.state.internals.StateStoreToS3.S3StateStoreConfig.{STATE_BUCKET, STATE_KEY_PREFIX}
import org.apache.logging.log4j.scala.Logging
import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.endpoints.Endpoint
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.endpoints.{S3EndpointParams, S3EndpointProvider}
import software.amazon.awssdk.services.s3.model._

import java.io.{File, FileOutputStream, InputStream, RandomAccessFile}
import java.net.URI
import java.nio.ByteBuffer
import java.util
import java.util.Properties
import java.util.concurrent.CompletableFuture
import scala.util.{Try, Using}

case class UploadS3ClientForStoreInner private (config: S3StateStoreConfig, storeKey: String) extends StorageUploader with Logging {
  val CHECKPOINT = ".checkpoint"
  val suffix = "tzr.gz"

  lazy val client: S3Client = buildClient()
  lazy val bucket: String = config.getString(STATE_BUCKET)
  lazy val basePathS3: String = buildPath(config.getString(STATE_KEY_PREFIX), storeKey)

  private def buildPath(parts: Any*): String = {
    parts
      .map(an => an.toString)
      .filter(s => !s.isBlank)
      .map(str => removeEndSlash(str))
      .mkString("/")
  }

  private def removeEndSlash(str: String): String = {
    val stripped = str.strip()
    if (stripped.endsWith("/")) stripped.substring(0, stripped.length - 1) else stripped
  }

  private def buildClient(): S3Client = {
    val region = Region.of(config.getString(S3StateStoreConfig.STATE_REGION))
    val endPoint = if (config.getString(S3StateStoreConfig.STATE_S3_ENDPOINT).endsWith("/"))
      config.getString(S3StateStoreConfig.STATE_S3_ENDPOINT) else config.getString(S3StateStoreConfig.STATE_S3_ENDPOINT) + "/"

    val client: S3Client =
      if (endPoint.isBlank) {
        S3Client.builder.region(region).build
      } else {
        S3Client.builder
          .endpointOverride(new URI(endPoint))
          .endpointProvider(new S3EndpointProvider {
            override def resolveEndpoint(endpointParams: S3EndpointParams): CompletableFuture[Endpoint] = {
              CompletableFuture.completedFuture(Endpoint.builder()
                .url(URI.create(endPoint + endpointParams.bucket()))
                .build());
            }
          })
          .region(region).build
      }
    client
  }

  def getCheckpointFile(partition: String, storeName: String, applicationId: String): Either[Throwable, OffsetCheckpoint] = {
    val rootPath = s"$applicationId/$partition/$storeName"
    val checkpointPath = s"$rootPath/$CHECKPOINT"
    Try {
      logger.info(s"Fetching checkpoint file from $checkpointPath")
      val res: ResponseInputStream[GetObjectResponse] = client.getObject(GetObjectRequest.builder().bucket(bucket).key(checkpointPath).build())
      val tempFile = File.createTempFile("checkpoint",".tmp")

      Using.resource(new FileOutputStream(tempFile)) {
        fos =>
          res.transferTo(fos)
          tempFile
      }
    }.map(new OffsetCheckpoint(_))
      .toEither
  }

  def getStateStores(partition: String, storeName: String, applicationId: String, offset: String): Either[Throwable, InputStream] = {
    val rootPath = s"$applicationId/$partition/$storeName"
    val stateFileCompressed = s"$rootPath/$offset.$suffix"
    logger.info(s"Fetching state store from $stateFileCompressed")
    Try {
      client.getObject(GetObjectRequest.builder().bucket(bucket).key(stateFileCompressed).build())
    }.toEither
  }

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

  override def configure(params: Properties, storeName: String): StorageUploader = this
}

// inner class with empty constructor to enable creation via Kafka's reflection. It will call `configure` method
// and it will create actual
class UploadS3ClientForStore extends StorageUploader {

  override def getCheckpointFile(partition: String, storeName: String, applicationId: String): Either[Throwable, OffsetCheckpoint] =
    throw new UnsupportedOperationException()

  override def getStateStores(partition: String, storeName: String, applicationId: String, offset: String): Either[Throwable, InputStream] =
    throw new UnsupportedOperationException()

  override def uploadStateStore(archiveFile: File, checkPoint: File): Either[Throwable, (String, String, Long)] =
    throw new UnsupportedOperationException()

  override def configure(params: Properties, storeName: String): StorageUploader =
    UploadS3ClientForStoreInner(S3StateStoreConfig(params), storeName)
}