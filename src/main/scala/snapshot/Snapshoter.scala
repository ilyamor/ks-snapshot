package snapshot

import io.confluent.examples.streams.SnapshotStoreListener.{SnapshotStoreListener, TppStore}
import org.apache.commons.compress.archivers.ArchiveEntry
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.streams.processor.{StateStore, StateStoreContext}
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl
import org.apache.kafka.streams.state.internals.OffsetCheckpoint
import org.apache.logging.log4j.scala.Logging
import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{GetObjectRequest, GetObjectResponse}
import tools.EitherOps.EitherOps
import tools.{Archiver, CheckPointCreator, UploadS3ClientForStore}

import java.io.{File, FileOutputStream}
import java.lang
import java.nio.file.Files
import scala.collection.convert.ImplicitConversions.`map AsScala`
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.jdk.CollectionConverters.{mapAsJavaMap, mapAsJavaMapConverter, mapAsScalaMapConverter, mutableMapAsJavaMapConverter}
import scala.util.{Try, Using}

class S3ClientWrapper(bucketName: String) extends Logging {

  val s3: S3Client = S3Client.builder
    .region(Region.EU_NORTH_1)
    .build


  val CHECKPOINT = ".checkpoint"
  val state = "state.tar.gz"
  val suffix = "tzr.gz"


  def getCheckpointFile(context: StateStoreContext, partition: String, storeName: String, applicationId: String): Either[Throwable, OffsetCheckpoint] = {
    val rootPath = s"$applicationId/$partition/$storeName"
    val checkpointPath = s"$rootPath/$CHECKPOINT"
    Try {
      logger.info(s"Fetching checkpoint file from $checkpointPath")
      val res: ResponseInputStream[GetObjectResponse] = s3.getObject(GetObjectRequest.builder().bucket(bucketName).key(checkpointPath).build())
      val tempFile = new File("checkpoint")

      Using.resource(new FileOutputStream(tempFile)) {
        fos =>
          res.transferTo(fos)
          tempFile
      }
    }.map(new OffsetCheckpoint(_))
      .toEither


  }

  def getStateStores(partition: String, storeName: String, applicationId: String, offset: String): Either[Throwable, ResponseInputStream[GetObjectResponse]] = {
    val rootPath = s"$applicationId/$partition/$storeName"
    val stateFileCompressed = s"$rootPath/$offset.$suffix"
    logger.info(s"Fetching state store from $stateFileCompressed")
    Try {
      s3.getObject(GetObjectRequest.builder().bucket(bucketName).key(stateFileCompressed).build())
    }.toEither


  }

}

case class Snapshoter(s3ClientWrapper: S3ClientWrapper,
                      s3ClientForStore: UploadS3ClientForStore,
                      snapshotStoreListener: SnapshotStoreListener,
                      context: ProcessorContextImpl,
                      storeName: String) extends Logging {

  def initFromSnapshot() = {
    Fetcher.initFromSnapshot()
  }

  def flushSnapshot() = {
    Flusher.flushSnapshot()
  }

  private object Fetcher {

    def initFromSnapshot() = {
      val topic = context.changelogFor(storeName)
      val partition = context.taskId.partition()
      val tp = new TopicPartition(topic, partition)
      if (!Option(snapshotStoreListener.taskStore.get(TppStore(tp, storeName))).getOrElse(false))
        getSnapshotStore(context)
    }

    private def getSnapshotStore(context: StateStoreContext) = {
      val localCheckPointFile = getLocalCheckpointFile(context).toOption
      val remoteCheckPoint = fetchRemoteCheckPointFile(context).toOption
      if (shouldFetchStateStoreFromSnapshot(localCheckPointFile, remoteCheckPoint)) {
        fetchAndWriteLocally(context, remoteCheckPoint) ->
          overrideLocalCheckPointFile(context, localCheckPointFile, remoteCheckPoint)
      }

    }

    private def overrideLocalCheckPointFile(context: StateStoreContext, localCheckPointFile: Option[OffsetCheckpoint], remoteCheckPoint: Option[OffsetCheckpoint]) = {
      val res = (localCheckPointFile, remoteCheckPoint) match {
        case (Some(local), Some(remote)) =>
          logger.info("Overriding local checkpoint file with existing one")
          val localOffsets = local.read().asScala
          val remoteOffsets = remote.read().asScala
          Right((localOffsets ++ remoteOffsets).asJava)

        case (None, Some(remote)) =>
          logger.info("Overriding local checkpoint file doesn't exist with existing one,using remote")
          Right(remote.read())

        case _ =>
          logger.error("Error while overriding local checkpoint file")
          Left(new IllegalArgumentException("Error while overriding local checkpoint file"))
      }
      res.flatMap { newOffsets =>

        val checkpointPath = context.stateDir().toString + "/" + ".checkpoint"
        logger.info(s"Writing new offsets to local checkpoint file: $checkpointPath with new offset $newOffsets")
        Try {
          new OffsetCheckpoint(new File(checkpointPath)).write(newOffsets)
        }.toEither
          .tapError(e => logger.error(s"Error while overriding local checkpoint file: $e", e))
      }
    }


    private def fetchAndWriteLocally(context: StateStoreContext, remoteCheckPoint: Option[OffsetCheckpoint]): Either[Throwable, Unit] = {
      remoteCheckPoint match {
        case Some(localCheckPointFile) =>
          localCheckPointFile.read().asScala.values.headOption match {
            case Some(offset) =>

              s3ClientWrapper.getStateStores(context.taskId.toString, storeName, context.applicationId(), offset.toString)
              .tapError(e => logger.error(s"Error while fetching remote state store: $e", e))
              .tapError(e => throw e)
              .map((response: ResponseInputStream[GetObjectResponse]) => {
                val destDir = s"${context.stateDir.getAbsolutePath}"
                extractAndDecompress(destDir, response)
                ()
              })

            case None =>
              logger.error("remote checkpoint file is offsets is empty")
              Left(new IllegalArgumentException("remote checkpoint file is offsets is empty"))

          }
        case None =>
          logger.error("Local checkpoint file is empty")
          Left(new IllegalArgumentException("remote-- checkpoint file is empty"))
      }
    }

    private def shouldFetchStateStoreFromSnapshot(localCheckPointFile: Option[OffsetCheckpoint], remoteCheckPoint: Option[OffsetCheckpoint]) = {
      (localCheckPointFile, remoteCheckPoint) match {
        case (_, None) => {
          logger.info("Remote checkpoint file not found, not fetching remote state store")
          false
        }
        case (None, _) => {
          logger.info("Local checkpoint file not found, fetching remote state store")
          true
        }

        case (Some(local), Some(remote)) => isOffsetBiggerThanMin(local, remote)
      }
    }

    private def isOffsetBiggerThanMin(local: OffsetCheckpoint, remote: OffsetCheckpoint) = {
      val remoteOffsets = remote.read().asScala
      if (remoteOffsets.size != 1) {
        logger.warn(s"Remote checkpoint has more than one offset: $remoteOffsets")
        false
      }
      else {
        remoteOffsets.exists({
          case (storeName, remoteOffset) =>
            val localOffset = local.read().asScala.get(storeName)
            localOffset match {
              case Some(offset) => {
                val should = offset + OFFSETTHRESHOLD < remoteOffset
                logger.info(s"Remote offset is bigger than local offset by more than $offset -$remoteOffset")
                should
              }
              case None => true
            }
        })
      }
    }


    private def getLocalCheckpointFile(context: StateStoreContext): Either[IllegalArgumentException, OffsetCheckpoint] = {
      val checkpoint = ".checkpoint"
      val stateDir = context.stateDir()
      val checkpointFile = stateDir.toString + "/" + checkpoint
      val file = new File(checkpointFile)
      if (file.exists())
        Right(new OffsetCheckpoint(file))
      else {
        Left(new IllegalArgumentException("Checkpoint file not found"))
      }

    }

    private def fetchRemoteCheckPointFile(context: StateStoreContext): Either[Throwable, OffsetCheckpoint] = {
      s3ClientWrapper.getCheckpointFile(context, context.taskId.toString, storeName, context.applicationId())
        .tapError(e => logger.error(s"Error while fetching remote checkpoint: $e"))
    }

    val OFFSETTHRESHOLD: Int = 10000


    private def extractAndDecompress(destDir: String, response: ResponseInputStream[GetObjectResponse]) = {
      try {
        val gzipInputStream = new GzipCompressorInputStream(response)
        val tarInputStream = new TarArchiveInputStream(gzipInputStream)
        try {
          var entry: ArchiveEntry = null
          do {
            entry = tarInputStream.getNextEntry
            if (entry != null) {
              val destPath = new File(destDir, entry.getName)
              if (entry.isDirectory) destPath.mkdirs // Create directories
              else {
                // Ensure parent directories exist
                destPath.getParentFile.mkdirs
                // Write file content
                try {
                  val outputStream = new FileOutputStream(destPath)
                  try {
                    val buffer = new Array[Byte](1024)
                    var bytesRead = 0
                    do {
                      bytesRead = tarInputStream.read(buffer)
                      if (bytesRead != -1)
                        outputStream.write(buffer, 0, bytesRead)
                    } while (bytesRead != -1)

                  } finally if (outputStream != null) outputStream.close()
                }
              }
            }
          }
          while ((entry != null))
        } finally {
          if (response != null) response.close()
          if (gzipInputStream != null) gzipInputStream.close()
          if (tarInputStream != null) tarInputStream.close()
        }
      }
    }
  }

  private object Flusher {
    def flushSnapshot(): Unit = {
      val stateDir = context.stateDir()
      val topic = context.changelogFor(storeName)
      val partition = context.taskId.partition()
      val tp = new TopicPartition(topic, partition)
      val offset = context.recordCollector().offsets().get(tp)
      val sourceTopic = context.topic()

      val tppStore = TppStore(tp, storeName)
      if (!snapshotStoreListener.taskStore.getOrDefault(tppStore, false)
        && !snapshotStoreListener.workingFlush.getOrDefault(tppStore, false)) {
        if (offset != null) {
          Future {
            val storePath = s"${stateDir.getAbsolutePath}/$storeName"
            val tempDir = Files.createTempDirectory(s"$partition-$storeName")
            snapshotStoreListener.workingFlush.put(tppStore, true)
            logger.info(s"starting to snapshot for task ${context.taskId()} store: " + storeName + " with offset: " + offset)

            val stateStore: StateStore = context.stateManager().getStore(storeName)
            val positions: Map[TopicPartition, lang.Long] = stateStore.getPosition.getPartitionPositions(context.topic())
              .toMap.map(tp => (new TopicPartition(sourceTopic, tp._1), tp._2))

            val files = for {
              archivedFile <- Archiver(tempDir.toFile, offset, new File(storePath)).archive()
              checkpointFile <- CheckPointCreator(tempDir.toFile, tp, offset).write()
              positionFile <- CheckPointCreator.create(tempDir.toFile, s"$storeName.position", positions).write()
              uploadResultQuarto <- s3ClientForStore.uploadStateStore(archivedFile, checkpointFile, positionFile)
            } yield uploadResultQuarto
            files
              .tap(
                e => logger.error(s"Error while uploading state store: $e", e),
                files => logger.info(s"Successfully uploaded state store: $files"))
            snapshotStoreListener.workingFlush.put(tppStore, false)
          }
        }
        println()
      }
    }
  }
}