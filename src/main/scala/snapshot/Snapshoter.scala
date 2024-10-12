package snapshot

import io.confluent.examples.streams.SnapshotStoreListener.{SnapshotStoreListener, TppStore}
import org.apache.commons.compress.archivers.ArchiveEntry
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.streams.processor.StateStoreContext
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
import java.nio.file.Files
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.jdk.CollectionConverters.{mapAsScalaMapConverter, mutableMapAsJavaMapConverter}
import scala.util.{Try, Using}

class S3ClientWrapper(bucketName: String) {

  val s3: S3Client = S3Client.builder
    .region(Region.EU_NORTH_1)
    .build


  val CHECKPOINT = ".checkpoint"
  val state = "state.tar.gz"


  def getCheckpointFile(context: StateStoreContext, partition: String, storeName: String, applicationId: String): Either[Throwable, OffsetCheckpoint] = {
    val rootPath = s"$applicationId/$partition/$storeName"
    val checkpointPath = s"$rootPath/$CHECKPOINT"
    Try {
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

  def getStateStores(partition: String, storeName: String, applicationId: String): Either[Throwable, ResponseInputStream[GetObjectResponse]] = {
    val rootPath = s"$applicationId/$partition/$storeName"
    //      val version = getLatestVersion(partition, applicationId)
    val stateFileCompressed = s"$rootPath/$state"

    Try {
      s3.getObject(GetObjectRequest.builder().bucket(bucketName).key(stateFileCompressed).build())
      //check if exists and read
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
      val localCheckPointFile = getLocalCheckpointFile(context)
      val remoteCheckPoint = fetchRemoteCheckPointFile(context)
      if (shouldFetchStateStoreFromSnapshot(localCheckPointFile, remoteCheckPoint)) {
        fetchAndWriteLocally(context) ->
          overrideLocalCheckPointFile(context, localCheckPointFile, remoteCheckPoint)
      }

    }

    private def overrideLocalCheckPointFile(context: StateStoreContext, localCheckPointFile: Either[IllegalArgumentException, OffsetCheckpoint], remoteCheckPoint: Either[Throwable, OffsetCheckpoint]) = {
      val res = (localCheckPointFile, remoteCheckPoint) match {
        case (Right(local), Right(remote)) =>
          logger.info("Overriding local checkpoint file with existing one")

          val localOffsets = local.read().asScala
          val remoteOffsets = remote.read().asScala
          Right((localOffsets ++ remoteOffsets).asJava)
        case (Left(_), Right(remote)) =>
          logger.info("Overriding local checkpoint file doesn't exist with existing one,using remote")

          Right(remote.read())
        case _ => {
          logger.error("Error while overriding local checkpoint file")
          Left(new IllegalArgumentException("Error while overriding local checkpoint file"))

        }
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


    private def fetchAndWriteLocally(context: StateStoreContext): Either[Throwable, Unit] = {
      s3ClientWrapper.getStateStores(context.taskId.toString, storeName, context.applicationId())
        .tapError(e => logger.error(s"Error while fetching remote state store: $e", e))
        .tapError(e => throw e)
        .map((response: ResponseInputStream[GetObjectResponse]) => {
          val destDir = s"${context.stateDir.getAbsolutePath}/${storeName}"
          extractAndDecompress(destDir, response)

        })
    }

    private def shouldFetchStateStoreFromSnapshot(localCheckPointFile: Either[IllegalArgumentException, OffsetCheckpoint], remoteCheckPoint: Either[Throwable, OffsetCheckpoint]) = {
      (localCheckPointFile, remoteCheckPoint) match {
        case (Left(_), _) => {
          logger.info("Local checkpoint file not found, fetching remote state store")
          true
        }
        case (_, Left(_)) => {
          logger.info("Remote checkpoint file not found, not fetching remote state store")
          false
        }
        case (Right(local), Right(remote)) => isOffsetBiggerThanMin(local, remote)
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
        .tapError(e => throw e)
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

      val tppStore = TppStore(tp, storeName)
      if (!snapshotStoreListener.taskStore.getOrDefault(tppStore, false)
        && snapshotStoreListener.workingFlush.getOrDefault(tppStore, true)) {
        if (offset != null) {
          Future {
            val storePath = s"${stateDir.getAbsolutePath}/$storeName"
            val tempDir = Files.createTempDirectory(s"$partition-$storeName")
            snapshotStoreListener.workingFlush.put(tppStore, true)
            val files = for {
              archivedFile <- Archiver(tempDir.toFile, offset: Long, new File(storePath)).archive()
              checkpointFile <- CheckPointCreator(tempDir.toFile, offset).create()
              uploadResultTriple <- s3ClientForStore.uploadStateStore(archivedFile, checkpointFile)
            } yield uploadResultTriple
            snapshotStoreListener.workingFlush.put(tppStore, false)
            files.foreach(triple => println(triple))
          }
        }
        println()
      }
    }
  }
}