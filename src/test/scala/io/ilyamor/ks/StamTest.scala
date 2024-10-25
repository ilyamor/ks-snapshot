package io.ilyamor.ks

import io.ilyamor.ks.snapshot.StoreFactory.KStreamOps
import io.minio.admin.{MinioAdminClient, UserInfo}
import io.minio.messages.Item
import io.minio.{GetObjectArgs, ListObjectsArgs, MakeBucketArgs, MinioClient, SetBucketPolicyArgs}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer, OffsetResetStrategy}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.streams.kstream.{Consumed, TimeWindows, implicitConversion}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{Grouped, Produced}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.serialization.Serdes.longSerde
import org.apache.kafka.streams.scala.serialization.Serdes.stringSerde
import org.apache.kafka.streams.state.internals.StateStoreToS3.S3StateStoreConfig
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.logging.log4j.scala.Logging
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.scalatest.flatspec.AnyFlatSpec
import org.testcontainers.containers.MinIOContainer
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.utility.DockerImageName
import software.amazon.awssdk.regions.Region

import java.io.File
import java.nio.file.Files
import java.time.Duration
import java.util.Properties
import java.util.stream.{Collectors, StreamSupport}
import scala.jdk.CollectionConverters.{ListHasAsScala, SeqHasAsJava}

class StamTest extends AnyFlatSpec with BeforeAndAfter with Logging with BeforeAndAfterAll{

  System.setProperty("aws.accessKeyId", "test")
  System.setProperty("aws.secretAccessKey", "testtest")

  val policy = new StringBuilder
  policy.append("{\n")
  policy.append("    \"Statement\": [\n")
  policy.append("        {\n")
  policy.append("            \"Action\": [\n")
  policy.append("                \"s3:GetBucketLocation\",\n")
  policy.append("                \"s3:ListBucket\",\n")
  policy.append("                \"s3:ListBucketMultipartUploads\"\n")
  policy.append("            ],\n")
  policy.append("            \"Effect\": \"Allow\",\n")
  policy.append("            \"Principal\": \"*\",\n")
  policy.append("            \"Resource\": \"arn:aws:s3:::test-bucket\"\n")
  policy.append("        },\n")
  policy.append("        {\n")
  policy.append("            \"Action\": [\n")
  policy.append("                \"s3:GetObject\",\n")
  policy.append("                \"s3:PutObject\",\n")
  policy.append("                \"s3:AbortMultipartUpload\",\n")
  policy.append("                \"s3:ListMultipartUploadParts\"\n")
  policy.append("            ],\n")
  policy.append("            \"Effect\": \"Allow\",\n")
  policy.append("            \"Principal\": \"*\",\n")
  policy.append("            \"Resource\": \"arn:aws:s3:::test-bucket/*\"\n")
  policy.append("        }\n")
  policy.append("    ],\n")
  policy.append("    \"Version\": \"2012-10-17\"\n")
  policy.append("}\n")

  val BUCKET_NAME = "test-bucket"
  val SOURCE_TOPIC_NAME = "source-test-topic"
  val DEST_TOPIC_NAME = "dest-test-topic"

  val minio: MinIOContainer = new MinIOContainer("minio/minio:RELEASE.2024-10-13T13-34-11Z")
    .withUserName("testadmin")
    .withPassword("testadmin")

  val kafka: KafkaContainer  =
    new KafkaContainer(DockerImageName.parse("apache/kafka:latest"))

  minio.start()
  kafka.start()

  val minioClient = MinioClient.builder()
    .endpoint(minio.getS3URL)
    .credentials(minio.getUserName, minio.getPassword)
    .build()

  override def beforeAll: Unit = {
    val adminProps: Properties = new Properties
    adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers)
    val kafkaAdmin = AdminClient.create(adminProps)
    kafkaAdmin.createTopics(List(
      new NewTopic(SOURCE_TOPIC_NAME, 1, 1.toShort),
      new NewTopic(DEST_TOPIC_NAME, 1, 1.toShort),
    ).asJava).all().get()
    kafkaAdmin.close(Duration.ofSeconds(30))

    val minioAdminClient = MinioAdminClient.builder()
      .endpoint(minio.getS3URL)
      .credentials(minio.getUserName, minio.getPassword)
      .build()

    minioAdminClient.addUser("test", UserInfo.Status.ENABLED, "testtest", null, null)

    minioAdminClient.addCannedPolicy("test", policy.toString)
    minioAdminClient.setPolicy("test", false, "test")

    minioClient.makeBucket(MakeBucketArgs.builder().bucket(BUCKET_NAME).region(Region.US_EAST_1.id()).build())

    minioClient.setBucketPolicy(SetBucketPolicyArgs.builder.bucket(BUCKET_NAME).region(Region.US_EAST_1.id).config(policy.toString).build)
  }

  override def afterAll: Unit = {
    minio.stop()
    kafka.stop()
  }

  "stam test" should "tsam" in {
    val runningDir = new File(ClassLoader.getSystemResource(".").toURI)
    val dataDir = new File(runningDir.getAbsolutePath + "/data" + System.currentTimeMillis() + "/")
    if (dataDir.exists())
      dataDir.delete()
    Files.createDirectory(dataDir.toPath)
    println(s" --------- data in ${dataDir.getAbsolutePath}")

    val bootstrapService = kafka.getBootstrapServers

    val producer = createProducer(bootstrapService)
    implicit val streamProps: Properties = createStreamProps(bootstrapService, minio.getS3URL, dataDir.getAbsolutePath)
    val streams = createStreams(SOURCE_TOPIC_NAME, DEST_TOPIC_NAME)
    streams.enableS3Snapshot().start()

    val rangeExcl = Range(0,20004)
    rangeExcl.foreach(v => producer.send(new ProducerRecord(SOURCE_TOPIC_NAME, v.toString, v.toLong)).get())

    val consumer = createConsumer(bootstrapService, Serdes.stringSerde.deserializer())
    consumer.subscribe(List(SOURCE_TOPIC_NAME).asJava)
    var hasData: Int = 0
    while(hasData < 20004) {
      val records = consumer.poll(Duration.ofMillis(Integer.MAX_VALUE))
      hasData += records.count()
    }

    consumer.subscribe(List(DEST_TOPIC_NAME).asJava)
    hasData = 0
    do {
      val records = consumer.poll(Duration.ofMillis(Integer.MAX_VALUE))
      hasData += records.count()
    } while (hasData <= 0)

    Thread.sleep(100)

    val iterable = minioClient.listObjects(ListObjectsArgs.builder()
      .prefix("global-stores-test/0_0/")
      .bucket(BUCKET_NAME).build())

    val ls = StreamSupport.stream(iterable.spliterator(), false).collect(Collectors.toList()).asScala
    assert(ls.size == 1)
    val result = ls.head
    val item: Item = result.get()
    val kstoreName = item.objectName()
    val bytes = minioClient.getObject(GetObjectArgs.builder().bucket(BUCKET_NAME).`object`(s"$kstoreName/.checkpoint").build())
      .readAllBytes()

    val checkpoint = new String(bytes)
    assert(!checkpoint.isBlank)
    println()
  }



  def createConsumer[K](bootstrapServers: String, deserializer: Deserializer[K]): KafkaConsumer[K, Long] = {
    val props: Properties = new Properties
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "test")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.toString)
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10002)

    new KafkaConsumer[K, Long](props, deserializer, Serdes.longSerde.deserializer())
  }

  def createProducer(bootstrapServers: String): KafkaProducer[String, Long] = {
    val props: Properties = new Properties
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(ProducerConfig.RETRIES_CONFIG, 10)
    new KafkaProducer[String, Long](props, Serdes.stringSerde.serializer(), Serdes.longSerde.serializer())
  }

  def createStreamProps(bootstrapServers: String, minioUrl: String, stateDirPath: String): Properties = {
    val streamsConfiguration: Properties = new Properties
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDirPath)
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "global-stores-test")
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    streamsConfiguration.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 18000)
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0)
    streamsConfiguration.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0)
    streamsConfiguration.put(StreamsConfig.STATE_CLEANUP_DELAY_MS_CONFIG, 20000000)
    streamsConfiguration.put(StreamsConfig.PROBING_REBALANCE_INTERVAL_MS_CONFIG, 60000)
    streamsConfiguration.put(StreamsConfig.ACCEPTABLE_RECOVERY_LAG_CONFIG, 10000)
    streamsConfiguration.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100000)
    streamsConfiguration.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 50 * 1024 * 1024)
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    streamsConfiguration.put(S3StateStoreConfig.STATE_BUCKET, BUCKET_NAME)
    streamsConfiguration.put(S3StateStoreConfig.STATE_REGION, Region.US_EAST_1.id)
    // should have leading slash
    streamsConfiguration.put(S3StateStoreConfig.STATE_S3_ENDPOINT, minioUrl+"/")
    streamsConfiguration
  }

  def createStreams(sourceTopic: String, destTopic: String)(implicit streamsConfiguration: Properties): KafkaStreams = {
    val builder = new StreamsBuilder

    // Get the stream of orders
    val ordersStream = builder.stream(sourceTopic)(Consumed.`with`(Serdes.stringSerde, Serdes.longSerde)).groupByKey(Grouped.`with`)

    ordersStream
      .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(1000000)))
      .aggregate(0L) { (k, v, agg) => v + agg }(implicitConversion.windowStoreToSnapshotStore)
      .toStream
      .map((k,v) => (k.key(), v))
      .to(destTopic)(Produced.`with`(Serdes.stringSerde, Serdes.longSerde))

    val topology = builder.build()
    println(topology.describe())
    val start = new KafkaStreams(builder.build, streamsConfiguration)

    /*start.setStateListener((newState, oldState) => {
      logger.info("changing state  " + oldState + newState.name())
    })*/
    //    start.streamsMetadataForStore("store1").asScala.map(_.)

    start
  }
}
