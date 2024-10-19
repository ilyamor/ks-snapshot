<<<<<<<< HEAD:src/test/scala/new_try.scala
========
package io.ilyamor.ks

>>>>>>>> b21b472 (move to sub package):src/main/scala/io/ilyamor/ks/new_try.scala
import io.ilyamor.ks.snapshot.StoreFactory.KStreamOps
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.{Consumed, TimeWindows, implicitConversion}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.state.RocksDBConfigSetter
import org.apache.kafka.streams.state.internals.StateStoreToS3.S3StateStoreConfig
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.logging.log4j.scala.Logging
import org.rocksdb.{BlockBasedTableConfig, Options}
import software.amazon.awssdk.regions.Region

import java.time.Duration
import java.util
import java.util.Properties
import scala.util.Random

import io.ilyamor.ks.utils.EitherOps

object GlobalStoresExample extends Logging {

  private val ORDER_TOPIC = "order"

  def main(args: Array[String]): Unit = {
    val bootstrapServers = if (args.length > 0) args(0)
    else "localhost:9092"
    val schemaRegistryUrl = if (args.length > 1) args(1)
    else "http://localhost:8081"
    val streams = createStreams(bootstrapServers, schemaRegistryUrl, "/tmp/kafka-streams-global-stores")
    // Always (and unconditionally) clean local state prior to starting the processing topology.
    // We opt for this unconditional call here because this will make it easier for you to play around with the example
    // when resetting the application for doing a re-run (via the Application Reset Tool,
    // https://docs.confluent.io/platform/current/streams/developer-guide/app-reset-tool.html).
    //
    // The drawback of cleaning up local state prior is that your app must rebuilt its local state from scratch, which
    // will take time and will require reading all the state-relevant data from the Kafka cluster over the network.
    // Thus in a production scenario you typically do not want to clean up always as we do here but rather only when it
    // is truly needed, i.e., only under certain conditions (e.g., the presence of a command line flag for your app).
    // See `ApplicationResetExample.java` for a production-like example.
    // start processing
    streams.enableS3Snapshot()
    streams.start()
    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    //    Runtime.getRuntime.addShutdownHook(new Thread(streams.close))
    while (true) {
      Thread.sleep(1000)
      println(streams.allLocalStorePartitionLags())
    }
    ()
  }

  def createStreams(bootstrapServers: String, schemaRegistryUrl: String, stateDir: String): KafkaStreams = {

    implicit val streamsConfiguration: Properties = new Properties
    // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
    // against which the application is run.
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "global-stores-test")
    // Where to find Kafka broker(s).

    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    streamsConfiguration.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 18000)
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, s"data${Random.nextInt(4)}")
    //    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDir)
    //streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "data")
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0)
    streamsConfiguration.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0)
//    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0)
//    streamsConfiguration.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0)
    //streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0)
    streamsConfiguration.put(StreamsConfig.STATE_CLEANUP_DELAY_MS_CONFIG, 20000000)
    streamsConfiguration.put(StreamsConfig.PROBING_REBALANCE_INTERVAL_MS_CONFIG, 60000)
    streamsConfiguration.put(StreamsConfig.ACCEPTABLE_RECOVERY_LAG_CONFIG, 10000)
    streamsConfiguration.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100000)

    streamsConfiguration.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 50 * 1024 * 1024)
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

    streamsConfiguration.put(S3StateStoreConfig.STATE_BUCKET, "cx-snapshot-test")
    streamsConfiguration.put(S3StateStoreConfig.STATE_REGION, Region.EU_NORTH_1.id)
    // should have leading slash
    //streamsConfiguration.put(S3StateStoreConfig.STATE_S3_ENDPOINT, "http://localhost:9000/")

    // Set to earliest so we don't miss any data that arrived in the topics before the process
    // started
    // create and configure the SpecificAvroSerdes required in this example

    val builder = new StreamsBuilder

    // Get the stream of orders
    val ordersStream = builder.stream(ORDER_TOPIC)(Consumed.`with`(Serdes.String(), Serdes.Long())).groupByKey

    ordersStream
      .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(1000000)))
      .aggregate(0l) { (k, v, agg) => v + agg }(implicitConversion.windowStoreToSnapshotStore).toStream.foreach((k, v) => {
        //                println(k + " " + v)
      })

    // Add a global store for customers. The data from this global store
    // will be fully replicated on each instance of this application.
    //    builder.addGlobalStore(storeBuilder1, CUSTOMER_TOPIC, Consumed.`with`(Serdes.String, Serdes.String), () => new GlobalStoresExample.GlobalStoreUpdater[String, String](CUSTOMER_STORE))
    //    // Add a global store for products. The data from this global store
    //    // will be fully replicated on each instance of this application.
    //    builder.addGlobalStore(storeBuilder2, PRODUCT_TOPIC, Consumed.`with`(Serdes.String, Serdes.String), () => new GlobalStoresExample.GlobalStoreUpdater[String, String](PRODUCT_STORE))

    //    builder.addGlobalStore(Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(CUSTOMER_STORE), Serdes.String, Serdes.String()), CUSTOMER_TOPIC, Consumed.`with`(Serdes.String, Serdes.String), () => new GlobalStoresExample.GlobalStoreUpdater[String, String](CUSTOMER_STORE))
    //    builder.addGlobalStore(Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(PRODUCT_STORE), Serdes.String, Serdes.String()), PRODUCT_TOPIC, Consumed.`with`(Serdes.String, Serdes.String), () => new GlobalStoresExample.GlobalStoreUpdater[String, String](CUSTOMER_STORE))

    // We transform each order with a value transformer which will access each
    // global store to retrieve customer and product linked to the order.
    val start = new KafkaStreams(builder.build, streamsConfiguration)

    start.setStateListener((newState, oldState) => {
      logger.info("changing state  " + oldState + newState.name())
    })
    //    start.streamsMetadataForStore("store1").asScala.map(_.)
    builder.build()
    //    start.setStateListener()

    class BoundedMemoryRocksDBConfig extends RocksDBConfigSetter {
      override def setConfig(
                              storeName: String,
                              options: Options,
                              configs: util.Map[String, AnyRef]
                            ): Unit = {

        val tableConfig = options.tableFormatConfig.asInstanceOf[BlockBasedTableConfig]

        options
          .setMaxWriteBufferNumber(100)
          .setLevel0FileNumCompactionTrigger(1000)
          .setWriteBufferSize(1000)
          .setDbWriteBufferSize(1000000000)
          //          .setTableFormatConfig(tableConfig)
          //          .setMaxWriteBufferNumberToMaintain(config.maxWriteBufferNumberToMaintain)
          .setMaxBackgroundJobs(10)
        ()
      }

      override def close(storeName: String, options: Options): Unit = ()
    }

    start
  }

}
