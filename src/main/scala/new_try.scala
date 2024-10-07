package io.confluent.examples.streams


import com.github.blemale.scaffeine.{Cache, Scaffeine}
import io.confluent.examples.streams.GlobalStoresExample.alala.CoralogixStoreBuilder
import io.confluent.examples.streams.SnapshotStoreListener.SnapshotStoreListener
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.common.utils.{SystemTime, Time}
import org.apache.kafka.streams.kstream.{Consumed, TimeWindows}
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.kstream.Materialized
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.scala.{ByteArrayWindowStore, StreamsBuilder}
import org.apache.kafka.streams.state.internals.CoralogixStore.WindowsCoralogixSupplier
import org.apache.kafka.streams.state.internals._
import org.apache.kafka.streams.state.{KeyValueIterator, KeyValueStore, RocksDBConfigSetter}
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig}
import org.apache.logging.log4j.scala.Logging
import org.rocksdb.{BlockBasedTableConfig, Options}

import java.time.Duration
import java.util
import java.util.Properties
import scala.concurrent.duration.DurationInt
import scala.util.Random

/*
 * Copyright Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/**
 * Demonstrates how to perform "joins" between  KStreams and GlobalStore, i.e. joins that
 * don't require re-partitioning of the input streams.
 * <p>
 * The {@link GlobalKTablesExample} shows another way to perform the same operation using
 * {@link org.apache.kafka.streams.kstream.GlobalKTable} and the join operator.
 * <p>
 * In this example, we join a stream of orders that reads from a topic named
 * "order" with a customers global store that reads from a topic named "customer", and a products
 * global store that reads from a topic "product". The join produces an EnrichedOrder object.
 * <p>
 * <br>
 * HOW TO RUN THIS EXAMPLE
 * <p>
 * 1) Start Zookeeper, Kafka, and Confluent Schema Registry. Please refer to <a href="http://docs.confluent.io/current/quickstart.html#quickstart">QuickStart</a>.
 * <p>
 * 2) Create the input/intermediate/output topics used by this example.
 * <pre>
 * {@code
 * $ bin/kafka-topics --create --topic order \
 * --zookeeper localhost:2181 --partitions 4 --replication-factor 1
 * $ bin/kafka-topics --create --topic customer \
 * --zookeeper localhost:2181 --partitions 3 --replication-factor 1
 * $ bin/kafka-topics --create --topic product \
 * --zookeeper localhost:2181 --partitions 2 --replication-factor 1
 * $ bin/kafka-topics --create --topic enriched-order \
 * --zookeeper localhost:2181 --partitions 4 --replication-factor 1
 * }</pre>
 * Note: The above commands are for the Confluent Platform. For Apache Kafka it should be
 * `bin/kafka-topics.sh ...`.
 * <p>
 * 3) Start this example application either in your IDE or on the command line.
 * <p>
 * If via the command line please refer to <a href="https://github.com/confluentinc/kafka-streams-examples#packaging-and-running">Packaging</a>.
 * Once packaged you can then run:
 * <pre>
 * {@code
 * $ java -cp target/kafka-streams-examples-6.0.1-standalone.jar io.confluent.examples.streams.GlobalStoresExample
 * }
 * </pre>
 * 4) Write some input data to the source topics (e.g. via {@link GlobalKTablesAndStoresExampleDriver}). The
 * already running example application (step 3) will automatically process this input data and write
 * the results to the output topic.
 * <pre>
 * {@code
 * # Here: Write input data using the example driver. The driver will exit once it has received
 * # all EnrichedOrders
 * $ java -cp target/kafka-streams-examples-6.0.1-standalone.jar io.confluent.examples.streams.GlobalKTablesAndStoresExampleDriver
 * }
 * </pre>
 * <p>
 * 5) Once you're done with your experiments, you can stop this example via {@code Ctrl-C}. If needed,
 * also stop the Confluent Schema Registry ({@code Ctrl-C}), then stop the Kafka broker ({@code Ctrl-C}), and
 * only then stop the ZooKeeper instance ({@code Ctrl-C}).
 */
object GlobalStoresExample extends Logging {
  object alala {
    class SerilizedKeyValueStore[K, V](name: String, keySerde: Serde[K], valueSerde: Serde[V]) extends KeyValueStore[K, V] {

      val map = new util.HashMap[K, V]()

      override def put(key: K, value: V): Unit = {
        map.put(key, value)
      }

      override def putIfAbsent(key: K, value: V): V = {
        map.putIfAbsent(key, value)
      }

      override def putAll(entries: util.List[KeyValue[K, V]]): Unit = {
        //convert list to map

        val map = new util.HashMap[K, V]()
        entries.forEach(entry => map.put(entry.key, entry.value))
        map.putAll(map)
      }

      override def delete(key: K): V = map.remove(key)

      override def get(key: K): V = map.get(key)

      override def range(from: K, to: K): KeyValueIterator[K, V] = ???

      override def all(): KeyValueIterator[K, V] = ???

      override def approximateNumEntries(): Long = map.size()

      override def name(): String = name


      override def flush(): Unit = ()

      override def close(): Unit = ()

      override def persistent(): Boolean = false

      override def isOpen: Boolean = false

      override def init(context: ProcessorContext, root: StateStore): Unit = {
        context.register(root, (key: Array[Byte], value: Array[Byte]) => {
          val keyDes = keySerde.deserializer().deserialize("", key)
          val valueDes = valueSerde.deserializer().deserialize("", value)
          println("inside global store")
          put(keyDes, valueDes)
        }
        )
      }
    }

    class CoralogixStoreBuilder[K, V](name: String, keySerdere: Serde[K], valueSerde: Serde[V], time: Time) extends AbstractStoreBuilder[K, V, SerilizedKeyValueStore[K, V]](name, keySerdere, valueSerde, time) {

      override def build(): SerilizedKeyValueStore[K, V] = {

        new SerilizedKeyValueStore[K, V](name, keySerdere, valueSerde)
      }
    }
  }

  private[streams] val ORDER_TOPIC = "order"
  private[streams] val CUSTOMER_TOPIC = "customer"
  private[streams] val PRODUCT_TOPIC = "product"
  private[streams] val CUSTOMER_STORE = "customer-store"
  private[streams] val PRODUCT_STORE = "product-store"
  private[streams] val ENRICHED_ORDER_TOPIC = "enriched-order"

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
    val storeBuilder1 = new CoralogixStoreBuilder[String, String](CUSTOMER_STORE, new StringSerde(), new StringSerde(), new SystemTime())
    val storeBuilder2 = new CoralogixStoreBuilder[String, String](PRODUCT_STORE, new StringSerde(), new StringSerde(), new SystemTime())

    val streamsConfiguration = new Properties
    // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
    // against which the application is run.
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "global-stores-example1")
    // Where to find Kafka broker(s).

    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    streamsConfiguration.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 18000)
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG,Random.alphanumeric.take(10).mkString)
    //    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDir)
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0)
    streamsConfiguration.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0)
    streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0)
    streamsConfiguration.put(StreamsConfig.STATE_CLEANUP_DELAY_MS_CONFIG, 20000000)
    streamsConfiguration.put(StreamsConfig.PROBING_REBALANCE_INTERVAL_MS_CONFIG, 60000)
    streamsConfiguration.put(StreamsConfig.ACCEPTABLE_RECOVERY_LAG_CONFIG, 10000)
    streamsConfiguration.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100000)

    streamsConfiguration.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 50 * 1024 * 1024)

    // Set to earliest so we don't miss any data that arrived in the topics before the process
    // started
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    // create and configure the SpecificAvroSerdes required in this example
    val snapshotStoreListener = new SnapshotStoreListener(null, "bucketName")

    val builder = new StreamsBuilder
    // Get the stream of orders
    val ordersStream = builder.stream(ORDER_TOPIC)(Consumed.`with`(Serdes.String(), Serdes.String())).peek((k, v) => {
    }).groupByKey

    val met: Materialized[String, String, ByteArrayWindowStore] = (Materialized.as(new WindowsCoralogixSupplier("store1", 1000000l, 1000000l, 1000000l, true, false, snapshotStoreListener))(Serdes.String(), Serdes.String()))

    val g = ordersStream
      .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(1000000)))
      .aggregate("") { (k, v, agg) => v + agg }(met).toStream.foreach((k, v) => {
        //        println(k + " " + v)
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
    start.setGlobalStateRestoreListener(snapshotStoreListener)
    start.setStandbyUpdateListener(snapshotStoreListener)
    start.setStateListener((newState, oldState) => {
      logger.info("changing state ilya " + oldState + newState.name())
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


// Processor that keeps the global store updated.
class GlobalStoreUpdater[K, V](private val storeName: String) extends Processor[K, V, Void, Void] {
  private var store: KeyValueStore[K, V] = null

  override def init(context: api.ProcessorContext[Void, Void]): Unit = {

    store = context.getStateStore(storeName).asInstanceOf[KeyValueStore[K, V]]
  }

  override def close(): Unit = {

    // No-op
  }

  override def process(record: api.Record[K, V]): Unit = {
    println(Thread.currentThread() + "restoring")
    store.put(record.key, record.value)

  }
}

object SnapshotStoreListener {
  case class TppStore(topicPartition: TopicPartition, storeName: String)

  case class SnapshotStoreListener(s3Client: Unit, bucketName: String) extends StateRestoreListener with StandbyUpdateListener{

    override def onUpdateStart(topicPartition: TopicPartition, storeName: String, startingOffset: Long): Unit = {
      onRestoreStart(topicPartition, storeName, startingOffset, 0)
    }

    override def onBatchLoaded(topicPartition: TopicPartition, storeName: String, taskId: TaskId, batchEndOffset: Long, batchSize: Long, currentEndOffset: Long): Unit = {
      onBatchRestored(topicPartition, storeName, batchEndOffset, batchSize)
    }

    override def onUpdateSuspended(topicPartition: TopicPartition, storeName: String, storeOffset: Long, currentEndOffset: Long, reason: StandbyUpdateListener.SuspendReason): Unit = {

      onRestoreSuspended(topicPartition, storeName, currentEndOffset)
    }


    val taskStore: Cache[TppStore, Boolean] = Scaffeine().expireAfterAccess(10.minutes).build[TppStore, Boolean]()

    override def onRestoreStart(topicPartition: TopicPartition, storeName: String, startingOffset: Long, endingOffset: Long): Unit = {
      println(Thread.currentThread() + "before restore topic" + topicPartition + " store " + storeName)
      taskStore.put(TppStore(topicPartition, storeName), true)
    }

    override def onBatchRestored(topicPartition: TopicPartition, storeName: String, batchEndOffset: Long, numRestored: Long): Unit = {
      taskStore.put(TppStore(topicPartition, storeName), true)
      println(Thread.currentThread() + "on ")
    }

    override def onRestoreEnd(topicPartition: TopicPartition, storeName: String, totalRestored: Long): Unit = {
      taskStore.put(TppStore(topicPartition, storeName), false)

      println(Thread.currentThread() + "on end")

    }

    override def onRestoreSuspended(topicPartition: TopicPartition, storeName: String, totalRestored: Long): Unit = {
      if (totalRestored <= 0)
        taskStore.put(TppStore(topicPartition, storeName), true)
      else {
        taskStore.put(TppStore(topicPartition, storeName), false)
      }
      super.onRestoreSuspended(topicPartition, storeName, totalRestored)
    }


  }
}