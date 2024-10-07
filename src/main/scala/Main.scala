//import io.confluent.examples.streams.GlobalStoresExample.alala.CoralogixStoreBuilder
//import org.apache.kafka.clients.consumer.ConsumerConfig
//import org.apache.kafka.common.serialization.Serdes.StringSerde
//import org.apache.kafka.common.utils.SystemTime
//import org.apache.kafka.streams.kstream.Consumed
//import org.apache.kafka.streams.processor.api
//import org.apache.kafka.streams.processor.api.{Processor, ProcessorSupplier}
//import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}
//
//import java.util.Properties
////import org.apache.kafka.streams.StreamsConfig
//
//object Main {
//  class CoralogixProcessor[K,V] extends Processor[K, V, Void, Void] {
//    override def process(record: api.Record[K, V]): Unit = {
//      ()
//    }
//  }
//  class CoralogiProcessorSupplier[K, V] extends ProcessorSupplier[K, V, Void, Void] {
//    override def get(): Processor[K, V, Void, Void] = {
//      new CoralogixProcessor[K,V]()
//
//
//    }
//  }
//
//  def main(args: Array[String]): Unit = {
//    val builder = new StreamsBuilder();
//
//    //change that to class definition
//    val consumed = Consumed.`with`(new StringSerde(), new StringSerde())
//
//
//    val storeBuilder = new CoralogixStoreBuilder[String, String]("store", new StringSerde(), new StringSerde(), new SystemTime())
//    builder.addGlobalStore(storeBuilder, "fun", consumed, new CoralogiProcessorSupplier[String, String]())
//    val streamsConfiguration = new Properties()
//    // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
//    // against which the application is run.
//    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "global-stores-example")
//    streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "global-stores-example-client")
//    // Where to find Kafka broker(s).
//    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
//    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "data2")
//    // Set to earliest so we don't miss any data that arrived in the topics before the process
//    // started
//    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
//
//
//    builder.stream("traces-gateway.spans").to("output")
//    new KafkaStreams(builder.build(), streamsConfiguration).start();
//
//    Thread.sleep(10000)
//
//
//  }
//
//
//
//}
