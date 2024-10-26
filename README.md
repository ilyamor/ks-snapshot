Kafka-Streams State Store Snapshot to Remote Storage
============================================

One of the strongest features of Kafka-Streams is possibility to use streams as table, and querying such tables and/or join with stream.
Usually, under the hood such tables are stored in local disk storage as RocksDB files.

State store data is backed by topic in Kafka, so during the promoting new instance/pod or resuming after long inactivity caused by some bug or misconfiguration,
the Kafka-Streams consuming data from backed topic in Kafka.

It's, usually, the pain point of Kafka-Streams application, when restoring data from Kafka topic takes too much time and can cause for unnecessary lag, and worse - rebalance on consumers.

We suggest to store snapshots of rocksdb to object storage, like s3 and during initializing the state, before consuming the data from kafka changelog topic, first download snapshot from object storage.

This solution does not work with Global State Stores, since it should have one restoring point between all consumers, and even few services which use global state store.

It works only when you state store is defined with explicit `Materialized`. 
It won't work for state stores created automatically by Kafka-Streams.

The snippet below creates `Materialized` implicitly, so even snapshot to object storage enabled, the state store won't be replicated to remote storage.

`java`
```java
    StreamsBuilder sb = .....;
    sb.stream("source topic").groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(5))).count();
    ..........
```

__Note:__ Currently, only `windowed stores` are supported.

In case you want to enable this feature, you should define `Materialized` and use it for aggregation method:

`java`
```java
    StreamsBuilder sb = new StreamsBuilder();
    Materialized<K, Long, KeyValueStore<Bytes, byte[]> materialized = .....;
    sb.stream("source topic").groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(5))).count(materialized);
    ..........
```

To define materialized with enabled snapshot to object storage, use:

`java`
```java
    Properties streamsConfiguration = new Properties();
    // filling properties
    
    StreamBuilder streamBuilder = new StreamsBuilder();
    S3StateBuilder s3StateBuilder = S3StateBuilder.builder(props);
    Materialized<K, V, S> m1 = s3StateBuilder.windowStoreToSnapshotStore(keySerde, valueSerde);
    Materialized<K, V, S> m2 = s3StateBuilder.windowStoreToSnapshotStore(keySerde, valueSerde);
    // you should use building windows and/or aggregations with methods receiving Materialized,
    // else state to s3 snapshoter won't be used
    streamBuilder.windowBy(...).count(m1).toStream(....);
    streamBuilder.windowBy(...).sum(m2).toStream(....);
    KafkaStream streams = new KafkaStream(...);
    s3StateBuilder.enable(streams).start();
```


`scala`
```scala
    import org.apache.kafka.streams.scala.ImplicitConversions._
    import io.ilyamor.ks.snapshot.StoreFactory.KStreamOps

    implicit val streamsConfiguration: Properties = new Properties
    // filling properties
  
    val builder = new StreamsBuilder
    val ordersStream = builder.stream("source topic")(Consumed.`with`(Serdes.String(), Serdes.Long())).groupByKey
    
    ordersStream
      .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(1000000)))
      .aggregate(0l) { (k, v, agg) => v + agg }(implicitConversion.windowStoreToSnapshotStore).toStream.foreach((k, v) => {
        //                println(k + " " + v)
      })
    val start = new KafkaStreams(builder.build, streamsConfiguration)
    streams.enableS3Snapshot().start()
```

Default implementation uses S3 to store data. To upload state snapshot to s3, there are few configuration should be added to stream properties:


`scala`
```scala
    implicit val streamsConfiguration: Properties = new Properties
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDirPath)
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "global-stores-test")
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    // any other stream configurations
    // -------------------------------
    // -------------------------------
    // -------------------------------
    streamsConfiguration.put(S3StateStoreConfig.STATE_BUCKET, "bucket name")
    streamsConfiguration.put(S3StateStoreConfig.STATE_REGION, Region.US_EAST_1.id)
    // if you need to use custom URL, uncomment this
    // streamsConfiguration.put(S3StateStoreConfig.STATE_S3_ENDPOINT, "custom s3 endpoint url or minio URL")
```


| Property Name                       | Type   | Default Value                   | Required | Description                                                                                                                                                                      |
|-------------------------------------|--------|---------------------------------|----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| state.s3.storage.uploader           | Class  | classOf[UploadS3ClientForStore] | __V__    | class to use for upload data to storage. Should implements `StorageUploader` trait in scala or `StorageUploaderJava` interface for java folks and should have empty constructor. |
| state.s3.bucket.name                | String | ""                              |          | S3 bucket to use to store state store.                                                                                                                                           |
| state.s3.key.prefix                 | String | ""                              |          | S3 bucket key prefix (a.k.a. sub folder) to store state store.                                                                                                                   |
| state.s3.region                     | String | ""                              |          | S3 region to use where to store state store.                                                                                                                                     |
| state.s3.endpoint                   | String | ""                              |          | Custom S3 endpoint (like MinIO).                                                                                                                                                 |
| state.s3.snapshot.frequency.seconds | Long   | 60                              |          | The frequency to flush state store to object storage in seconds.                                                                                                                 |
| state.s3.offset.threshold           | Int    | 10000                           |          | The threshold to restore data from s3. If the value is less, than restoring from kafka. Should be grater than 100.                                                               |

### Notes:

1. During the snapshot, rocksdb files copied to avoid race conditions. During the copy, rocks db compaction disabled. After files are copied, compaction is enabled again.
    It means, you need to attach appropriate amount of storage to your kafka-streams service/pod
2. Before uploading data to remote storage, it's compressed. During initializing the data is uncompressed. So take in account, you'll need more CPU during the compression.
3. You can implement your own `StorageUploader`/`StorageUploaderJava`.