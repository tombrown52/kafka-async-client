Kafka async client
==================

Currently supports Kafka version 0.7.x

Built with the following features as the goal:

1. Publish request pipelining for higher write throughput.
2. Publish confirmation for the 0.7.x branch.
3. Asynchronous IO allows fewer threads, enabling increased throughput.
4. All server functions are exposed through the same client.


Example: produce request (with confirmation)
----------------------------------

Usage example for the confirmed produce request

```java
    String topic = "test";
    
    List<KafkaBrokerIdentity> brokers = new ArrayList<KafkaBrokerIdentity>();        
    brokers.add(new KafkaBrokerIdentity("broker1-host",9092));
    brokers.add(new KafkaBrokerIdentity("broker2-host",9092));
    StaticConfiguration config = new StaticConfiguration(brokers, topic.getBytes("ASCII"), 2);
    KafkaAsyncClient client = new KafkaAsyncClient(config);
    client.open();


    KafkaPartitionIdentity[] partitions = config.getPartitionManager().all().toArray(new KafkaPartitionIdentity[0]);


    while (...) {
    
        // Each message is an individual byte array
        List<byte[]> messages = ...
    
        // Send this produce request to a random partition
        part = Math.random() * partitions.length;
    
        ConfirmedProduceRequest request = new ConfirmedProduceRequest(
            partitions[part],messages);
    
        client.execute(request);
        
        // Will throw an exception if the server confirmation was
        // not received.
        request.getResult().get();
        
    }
```


Example: produce request (with compression)
----------------------------------

Usage example for the confirmed produce request

```java
    String topic = "test";
    
    List<KafkaBrokerIdentity> brokers = new ArrayList<KafkaBrokerIdentity>();        
    brokers.add(new KafkaBrokerIdentity("broker1-host",9092));
    brokers.add(new KafkaBrokerIdentity("broker2-host",9092));
    StaticConfiguration config = new StaticConfiguration(brokers, topic.getBytes("ASCII"), 2);
    KafkaAsyncClient client = new KafkaAsyncClient(config);
    client.open();


    KafkaPartitionIdentity[] partitions = config.getPartitionManager().all().toArray(new KafkaPartitionIdentity[0]);


    while (...) {
    
        // Each message is an individual byte array
        List<byte[]> messages = ...
    
        // Send this produce request to a random partition
        part = Math.random() * partitions.length;
    
        ConfirmedProduceRequest request = new ConfirmedProduceRequest(
            partitions[part],
            1,          // Compression algorithm (0=none, 1=gzip)
            true,       // Compress in the IO thread before sending
                        // (if false, assume each message is already compressed)
            messages);
    
        client.execute(request);
        
        // Will throw an exception if the server confirmation was
        // not received.
        request.getResult().get();
        
    }
```


Example: fetch request
----------------------


```java
    String topic = "test";
    
    List<KafkaBrokerIdentity> brokers = new ArrayList<KafkaBrokerIdentity>();        
    brokers.add(new KafkaBrokerIdentity("broker1-host",9092));
    brokers.add(new KafkaBrokerIdentity("broker2-host",9092));
    StaticConfiguration config = new StaticConfiguration(brokers, topic.getBytes("ASCII"), 2);
    KafkaAsyncClient client = new KafkaAsyncClient(config);
    client.open();

    KafkaPartitionIdentity partition = new KafkaPartitionIdentity(
        new KafkaBrokerIdentity("broker1-host",9092), // broker
        topic.getBytes("ASCII"),                      // topic
        0);                                           // partition

    FetchRequest fetch = new FetchRequest(
        partition,  // <-- source
        0,          // <-- offset
        1024*1024); // <-- max request size

    client.execute(fetch);
    
    MessageSet messages = fetch.getResult().get();
    
    for (Message m : messages) {
        ByteBuffer payload = m.getUncompressedContents();
        int size = payload.remaining();
    	payload.get(bits,0,size);
    	System.out.println("Message at "+m.startOffset+" length "+(m.endOffset-m.startOffset));
    }
```


Example: offsets request
------------------------


```java
    String topic = "test";
    
    List<KafkaBrokerIdentity> brokers = new ArrayList<KafkaBrokerIdentity>();        
    brokers.add(new KafkaBrokerIdentity("broker1-host",9092));
    brokers.add(new KafkaBrokerIdentity("broker2-host",9092));
    StaticConfiguration config = new StaticConfiguration(brokers, topic.getBytes("ASCII"), 2);
    KafkaAsyncClient client = new KafkaAsyncClient(config);
    client.open();

    KafkaPartitionIdentity partition = new KafkaPartitionIdentity(
        new KafkaBrokerIdentity("broker1-host",9092), // broker
        topic.getBytes("ASCII"),                      // topic
        0);                                           // partition
        
    OffsetsRequest req = new FetchRequest(
        partition,  // <-- source
        -1,         // <-- time (-2=earliest, -1=most recent) 
        1024*1024); // <-- max # offsets to return
    
    client.execute(req);
    
    List<Long> offsets = req.getResult().get();
    
    // Offsets are now in "offsets"
```