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
            partitions[part].broker,
            partitions[part].topicName,
            partitions[part].partition,
            messages);
    
        client.execute(request);
        request.getResult().get();
        
    }
```
