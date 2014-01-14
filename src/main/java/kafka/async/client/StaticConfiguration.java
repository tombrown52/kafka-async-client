package kafka.async.client;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import kafka.async.BrokerPool;
import kafka.async.KafkaBrokerIdentity;
import kafka.async.KafkaPartitionIdentity;
import kafka.async.PartitionManager;

public class StaticConfiguration implements ClientConfiguration {
	
	private ManualPartitionManager partitionManager;
	private Set<KafkaBrokerIdentity> brokers;
	
	public StaticConfiguration(List<KafkaBrokerIdentity> brokers, byte[] topicName, int partitionsPerBroker) {
		partitionManager = new ManualPartitionManager();
		this.brokers = new HashSet<KafkaBrokerIdentity>(brokers);
		
		for (KafkaBrokerIdentity broker : brokers) {
			for (int partition=0; partition<partitionsPerBroker; ++partition) {
				partitionManager.addPartition(new KafkaPartitionIdentity(broker, topicName, partition));
			}
		}
	}
	
	public StaticConfiguration(Set<KafkaPartitionIdentity> partitions) {
		partitionManager = new ManualPartitionManager();
		
		Set<KafkaBrokerIdentity> brokers = new HashSet<KafkaBrokerIdentity>();
		for (KafkaPartitionIdentity partition : partitions) {
			if (!brokers.contains(partition.broker)) {
				brokers.add(partition.broker);
			}
		}
		partitionManager.addAllPartitions(partitions);
	}
	
	public PartitionManager getPartitionManager() {
		return partitionManager;
	}
	
	@Override
	public void addBrokerPool(BrokerPool brokerPool) {
		for (KafkaBrokerIdentity broker : brokers) {
			brokerPool.addBroker(broker);
		}
	}
	
	@Override
	public void removeBrokerPool(BrokerPool brokerPool) {
	}
}
