package kafka.async.client;

import kafka.async.BrokerPool;
import kafka.async.PartitionManager;

public interface ClientConfiguration {
	public PartitionManager getPartitionManager();
	public void addBrokerPool(BrokerPool brokerPool);
	public void removeBrokerPool(BrokerPool brokerPool);
}
