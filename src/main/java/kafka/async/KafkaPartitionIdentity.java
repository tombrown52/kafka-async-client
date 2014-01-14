package kafka.async;

import java.util.Arrays;

public class KafkaPartitionIdentity implements Comparable<KafkaPartitionIdentity> {

	public final KafkaBrokerIdentity broker;
	public final byte[] topicName;
	public final int partition;
	
	public KafkaPartitionIdentity(KafkaBrokerIdentity broker, byte[] topicName, int partition) {
		if (broker == null) {
			throw new IllegalArgumentException("broker must not be null");
		}
		if (topicName == null) {
			throw new IllegalArgumentException("topicName must not be null");
		}
		
		this.broker = broker;
		this.topicName = topicName;
		this.partition = partition;
	}


	@Override
	public int compareTo(KafkaPartitionIdentity other) {
		int cmp = broker.compareTo(other.broker);
		// TODO: Optimize topicName comparison
		if (cmp == 0) {
			if (partition == other.partition) {
				return Arrays.equals(topicName, other.topicName) ? 0 : new String(topicName).compareTo(new String(other.topicName));
			} else if (partition < other.partition) {
				return -1;
			} else if (partition > other.partition) {
				return 1;
			}
		}
		return cmp;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == null || obj.getClass() != KafkaPartitionIdentity.class) {
			return false;
		}
		return equals((KafkaPartitionIdentity)obj);
	}
	
	public boolean equals(KafkaPartitionIdentity other) {
		if (other == null) {
			return false;
		}
		
		if (!broker.equals(other.broker)) {
			return false;
		}
		
		if (topicName.length != other.topicName.length) {
			return false;
		}
		for (int i=0; i<topicName.length; ++i) {
			if (topicName[i] != other.topicName[i]) {
				return false;
			}
		}
		
		return partition == other.partition;
	}
	
	@Override
	public int hashCode() {
		int hash = broker.hashCode();
		for (byte b : topicName) {
			hash = hash*17 + b;
		}
		hash = hash*17 + partition;
		return hash;
	}
	
	@Override
	public String toString() {
		return "[kafka://"+broker.host+":"+broker.port+"/"+new String(topicName)+"#"+partition+"]";
	}
	
	
}
