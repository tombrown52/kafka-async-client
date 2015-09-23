package kafka.async;

import java.text.ParseException;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
	
	private static Pattern PARSER = Pattern.compile("kafka://([a-z0-9\\.\\-\\_]+)(:([0-9]+))?/([^#]+)#([0-9]+)", Pattern.CASE_INSENSITIVE);
	public static KafkaPartitionIdentity parseIdentity(String str) throws ParseException {
		Matcher m = PARSER.matcher(str);
		if (!m.matches()) {
			throw new ParseException("Invalid kafka partition identity string", 0);
		}
		String host = m.group(1);
		int port = m.group(3) != null ? Integer.parseInt(m.group(3)) : 9092;
		String topicName = m.group(4);
		int partition = Integer.parseInt(m.group(5));
		return new KafkaPartitionIdentity(new KafkaBrokerIdentity(host, port), topicName.getBytes(), partition);
	}
	
}
