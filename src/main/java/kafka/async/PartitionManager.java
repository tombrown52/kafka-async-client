package kafka.async;

import java.util.Set;

/**
 * An interface for managing the available partitions for a particular kafka client.<p>
 * 
 * The observable state should remain static unless the changed method has been
 * invoked. If the state has changed (partitions were added or removed), changed()
 * will return true and the deltas will be updated.<p>
 * 
 * 
 * 
 * @author tbrown
 *
 */
public interface PartitionManager {
	public boolean changed();
	public Set<KafkaPartitionIdentity> all();
	public Set<KafkaPartitionIdentity> added();
	public Set<KafkaPartitionIdentity> removed();
}
