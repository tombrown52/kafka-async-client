package kafka.async.client;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import kafka.async.KafkaPartitionIdentity;
import kafka.async.PartitionManager;

public class ManualPartitionManager implements PartitionManager {

	private Object lock = new Object();
	
	private AtomicInteger pendingRev = new AtomicInteger(0);
	private Set<KafkaPartitionIdentity> pendingAdded = new HashSet<KafkaPartitionIdentity>();
	private Set<KafkaPartitionIdentity> pendingRemoved = new HashSet<KafkaPartitionIdentity>();
	
	private int rev = 0;
	private Set<KafkaPartitionIdentity> all = new HashSet<KafkaPartitionIdentity>();
	private Set<KafkaPartitionIdentity> added = new HashSet<KafkaPartitionIdentity>();
	private Set<KafkaPartitionIdentity> removed = new HashSet<KafkaPartitionIdentity>();
	
	@Override
	public boolean changed() {
		if (rev == pendingRev.get()) {
			return false;
		}
		
		synchronized (lock) {
			added = pendingAdded;
			removed = pendingRemoved;
			pendingAdded = new HashSet<KafkaPartitionIdentity>();
			pendingRemoved = new HashSet<KafkaPartitionIdentity>();

			all.removeAll(removed);
			all.addAll(added);
			
			rev = pendingRev.get();
		}
		
		return true;
	}

	@Override
	public Set<KafkaPartitionIdentity> all() {
		return all;
	}

	@Override
	public Set<KafkaPartitionIdentity> added() {
		return added;
	}

	@Override
	public Set<KafkaPartitionIdentity> removed() {
		return removed;
	}

	public void addAllPartitions(Set<KafkaPartitionIdentity> partitions) {
		synchronized (lock) {
			// Only add the partitions that do not already exist in "all"
			HashSet<KafkaPartitionIdentity> toAdd = new HashSet<KafkaPartitionIdentity>(partitions);
			toAdd.removeAll(all);
			pendingAdded.addAll(toAdd);
			
			// Any partition in the provided set that has been removed (but not
			// yet committed) should be added by this change.
			pendingRemoved.removeAll(partitions);

			pendingRev.incrementAndGet();
		}
	}
	
	public void addPartition(KafkaPartitionIdentity partition) {
		synchronized (lock) {
			pendingRemoved.remove(partition);
			if (!all.contains(partition)) {
				pendingAdded.add(partition);
			}
			pendingRev.incrementAndGet();
		}
	}

	public void removeAllPartitions(Set<KafkaPartitionIdentity> partitions) {
		synchronized (lock) {
			// Only remove the partitions that exist in "all"
			HashSet<KafkaPartitionIdentity> toRemove = new HashSet<KafkaPartitionIdentity>(partitions);
			toRemove.retainAll(all);
			pendingRemoved.addAll(toRemove);
			
			// Any partition in the provided set that has been added (but not
			// yet committed) should be removed by this change.
			pendingAdded.removeAll(partitions);
			
			pendingRev.incrementAndGet();
		}
	}
	
	public void removePartition(KafkaPartitionIdentity partition) {
		synchronized (lock) {
			pendingAdded.remove(partition);
			if (all.contains(partition)) {
				pendingRemoved.add(partition);
			}
			pendingRev.incrementAndGet();
		}
	}
	
}
