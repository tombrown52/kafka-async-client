package kafka.async.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import kafka.async.KafkaPartitionIdentity;
import kafka.async.futures.SettableFuture;
import kafka.async.futures.ValueFuture;
import kafka.async.ops.LateBindingConfirmedProduceRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a high-level object that sits on top of the low-level async client
 * interface and enables simple production to all partitions of a cluster.
 * @author tbrown
 */
public class PartitionProducer {

	private final static int MAX_BATCH = 300;
	private final KafkaAsyncClient client;
	
	public final static Logger logger = LoggerFactory.getLogger(PartitionProducer.class);

	public final class PartitionState {
		private final Object lock = new Object();

		private final KafkaPartitionIdentity partition;
		private final ArrayList<byte[]> queuedMessages;
		private final ArrayList<SettableFuture<Boolean>> queuedConfirmations;
		
		private int operationsWaitingToStart = 0;
		private int outstandingOperations = 0;
		
		public PartitionState(KafkaPartitionIdentity partition) {
			this.partition = partition;
			queuedMessages = new ArrayList<byte[]>(MAX_BATCH);
			queuedConfirmations = new ArrayList<SettableFuture<Boolean>>(MAX_BATCH);
		}
		
		/**
		 * Executes within the IO processing thread to notify this queue that the
		 * next request has reached the front of the line and is ready for a batch
		 * of messages.<p>
		 * @return The messages that should be included in the batch 
		 */
		public void getMessages(List<byte[]> messages, List<SettableFuture<Boolean>> confirmations) {
			synchronized (lock) {
				int queueSize = queuedMessages.size();
				List<byte[]> subMessages = queuedMessages.subList(0, Math.min(MAX_BATCH, queueSize));
				List<SettableFuture<Boolean>> subConfirmations = queuedConfirmations.subList(0, Math.min(MAX_BATCH, queueSize));
				if (logger.isTraceEnabled()) {
					logger.trace("Moving messages from queue to LateBindingConfirmedProduceRequest. Size is "+queueSize+" - "+subMessages.size()+"");
				}
				messages.addAll(subMessages);
				confirmations.addAll(subConfirmations);
				subMessages.clear();
				subConfirmations.clear();
			}
		}
		
		public SettableFuture<Boolean> produce(byte[] message) {
			SettableFuture<Boolean> confirmation = new ValueFuture<Boolean>();
			
			synchronized (lock) {
				if (logger.isTraceEnabled()) {
					logger.trace("Adding message to "+partition+" queue. Size is "+queuedMessages.size()+" + 1");
				}
				queuedMessages.add(message);
				queuedConfirmations.add(confirmation);
				if (operationsWaitingToStart == 0) {
					if (logger.isTraceEnabled()) {
						logger.trace("Creating new produce request for "+partition+". Reason: produce(messages)");
					}
					LateBindingConfirmedProduceRequest request = new LateBindingConfirmedProduceRequest(partition, this);
					client.execute(request);
					operationsWaitingToStart++;
					outstandingOperations++;
				}
			}
			
			return confirmation;
		}
		
		public List<SettableFuture<Boolean>> produce(List<byte[]> messages) {
			List<SettableFuture<Boolean>> confirmations = new ArrayList<SettableFuture<Boolean>>(messages.size());
			for (int i=0; i<messages.size(); ++i) {
				confirmations.add(new ValueFuture<Boolean>());
			}

			synchronized (lock) {
				queuedMessages.addAll(messages);
				queuedConfirmations.addAll(confirmations);
				if (logger.isTraceEnabled()) {
					logger.trace("Adding messages to "+partition+" queue. Size is "+queuedMessages.size()+" + "+messages.size());
				}
				if (operationsWaitingToStart == 0) {
					if (logger.isTraceEnabled()) {
						logger.trace("Creating new produce request for "+partition+". Reason: produce(list)");
					}
					LateBindingConfirmedProduceRequest request = new LateBindingConfirmedProduceRequest(partition, this);
					client.execute(request);
					operationsWaitingToStart++;
					outstandingOperations++;
				}
			}
			
			return confirmations;
		}
		
		/**
		 * Executes within the IO processing thread when the current request has been
		 * started and can no longer accept new messages.
		 */
		public void requestStarted() {
			synchronized (lock) {
				operationsWaitingToStart--;
				if (queuedMessages.size() > 0) {
					logger.trace("Creating new produce request for "+partition+". Reason: previous request started");
					LateBindingConfirmedProduceRequest request = new LateBindingConfirmedProduceRequest(partition, this);
					client.execute(request);
					operationsWaitingToStart++;
					outstandingOperations++;
				}
			}
		}
		
		/**
		 * Executes within the IO processing thread when the request has been completed
		 */
		public void requestComplete() {
			synchronized (lock) {
				outstandingOperations--;
				if (logger.isTraceEnabled()) {
					logger.trace("Request completed. Outstanding operations: "+outstandingOperations);
				}
				if (outstandingOperations == 0) {
					lock.notifyAll();
				}
			}
		}
		
		public void waitForEmpty() throws InterruptedException {
			synchronized (lock) {
				while (outstandingOperations > 0) {
					lock.wait();
				}
			}
		}
		
		public void waitForEmpty(long timeout) throws InterruptedException, TimeoutException {
			waitForEmpty(timeout, TimeUnit.MILLISECONDS);
		}
		
		public void waitForEmpty(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
			long endTime = System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(timeout, unit);
			synchronized (lock) {
				while (outstandingOperations > 0) {
					long waitTime = endTime - System.currentTimeMillis();
					if (waitTime <= 0) {
						throw new TimeoutException("Not empty");
					}
					unit.timedWait(lock, timeout);
				}
			}
		}

		/**
		 * Executes within the IO processing thread when a connection-level error occurs
		 * before the request has been completely written. 
		 * 
		 * This will only execute if the request has been started.<p>
		 * @param reason
		 */
		public void requestFailed(Exception reason) {
			synchronized (lock) {
				outstandingOperations--;
				lock.notifyAll();
			}
		}

		/**
		 * Executes within the IO processing thread when a connection-level error occurs
		 * before the response has been completely processed.<p>
		 * 
		 * This will only execute if the request has been started.<p>
		 * @param reason
		 */
		public void responseFailed(Exception reason) {
			synchronized (lock) {
				outstandingOperations--;
				lock.notifyAll();
			}
		}
		
		/**
		 * Executes within the IO processing thread when all connections to a specific
		 * broker have been failed.<p>
		 * 
		 * This will only execute if the request has not been started.<p>
		 * @param reason
		 */
		public void brokerFailed(Exception reason) {
			synchronized (lock) {
				operationsWaitingToStart--;
				outstandingOperations--;
				
				for (SettableFuture<Boolean> f : queuedConfirmations) {
					f.completeWithException(reason);
				}
				queuedMessages.clear();
				queuedConfirmations.clear();
				
				lock.notifyAll();
			}
		}
		
		public void close() {
			synchronized (lock) {
				RuntimeException reason = new RuntimeException("Partition was closed");
				for (SettableFuture<Boolean> f : queuedConfirmations) {
					f.completeWithException(reason);
				}
				queuedMessages.clear();
				queuedConfirmations.clear();
			}
		}
		
	}
	
	public PartitionProducer(KafkaAsyncClient client) {
		this.client = client;
	}
	
	private Map<KafkaPartitionIdentity,PartitionState> partitions = new HashMap<KafkaPartitionIdentity,PartitionState>();

	public void addPartition(KafkaPartitionIdentity partition) {
		partitions.put(partition, new PartitionState(partition));
	}
	
	public void removePartition(KafkaPartitionIdentity partition) {
		PartitionState state = partitions.remove(partition);
		if (state != null) {
			state.close();
		}
	}
	
	public SettableFuture<Boolean> produce(KafkaPartitionIdentity partition, byte[] message) {
		PartitionState state = partitions.get(partition);
		if (state == null) {
			throw new IllegalArgumentException("Unknown partition: "+partition+"");
		}
		return state.produce(message);
	}
	
	public List<SettableFuture<Boolean>> produce(KafkaPartitionIdentity partition, List<byte[]> messages) {
		PartitionState state = partitions.get(partition);
		if (state == null) {
			throw new IllegalArgumentException("Unknown partition: "+partition+"");
		}
		return state.produce(messages);
	}
	
	public void waitForEmpty() throws InterruptedException {
		for (PartitionState state : partitions.values()) {
			state.waitForEmpty();
		}
	}
	
	public void waitForEmpty(long timeout) throws InterruptedException, TimeoutException {
		waitForEmpty(timeout, TimeUnit.MILLISECONDS);
	}

	public void waitForEmpty(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
		long endTime = System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(timeout, unit);
		
		for (PartitionState state : partitions.values()) {
			long waitTime = endTime - System.currentTimeMillis();
			if (waitTime <= 0) {
				throw new TimeoutException("Not empty");
			}
			state.waitForEmpty(waitTime, TimeUnit.MILLISECONDS);
		}
	}
}
