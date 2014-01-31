package kafka.async.ops;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import kafka.async.KafkaBrokerIdentity;
import kafka.async.KafkaOperation;
import kafka.async.KafkaPartitionIdentity;
import kafka.async.client.PartitionProducer;
import kafka.async.futures.SettableFuture;

public class LateBindingConfirmedProduceRequest implements KafkaOperation {

	private final KafkaPartitionIdentity partition;
	private ProduceRequest produceRequest; // not set until "start" is called the first time
	private final OffsetsRequest offsetsRequest;
	private final PartitionProducer.PartitionState state;
	
	private final List<SettableFuture<Boolean>> confirmations;
	
	public LateBindingConfirmedProduceRequest(KafkaPartitionIdentity partition, PartitionProducer.PartitionState state) {
		synchronized (this) {
			this.partition = partition;
			this.state = state;
			this.offsetsRequest = new OffsetsRequest(partition.broker, partition.topicName, partition.partition, -1, 1);
			this.confirmations = new ArrayList<SettableFuture<Boolean>>(1048);
		}
	}
	
	public Future<List<Long>> getConfirmation() {
		return offsetsRequest.getResult();
	}
	
	@Override
	public KafkaBrokerIdentity getTargetBroker() {
		return partition.broker;
	}

	@Override
	public boolean start() {
		synchronized (this) {
			if (produceRequest == null) {
				List<byte[]> messages = new ArrayList<byte[]>(1048);
				state.getMessages(messages,confirmations);
				
				produceRequest = new ProduceRequest(partition.broker, partition.topicName, partition.partition, messages);
				produceRequest.start();
				
				offsetsRequest.start();
				state.requestStarted();
				return true;
			}
		}
		return false;
	}

	@Override
	public void executeWrite(ByteBuffer buffer) {
		produceRequest.executeWrite(buffer);
		offsetsRequest.executeWrite(buffer);
	}

	@Override
	public void writeComplete() {
	}
	
	@Override
	public boolean executeRead(ByteBuffer buffer) {
		if (offsetsRequest.executeRead(buffer)) {
			for (SettableFuture<Boolean> f : confirmations) {
				f.completeWithValue(true);
			}
			state.requestComplete();
			return true;
		}
		return false;
	}

	@Override
	public boolean canRead() {
		return true;
	}

	@Override
	public void responseFailed(Exception reason) {
		for (SettableFuture<Boolean> f : confirmations) {
			f.completeWithException(reason);
		}
		offsetsRequest.responseFailed(reason);
		state.responseFailed(reason);
	}

	@Override
	public void requestFailed(Exception reason) {
		for (SettableFuture<Boolean> f : confirmations) {
			f.completeWithException(reason);
		}
		offsetsRequest.requestFailed(reason);
		state.requestFailed(reason);
	}
	
	@Override
	public void brokerFailed(Exception reason) {
		for (SettableFuture<Boolean> f : confirmations) {
			f.completeWithException(reason);
		}
		offsetsRequest.brokerFailed(reason);
		state.brokerFailed(reason);
	}
}
