package kafka.async.ops;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Future;

import kafka.async.KafkaBrokerIdentity;
import kafka.async.KafkaOperation;
import kafka.async.KafkaPartitionIdentity;
import kafka.async.client.Message;


public class ConfirmedProduceRequest implements KafkaOperation {
	
	private final ProduceRequest produceRequest;
	private final OffsetsRequest offsetsRequest;
	
	public ConfirmedProduceRequest(KafkaPartitionIdentity partition, List<byte[]> messages) {
		this(partition, Message.COMPRESSION_NONE, false, messages);
	}
	
	public ConfirmedProduceRequest(KafkaPartitionIdentity partition, int compression, boolean compress, List<byte[]> messages) {
		produceRequest = new ProduceRequest(partition, compression, compress, messages);
		offsetsRequest = new OffsetsRequest(partition, -1, 1);
	}
	
	@Override
	public String operationId() {
		return "K_PRODUCE_WITH_ACK";
	}
	
	@Override
	public boolean start() {
		return offsetsRequest.start();
	}
	
	@Override
	public boolean canRead() {
		return true;
	}
	
	@Override
	public boolean executeRead(ByteBuffer buffer) {
		return offsetsRequest.executeRead(buffer);
	}
	
	@Override
	public void executeWrite(ByteBuffer buffer) {
		produceRequest.executeWrite(buffer);
		offsetsRequest.executeWrite(buffer);
	}
	
	@Override
	public void writeComplete() {
		produceRequest.writeComplete();
		offsetsRequest.writeComplete();
	}
	
	public Future<List<Long>> getResult() {
		return offsetsRequest.getResult();
	}
	
	@Override
	public void responseFailed(Exception reason) {
		offsetsRequest.responseFailed(reason);
	}
	
	@Override
	public void requestFailed(Exception reason) {
		offsetsRequest.requestFailed(reason);
	}
	
	@Override
	public void brokerFailed(Exception reason) {
		offsetsRequest.brokerFailed(reason);
	}
	
	@Override
	public KafkaBrokerIdentity getTargetBroker() {
		return produceRequest.getTargetBroker();
	}

}