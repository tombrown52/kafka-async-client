package kafka.async.ops;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Future;

import kafka.async.KafkaBrokerIdentity;
import kafka.async.KafkaOperation;


public class ConfirmedProduceRequest implements KafkaOperation {
	
	private ProduceRequest produceRequest;
	private OffsetsRequest offsetsRequest;
	
	public ConfirmedProduceRequest(KafkaBrokerIdentity broker, byte[] topicName, int partition, List<byte[]> messages) {
		produceRequest = new ProduceRequest(broker, topicName, partition, messages);
		offsetsRequest = new OffsetsRequest(broker, topicName, partition, -1, 1);
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
	
	public Future<List<Long>> getResult() {
		return offsetsRequest.future;
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