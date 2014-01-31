package kafka.async.ops;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.zip.CRC32;

import kafka.async.KafkaAsyncProcessor;
import kafka.async.KafkaBrokerIdentity;
import kafka.async.KafkaOperation;
import kafka.async.futures.SelectableFuture;
import kafka.async.futures.ValueFuture;


public class ProduceRequest implements KafkaOperation {
	final KafkaBrokerIdentity broker;
	final byte[] topicName;
	final int partition;
	final List<byte[]> messages;
	final ValueFuture<Boolean> result;
	
	public ProduceRequest(KafkaBrokerIdentity broker, byte[] topicName, int partition, List<byte[]> messages) {
		this.broker = broker;
		this.topicName = topicName;
		this.partition = partition;
		this.messages = messages;
		this.result = new ValueFuture<Boolean>();
	}

	public SelectableFuture<Boolean> getResult() {
		return result;
	}
	
	@Override
	public boolean start() {
		return true;
	}
	
	@Override
	public boolean canRead() {
		return false;
	}
	
	@Override
	public boolean executeRead(ByteBuffer buffer) {
		return true;
	}
	
	@Override
	public void executeWrite(ByteBuffer buffer) {
		int size = 0;
		
		short requestType = 0; // 0=PRODUCE
		
		// Request length (int32) placeholder
		int requestSizePosition = buffer.position();
		buffer.putInt(0);
		
		// Request type (int16)
		buffer.putShort(requestType);
		
		// Topic length (int16)
		buffer.putShort((short)topicName.length);
		
		// Topic (byte[])
		buffer.put(topicName);
		
		// Partition (int32)
		buffer.putInt(partition);
		
		// Message-section size (int32) placeholder
		int messageSectionSizePosition = buffer.position();
		buffer.putInt(0);
		
		CRC32 checksum = new CRC32();
		
		for (byte[] message : messages) {
			
			// Individual message size (int32) placeholder
			int messageSizePosition = buffer.position();
			buffer.putInt(0);
			
			// Magic number (int8) (1 = Compression byte exists)
			buffer.put((byte)1);
			
			// Compression (int8) (0 = No compression)
			buffer.put((byte)0);
			
			// Payload checksum (int32) using CRC32
			checksum.reset();
			checksum.update(message);
			buffer.putInt((int)(checksum.getValue() & 0xFFFFFFFFL));

			// Message (byte[])
			buffer.put(message);
			
			size = buffer.position() - messageSizePosition - KafkaAsyncProcessor.SIZEOF_INT32;
			buffer.putInt(messageSizePosition,size);
		}

		size = buffer.position() - messageSectionSizePosition - KafkaAsyncProcessor.SIZEOF_INT32;
		buffer.putInt(messageSectionSizePosition,size);

		size = buffer.position() - requestSizePosition - KafkaAsyncProcessor.SIZEOF_INT32;
		buffer.putInt(requestSizePosition,size);
	}
	
	@Override
	public void writeComplete() {
		result.completeWithValue(true);
	}
	
	@Override
	public void responseFailed(Exception reason) {
		// No response is expected, so the response can't fail.
	}
	
	@Override
	public void requestFailed(Exception reason) {
		result.completeWithException(reason);
	}

	@Override
	public void brokerFailed(Exception reason) {
		result.completeWithException(reason);
	}
	
	@Override
	public KafkaBrokerIdentity getTargetBroker() {
		return broker;
	}
	
}