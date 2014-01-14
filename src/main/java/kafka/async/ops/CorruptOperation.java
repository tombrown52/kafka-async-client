package kafka.async.ops;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;

import kafka.async.KafkaAsyncProcessor;
import kafka.async.KafkaBrokerIdentity;
import kafka.async.KafkaOperation;
import kafka.async.futures.ValueFuture;

public class CorruptOperation implements KafkaOperation {

	private KafkaBrokerIdentity brokerIdentity;
	private ValueFuture<List<Long>> broken;

	public CorruptOperation(KafkaBrokerIdentity brokerIdentity) {
		super();
		this.brokerIdentity = brokerIdentity;
		this.broken = new ValueFuture<List<Long>>();
	}

	@Override
	public KafkaBrokerIdentity getTargetBroker() {
		return brokerIdentity;
	}
	
	public Future<List<Long>> getBroken() {
		return broken;
	}

	@Override
	public boolean start() {
		return true;
	}
	
	@Override
	public void executeWrite(ByteBuffer buffer) {
		int size = 0;
		
		byte[] topicName = "test".getBytes();
		int partition = 999999;
		long time = 0;
		
		short requestType = 4; // 4=OFFSETS
		
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
		
		// Time (int64)
		buffer.putLong(time);
		
		// Max number of offsets (int32);
		buffer.putInt(1);

		size = buffer.position() - requestSizePosition - KafkaAsyncProcessor.SIZEOF_INT32;
		buffer.putInt(requestSizePosition,size);
	}

	@Override
	public boolean executeRead(ByteBuffer buffer) {
		int size = buffer.getInt(0) + KafkaAsyncProcessor.SIZEOF_INT32;
		if (buffer.position() < size) {
			return false;
		}
		
		buffer.flip();
		buffer.getInt();
		int errorCode = buffer.getShort();
		if (errorCode != 0) {
			throw new RuntimeException("Kafka reported error code "+errorCode);
		}
		
		int numberOfOffsets = buffer.getInt();
		Long[] offsets = new Long[numberOfOffsets];
		for (int i=0; i<numberOfOffsets; ++i) {
			offsets[i] = buffer.getLong();
		}
		
		broken.completeWithValue(Arrays.asList(offsets));
		return true;
	}

	@Override
	public boolean canRead() {
		return true;
	}

	@Override
	public void responseFailed(Exception reason) {
		broken.completeWithException(reason);
	}
	
	@Override
	public void requestFailed(Exception reason) {
		broken.completeWithException(reason);
	}
	
	@Override
	public void brokerFailed(Exception reason) {
		broken.completeWithException(reason);
	}

}
