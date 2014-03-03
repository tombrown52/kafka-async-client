package kafka.async.ops;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;

import kafka.async.KafkaAsyncProcessor;
import kafka.async.KafkaBrokerIdentity;
import kafka.async.KafkaOperation;
import kafka.async.KafkaPartitionIdentity;
import kafka.async.futures.ValueFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OffsetsRequest implements KafkaOperation {
	private KafkaPartitionIdentity partition;
	private final long time;
	private final int maxOffsets;
	private final ValueFuture<List<Long>> future;
	
	static Logger logger = LoggerFactory.getLogger(OffsetsRequest.class);

	public OffsetsRequest(KafkaPartitionIdentity partition, long time, int maxOffsets) {
		this.partition = partition;
		this.time = time;
		this.maxOffsets = maxOffsets;
		
		future = new ValueFuture<List<Long>>();
	}
	
	@Override
	public boolean start() {
		return future.beginExecution();
	}
	
	@Override
	public boolean canRead() {
		return true;
	}
	
	@Override
	public boolean executeRead(ByteBuffer buffer) {
		int size = buffer.getInt(0) + KafkaAsyncProcessor.SIZEOF_INT32;
		if (buffer.position() < size) {
			return false;
		}

		if (logger.isTraceEnabled()) {
			logger.trace("Read "+buffer.position()+" bytes (response is "+size+" bytes)");
		}
		
		buffer.flip();
		buffer.getInt();
		int errorCode = buffer.getShort();
		if (logger.isTraceEnabled()) {
			logger.trace("Response status was code: "+errorCode);
		}
		if (errorCode != 0) {
			throw new RuntimeException("Kafka reported error code "+errorCode);
		}
		
		int numberOfOffsets = buffer.getInt();
		if (logger.isTraceEnabled()) {
			logger.trace("Response contains "+numberOfOffsets+" offset(s)");
		}
		Long[] offsets = new Long[numberOfOffsets];
		for (int i=0; i<numberOfOffsets; ++i) {
			offsets[i] = buffer.getLong();
			if (logger.isTraceEnabled()) {
				logger.trace("Response offset["+i+"] is "+offsets[i]);
			}
		}
		
		future.completeWithValue(Arrays.asList(offsets));
		return true;
	}
	
	@Override
	public void executeWrite(ByteBuffer buffer) {
		int size = 0;
		
		short requestType = 4; // 4=OFFSETS
		
		// Request length (int32) placeholder
		int requestSizePosition = buffer.position();
		buffer.putInt(0);
		
		// Request type (int16)
		buffer.putShort(requestType);
		
		// Topic length (int16)
		buffer.putShort((short)partition.topicName.length);
		
		// Topic (byte[])
		buffer.put(partition.topicName);
		
		// Partition (int32)
		buffer.putInt(partition.partition);
		
		// Time (int64)
		buffer.putLong(time);
		
		// Max number of offsets (int32);
		buffer.putInt(maxOffsets);

		size = buffer.position() - requestSizePosition - KafkaAsyncProcessor.SIZEOF_INT32;
		buffer.putInt(requestSizePosition,size);
	}
	
	@Override
	public void writeComplete() {
	}
	
	public Future<List<Long>> getResult() {
		return future;
	}
	
	@Override
	public void responseFailed(Exception reason) {
		future.completeWithException(reason);
	}
	
	@Override
	public void requestFailed(Exception reason) {
		future.completeWithException(reason);
	}
	
	@Override
	public void brokerFailed(Exception reason) {
		future.completeWithException(reason);
	}

	@Override
	public KafkaBrokerIdentity getTargetBroker() {
		return partition.broker;
	}
}