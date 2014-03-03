package kafka.async.ops;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import kafka.async.KafkaAsyncProcessor;
import kafka.async.KafkaBrokerIdentity;
import kafka.async.KafkaOperation;
import kafka.async.KafkaPartitionIdentity;
import kafka.async.client.Message;
import kafka.async.client.Message.MessageStream;
import kafka.async.futures.SelectableFuture;
import kafka.async.futures.ValueFuture;


public class ProduceRequest implements KafkaOperation {
	final KafkaPartitionIdentity partition;
	final List<byte[]> messages;
	final int compression;
	final boolean compress;
	final ValueFuture<Boolean> result;

	public ProduceRequest(KafkaPartitionIdentity partition, List<byte[]> messages) {
		this(partition, Message.COMPRESSION_NONE, false, messages);
	}
	
	public ProduceRequest(KafkaPartitionIdentity partition, int compression, boolean compress, List<byte[]> messages) {
		this.partition = partition;
		this.messages = messages;
		this.compression = compression;
		this.compress = compress;
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
		buffer.putShort((short)partition.topicName.length);
		
		// Topic (byte[])
		buffer.put(partition.topicName);
		
		// Partition (int32)
		buffer.putInt(partition.partition);
		
		// Message-section size (int32) placeholder
		int messageSectionSizePosition = buffer.position();
		buffer.putInt(0);
		
		Message.ByteBufferBackedMessageOutputStream wrapper = new Message.ByteBufferBackedMessageOutputStream(buffer);
		try {
			if (compression == Message.COMPRESSION_NONE || !compress) {
				for (byte[] message : messages) {
					wrapper.startMessage(compression);
					wrapper.write(message);
					wrapper.finishMessage();
				}
			} else if (compression == Message.COMPRESSION_GZIP) {
				try {
					wrapper.startMessage(compression);
					GZIPOutputStream stream = new GZIPOutputStream(wrapper);
					MessageStream out = new MessageStream(stream);
					for (byte[] message : messages) {
						out.writeMessage(Message.COMPRESSION_NONE, message);
					}
					out.close();
					wrapper.finishMessage();
				} catch (IOException e) {
					throw new RuntimeException("Error occurred while compressing with gzip", e);
				}
			} else if (compression == Message.COMPRESSION_SNAPPY) {
				throw new UnsupportedOperationException("Snappy compression not implemented");
			} else {
				throw new UnsupportedOperationException("Unknown compression specified: "+compression);
			}
		} finally {
			wrapper.close();
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
		return partition.broker;
	}
	
}