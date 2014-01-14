package kafka.async.client;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class MessageSet implements Iterable<Message> {

	public final long startOffset;
	private final ByteBuffer sourceBuffer;
	private final long sizeInBytes;
	
	public MessageSet(long startOffset, ByteBuffer sourceBuffer) {
		this.startOffset = startOffset;
		this.sourceBuffer = sourceBuffer;
		this.sizeInBytes = sourceBuffer.remaining();
	}
	
	public long sizeInBytes() {
		return sizeInBytes;
	}
	
	@Override
	public Iterator<Message> iterator() {
		final ByteBuffer copy = sourceBuffer.slice();
		
		return new Iterator<Message>() {
			long nextStartOffset = startOffset;
			
			@Override
			public boolean hasNext() {
				return Message.hasCompleteMessage(copy);
			}
			
			@Override
			public Message next() {
				if (!Message.hasCompleteMessage(copy)) {
					throw new NoSuchElementException("No more messages");
				}
				Message result = new Message(nextStartOffset,copy);
				nextStartOffset = result.endOffset;
				return result;
			}
			
			@Override
			public void remove() {
				throw new UnsupportedOperationException("Cannot remove messages");
			}
		};
	}
	
}
