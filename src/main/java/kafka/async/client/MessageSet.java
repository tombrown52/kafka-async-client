package kafka.async.client;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class MessageSet implements Iterable<Message> {

	private final boolean isNested;
	public final long offset;
	public final long nextOffset;
	public final int startOffset;
	private final ByteBuffer sourceBuffer;
	
	public static MessageSet createMessageSet(long offset, int startOffset, ByteBuffer buffer) {
		return new MessageSet(false, offset, -1, startOffset, buffer);
	}
	
	public static MessageSet createNestedMessageSet(long offset, long nextOffset, int startOffset, ByteBuffer buffer) {
		return new MessageSet(true, offset, nextOffset, startOffset, buffer);
	}
	
	/**
	 * Creates a message set backed by the specified byte buffer. The message set starts at
	 * <i>startOffset</i> and continues until the end of the buffer. A partial message is
	 * allowed at the end of the buffer, and will not be returned via iteration.
	 * @param isNested Whether or not this message set is nested within another message set.
	 * @param offset The offset of the beginning of this message set within the partition.
	 * If this message set is nested, this should be the offset of the containing message.
	 * @param size The size in bytes of this message set. If this message set is nested,
	 * this should be the size in bytes of the containing message.
	 * @param startOffset The offset within the sourceBuffer where the messages start.
	 * @param sourceBuffer
	 */
	private MessageSet(boolean isNested, long offset, long nextOffset, int startOffset, ByteBuffer sourceBuffer) {
		this.isNested = isNested;
		this.offset = offset;
		this.nextOffset = nextOffset;
		this.startOffset = startOffset;
		this.sourceBuffer = sourceBuffer;
	}
	
	public Iterator<Message> iterator(final boolean deep) {
		final ByteBuffer copy = sourceBuffer.slice();
		
		return new Iterator<Message>() {
			int nextStartOffset = startOffset;
			Iterator<Message> childIterator;
			
			@Override
			public boolean hasNext() {
				if (childIterator != null) {
					if (childIterator.hasNext()) {
						return true;
					} else {
						childIterator = null;
					}
				}
				
				return Message.hasCompleteMessage(copy);
			}
			
			@Override
			public Message next() {
				if (childIterator != null) {
					return childIterator.next();
				}
				
				if (!Message.hasCompleteMessage(copy)) {
					throw new NoSuchElementException("No more messages");
				}
				Message result;
				if (isNested) {
					result = Message.createNestedMessage(offset, nextOffset, nextStartOffset, copy);
				} else {
					result = Message.createMessage(offset+nextStartOffset, nextStartOffset, copy);
				}
				nextStartOffset = result.endOffset;
				if (deep && result.isMessageSet()) {
					childIterator = result.asMessageSet().iterator(true);
					return childIterator.next();
				}
				return result;
			}
			
			@Override
			public void remove() {
				throw new UnsupportedOperationException("Cannot remove messages");
			}
		};
	}
	
	/**
	 * Returns an iterator that will iterate through all nested messages. By default
	 * nested messages will all have the same offset as the parent message. 
	 */
	@Override
	public Iterator<Message> iterator() {
		return iterator(true);
	}
	
}
