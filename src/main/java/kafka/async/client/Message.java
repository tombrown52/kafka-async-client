package kafka.async.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;
import java.util.zip.GZIPInputStream;

import kafka.async.KafkaAsyncProcessor;

public class Message {

	public final static int COMPRESSION_NONE = 0;
	public final static int COMPRESSION_GZIP = 1;
	public final static int COMPRESSION_SNAPPY = 2;
	
	/*
	 * Message size: int32
	 * Magic:        int8
	 * Compression:  int8
	 * CRC32:        int32
	 * Message:      byte[] 
	 */
	protected static final int SIZE_POS = 0;
	protected static final int MAGIC_POS = SIZE_POS + 4;
	protected static final int COMPRESSION_POS = MAGIC_POS + 1;
	protected static final int CHECKSUM_POS = COMPRESSION_POS + 1;
	protected static final int MESSAGE_POS = CHECKSUM_POS + 4;
	
	public final long offset;
	public final long nextOffset;

	public final int startOffset;
	public final int endOffset;
	public final int magic;
	public final int compression;
	public final int checksum;
	public final ByteBuffer contents;

	/**
	 * Determines whether a complete message is contained in the buffer (assuming
	 * the position of the source buffer is the start of a message)
	 * @param sourceBuffer
	 * @return true if a complete message is contained, false otherwise
	 */
	public static boolean hasCompleteMessage(ByteBuffer sourceBuffer) {
		if (sourceBuffer.remaining() < KafkaAsyncProcessor.SIZEOF_INT32) {
			return false;
		}
		
		int length = sourceBuffer.getInt(sourceBuffer.position()) + KafkaAsyncProcessor.SIZEOF_INT32;
		if (sourceBuffer.remaining() < length) {
			return false;
		}
		
		return true;
	}
	
	public static Message createMessage(long offset, int startOffset, ByteBuffer sourceBuffer) {
		sourceBuffer.position(startOffset);
		int length = sourceBuffer.getInt();
		long nextOffset = offset + KafkaAsyncProcessor.SIZEOF_INT32 + length;
		int endOffset = startOffset + KafkaAsyncProcessor.SIZEOF_INT32 + length;

		int headerPosition = sourceBuffer.position();
		int magic = sourceBuffer.get();
		int compression;
		if (magic == 1) {
			compression = sourceBuffer.get();
		} else {
			compression = 0;
		}
		int checksum = sourceBuffer.getInt();
		int headerSize = sourceBuffer.position() - headerPosition;
		
		ByteBuffer contents = sourceBuffer.slice();
		contents.limit(length - headerSize);
		
		sourceBuffer.position(headerPosition + length);
		
		return new Message(offset,nextOffset,startOffset,endOffset,magic,compression,checksum,contents);
	}
	
	public static Message createNestedMessage(long offset, long nextOffset, int startOffset, ByteBuffer sourceBuffer) {
		sourceBuffer.position(startOffset);
		int length = sourceBuffer.getInt();
		int endOffset = startOffset + KafkaAsyncProcessor.SIZEOF_INT32 + length;

		int headerPosition = sourceBuffer.position();
		int magic = sourceBuffer.get();
		int compression;
		if (magic == 1) {
			compression = sourceBuffer.get();
		} else {
			compression = 0;
		}
		int checksum = sourceBuffer.getInt();
		int headerSize = sourceBuffer.position() - headerPosition;
		
		ByteBuffer contents = sourceBuffer.slice();
		contents.limit(length - headerSize);
		
		sourceBuffer.position(headerPosition + length);
		
		return new Message(offset,nextOffset,startOffset,endOffset,magic,compression,checksum,contents);
	}
	
	/**
	 * Creates a message and buffer slice from the source buffer. Assumes the
	 * position of the source buffer is the start of the message (the SIZE int32)
	 * and populates the rest of the message from that.
	 * 
	 * @param offset - If this message is nested within another message, this will be
	 * the offset of the containing message. Otherwise, this will be the same as
	 * startOffset.
	 * @param nextOffset - If this message is nested within another message, this will be
	 * the start of the message after this one.   
	 * @param startOffset - The offset within the sourceBuffer where the message
	 * starts.
	 * @param sourceBuffer - A buffer containing the contents of just this message.
	 */
	private Message(long offset, long nextOffset, int startOffset, int endOffset, int magic, int compression, int checksum, ByteBuffer buffer) {
		this.offset = offset;
		this.nextOffset = nextOffset;
		this.startOffset = startOffset;
		this.endOffset = endOffset;
		this.magic = magic;
		this.checksum = checksum;
		this.compression = compression;
		this.contents = buffer;
	}
	
	/**
	 * Compares the stored checksum with the computed checksum and throws an exception
	 * if the checksum is invalid.
	 * @throws RuntimeException
	 */
	public void validateChecksum() {
		CRC32 crc32 = new CRC32();
		int oldPosition = contents.position();
		contents.position(0);
		
		if (contents.hasArray()) {
			byte[] buffer = contents.array();
			int size = contents.limit();
			crc32.update(buffer,contents.arrayOffset(),size);
		} else {
			byte[] buffer = new byte[contents.limit()];
			contents.get(buffer);
			crc32.update(buffer);
		}
		
		contents.position(oldPosition);
		
		int computedChecksum = (int)(crc32.getValue() & 0xFFFFFFFFL);
		if (computedChecksum != checksum) {
			throw new RuntimeException("Stream corruption at "+startOffset+". Stored checksum was "+checksum+" but computed checksum was "+computedChecksum+".");
		}
	}
	
	public ByteBuffer getUncompressedContents() {
		if (compression == COMPRESSION_NONE) {
			return contents;
		} else if (compression == COMPRESSION_GZIP) {
			
			try {
				GZIPInputStream stream = new GZIPInputStream(new ByteBufferBackedInputStream(contents));
				ByteArrayOutputStream storage = new ByteArrayOutputStream((int)((endOffset - startOffset) * 2));
				byte[] tmp = new byte[8096];
				int cnt = stream.read(tmp);
				while (cnt >= 0) {
					storage.write(tmp,0,cnt);
					cnt = stream.read(tmp);
				}
				stream.close();
				storage.close();
				return ByteBuffer.wrap(storage.toByteArray());
				
			} catch (IOException e) {
				throw new RuntimeException("An unexpected IO exception occurred",e);
			}
			
			
		} else if (compression == COMPRESSION_SNAPPY) {
			throw new RuntimeException("Snappy compression codec not implemented");
		} else {
			throw new RuntimeException("Unknown compression codec: "+compression);
		}
	}
	
	public boolean isMessageSet() {
		return compression != 0;
	}
	
	public MessageSet asMessageSet() {
		return MessageSet.createNestedMessageSet(offset, nextOffset, 0, getUncompressedContents());
	}
	
	public static class ByteBufferBackedInputStream extends InputStream {

	    ByteBuffer buf;

	    public ByteBufferBackedInputStream(ByteBuffer buf) {
	        this.buf = buf;
	    }

	    public int read() throws IOException {
	        if (!buf.hasRemaining()) {
	            return -1;
	        }
	        return buf.get() & 0xFF;
	    }

	    public int read(byte[] bytes, int off, int len)
	            throws IOException {
	        if (!buf.hasRemaining()) {
	            return -1;
	        }

	        len = Math.min(len, buf.remaining());
	        buf.get(bytes, off, len);
	        return len;
	    }
	}

	/**
	 * A class that is useful for writing a message to a stream assuming
	 * you know the contents at once.
	 * @author tbrown
	 *
	 */
	public static class MessageStream {

	    public final static int PARTIAL_HEADER_SIZE =
	    		+ KafkaAsyncProcessor.SIZEOF_INT8   // magic
	    		+ KafkaAsyncProcessor.SIZEOF_INT8   // compression
	    		+ KafkaAsyncProcessor.SIZEOF_INT32; // checksum

		public final static int FULL_HEADER_SIZE =
				  KafkaAsyncProcessor.SIZEOF_INT32 // size
				+ PARTIAL_HEADER_SIZE;             // rest of header
		
	    
		private OutputStream stream;
		private CRC32 checksum = new CRC32();
		private ByteBuffer header = ByteBuffer.allocate(FULL_HEADER_SIZE);
		
		public MessageStream(OutputStream stream) {
			this.stream = stream;
		}
		
		public void writeMessage(int compression, byte[] payload) throws IOException {
			int size = payload.length + PARTIAL_HEADER_SIZE;
			checksum.reset();
			checksum.update(payload);
			header.position(0);
			header.putInt(size);
			header.put((byte)1);
			header.put((byte)compression);
			header.putInt((int)(checksum.getValue() & 0xFFFFFFFFL));
			stream.write(header.array());
			stream.write(payload);
		}
		
		public void close() throws IOException {
			stream.close();
		}
	}

	/**
	 * A class that is useful for writing a message to a buffer when you don't
	 * know the contents of the message up front.
	 * @author tbrown
	 */
	public static class ByteBufferBackedMessageOutputStream extends OutputStream {
	    ByteBuffer buffer;
	    int compression;
	    CRC32 checksum = new CRC32();
	    int messageSizePosition = -1;
	    int checksumPosition;
	    
	    public ByteBufferBackedMessageOutputStream(ByteBuffer buffer) {
	        this.buffer = buffer;
	    }
	    
	    public void startMessage(int compression) {
	    	if (messageSizePosition != -1) {
	    		throw new IllegalStateException("Unbalanced finishMessage and startMessage. Previous message not finished");
	    	}
	    	
	        // Individual message size (int32) placeholder
	        messageSizePosition = buffer.position();
	        buffer.putInt(0);
	        
	        // Magic number (int8) (1 = Compression byte exists)
	        buffer.put((byte)1);
	        
	        // Compression (int8) (0 = none, 1 = gzip, 2 = snappy)
	        buffer.put((byte)compression);
	        
	        // Payload checksum (int32) placeholder
	        checksumPosition = buffer.position();
	        checksum.reset();
	        buffer.putInt(0);
	    }
	    
	    public void finishMessage() {
	    	if (messageSizePosition == -1) {
	    		throw new IllegalStateException("Unbalanced finishMessage and startMessage. Cannot finish without a start");
	    	}
	    	int size = buffer.position() - messageSizePosition - KafkaAsyncProcessor.SIZEOF_INT32;
	    	buffer.putInt(messageSizePosition,size);
	    	buffer.putInt(checksumPosition,(int)(checksum.getValue() & 0xFFFFFFFFL));
	    	messageSizePosition = -1;
	    }

	    @Override
	    public void write(int b) {
	    	checksum.update(b);
	        buffer.put((byte) b);
	    }

	    @Override
	    public void write(byte[] bytes, int offset, int len) {
	    	checksum.update(bytes, offset, len);
	        buffer.put(bytes, offset, len);
	    }
	    
	    @Override
	    public void write(byte[] bytes) {
	    	checksum.update(bytes);
	    	buffer.put(bytes);
	    }

	    @Override
	    public void close() {
	    	// Do nothing
	    }
	}
}
