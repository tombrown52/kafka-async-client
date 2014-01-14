package kafka.async.client;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import kafka.async.KafkaAsyncProcessor;

public class Message {

	/*
	 * Message size: int32
	 * Magic:        int8
	 * Compression:  int8
	 * CRC32:        int32
	 * Message:      byte[] 
	 */
	static final int SIZE_POS = 0;
	static final int MAGIC_POS = SIZE_POS + 4;
	static final int COMPRESSION_POS = MAGIC_POS + 1;
	static final int CHECKSUM_POS = COMPRESSION_POS + 1;
	static final int MESSAGE_POS = CHECKSUM_POS + 4;
	
	public final long startOffset;
	public final long endOffset;

	public final ByteBuffer contents;
	public final int magic;
	public final int compression;
	public final int checksum;

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
	
	/**
	 * Creates a message and buffer slice from the source buffer. Assumes the
	 * position of the source buffer is the start of the message (the SIZE int32)
	 * and populates the rest of the message from that.
	 * 
	 * @param startOffset
	 * @param sourceBuffer
	 */
	Message(long startOffset, ByteBuffer sourceBuffer) {
		int length = sourceBuffer.getInt();
		this.startOffset = startOffset;
		this.endOffset = startOffset + KafkaAsyncProcessor.SIZEOF_INT32 + length;

		int headerPosition = sourceBuffer.position();
		this.magic = sourceBuffer.get();
		if (magic == 1) {
			this.compression = sourceBuffer.get();
		} else {
			this.compression = 0;
		}
		this.checksum = sourceBuffer.getInt();
		int headerSize = sourceBuffer.position() - headerPosition;
		
		this.contents = sourceBuffer.slice();
		this.contents.limit(length - headerSize);
		
		sourceBuffer.position(headerPosition + length);
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
		if (compression == 0) {
			return contents;
		} else {
			// TODO: Handle decompression
			throw new UnsupportedOperationException("Cannot decompress messages at this time :(");
		}
	}
	
	public MessageSet asMessageSet() {
		return new MessageSet(0,contents);
	}
}
