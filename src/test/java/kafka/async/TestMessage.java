package kafka.async;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import kafka.async.client.Message;
import kafka.async.client.Message.ByteBufferBackedMessageOutputStream;
import kafka.async.client.Message.MessageStream;
import kafka.async.client.MessageSet;

import org.junit.Test;

public class TestMessage {

	public static Charset ASCII = Charset.forName("ASCII");
	
	@Test
	public void testMessageContents() throws Exception {
		ByteBuffer buffer = ByteBuffer.allocate(1024*1024);
		List<byte[]> messages = Arrays.asList(new byte[][] {
			"abcdefg".getBytes(ASCII),	
			"2abcdefg".getBytes(ASCII),
			"3abcdefg".getBytes(ASCII),
		});
		writeMessagesToBuffer(messages,buffer);

		buffer.flip();
		MessageSet messageSet = MessageSet.createMessageSet(0, 0, buffer);
		
		Iterator<Message> i = messageSet.iterator();
		Message message;
		
		assertTrue(i.hasNext());
		message = i.next();
		assertEquals("abcdefg",byteBufferToString(message.getUncompressedContents(), ASCII));
		
		assertTrue(i.hasNext());
		message = i.next();
		assertEquals("2abcdefg",byteBufferToString(message.getUncompressedContents(), ASCII));

		assertTrue(i.hasNext());
		message = i.next();
		assertEquals("3abcdefg",byteBufferToString(message.getUncompressedContents(), ASCII));
		
		assertFalse(i.hasNext());
	}
	
	@Test
	public void testMessageOffsetsContents() throws Exception {
		ByteBuffer buffer = ByteBuffer.allocate(1024*1024);
		List<byte[]> messages = Arrays.asList(new byte[][] {
			"abcdefg".getBytes(ASCII),	
			"2abcdefg".getBytes(ASCII),
			"3abcdefg".getBytes(ASCII),
		});
		writeMessagesToBuffer(messages,buffer);
		writeCompoundMessageToBufferGzip(messages,buffer);

		buffer.flip();
		MessageSet messageSet = MessageSet.createMessageSet(10245, 0, buffer);
		
		Iterator<Message> i = messageSet.iterator();
		Message message;
		
		assertTrue(i.hasNext());
		message = i.next();
		assertEquals("abcdefg",byteBufferToString(message.getUncompressedContents(), ASCII));
		assertEquals(10245,message.offset);
		assertEquals(10262,message.nextOffset);
		
		assertTrue(i.hasNext());
		message = i.next();
		assertEquals("2abcdefg",byteBufferToString(message.getUncompressedContents(), ASCII));
		assertEquals(10262,message.offset);
		assertEquals(10280,message.nextOffset);

		assertTrue(i.hasNext());
		message = i.next();
		assertEquals("3abcdefg",byteBufferToString(message.getUncompressedContents(), ASCII));
		assertEquals(10280,message.offset);
		assertEquals(10298,message.nextOffset);

		assertTrue(i.hasNext());
		message = i.next();
		assertEquals("abcdefg",byteBufferToString(message.getUncompressedContents(), ASCII));
		assertEquals(10298,message.offset);
		assertEquals(10365,message.nextOffset);
		assertEquals(0,message.startOffset);

		assertTrue(i.hasNext());
		message = i.next();
		assertEquals("2abcdefg",byteBufferToString(message.getUncompressedContents(), ASCII));
		assertEquals(10298,message.offset);
		assertEquals(10365,message.nextOffset);
		assertEquals(17,message.startOffset);

		assertTrue(i.hasNext());
		message = i.next();
		assertEquals("3abcdefg",byteBufferToString(message.getUncompressedContents(), ASCII));
		assertEquals(10298,message.offset);
		assertEquals(10365,message.nextOffset);
		assertEquals(35,message.startOffset);
		
		assertFalse(i.hasNext());
	}
	
	@Test
	public void testGzipMessageContents() throws Exception {
		ByteBuffer buffer = ByteBuffer.allocate(1024*1024);
		List<byte[]> messages = Arrays.asList(new byte[][] {
			"abcdefg".getBytes(ASCII),	
			"2abcdefg".getBytes(ASCII),
			"3abcdefg".getBytes(ASCII),
		});
		writeCompoundMessageToBufferGzip(messages,buffer);
		buffer.flip();
		
		MessageSet messageSet = MessageSet.createMessageSet(0, 0, buffer);
		Iterator<Message> i = messageSet.iterator();
		Message message;
		
		assertTrue(i.hasNext());
		message = i.next();
		assertEquals("abcdefg",byteBufferToString(message.getUncompressedContents(), ASCII));
		
		assertTrue(i.hasNext());
		message = i.next();
		assertEquals("2abcdefg",byteBufferToString(message.getUncompressedContents(), ASCII));

		assertTrue(i.hasNext());
		message = i.next();
		assertEquals("3abcdefg",byteBufferToString(message.getUncompressedContents(), ASCII));
		
		assertFalse(i.hasNext());
	}


	@Test
	public void testShallowIterator() throws Exception {
		ByteBuffer buffer = ByteBuffer.allocate(1024*1024);
		List<byte[]> messages = Arrays.asList(new byte[][] {
			"abcdefg".getBytes(ASCII),	
			"2abcdefg".getBytes(ASCII),
			"3abcdefg".getBytes(ASCII),
		});
		writeCompoundMessageToBufferGzip(messages,buffer);
		writeCompoundMessageToBufferGzip(messages,buffer);
		buffer.flip();
		
		MessageSet messageSet = MessageSet.createMessageSet(0, 0, buffer);
		Iterator<Message> shallow = messageSet.iterator(false);
		
		assertTrue(shallow.hasNext());
		assertTrue(shallow.next().isMessageSet());
		assertTrue(shallow.hasNext());
		assertTrue(shallow.next().isMessageSet());
		assertFalse(shallow.hasNext());
	}
	
	@Test
	public void testDeepIterator() throws Exception {
		ByteBuffer buffer = ByteBuffer.allocate(1024*1024);
		List<byte[]> messages = Arrays.asList(new byte[][] {
			"abcdefg".getBytes(ASCII),	
			"2abcdefg".getBytes(ASCII),
			"3abcdefg".getBytes(ASCII),
		});
		writeCompoundMessageToBufferGzip(messages,buffer);
		long secondStartOffset = buffer.position();
		writeCompoundMessageToBufferGzip(messages,buffer);
		long finalOffset = buffer.position();
		buffer.flip();
		
		Message message;
		MessageSet messageSet = MessageSet.createMessageSet(0, 0, buffer);
		Iterator<Message> deep = messageSet.iterator();
		
		assertTrue(deep.hasNext());
		message = deep.next();
		assertFalse(message.isMessageSet());
		assertEquals("abcdefg",byteBufferToString(message.getUncompressedContents(), ASCII));
		assertEquals(0,message.startOffset);
		assertEquals(0,message.offset);
		assertEquals(secondStartOffset,message.nextOffset);

		assertTrue(deep.hasNext());
		message = deep.next();
		assertFalse(message.isMessageSet());
		assertEquals("2abcdefg",byteBufferToString(message.getUncompressedContents(), ASCII));
		assertEquals(17,message.startOffset);
		assertEquals(0,message.offset);
		assertEquals(secondStartOffset,message.nextOffset);

		assertTrue(deep.hasNext());
		message = deep.next();
		assertFalse(message.isMessageSet());
		assertEquals("3abcdefg",byteBufferToString(message.getUncompressedContents(), ASCII));
		assertEquals(35,message.startOffset);
		assertEquals(0,message.offset);
		assertEquals(secondStartOffset,message.nextOffset);

		assertTrue(deep.hasNext());
		message = deep.next();
		assertFalse(message.isMessageSet());
		assertEquals("abcdefg",byteBufferToString(message.getUncompressedContents(), ASCII));
		assertEquals(0,message.startOffset);
		assertEquals(secondStartOffset,message.offset);
		assertEquals(finalOffset,message.nextOffset);

		assertTrue(deep.hasNext());
		message = deep.next();
		assertFalse(message.isMessageSet());
		assertEquals("2abcdefg",byteBufferToString(message.getUncompressedContents(), ASCII));
		assertEquals(17,message.startOffset);
		assertEquals(secondStartOffset,message.offset);
		assertEquals(finalOffset,message.nextOffset);

		assertTrue(deep.hasNext());
		message = deep.next();
		assertFalse(message.isMessageSet());
		assertEquals("3abcdefg",byteBufferToString(message.getUncompressedContents(), ASCII));
		assertEquals(35,message.startOffset);
		assertEquals(secondStartOffset,message.offset);
		assertEquals(finalOffset,message.nextOffset);

		assertFalse(deep.hasNext());
	}
	
	
	public static void writeCompoundMessageToBufferGzip(List<byte[]> messages, ByteBuffer buffer) throws IOException {
		
		ByteBufferBackedMessageOutputStream wrapper = new ByteBufferBackedMessageOutputStream(buffer);
		wrapper.startMessage(Message.COMPRESSION_GZIP);
		
		GZIPOutputStream stream = new GZIPOutputStream(wrapper);
		MessageStream out = new MessageStream(stream);
		for (byte[] message : messages) {
			out.writeMessage(Message.COMPRESSION_NONE, message);
		}
		out.close();
		
		wrapper.finishMessage();
	}
	
	public static void writeMessagesToBuffer(List<byte[]> messages, ByteBuffer buffer) {
		ByteBufferBackedMessageOutputStream out = new ByteBufferBackedMessageOutputStream(buffer);
		for (byte[] message : messages) {
			out.startMessage(0);
			out.write(message);
			out.finishMessage();
		}
		out.close();
	}
	
	public static String byteBufferToString(ByteBuffer buffer,Charset charset) {
		byte[] bytes = new byte[buffer.remaining()];
		int size = byteBufferToArray(buffer,bytes);
		return new String(bytes,0,size,charset);
	}
	
	public static int byteBufferToArray(ByteBuffer buffer, byte[] array) {
		int len = buffer.remaining();
		buffer.get(array,0,buffer.remaining());
		return len;
	}
}
