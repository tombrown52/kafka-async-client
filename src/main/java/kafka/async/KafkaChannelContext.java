package kafka.async;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.async.futures.ValueFuture;

public class KafkaChannelContext implements ChannelContext {
	
	static Logger logger = LoggerFactory.getLogger(KafkaChannelContext.class);

	private SelectionKey selectionKey;
	private AtomicInteger interestOps = new AtomicInteger();
	
	private ByteBuffer readBuffer;
	private ByteBuffer writeBuffer;
	
	KafkaOperation currentWriteOp;
	private LinkedList<KafkaOperation> readQueue = new LinkedList<KafkaOperation>();
	
	private SocketChannel socket;
	
	private ValueFuture<Boolean> connected;
	
	public final KafkaBrokerIdentity brokerIdentity;
	private final ConnectionManager connectionManager;
	
	public KafkaChannelContext(KafkaBrokerIdentity brokerIdentity, ConnectionManager connectionManager, int maxRequestSize, int maxResponseSize) {
		this.brokerIdentity = brokerIdentity;
		this.connectionManager = connectionManager;
		
		writeBuffer = ByteBuffer.allocate(maxRequestSize);
		writeBuffer.flip();
		readBuffer = ByteBuffer.allocate(maxResponseSize);
		
		connected = new ValueFuture<Boolean>();
	}

	@Override
	public KafkaBrokerIdentity broker() {
		return brokerIdentity;
	}
	
	@Override
	public ConnectionManager manager() {
		return connectionManager;
	}
	
	@Override
	public void initAndRegister(SocketAddress address, Selector selector) throws IOException {
		try {
			socket = SocketChannel.open();
			socket.socket().setReceiveBufferSize(1024*1024*2);
			socket.socket().setSendBufferSize(1024*1024*2);
			socket.socket().setTcpNoDelay(true);
			socket.configureBlocking(false);
			socket.connect(address);
			selectionKey = socket.register(selector, SelectionKey.OP_CONNECT, this);
			addSelectionKeyInterestOps(SelectionKey.OP_CONNECT);
		} catch (UnresolvedAddressException e) {
			socket = null;
			throw new IOException(e);
		} catch (IOException e) {
			if (socket != null) {
				try { socket.close(); } catch(IOException ignoreThis) {}
				socket = null;
			}
			throw e;
		}
	}
	
	public boolean waitForConnection() throws InterruptedException {
		try {
			return connected.get();
		} catch (ExecutionException e) {
			throw new RuntimeException("Unable to connect", e.getCause());
		}
	}
	
	public boolean waitForConnection(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
		try {
			return connected.get(timeout,unit);
		} catch (ExecutionException e) {
			throw new RuntimeException("Unable to connect", e.getCause());
		}
	}

	/**
	 * Removes a set of ops from this selection key's interestOps set. Thread safety
	 * is guaranteed by using AtomicInteger CAS
	 * @param opsToRemove
	 */
	private void removeSelectionKeyInterestOps(int opsToRemove) {
		int currentValue;
		int newValue;
		do {
			currentValue = interestOps.get();
			newValue = currentValue & ~opsToRemove;
		} while (!interestOps.compareAndSet(currentValue, newValue));
		selectionKey.interestOps(interestOps.get());
	}
	
	/**
	 * Adds a set of ops to this selection key's interestOps set. Thread safety is
	 * guaranteed by using AtomicInteger CAS
	 * @param opsToAdd
	 */
	private void addSelectionKeyInterestOps(int opsToAdd) {
		int currentValue;
		int newValue;
		do {
			currentValue = interestOps.get();
			newValue = currentValue | opsToAdd;
		} while (!interestOps.compareAndSet(currentValue, newValue));
		selectionKey.interestOps(interestOps.get());
	}
	
	/**
	 * Causes this channel context to wake-up so it can find the next operation.<p>
	 */
	@Override
	public void wakeup() {
		logger.trace("Waking up connection (Adding OP_READ and OP_WRITE to interest ops)");
		if (selectionKey != null) {
			addSelectionKeyInterestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
			selectionKey.selector().wakeup();
		}
	}
	
	@Override
	public void doConnect() {
		connected.completeWithValue(true);
		logger.trace("Connection complete. Queuing first operation");
		removeSelectionKeyInterestOps(SelectionKey.OP_CONNECT);
		addSelectionKeyInterestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
	}
	
	@Override
	public void doRead(SocketChannel channel) throws IOException {
		int bytes = channel.read(readBuffer);
		if (bytes == -1) {
			throw new IOException("Channel is closed");
		}
		if (logger.isTraceEnabled()) {
			String opType = readQueue.isEmpty() ? "NOOP" : readQueue.getFirst().operationId();
			logger.trace("Read "+bytes+" new bytes for "+readBuffer.position()+" total response bytes for "+opType);
		}
		while (bytes > 0) {
			bytes = channel.read(readBuffer);
			if (bytes == -1) {
				throw new IOException("Channel is closed");
			}
			if (logger.isTraceEnabled()) {
				String opType = readQueue.isEmpty() ? "NOOP" : readQueue.getFirst().operationId();
				logger.trace("Read "+bytes+" new bytes for "+readBuffer.position()+" total response bytes for "+opType);
			}
		}

		while (!readQueue.isEmpty()) {
			if (readBuffer.position() >= KafkaAsyncProcessor.SIZEOF_INT32) {
				int totalResponseSize = readBuffer.getInt(0) + KafkaAsyncProcessor.SIZEOF_INT32;
				if (totalResponseSize > readBuffer.capacity()) {
					throw new IOException("Response is "+totalResponseSize+" bytes. Maximum response is "+readBuffer.capacity()+" bytes");
				}
			}
			if (readQueue.getFirst().executeRead(readBuffer)) {
				if (logger.isTraceEnabled()) {
					logger.trace("Response complete, removing 1 of "+readQueue.size()+" responses");
				}
				readQueue.removeFirst();
				readBuffer.compact();
			} else {
				break;
			}
		}

		if (readQueue.isEmpty()) {
			if (readBuffer.position() > 0) {
				throw new RuntimeException("Stream is corrupted. There are "+readBuffer.position()+" bytes remaining in the read buffer, but no request has been sent");
			}
			removeSelectionKeyInterestOps(SelectionKey.OP_READ);
			logger.trace("Read queue is empty. Socket removing OP_READ from interest ops");
		}
	}
	
	@Override
	public void doWrite(SocketChannel channel) throws IOException {
		if (writeBuffer.hasRemaining()) {
			int bytes = channel.write(writeBuffer);
			if (logger.isTraceEnabled()) {
				logger.trace("Wrote "+bytes+" bytes. "+writeBuffer.remaining()+" bytes left");
			}
		}
		
		if (!writeBuffer.hasRemaining()) {
			if (currentWriteOp != null) {
				logger.trace("Write for operation is complete");
				currentWriteOp.writeComplete();
				currentWriteOp = null;
			}
			
			logger.trace("No write pending. Socket removing OP_WRITE from interest ops");
			removeSelectionKeyInterestOps(SelectionKey.OP_WRITE);
			
			KafkaOperation nextOp = connectionManager.getNextOperationFor(this);

			if (nextOp != null) {
				logger.trace("Next operation is ready. Socket adding OP_WRITE to interest ops");
				addSelectionKeyInterestOps(SelectionKey.OP_WRITE);
				currentWriteOp = nextOp;
				
				writeBuffer.clear();
				if (nextOp.canRead()) {
					readQueue.add(nextOp);
					logger.trace("Next operation requires a response. Socket adding OP_READ to interest ops");
					addSelectionKeyInterestOps(SelectionKey.OP_READ);
				}
				logger.trace("Filling buffer with next write request");
				nextOp.executeWrite(writeBuffer);
				writeBuffer.flip();
			} else {
				logger.trace("No operations waiting");
			}
		}
	}

	@Override
	public synchronized void closedWithException(Exception e) {
		if (logger.isTraceEnabled()) {
			logger.trace("Notified socket was closed", e);
		}
		Iterator<KafkaOperation> pendingReads = readQueue.iterator();
		while (pendingReads.hasNext()) {
			KafkaOperation op = pendingReads.next();
			pendingReads.remove();
			op.responseFailed(e);
		}
		if (selectionKey != null) {
			selectionKey.cancel();
			selectionKey.attach(null);
			selectionKey = null;
		}
		if (socket != null) {
			if (socket.isOpen()) {
				try {
					socket.close();
				} catch (IOException e2) {
					logger.warn("An IOException occurred while closing the socket", e2);
				}
			}
			socket = null;
		}
		connectionManager.connectionClosed(this, e);
	}
	
	@Override
	public void close() {
		if (socket != null) {
			try {
				socket.close();
			} catch(IOException e) {
				logger.warn("Unable to close socket!", e);
			}
		}
	}
}