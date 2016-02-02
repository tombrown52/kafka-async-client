package kafka.async;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import kafka.async.futures.Wakeable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaAsyncProcessor implements Wakeable {

	public final static int SIZEOF_INT64 = 8;
	public final static int SIZEOF_INT32 = 4;
	public final static int SIZEOF_INT16 = 2;
	public final static int SIZEOF_INT8 = 1;

	private Object lock = new Object();
	private AtomicBoolean open = new AtomicBoolean(false);
	private Selector selector;
	
	static Logger logger = LoggerFactory.getLogger(KafkaAsyncProcessor.class); 
	
	private ConnectionManager[] managers = new ConnectionManager[0];
	private int managersRev = 0;
	private List<ConnectionManager> allManagers = new ArrayList<ConnectionManager>();
	private AtomicInteger allManagersRev = new AtomicInteger(0);
	
	public KafkaAsyncProcessor() {
	}
	
	@Override
	public void wakeup() {
		selector.wakeup();
	}
	
	public void close() {
		open.set(false);
		synchronized (lock) {
			selector.wakeup();
		}
	}
	
	public boolean isOpen() {
		return open.get();
	}
	
	public void addConnectionManager(ConnectionManager manager) {
		synchronized (lock) {
			allManagers.add(manager);
			allManagersRev.incrementAndGet();
			selector.wakeup();
		}
	}
	
	public void open() throws IOException {
		synchronized (lock) {
			if (open.get()) {
				throw new IllegalStateException("Cannot open client twice");
			}
			open.set(true);
			selector = Selector.open();
		}
		
		new Thread(new Runnable() {
			public void processSelectionKey(SelectionKey key) {
				SocketChannel socket = (SocketChannel)key.channel();
				ChannelContext context = (ChannelContext)key.attachment();

				if (!key.isValid()) {
					logger.trace("Key is no longer valid");
				} else {
					if (logger.isTraceEnabled()) {
						logger.trace("Key interest ops: "+opString(key.interestOps())+" ready ops: "+opString(key.readyOps()));
					}
					try {
						if (key.isConnectable()) {
							logger.trace("Socket is ready to connect");
							if (socket.finishConnect()) {
								logger.trace("Socket finished connect");
								context.doConnect();
							}
						}
						
						if (key.isReadable()) {
							logger.trace("Socket is ready to read");
							context.doRead(socket);
						}
						
						if (key.isWritable()) {
							logger.trace("Socket is ready to write");
							context.doWrite(socket);
						}
					} catch (IOException e) {
						logger.warn("Error while processing socket. (Cancelling selection key as a result)",e);
						context.closedWithException(e);
					}
				}
			}
			
			public void processPendingConnections() {
				if (allManagersRev.get() != managersRev) {
					synchronized (lock) {
						int newIndex = managers.length;
						managers = allManagers.toArray(new ConnectionManager[allManagers.size()]);
						managersRev = allManagersRev.get();
						
						for (; newIndex < managers.length; ++newIndex) {
							managers[newIndex].attach(KafkaAsyncProcessor.this);
						}
					}
				}
				
				for (ConnectionManager manager : managers) {
					List<ChannelContext> connections = manager.takeNewConnections();
					if (connections != null) {
						for (ChannelContext connection : connections) {
							KafkaBrokerIdentity broker = connection.broker();
							if (logger.isDebugEnabled()) {
								logger.debug("Adding connection to "+broker);
							}
							SocketAddress address = new InetSocketAddress(broker.host, broker.port);
							try {
								connection.initAndRegister(address, selector);
							} catch (IOException e) {
								connection.closedWithException(e);
							}
							
						}
					}
				}
			}
			
			public int select() throws IOException {
				return selector.select(100);
			}
			
			@Override
			public void run() {
				
				IOException shutdownReason = null;

				long lastPendingCheck = System.nanoTime();
				final long RECONNECT_TIME_NANOS = TimeUnit.NANOSECONDS.convert(250, TimeUnit.MILLISECONDS);
				
				try {
					while (open.get()) {
						boolean isTraceEnabled = logger.isTraceEnabled();
						
						
						long elapsed = System.nanoTime() - lastPendingCheck;
						// Only check for new connections every 250ms
						// (This prevents a bad connection from spamming reconnect attempts)
						if (elapsed > RECONNECT_TIME_NANOS) {
							processPendingConnections();
							lastPendingCheck = System.nanoTime();
						}
						
						logger.trace("Beginning select");
						int changed = 0;
						changed = select();
	
						Set<SelectionKey> selectedKeys = selector.selectedKeys();
						if (isTraceEnabled) {
							logger.trace("Select complete. Found "+changed+" changed, and "+selectedKeys.size()+" keys needing attention");
						}
						
						for (SelectionKey key : selectedKeys) {
							processSelectionKey(key);
						}
						selectedKeys.clear();
					}
				} catch(Exception e) {
					logger.warn("Error while processing selector",e);
					synchronized (lock) {
						open.set(false);
					}
				} finally {
					logger.debug("Shutting down processor and closing selector");
					if (shutdownReason == null) {
						 shutdownReason = new IOException("Processor closed");
					}
					for (SelectionKey key : selector.keys()) {
						ChannelContext context = (ChannelContext)key.attachment();
						context.closedWithException(shutdownReason);
					}
					
					try {
						selector.close();
					} catch (IOException e) {
						logger.warn("Error while closing selector",e);
					}
					
					for (ConnectionManager manager : managers) {
						manager.detach(KafkaAsyncProcessor.this);
					}
				}
				
//				System.out.println("End of processing loop. Loop executions with/without something to do: "+loopsWithAction+"/"+loopsWithoutAction);
			}
		}).start();
	}
	
	static String opString(int ops) {
		StringBuilder buffer = new StringBuilder();
		if ((ops & SelectionKey.OP_ACCEPT) == SelectionKey.OP_ACCEPT) {
			buffer.append(",OP_ACCEPT");
		}
		if ((ops & SelectionKey.OP_CONNECT) == SelectionKey.OP_CONNECT) {
			buffer.append(",OP_CONNECT");
		}
		if ((ops & SelectionKey.OP_READ) == SelectionKey.OP_READ) {
			buffer.append(",OP_READ");
		}
		if ((ops & SelectionKey.OP_WRITE) == SelectionKey.OP_WRITE) {
			buffer.append(",OP_WRITE");
		}
		if (buffer.length() == 0) {
			buffer.append("no_ops");
		} else {
			buffer.replace(0, 1, "[");
			buffer.append("]");
		}
		return buffer.toString();
	}
}
