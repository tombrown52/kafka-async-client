package kafka.async.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import kafka.async.BrokerPool;
import kafka.async.ChannelContext;
import kafka.async.ConnectionManager;
import kafka.async.KafkaAsyncProcessor;
import kafka.async.KafkaBrokerIdentity;
import kafka.async.KafkaChannelContext;
import kafka.async.KafkaOperation;
import kafka.async.KafkaPartitionIdentity;
import kafka.async.futures.Wakeable;
import kafka.async.ops.FetchRequest;
import kafka.async.ops.OffsetsRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaAsyncClient implements BrokerPool, ConnectionManager {

	static Logger logger = LoggerFactory.getLogger(KafkaAsyncClient.class);

	private KafkaAsyncProcessor processor;
	private boolean manageProcessor;
	
	private int connectionsPerHost = 1;
	private int maxResponseSize = 1024*1024 + 1024;  // 1K header + 1MB data
	private int maxRequestSize = 1024*1024 + 1024;   // 1k header + 1MB data
	
	private HashMap<KafkaBrokerIdentity,BrokerState> brokers = new HashMap<KafkaBrokerIdentity,BrokerState>();
	private HashSet<Wakeable> selectors = new HashSet<Wakeable>();
	
	private final AtomicInteger pendingConnectionsRev = new AtomicInteger(0);
	private int currentConnectionsRev = 0;
	private ClientConfiguration config;
	
	private static class BrokerState {
		private BrokerState() {}
		private LinkedList<KafkaOperation> operationQueue = new LinkedList<KafkaOperation>();
		private LinkedList<ChannelContext> openConnections = new LinkedList<ChannelContext>();
		private LinkedList<ChannelContext> idleConnections = new LinkedList<ChannelContext>();
		private LinkedList<ChannelContext> closingConnections = new LinkedList<ChannelContext>();
		private int pendingCount = 0;
		private int connectionCount = 0;
		private int targetCount = 0;
	}
	
	public KafkaAsyncClient(ClientConfiguration config) {
		this.config = config;
	}

	public int getConnectionsPerHost() {
		return connectionsPerHost;
	}
	
	public void setConnectionsPerHost(int connectionsPerHost) {
		if (connectionsPerHost < 1) {
			throw new IllegalArgumentException("Connections per host must be an integer greater than 0");
		}
		this.connectionsPerHost = connectionsPerHost;
	}
	
	public int getMaxRequestSize() {
		return maxRequestSize;
	}
	
	public void setMaxRequestSize(int maxRequestSize) {
		this.maxRequestSize = maxRequestSize;
	}
	
	public int getMaxResponseSize() {
		return maxResponseSize;
	}
	
	public void setMaxResponseSize(int maxResponseSize) {
		this.maxResponseSize = maxResponseSize;
	}
	
	public void open() throws IOException {
		if (processor == null) {
			processor = new KafkaAsyncProcessor();
			manageProcessor = true;
			processor.open();
		} else {
			if (!processor.isOpen()) {
				throw new IllegalStateException("Client attempted to open. Processor is not managed by client, but is not open");
			}
		}
		
		config.addBrokerPool(this);
		processor.addConnectionManager(this);
	}

	public void close() {
		config.removeBrokerPool(this);
		synchronized (this) {
			Set<KafkaBrokerIdentity> brokersToRemove = new HashSet<KafkaBrokerIdentity>(brokers.keySet());
			for (KafkaBrokerIdentity broker : brokersToRemove) {
				removeBroker(broker);
			}
		}
		// TODO: Implement this
		// processor.removeChannelManager(this);
		if (manageProcessor) {
			processor.close();
		}
	}

	@Override
	public synchronized void addBroker(KafkaBrokerIdentity broker) {
		logger.info("Adding kafka broker "+broker+" to the client");
		BrokerState state = brokers.get(broker);
		if (state == null) {
			brokers.put(broker, state = new BrokerState()); 
		}
		
		state.targetCount = connectionsPerHost;
		state.pendingCount = state.targetCount - state.connectionCount;
		if (state.pendingCount > 0) {
			pendingConnectionsRev.incrementAndGet();
		}
		processor.wakeup();
	}
	
	@Override
	public synchronized void removeBroker(KafkaBrokerIdentity broker) {
		logger.info("Removing kafka broker "+broker+" to the client");
		BrokerState state = brokers.get(broker);
		state.targetCount = 0;
		state.connectionCount -= state.pendingCount;
		for (ChannelContext connection : state.openConnections) {
			connection.close();
		}
		processor.wakeup();
	}
	
	/**
	 * <b>Note:</b> Must be called within a synchronized section for this broker
	 * @param broker
	 */
	
	@Override
	public void attach(Wakeable selector) {
		selectors.add(selector);
	}
	
	@Override
	public void detach(Wakeable selector) {
		selectors.remove(selector);
	}
	
	@Override
	public List<ChannelContext> takeNewConnections() {
		if (currentConnectionsRev == pendingConnectionsRev.get()) {
			return null;
		}
		ArrayList<ChannelContext> newConnections = new ArrayList<ChannelContext>();
		synchronized (this) {
			for (Entry<KafkaBrokerIdentity,BrokerState> entry : brokers.entrySet()) {
				KafkaBrokerIdentity broker = entry.getKey();
				BrokerState state = entry.getValue();
				
				for (int i=0; i<state.pendingCount; ++i) {
					ChannelContext connection = new KafkaChannelContext(broker, this, maxRequestSize, maxResponseSize);
					newConnections.add(connection);
					state.openConnections.add(connection);
				}
				state.pendingCount = 0;
			}
			
			currentConnectionsRev = pendingConnectionsRev.get();
		}
		return newConnections;
	}
	
	@Override
	public synchronized KafkaOperation getNextOperationFor(ChannelContext connection) {
		if (logger.isTraceEnabled()) {
			logger.trace("Connection "+connection+" to "+connection.broker()+" requested next operation");
		}
		KafkaBrokerIdentity broker = connection.broker();
		BrokerState state = brokers.get(broker);
		
		while (!state.operationQueue.isEmpty()) {
			if (logger.isTraceEnabled()) {
				logger.trace("Next operation from queue: removing 1 of "+state.operationQueue.size()+" and returning");
			}
			KafkaOperation op = state.operationQueue.removeFirst();
			if (op.start()) {
				return op;
			}
		}

		if (!state.idleConnections.contains(connection)) {
			if (logger.isTraceEnabled()) {
				logger.trace("No operations in queue. Adding connection to idle list (size is 1 + "+state.idleConnections.size()+")");
			}
			state.idleConnections.push(connection);
		}
		return null;
	}
	
	@Override
	public synchronized void connectionClosed(ChannelContext connection, Exception reason) {
		if (logger.isTraceEnabled()) {
			logger.trace("Connection "+connection+" to "+connection.broker()+" is closed");
		}
		KafkaBrokerIdentity broker = connection.broker();
		BrokerState state = brokers.get(broker);

		state.idleConnections.remove(connection);
		state.openConnections.remove(connection);
		state.closingConnections.remove(connection);

		state.connectionCount--;
		
		if (state.connectionCount < state.targetCount) {
			state.pendingCount++;
			state.connectionCount++;
			pendingConnectionsRev.getAndIncrement();
		} else if (state.connectionCount == 0) {
			// There are no other connections to this host, so fail all pending operations
			int i = 0;
			for (KafkaOperation op : state.operationQueue) {
				if (logger.isTraceEnabled()) {
					logger.trace("Aborting operation from queue: "+(++i)+" of "+state.operationQueue.size()+"");
				}
				op.requestFailed(reason);
			}
			state.operationQueue.clear();
		}
	}
	
	public synchronized void execute(KafkaOperation op) {
		KafkaBrokerIdentity broker = op.getTargetBroker();
		if (logger.isTraceEnabled()) {
			logger.trace("Execution requested for operation on "+broker+"");
		}
		
		BrokerState state = brokers.get(broker);

		if (state == null) {
			throw new NullPointerException("State for broker "+broker+" was not found");
		}
		if (logger.isTraceEnabled()) {
			logger.trace("Adding operation to queue (size is 1 + "+state.operationQueue.size()+"). Connections: target="+state.targetCount+", open="+state.connectionCount+", pending="+state.pendingCount+", idle="+state.idleConnections.size());
		}
		state.operationQueue.push(op);
		
		Iterator<ChannelContext> idleForThisBroker = state.idleConnections.iterator();
		int idleCount = state.idleConnections.size();
		int i = 0;
		while (idleForThisBroker.hasNext()) {
			ChannelContext connection = idleForThisBroker.next();
			if (logger.isTraceEnabled()) {
				logger.trace("Waking up connection "+connection+" to "+broker+" ("+(++i)+" of "+idleCount+")");
			}
			connection.wakeup();
			idleForThisBroker.remove();
		}
	}
	
	public Future<List<Long>> requestOffsets(KafkaPartitionIdentity partition, long time, int maxOffsets) {
		OffsetsRequest request = new OffsetsRequest(partition, time, maxOffsets);
		execute(request);
		return request.getResult();
	}
	
	public Future<MessageSet> fetch(KafkaPartitionIdentity partition, long offset, int maxSize) {
		if (maxSize > maxResponseSize) {
			throw new IllegalArgumentException("Requested max response size of "+maxSize+". Maximum possible size is "+maxResponseSize);
		}
		FetchRequest fetch = new FetchRequest(partition, offset, maxSize);
		execute(fetch);
		return fetch.getResult();
	}
	
}
