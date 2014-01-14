package kafka.async;

import java.util.List;

import kafka.async.futures.Wakeable;

/**
 * Manages a pool of connections to a set of brokers. Specifically this
 * means determining when connections are idle, and when connections should be
 * shut down.
 * @author tbrown
 */
public interface ConnectionManager {
	/**
	 * Allows the connection manager to wakeup the selection thread whenever a
	 * new broker has been added. There is no guarantee as to how long it will
	 * take the selection thread to take new connections.<p>
	 * 
	 * <b>Note:</b> Will be invoked by the connection processor.
	 * @param selector
	 */
	public void attach(Wakeable selector);
	
	/**
	 * Allows the connection manager to remove a particular selection thread from
	 * the list of threads that will be woken up to take new connections. However,
	 * a detached thread may still be woken up inadvertently though a connection
	 * object that it has claimed.<p> 
	 * 
	 * <b>Note:</b> Will be invoked by the connection processor.
	 * @param selector
	 */
	public void detach(Wakeable selector);
	
	/**
	 * Retrieves any new connections that have been created since the last time
	 * this method was called. Any connection returned by this method should be
	 * considered open.<p>
	 * 
	 * <b>Note:</b> Will be invoked by the connection processor.
	 */
	public List<ChannelContext> takeNewConnections();
	
	/**
	 * Requests the next operation Notifies the channel manager that the current operation has been sent and
	 * that it is ready to send another operation.<p>
	 * 
	 * The connection should be considered idle if it has completed its operation
	 * and no new operations are pending for it (e.g. if this method returns null).<p>
	 * @param connection An open channel to a specified broker.
	 * @return The next operation, or null if no operations are pending.
	 */
	public KafkaOperation getNextOperationFor(ChannelContext connection);

	/**
	 * Notifies the connection manager that a particular connection has been
	 * closed. If necessary, the connection manager should create another
	 * connection to replace this one.<p>
	 * 
	 * <b>Note:</b> Will be invoked by the connection processor.
	 * @param connection
	 */
	public void connectionClosed(ChannelContext connection, Exception reason);
}
