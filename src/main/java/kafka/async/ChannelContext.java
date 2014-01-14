package kafka.async;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

public interface ChannelContext {

	/**
	 * Returns the broker to which this channel is associated.
	 * @return
	 */
	public KafkaBrokerIdentity broker();
	
	/**
	 * Returns the connection manager to which this channel is associated.
	 * @return
	 */
	public ConnectionManager manager();
	
	/**
	 * Initializes this channel with the specified socket address and allows it
	 * to register with the specified selector. This will be called from the
	 * thread that is managing the selector.<p>
	 * 
	 * This method should be used to create a socket and begin connecting to the
	 * host address.<p>
	 * 
	 * Note: This method should only be called by the IO processing thread.
	 * @param address
	 * @param selector
	 * @throws IOException
	 */
	public void initAndRegister(SocketAddress address, Selector selector) throws IOException;

	/**
	 * Causes the selection thread to stop an existing select operation and
	 * to process this item the next time through the selection loop.<p>
	 */
	public void wakeup();
	
	/**
	 * Informs this context that the channel has finished connecting.<p>
	 * 
	 * Note: This method should only be called by the IO processing thread. 
	 */
	public void doConnect() throws IOException;
	
	/**
	 * Informs this context that the channel is ready to read. Any available data is
	 * read and passed to the first waiting response handler. If the handler
	 * determines that the entire response has been read, the handler consumes the
	 * request bytes from the buffer and this method compacts the remainder.<p>
	 * 
	 * Note: This method should only be called by the IO processing thread. 
	 * @param channel
	 * @throws IOException
	 */
	public void doRead(SocketChannel channel) throws IOException;
	
	/**
	 * Informs this context that the channel is ready to write. If there is data from
	 * a previous request that is buffered, that data is sent first. If the entire
	 * current request has been sent and there is a queued request compatible with
	 * this context, it will be buffered.<p>
	 *  
	 * Note: This method should only be called by the IO processing thread. 
	 * @param channel
	 * @throws IOException
	 */
	public void doWrite(SocketChannel channel) throws IOException;
	
	/**
	 * Informs this context that the socket has been closed and that resources
	 * should be cleaned up.<p>
	 * 
	 * Note: This method should only be called by the IO processing thread. 
	 * 
	 * @param e
	 */
	public void closedWithException(Exception e);

	/**
	 * Closes this context. If the context has already been closed, nothing should
	 * happen.
	 */
	public void close();
}
