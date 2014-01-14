package kafka.async;

import java.nio.ByteBuffer;

public interface KafkaOperation {
	/**
	 * Returns the target broker against which this request should be executed.
	 * @return
	 */
	public KafkaBrokerIdentity getTargetBroker();
	
	/**
	 * Notifies the operation that execution is starting. The operation should return
	 * false if the operation should no longer be executed (e.g. if it has been
	 * cancelled). This method will only be called once, and the operation will either
	 * succeed or fail from this point.
	 */
	public boolean start();
	
	/**
	 * Fills the provided ByteBuffer with the data of this request. It is expected
	 * that this method will be called exactly once for each KafkaOperation.
	 * @return
	 */
	public void executeWrite(ByteBuffer buffer);

	/**
	 * Any time new data is read from the channel, this operation will be invoked
	 * on the active read operation. The buffer is not reset or flipped between calls
	 * to this method.<p>
	 * 
	 * It is guaranteed that the buffer position 0 will be the start of the response
	 * to this operation.<p>
	 * @param buffer
	 * @return true when the entire response has been read and processed. Returns
	 * false if additional data should be read from the socket.
	 */
	public boolean executeRead(ByteBuffer buffer);
	
	/**
	 * Specifies whether this operation requires a response from the server.
	 * @return
	 */
	public boolean canRead();

	/**
	 * Called to notify this operation that the connection has failed after the
	 * request was sent but before a full response could be read.<p>
	 * 
	 * Primarily useful in allowing futures to be failed.
	 * @param reason 
	 */
	public void responseFailed(Exception reason);
	
	/**
	 * Called to notify this operation that the connection failed before it could be
	 * completely sent.<p>
	 *
	 * Primarily useful in allowing futures to be failed.
	 * @param reason
	 */
	public void requestFailed(Exception reason);
	
	/**
	 * Called to notify this operation that the connection failed before it could
	 * even be started. This should only be called on operations that are in a
	 * queue for a broker, and all connections to that broker are closed.<p>
	 * 
	 * Primarily useful in allowing futures to be failed.
	 * @param reason
	 */
	public void brokerFailed(Exception reason);
}