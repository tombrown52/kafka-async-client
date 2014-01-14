package kafka.async;

public interface BrokerPool {
	/**
	 * Adds a broker to the pool. Will cause new connection(s) to be created to
	 * this broker. Should be thread-safe.<p>
	 * @param broker
	 */
	public void addBroker(KafkaBrokerIdentity broker);
	
	/**
	 * Removes a broker from the pool. Will cause all connections to this broker
	 * to begin shutting down. Should be thread-safe.<p>
	 * @param broker
	 */
	public void removeBroker(KafkaBrokerIdentity broker);
}
