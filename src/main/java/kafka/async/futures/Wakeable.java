package kafka.async.futures;

public interface Wakeable {
	/**
	 * Causes the object to wakeup and perform some work.
	 */
	public void wakeup();
}
