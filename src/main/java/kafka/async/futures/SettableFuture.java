package kafka.async.futures;

import java.util.concurrent.Future;

public interface SettableFuture<T> extends Future<T> {
	public boolean beginExecution();
	public void completeWithValue(T value);
	public void completeWithException(Exception reason);
}
