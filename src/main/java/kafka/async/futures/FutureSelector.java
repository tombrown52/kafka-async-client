package kafka.async.futures;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public interface FutureSelector<T> {
	public void futureReady(Future<T> future);
	
	public void register(Future<T> future);
	public void registerAll(Collection<? extends Future<T>> futures);
	public void unregister(Future<T> future);
	public Set<Future<T>> readySet();
	
	public int select() throws InterruptedException;
	public int select(long timeout, TimeUnit unit) throws InterruptedException; 
	public int selectNow();
	
	public int waitingCount();
	public int readyCount();
	public boolean isEmpty();
}
