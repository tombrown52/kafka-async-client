package kafka.async.futures;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class PollingFutureSelector<T> implements FutureSelector<T> {
	private Set<Future<T>> awaiting = new HashSet<Future<T>>();
	private Set<Future<T>> ready = new HashSet<Future<T>>();
	private long spinDelayMs = 5;
	
	public PollingFutureSelector() {
		this(100,TimeUnit.MILLISECONDS);
	}
	
	public PollingFutureSelector(long pollDelay, TimeUnit p) {
		spinDelayMs = p.toMillis(pollDelay);
	}

	@Override
	public void register(Future<T> future) {
		awaiting.add(future);
	}
	
	@Override
	public void registerAll(Collection<? extends Future<T>> futures) {
		for (Future<T> f : futures) {
			register(f);
		}
	}
	
	@Override
	public void unregister(Future<T> future) {
		awaiting.remove(future);
		ready.remove(future);
	}
	
	@Override
	public void futureReady(Future<T> future) {
		// Do nothing here
	}
	
	public int select() throws InterruptedException {
		return select(10000,TimeUnit.DAYS);
	}
	
	public int select(long timeout, TimeUnit unit) throws InterruptedException {
		long endTime = System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(timeout, unit);

		while (ready.isEmpty()) {
			selectNow();
			if (ready.isEmpty()) {
				Thread.sleep(Math.min(endTime-System.currentTimeMillis(), spinDelayMs));
			}
		}
		
		return ready.size();
	}
	
	public int selectAll() throws InterruptedException, TimeoutException {
		return selectAll(10000,TimeUnit.DAYS);
	}
	
	public int selectAll(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
		long endTime = System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(timeout, unit);

		while (!awaiting.isEmpty()) {
			selectNow();
			if (!awaiting.isEmpty()) {
				Thread.sleep(Math.min(endTime-System.currentTimeMillis(), spinDelayMs));
				if (System.currentTimeMillis() > endTime) {
					throw new TimeoutException("Timed out waiting for all operations to finish");
				}
			}
		}
		
		return ready.size();
	}
	
	public int selectNow() {
		Iterator<Future<T>> iterator = awaiting.iterator();
		while (iterator.hasNext()) {
			Future<T> f = iterator.next();
			if (f.isDone()) {
				ready.add(f);
				iterator.remove();
			}
		}
		return ready.size();
	}
	
	@Override
	public int readyCount() {
		return ready.size();
	}
	
	@Override
	public int waitingCount() {
		return awaiting.size();
	}
	
	@Override
	public boolean isEmpty() {
		return awaiting.isEmpty();
	}
	
	@Override
	public Set<Future<T>> readySet() {
		return ready;
	}
	
	public void cancelAll(boolean mayInterruptIfRunning) {
		for (Future<T> item : awaiting) {
			item.cancel(mayInterruptIfRunning);
		}
	}
}