package kafka.async.futures;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class WaitingFutureSelector<T> implements FutureSelector<T> {

	enum State {
		WAITING, READY;
	}
	private Object lock = new Object();
	private State state = State.WAITING;
	
	private Set<SelectableFuture<T>> futures = new HashSet<SelectableFuture<T>>();
	private Set<Future<T>> ready = new HashSet<Future<T>>();
	
	@Override
	public void futureReady(Future<T> future) {
		synchronized (lock) {
			state = State.READY;
			lock.notifyAll();
		}
	}
	
	@Override
	public Set<Future<T>> readySet() {
		synchronized (lock) {
			state = State.WAITING;
		}
		return ready;
	}
	
	@Override
	public void register(Future<T> future) {
		if (!(future instanceof SelectableFuture<?>)) {
			throw new IllegalArgumentException("WaitingFutureSelector requires 'SelectableFuture'");
		}
		
		SelectableFuture<T> f = (SelectableFuture<T>)future;
		futures.add(f);
		f.addSelector(this);
	}
	
	@Override
	public void registerAll(Collection<? extends Future<T>> futures) {
		for (Future<T> f : futures) {
			register(f);
		}
	}
	
	@Override
	public void unregister(Future<T> future) {
		if (!(future instanceof SelectableFuture<?>)) {
			throw new IllegalArgumentException("WaitingFutureSelector requires 'SelectableFuture'");
		}

		SelectableFuture<T> f = (SelectableFuture<T>)future;
		futures.remove(f);
		f.removeSelector(this);
	}
	
	@Override
	public int selectNow() {
		Iterator<SelectableFuture<T>> iterator = futures.iterator();
		while (iterator.hasNext()) {
			SelectableFuture<T> f = iterator.next();
			if (f.isDone()) {
				f.removeSelector(this);
				iterator.remove();
				ready.add(f);
			}
		}
		return ready.size();
	}
	
	@Override
	public int select() throws InterruptedException {
		return select(1000,TimeUnit.DAYS);
	}
	
	@Override
	public int select(long timeout, TimeUnit unit) throws InterruptedException {
		synchronized (lock) {
			long endTime = unit.toMillis(timeout) + System.currentTimeMillis();
			while (state == State.WAITING) {
				long now = System.currentTimeMillis();
				if (now >= endTime) {
					return 0;
				}
				TimeUnit.MILLISECONDS.timedWait(lock, endTime - now);
			}
		}
			
		return selectNow();
	}
	
	@Override
	public int readyCount() {
		return ready.size();
	}
	
	@Override
	public int waitingCount() {
		return futures.size();
	}
	
	@Override
	public boolean isEmpty() {
		return futures.isEmpty();
	}
}
