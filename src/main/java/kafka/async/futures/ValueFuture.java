package kafka.async.futures;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ValueFuture<T> implements Future<T>, SelectableFuture<T>, SettableFuture<T> {
	private final Object lock = new Object();

	static enum State {
		WAITING, CANCELLED, EXECUTING, COMPLETED;
	};
	private ValueFuture.State state = State.WAITING;
	private T value = null;
	private Exception error = null;
	private Set<FutureSelector<T>> selectors = null;
	
	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		synchronized (lock) {
			switch (state) {
			case WAITING:
				state = State.CANCELLED;
				lock.notifyAll();
			case CANCELLED:
				return true;
			default:
				return false;
			}
		}
	}
	
	@Override
	public void addSelector(FutureSelector<T> selector) {
		synchronized (lock) {
			if (selectors == null) {
				selectors = new HashSet<FutureSelector<T>>();
			}
			selectors.add(selector);
		}
	}
	
	@Override
	public void removeSelector(FutureSelector<T> selector) {
		synchronized (lock) {
			if (selectors != null) {
				selectors.remove(selector);
			}
		}
	}
	
	@Override
	public boolean beginExecution() {
		synchronized (lock) {
			if (state == State.WAITING) {
				state = State.EXECUTING;
				return true;
			} else {
				return false;
			}
		}
	}
	
	@Override
	public void completeWithException(Exception e) {
		synchronized (lock) {
			if (state == State.COMPLETED || state == State.CANCELLED) {
				throw new IllegalStateException("Future has already been completed");
			}
			
			state = State.COMPLETED;
			error = e;
			lock.notifyAll();
			
			if (selectors != null) {
				for (FutureSelector<T> s : selectors) {
					s.futureReady(this);
				}
			}
		}
	}
	
	@Override
	public void completeWithValue(T newValue) {
		synchronized (lock) {
			if (state == State.COMPLETED || state == State.CANCELLED) {
				throw new IllegalStateException("Future has already been completed");
			}
			
			state = State.COMPLETED;
			value = newValue;
			lock.notifyAll();

			if (selectors != null) {
				for (FutureSelector<T> s : selectors) {
					s.futureReady(this);
				}
			}
		}
	}
	
	@Override
	public T get() throws InterruptedException, ExecutionException {
		synchronized (lock) {
			while (state != State.COMPLETED && state != State.CANCELLED) {
				lock.wait();
			}
			if (state == State.CANCELLED) {
				throw new CancellationException("Operation was cancelled");
			}
			if (error != null) {
				throw new ExecutionException(error);
			}
			return value;
		}
	}
	
	@Override
	public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		long endTime = System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(timeout, unit);
		
		synchronized (lock) {
			while (state != State.COMPLETED && state != State.CANCELLED) {
				long waitTime = endTime - System.currentTimeMillis();
				if (waitTime <= 0) {
					throw new TimeoutException("Value not provided");
				}
				unit.timedWait(lock, timeout);
			}
			if (state == State.CANCELLED) {
				throw new CancellationException("Operation was cancelled");
			}
			if (error != null) {
				throw new ExecutionException(error);
			}
			return value;
		}
	}
	
	@Override
	public boolean isCancelled() {
		synchronized (lock) {
			return state == State.CANCELLED;
		}
	}
	
	@Override
	public boolean isDone() {
		synchronized (lock) {
			return (state == State.COMPLETED || state == State.CANCELLED);
		}
	}
}