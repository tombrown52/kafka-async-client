package kafka.async.futures;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class FutureWithAttachment<FutureReturnType,AttachType> implements Future<FutureReturnType>, SelectableFuture<FutureReturnType> {

	private final Future<FutureReturnType> future;
	private final AttachType attachment;
	
	public FutureWithAttachment(Future<FutureReturnType> future, AttachType attachment) {
		this.future = future;
		this.attachment = attachment;
	}

	@Override
	public void addSelector(FutureSelector<FutureReturnType> selector) {
		if (! (future instanceof SelectableFuture<?>)) {
			throw new IllegalStateException("Future did not inherit from SelectableFuture");
		}
		((SelectableFuture<FutureReturnType>)future).addSelector(selector);
	}
	
	@Override
	public void removeSelector(FutureSelector<FutureReturnType> selector) {
		if (! (future instanceof SelectableFuture<?>)) {
			throw new IllegalStateException("Future did not inherit from SelectableFuture");
		}
		((SelectableFuture<FutureReturnType>)future).removeSelector(selector);
	}
	
	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return future.cancel(mayInterruptIfRunning);
	}
	
	@Override
	public FutureReturnType get() throws InterruptedException, ExecutionException {
		return future.get();
	}
	
	@Override
	public FutureReturnType get(long timeout, TimeUnit unit)
			throws InterruptedException, ExecutionException, TimeoutException {
		return future.get(timeout, unit);
	}
	
	@Override
	public boolean isCancelled() {
		return future.isCancelled();
	}
	
	@Override
	public boolean isDone() {
		return future.isDone();
	}
	
	public AttachType attachment() {
		return attachment;
	}
}
