package kafka.async;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import kafka.async.futures.ValueFuture;
import kafka.async.futures.WaitingFutureSelector;

import org.junit.Assert;
import org.junit.Test;

public class TestSelector {

	@Test
	public void testConcurrency() throws Exception {
		
		final WaitingFutureSelector<Boolean> selector = new WaitingFutureSelector<Boolean>();
		final ValueFuture<Boolean> promise = new ValueFuture<Boolean>();
		final CountDownLatch starter = new CountDownLatch(2);
		final CountDownLatch latch = new CountDownLatch(2);
		final AtomicInteger readyCount = new AtomicInteger(0);
		
		selector.register(promise);
		
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					starter.countDown();
					starter.await();

					selector.select(1, TimeUnit.MILLISECONDS);

					// Sleep enough time to make sure that the future
					// can be completed before we call readySet()
					Thread.sleep(1000);
					selector.readySet();

					// Attempt to select again
					readyCount.set(selector.select(1, TimeUnit.MILLISECONDS));
					
				} catch (Exception e) {
					throw new RuntimeException (e);
				} finally {
					latch.countDown();
				}
			}
		}).start();

		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					starter.countDown();
					starter.await();

					// Sleep enough time to make sure we complete the
					// future between the "select()" and "readySet()"
					// calls in the other thread.
					Thread.sleep(500);
					promise.completeWithValue(Boolean.TRUE);
					
				} catch (Exception e) {
					throw new RuntimeException (e);
				} finally {
					latch.countDown();
				}
			}
		}).start();

		
		latch.await();
		
		Assert.assertEquals(1,readyCount.get());
		Assert.assertEquals(1,selector.readySet().size());
		System.out.println("Both finished");
	}
}
