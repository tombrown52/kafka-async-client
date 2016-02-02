package kafka.async;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import kafka.async.client.KafkaAsyncClient;
import kafka.async.client.StaticConfiguration;

public class TestNoConnect extends TestWithLog4j {

	@Test
	public void testUnknownHost() throws Exception {
		
		Set<KafkaPartitionIdentity> hosts = new HashSet<KafkaPartitionIdentity>();
		hosts.add(new KafkaPartitionIdentity(new KafkaBrokerIdentity("asdf-this-host-name-should-not-exist", 1234), "topic".getBytes(), 0));
		StaticConfiguration config = new StaticConfiguration(hosts);
		final AtomicInteger counter = new AtomicInteger();
		KafkaAsyncClient client = new KafkaAsyncClient(config) {
			@Override
			public synchronized void connectionClosed(ChannelContext connection, Exception reason) {
				counter.incrementAndGet();
				super.connectionClosed(connection, reason);
			}
		};
		
		
		client.open();
		Thread.sleep(5000);
		client.close();
		
		int connectFailures = counter.get();
		
		// If re-connects happen as fast as possible, there will
		// be a lot more connect failures in 5 seconds.
		// These numbers are based on 2-5 failures per second
		// (repeated unknown host attempts fail very fast after the first one) 
		Assert.assertTrue(connectFailures <= 25);
		Assert.assertTrue(connectFailures >= 10);
		
	}


	@Test
	public void testConnectionRefused() throws Exception {
		
		Set<KafkaPartitionIdentity> hosts = new HashSet<KafkaPartitionIdentity>();
		hosts.add(new KafkaPartitionIdentity(new KafkaBrokerIdentity("127.0.0.1", 65432), "topic".getBytes(), 0));
		StaticConfiguration config = new StaticConfiguration(hosts);
		final AtomicInteger counter = new AtomicInteger();
		KafkaAsyncClient client = new KafkaAsyncClient(config) {
			@Override
			public synchronized void connectionClosed(ChannelContext connection, Exception reason) {
				counter.incrementAndGet();
				super.connectionClosed(connection, reason);
			}
		};
		
		
		client.open();
		Thread.sleep(5000);
		client.close();
		
		int connectFailures = counter.get();
		
		// If re-connects happen as fast as possible, there will
		// be a lot more connect failures in 5 seconds.
		// (connection refused failures are pretty slow, but there
		// should be at least one)
		Assert.assertTrue(connectFailures <= 25);
		Assert.assertTrue(connectFailures > 0);
		
	}
	
}
