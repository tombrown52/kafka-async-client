package kafka.async;

import java.nio.charset.Charset;

import static org.junit.Assert.*;
import org.junit.Test;

public class TestKafkaPartitionIdentity {


	public static Charset ASCII = Charset.forName("ASCII");
	
	@Test
	public void testEquality() {
		KafkaBrokerIdentity broker = new KafkaBrokerIdentity("b",1);
		KafkaBrokerIdentity broker2 = new KafkaBrokerIdentity("b2",1);
		
		KafkaPartitionIdentity a = new KafkaPartitionIdentity(broker, "foo".getBytes(ASCII), 1);
		KafkaPartitionIdentity b = new KafkaPartitionIdentity(broker, "foo".getBytes(ASCII), 1);

		assertEquals(a,b);
		
		KafkaPartitionIdentity c = new KafkaPartitionIdentity(broker2, "bar".getBytes(ASCII), 1);
		KafkaPartitionIdentity d = new KafkaPartitionIdentity(broker, "bar".getBytes(ASCII), 1);
		KafkaPartitionIdentity e = new KafkaPartitionIdentity(broker, "foo".getBytes(ASCII), 2);
		
		assertFalse(a.equals(c));
		assertFalse(a.equals(d));
		assertFalse(a.equals(e));
	}
	
	public void testOrder() {
		
		KafkaBrokerIdentity broker_a = new KafkaBrokerIdentity("a",1);
		KafkaBrokerIdentity broker_b = new KafkaBrokerIdentity("b",2);
		
		
		KafkaPartitionIdentity aa1 = new KafkaPartitionIdentity(broker_a,"a".getBytes(ASCII), 1);
		KafkaPartitionIdentity aa2 = new KafkaPartitionIdentity(broker_a,"a".getBytes(ASCII), 2);
		KafkaPartitionIdentity ab1 = new KafkaPartitionIdentity(broker_a,"b".getBytes(ASCII), 1);
		KafkaPartitionIdentity ab2 = new KafkaPartitionIdentity(broker_a,"b".getBytes(ASCII), 2);
		KafkaPartitionIdentity ba1 = new KafkaPartitionIdentity(broker_b,"a".getBytes(ASCII), 1);
		KafkaPartitionIdentity ba2 = new KafkaPartitionIdentity(broker_b,"a".getBytes(ASCII), 2);
		KafkaPartitionIdentity bb1 = new KafkaPartitionIdentity(broker_b,"b".getBytes(ASCII), 1);
		KafkaPartitionIdentity bb2 = new KafkaPartitionIdentity(broker_b,"b".getBytes(ASCII), 2);
		
		assertTrue(aa1.compareTo(aa1) == 0);
		assertTrue(aa1.compareTo(aa2) <  0);
		assertTrue(aa1.compareTo(ab1) <  0);
		assertTrue(aa1.compareTo(ab2) <  0);
		assertTrue(aa1.compareTo(ba1) <  0);
		assertTrue(aa1.compareTo(ba2) <  0);
		assertTrue(aa1.compareTo(bb1) <  0);
		assertTrue(aa1.compareTo(bb2) <  0);
		
		assertTrue(aa2.compareTo(aa1) <  0);
		assertTrue(aa2.compareTo(aa2) == 0);
		assertTrue(aa2.compareTo(ab1) >  0);
		assertTrue(aa2.compareTo(ab2) >  0);
		assertTrue(aa2.compareTo(ba1) >  0);
		assertTrue(aa2.compareTo(ba2) >  0);
		assertTrue(aa2.compareTo(bb1) >  0);
		assertTrue(aa2.compareTo(bb2) >  0);
		
		assertTrue(ab1.compareTo(aa1) <  0);
		assertTrue(ab1.compareTo(aa2) <  0);
		assertTrue(ab1.compareTo(ab1) == 0);
		assertTrue(ab1.compareTo(ab2) >  0);
		assertTrue(ab1.compareTo(ba1) >  0);
		assertTrue(ab1.compareTo(ba2) >  0);
		assertTrue(ab1.compareTo(bb1) >  0);
		assertTrue(ab1.compareTo(bb2) >  0);
		
		assertTrue(ab2.compareTo(aa1) <  0);
		assertTrue(ab2.compareTo(aa2) <  0);
		assertTrue(ab2.compareTo(ab1) <  0);
		assertTrue(ab2.compareTo(ab2) == 0);
		assertTrue(ab2.compareTo(ba1) >  0);
		assertTrue(ab2.compareTo(ba2) >  0);
		assertTrue(ab2.compareTo(bb1) >  0);
		assertTrue(ab2.compareTo(bb2) >  0);
		
		assertTrue(ba1.compareTo(aa1) <  0);
		assertTrue(ba1.compareTo(aa2) <  0);
		assertTrue(ba1.compareTo(ab1) <  0);
		assertTrue(ba1.compareTo(ab2) <  0);
		assertTrue(ba1.compareTo(ba1) == 0);
		assertTrue(ba1.compareTo(ba2) >  0);
		assertTrue(ba1.compareTo(bb1) >  0);
		assertTrue(ba1.compareTo(bb2) >  0);
		
		assertTrue(ba2.compareTo(aa1) <  0);
		assertTrue(ba2.compareTo(aa2) <  0);
		assertTrue(ba2.compareTo(ab1) <  0);
		assertTrue(ba2.compareTo(ab2) <  0);
		assertTrue(ba2.compareTo(ba1) <  0);
		assertTrue(ba2.compareTo(ba2) == 0);
		assertTrue(ba2.compareTo(bb1) >  0);
		assertTrue(ba2.compareTo(bb2) >  0);
		
		assertTrue(bb1.compareTo(aa1) <  0);
		assertTrue(bb1.compareTo(aa2) <  0);
		assertTrue(bb1.compareTo(ab1) <  0);
		assertTrue(bb1.compareTo(ab2) <  0);
		assertTrue(bb1.compareTo(ba1) <  0);
		assertTrue(bb1.compareTo(ba2) <  0);
		assertTrue(bb1.compareTo(bb1) == 0);
		assertTrue(bb1.compareTo(bb2) >  0);
		
		assertTrue(bb2.compareTo(aa1) <  0);
		assertTrue(bb2.compareTo(aa2) <  0);
		assertTrue(bb2.compareTo(ab1) <  0);
		assertTrue(bb2.compareTo(ab2) <  0);
		assertTrue(bb2.compareTo(ba1) <  0);
		assertTrue(bb2.compareTo(ba2) <  0);
		assertTrue(bb2.compareTo(bb1) <  0);
		assertTrue(bb2.compareTo(bb2) == 0);
		
	}
}
