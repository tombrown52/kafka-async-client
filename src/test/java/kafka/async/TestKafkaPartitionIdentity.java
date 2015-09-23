package kafka.async;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.charset.Charset;
import java.text.ParseException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TestKafkaPartitionIdentity {

	@Rule
    public ExpectedException thrown = ExpectedException.none();

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
	
	
	@Test
	public void testParse() throws Exception {
		KafkaPartitionIdentity ident;
		
		int DEFAULT_PORT = 9092;
		
		ident = KafkaPartitionIdentity.parseIdentity("kafka://b:9/t#2");
		assertEquals("b", ident.broker.host);
		assertEquals(9, ident.broker.port);
		assertEquals("t", new String(ident.topicName));
		assertEquals(2, ident.partition);
	}
	
	@Test
	public void testParseFail() throws Exception {
		thrown.expect(ParseException.class);
		KafkaPartitionIdentity.parseIdentity("b:9");
	}
	
	@Test
	public void testParseFail2() throws Exception {
		thrown.expect(ParseException.class);
		KafkaPartitionIdentity.parseIdentity("kafka://b:b");
	}

}
