package kafka.async;

import static org.junit.Assert.*;
import org.junit.Test;

public class TestKafkaBrokerIdentity {

	@Test
	public void testEquality() {
		KafkaBrokerIdentity a1_1 = new KafkaBrokerIdentity("a", 1);
		KafkaBrokerIdentity a1_2 = new KafkaBrokerIdentity("a", 1);
		
		KafkaBrokerIdentity a2_1 = new KafkaBrokerIdentity("a", 2);

		KafkaBrokerIdentity b1_1 = new KafkaBrokerIdentity("b", 1);
		KafkaBrokerIdentity b2_1 = new KafkaBrokerIdentity("b", 2);
		
		
		assertEquals(a1_1, a1_2);
		assertEquals(a1_2, a1_1);
		
		assertFalse(a1_1.equals(a2_1));
		assertFalse(a1_1.equals(b1_1));
		assertFalse(a1_1.equals(b2_1));
	}
	
	@Test
	public void testOrder() {

		KafkaBrokerIdentity a1 = new KafkaBrokerIdentity("a", 1);
		KafkaBrokerIdentity a2 = new KafkaBrokerIdentity("a", 2);
		KafkaBrokerIdentity b1 = new KafkaBrokerIdentity("b", 1);
		KafkaBrokerIdentity b2 = new KafkaBrokerIdentity("b", 2);
		
		assertTrue(a1.compareTo(a1) == 0);
		assertTrue(a1.compareTo(a2) <  0);
		assertTrue(a1.compareTo(b1) <  0);
		assertTrue(a1.compareTo(b2) <  0);

		assertTrue(a2.compareTo(a1) >  0);
		assertTrue(a2.compareTo(a2) == 0);
		assertTrue(a2.compareTo(b1) <  0);
		assertTrue(a2.compareTo(b2) <  0);

		assertTrue(b1.compareTo(a1) >  0);
		assertTrue(b1.compareTo(a2) >  0);
		assertTrue(b1.compareTo(b1) == 0);
		assertTrue(b1.compareTo(b2) <  0);

		assertTrue(b2.compareTo(a1) >  0);
		assertTrue(b2.compareTo(a2) >  0);
		assertTrue(b2.compareTo(b1) >  0);
		assertTrue(b2.compareTo(b2) == 0);
		
	}

}
