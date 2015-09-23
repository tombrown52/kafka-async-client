package kafka.async;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TestKafkaBrokerIdentity {

	@Rule
    public ExpectedException thrown = ExpectedException.none();

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
	
	@Test
	public void testParse() throws Exception {
		KafkaBrokerIdentity ident;
		
		int DEFAULT_PORT = 9092;
		
		ident = KafkaBrokerIdentity.parseIdentity("kafka://b:9/");
		assertEquals("b", ident.host);
		assertEquals(9, ident.port);
		
		ident = KafkaBrokerIdentity.parseIdentity("kafka://b/");
		assertEquals("b", ident.host);
		assertEquals(DEFAULT_PORT, ident.port);
	}
	
	@Test
	public void testParseFail() throws Exception {
		thrown.expect(ParseException.class);
		KafkaBrokerIdentity.parseIdentity("b:9");
	}
	
	@Test
	public void testParseFail2() throws Exception {
		thrown.expect(ParseException.class);
		KafkaBrokerIdentity.parseIdentity("kafka://b:b");
	}

}
