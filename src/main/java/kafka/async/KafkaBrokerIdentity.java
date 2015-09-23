package kafka.async;

import java.text.ParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class KafkaBrokerIdentity implements Comparable<KafkaBrokerIdentity> {

	public final String host;
	public final int port;
	
	public KafkaBrokerIdentity(String host, int port) {
		super();
		if (host == null) {
			throw new IllegalArgumentException("Parameter 'host' cannot be null");
		}
		this.host = host;
		this.port = port;
	}

	@Override
	public int compareTo(KafkaBrokerIdentity other) {
		int cmp = host.compareTo(other.host);
		if (cmp == 0) {
			cmp = port < other.port ? -1 : (port > other.port ? 1 : 0);
		}
		return cmp;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj == null || obj.getClass() != KafkaBrokerIdentity.class) {
			return false;
		}
		return equals((KafkaBrokerIdentity)obj);
	}
	
	public boolean equals(KafkaBrokerIdentity other) {
		if (other == null) {
			return false;
		}
		
		if (port != other.port) {
			return false;
		}
		
		return host.equals(other.host);
	}
	
	@Override
	public int hashCode() {
		return host.hashCode() ^ port;
	}
	
	@Override
	public String toString() {
		return "[kafka://"+host+":"+port+"]";
	}
	
	private static Pattern PARSER = Pattern.compile("kafka://([a-z0-9\\.\\-\\_]+)(:([0-9]+))?/?", Pattern.CASE_INSENSITIVE);
	public static KafkaBrokerIdentity parseIdentity(String str) throws ParseException {
		Matcher m = PARSER.matcher(str);
		if (!m.matches()) {
			throw new ParseException("Invalid kafka broker identity string", 0);
		}
		String host = m.group(1);
		int port = m.group(3) != null ? Integer.parseInt(m.group(3)) : 9092;
		return new KafkaBrokerIdentity(host, port);
	}
	
}
