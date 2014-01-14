package kafka.async;

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
}
