package com.storm;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class CassandraConnection {
	private Cluster cluster;
	private Session session;
	
	@SuppressWarnings("finally")
	public Session getConnection(final String db, final String node) {
		try {
			System.out.print("connecting to host " + node);
			cluster = Cluster.builder()
		            .addContactPoint(node)
		            .build();
			System.out.println("connecting to " + db);
			session = cluster.connect(db);
		} catch (Exception e) {
			System.out.print(e);
		}
		finally {
			return session;
		}
	}
	
	public void close() {
		cluster.close();
		session.close();
	}
}