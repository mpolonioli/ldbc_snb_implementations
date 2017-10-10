package net.mpolonioli.ldbcimpls.janusgraph.interactive;

import java.io.IOException;

import com.ldbc.driver.DbConnectionState;

import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;

import java.util.logging.Level;

public class JanusGraphDbConnectionState extends DbConnectionState{

	private final JanusGraph client;

	public JanusGraphDbConnectionState() {

		// create the connection via configuration file  
		client = JanusGraphFactory.open("//opt//janusgraph//janusgraph-0.1.1-hadoop2//conf//janusgraph-hbase.properties");
	}

	public JanusGraph getClient() {
		return client;
	}

	@Override
	public void close() throws IOException {
		try {
			client.close();
		} catch (Exception ex) {
			java.util.logging.Logger.getLogger(JanusGraphDb.class.getName())
			.log(Level.SEVERE, null, ex);
		}
	}

}
