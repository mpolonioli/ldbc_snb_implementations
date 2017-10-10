package net.mpolonioli.ldbcimpls.janusgraph.interactive;

import java.io.IOException;

import com.ldbc.driver.DbConnectionState;

import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;

import org.apache.commons.configuration.BaseConfiguration;

import java.util.logging.Level;

public class JanusGraphDbConnectionState extends DbConnectionState{

	private JanusGraph client;

	  public JanusGraphDbConnectionState() {
	    BaseConfiguration config = new BaseConfiguration();
	    config.setDelimiterParsingDisabled(true);
	    config.setProperty("storage.backend", "hbase");
	    config.setProperty("storage.hostname", "siti-rack.siti.disco.unimib.it");
	    config.setProperty("storage.hbase.keyspace", "ldbc_snb_socialnet");
	    client = JanusGraphFactory.open(config);
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
