package net.mpolonioli.ldbcimpls.janusgraph.interactive;

import java.io.IOException;

import com.ldbc.driver.DbConnectionState;

import org.janusgraph.core.JanusGraphFactory;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.logging.Level;

public class JanusGraphDbConnectionState extends DbConnectionState{

	private Graph client;

	  public JanusGraphDbConnectionState() {
	    BaseConfiguration config = new BaseConfiguration();
	    config.setDelimiterParsingDisabled(true);
	    config.setProperty("storage.backend", "hbase");
	    config.setProperty("storage.hostname", "127.0.0.1");
	    config.setProperty("storage.hbase.keyspace", "ldbc_snb_socialnet");
	    client = JanusGraphFactory.open(config);
	  }

	  public Graph getClient() {
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
