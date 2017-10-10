package net.mpolonioli.ldbcimpls.janusgraph.interactive;

import java.io.IOException;

import com.ldbc.driver.DbConnectionState;

import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;

import org.apache.commons.configuration.BaseConfiguration;

import java.util.logging.Level;

public class JanusGraphDbConnectionState extends DbConnectionState{

	private final JanusGraph client;

	  public JanusGraphDbConnectionState() {
		  
	    //create the connection specifing all the configurations
	    /*
	    BaseConfiguration config = new BaseConfiguration();
	    config.setDelimiterParsingDisabled(true);
	    config.setProperty("storage.backend", "hbase");
	    config.setProperty("storage.hostname", "127.0.0.1");
	    config.setProperty("storage.hbase.keyspace", "ldbc_snb_socialnet");
	    config.setProperty("storage.hbase.table", "janusgraph");  
	    config.setProperty("cache.db-cache", "true");
	    config.setProperty("cache.db-cache-size", "0.5");
	    config.setProperty("gremlin.graph", "org.janusgraph.core.JanusGraphFactory");
	    client = JanusGraphFactory.open(config);
	    */
		  
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
