package net.mpolonioli.ldbcimpls.incubator.rya.interactive;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.prospector.service.ProspectorServiceEvalStatsDAO;
import org.apache.rya.rdftriplestore.RdfCloudTripleStore;
import org.apache.rya.rdftriplestore.RyaSailRepository;
import org.apache.rya.rdftriplestore.inference.InferenceEngine;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;

import com.ldbc.driver.DbConnectionState;

public class RyaConnectionState extends DbConnectionState{

	private final Repository myRepository;
	private final RepositoryConnection ryaClient;

	@SuppressWarnings("deprecation")
	public RyaConnectionState() throws AccumuloException, AccumuloSecurityException, RepositoryException {
		Connector connector = new ZooKeeperInstance("accumulo", "localhost").getConnector("rya", "q268smwHa13X5Bf");
		final RdfCloudTripleStore store = new RdfCloudTripleStore();
		AccumuloRyaDAO crdfdao = new AccumuloRyaDAO();
		crdfdao.setConnector(connector);

		AccumuloRdfConfiguration conf = new AccumuloRdfConfiguration();
		conf.setTablePrefix("rya_");
		conf.setDisplayQueryPlan(true);
		crdfdao.setConf(conf);
		store.setRyaDAO(crdfdao);

		ProspectorServiceEvalStatsDAO evalDao = new ProspectorServiceEvalStatsDAO(connector, conf);
		evalDao.init();
		store.setRdfEvalStatsDAO(evalDao);

		InferenceEngine inferenceEngine = new InferenceEngine();
		inferenceEngine.setRyaDAO(crdfdao);
		inferenceEngine.setConf(conf);
		store.setInferenceEngine(inferenceEngine);

		myRepository = new RyaSailRepository(store);
		myRepository.initialize();
		ryaClient = myRepository.getConnection();	
	}

	public RepositoryConnection getClient() {
		return ryaClient;
	}

	public void close() {
		try {
			ryaClient.close();
		} catch (RepositoryException e) {
			e.printStackTrace();
		}
		try {
			myRepository.shutDown();
		} catch (RepositoryException e) {
			e.printStackTrace();
		}
	}
}
