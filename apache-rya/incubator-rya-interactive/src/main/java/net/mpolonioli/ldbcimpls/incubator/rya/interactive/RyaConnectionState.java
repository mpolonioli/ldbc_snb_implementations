package net.mpolonioli.ldbcimpls.incubator.rya.interactive;

import java.io.IOException;
import java.util.Map;

import com.ldbc.driver.DbConnectionState;

public class RyaConnectionState extends DbConnectionState{
	
	private RyaClient ryaClient;
	
	public RyaConnectionState(Map<String, String> ryaDbProperties) {
		ryaClient = new RyaClient(ryaDbProperties.get("baseQueryUrl"), ryaDbProperties.get("baseLoadRdfUrl"));
	}
	
	public RyaConnectionState(String baseQueryUrl, String baseLoadRdfUrl) {
		ryaClient = new RyaClient(baseQueryUrl, baseLoadRdfUrl);
	}

	RyaClient getClient() {
		return ryaClient;
	}

	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}
}
