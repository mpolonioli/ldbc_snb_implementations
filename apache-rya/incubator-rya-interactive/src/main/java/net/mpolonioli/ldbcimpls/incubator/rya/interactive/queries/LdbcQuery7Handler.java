package net.mpolonioli.ldbcimpls.incubator.rya.interactive.queries;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7Result;

import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaClient;
import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaConnectionState;

public class LdbcQuery7Handler implements OperationHandler<LdbcQuery7, DbConnectionState> {
	
	private static DateFormat creationDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");

	public void executeOperation(
			LdbcQuery7 ldbcQuery7,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		RyaClient ryaClient = (((RyaConnectionState) dbConnectionState).getClient());

		List<LdbcQuery7Result> resultList = new ArrayList<LdbcQuery7Result>();
		int resultsCount = 0;

		long id = ldbcQuery7.personId();
		int limit = ldbcQuery7.limit();

		String query =
				"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" + 
						"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
						"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
						"\n" + 
						"SELECT DISTINCT ?likerId ?firstName ?lastName ?likeDate ?messageId ?content ?messageDate ?isNew\n" + 
						"WHERE {\n" + 
						"{\n" + 
						"SELECT DISTINCT ?likerId ?firstName ?lastName ?likeDate ?messageId ?content ?messageDate ?isNew\n" + 
						"WHERE {\n" + 
						"\n" + 
						"?person rdf:type snvoc:Person ;\n" + 
						"	snvoc:id \""+ id +"\"^^xsd:long .\n" + 
						"\n" + 
						"?post snvoc:hasCreator ?person ;\n" + 
						"	rdf:type snvoc:Post ;\n" + 
						"	snvoc:creationDate ?messageDate ;\n" + 
						"	snvoc:content | snvoc:imageFile ?content ;\n" + 
						"	snvoc:id ?messageId .\n" + 
						"\n" + 
						"?like snvoc:hasPost ?post ;\n" + 
						"	snvoc:creationDate ?likeDate .\n" + 
						"\n" + 
						"?liker snvoc:likes ?like ;\n" + 
						"	snvoc:id ?likerId ;\n" + 
						"	snvoc:firstName ?firstName ;\n" + 
						"	snvoc:lastName ?lastName .\n" + 
						"\n" + 
						"BIND(EXISTS{?liker snvoc:knows ?knowObject . ?knowObject snvoc:hasPerson ?person .} AS ?isNew)\n" + 
						"\n" + 
						"}\n" + 
						"}\n" + 
						"UNION\n" + 
						"{\n" + 
						"SELECT DISTINCT ?likerId ?firstName ?lastName ?likeDate ?messageId ?content ?messageDate ?isNew\n" + 
						"WHERE {\n" + 
						"?person rdf:type snvoc:Person ;\n" + 
						"	snvoc:id \"" + id + "\"^^xsd:long .\n" + 
						"\n" + 
						"?comment snvoc:hasCreator ?person ;\n" + 
						"	rdf:type snvoc:Comment ;\n" + 
						"	snvoc:creationDate ?messageDate ;\n" + 
						"	snvoc:content ?content ;\n" + 
						"	snvoc:id ?messageId .\n" + 
						"\n" + 
						"?like snvoc:hasPost ?comment ;\n" + 
						"	snvoc:creationDate ?likeDate .\n" + 
						"\n" + 
						"?liker snvoc:likes ?like ;\n" + 
						"	snvoc:id ?likerId ;\n" + 
						"	snvoc:firstName ?firstName ;\n" + 
						"	snvoc:lastName ?lastName .\n" + 
						"\n" + 
						"BIND(EXISTS{?liker snvoc:knows ?knowObject . ?knowObject snvoc:hasPerson ?person .} AS ?isNew)\n" + 
						"}\n" + 
						"}\n" + 
						"}\n" + 
						"ORDER BY DESC(?likeDate) ASC(?likerId)\n" + 
						"LIMIT " + limit
						;

		JSONArray jsonBindings = ryaClient.executeReadQuery(query);


		resultsCount = jsonBindings.length();

		for(int i = 0; i < resultsCount; i++) {
			JSONObject jsonObject = jsonBindings.getJSONObject(i);

			long likeDate = 0;
			long messageDate = 0;
			try {
				likeDate = creationDateFormat.parse(jsonObject.getJSONObject("likeDate").getString("value")).getTime();
				messageDate = creationDateFormat.parse(jsonObject.getJSONObject("messageDate").getString("value")).getTime();
			} catch (JSONException e) {
				e.printStackTrace();
			} catch (ParseException e) {
				e.printStackTrace();
			}
			int latency =(int)((likeDate - messageDate) / (1000 * 60));
			long likerId = jsonObject.getJSONObject("likerId").getLong("value");
			long messageId = jsonObject.getJSONObject("messageId").getLong("value");
			String firstName = jsonObject.getJSONObject("firstName").getString("value");
			String lastName = jsonObject.getJSONObject("lastName").getString("value");
			String content = jsonObject.getJSONObject("content").getString("value");
			boolean isNew = jsonObject.getJSONObject("isNew").getBoolean("value");

			resultList.add(new LdbcQuery7Result(likerId, firstName, lastName, likeDate, messageId, content, latency, isNew));
		}

		resultReporter.report(resultsCount, resultList, ldbcQuery7);		
	}
}
