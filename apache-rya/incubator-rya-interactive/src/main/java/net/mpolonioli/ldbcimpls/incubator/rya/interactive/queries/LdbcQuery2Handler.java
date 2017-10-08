package net.mpolonioli.ldbcimpls.incubator.rya.interactive.queries;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2Result;

import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaClient;
import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaConnectionState;

public class LdbcQuery2Handler implements OperationHandler<LdbcQuery2, DbConnectionState> {

	private static DateFormat creationDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");

	public void executeOperation(
			LdbcQuery2 ldbcQuery2,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		RyaClient ryaClient = (((RyaConnectionState) dbConnectionState).getClient());

		List<LdbcQuery2Result> resultList = new ArrayList<LdbcQuery2Result>();
		int resultsCount = 0;

		long id = ldbcQuery2.personId();
		Date maxDate = ldbcQuery2.maxDate();
		int limit = ldbcQuery2.limit();

		String query = 
				"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" + 
						"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
						"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
						"\n" + 
						"SELECT ?friendId ?firstName ?lastName ?messageId ?content ?creationDate\n" + 
						"WHERE {\n" + 
						"{\n" + 
						"?person rdf:type snvoc:Person ;\n" + 
						"	snvoc:id \"" + id + "\"^^xsd:long .\n" + 
						"\n" + 
						"?person snvoc:knows ?knowObject .\n" + 
						"\n" + 
						"?knowObject snvoc:hasPerson ?friend .\n" + 
						"\n" + 
						"?post rdf:type snvoc:Post ;\n" + 
						"        snvoc:id ?messageId ;\n" + 
						"	snvoc:hasCreator ?friend ;\n" + 
						"	snvoc:creationDate ?creationDate ;\n" + 
						"	snvoc:imageFile | snvoc:content ?content .\n" + 
						"\n" + 
						"?friend snvoc:id ?friendId ;\n" + 
						"	snvoc:firstName ?firstName;\n" + 
						"	snvoc:lastName ?lastName .\n" + 
						"\n" + 
						"FILTER(?creationDate <= \"" + creationDateFormat.format(maxDate) + ":00\"^^xsd:dateTime)\n" + 
						"}\n" + 
						"UNION\n" + 
						"{\n" + 
						"?person rdf:type snvoc:Person ;\n" + 
						"	snvoc:id \"" + id + "\"^^xsd:long .\n" + 
						"\n" + 
						"?person snvoc:knows ?knowObject .\n" + 
						"\n" + 
						"?knowObject snvoc:hasPerson ?friend .\n" + 
						"\n" + 
						"?comment rdf:type snvoc:Comment ;\n" + 
						"snvoc:id ?messageId ;\n" + 
						"	snvoc:hasCreator ?friend ;\n" + 
						"	snvoc:creationDate ?creationDate ;\n" + 
						"	snvoc:content ?content .\n" + 
						"\n" + 
						"?friend snvoc:id ?friendId ;\n" + 
						"	snvoc:firstName ?firstName;\n" + 
						"	snvoc:lastName ?lastName .\n" + 
						"\n" + 
						"FILTER(?creationDate <= \"" + creationDateFormat.format(maxDate) + ":00\"^^xsd:dateTime)\n" + 
						"}\n" + 
						"}\n" + 
						"ORDER BY DESC(?creationDate) ASC(?id)\n" + 
						"LIMIT " + limit
						;

		JSONArray jsonBindings = ryaClient.executeReadQuery(query);

		resultsCount = jsonBindings.length();

		for(int i = 0; i < resultsCount; i++) {
			JSONObject jsonObject = jsonBindings.getJSONObject(i);
			long friendId = jsonObject.getJSONObject("friendId").getLong("value");
			String firstName = jsonObject.getJSONObject("firstName").getString("value");
			String lastName = jsonObject.getJSONObject("lastName").getString("value");
			long messageId = jsonObject.getJSONObject("messageId").getLong("value");
			String content = jsonObject.getJSONObject("content").getString("value");
			long creationDate = 0;
			try {
				creationDate = creationDateFormat.parse(jsonObject.getJSONObject("creationDate").getString("value")).getTime();
			} catch (JSONException e) {
				e.printStackTrace();
			} catch (ParseException e) {
				e.printStackTrace();
			}
			resultList.add(new LdbcQuery2Result(friendId, firstName, lastName, messageId, content, creationDate));
		}

		resultReporter.report(resultsCount, resultList, ldbcQuery2);
	}
}
