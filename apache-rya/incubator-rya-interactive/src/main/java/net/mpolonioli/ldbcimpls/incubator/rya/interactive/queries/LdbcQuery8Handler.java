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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8Result;

import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaClient;
import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaConnectionState;

public class LdbcQuery8Handler implements OperationHandler<LdbcQuery8, DbConnectionState> {
	
	private static DateFormat creationDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");

	public void executeOperation(
			LdbcQuery8 ldbcQuery8,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		RyaClient ryaClient = (((RyaConnectionState) dbConnectionState).getClient());

		List<LdbcQuery8Result> resultList = new ArrayList<LdbcQuery8Result>();
		int resultsCount = 0;

		long id = ldbcQuery8.personId();
		int limit = ldbcQuery8.limit();

		String query =
				"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" + 
						"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
						"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
						"\n" + 
						"SELECT DISTINCT ?replierId ?firstName ?lastName ?commentDate ?commentId ?content\n" + 
						"WHERE {\n" + 
						"{\n" + 
						"?person rdf:type snvoc:Person ;\n" + 
						"	snvoc:id \"" + id + "\"^^xsd:long .\n" + 
						"\n" + 
						"?message rdf:type snvoc:Post ;\n" + 
						"	 snvoc:hasCreator ?person .\n" + 
						"\n" + 
						"\n" + 
						"?comment rdf:type snvoc:Comment ;\n" + 
						"	snvoc:replyOf ?message ;\n" + 
						"	snvoc:id ?commentId ;\n" + 
						"	snvoc:content ?content ;\n" + 
						"	snvoc:creationDate ?commentDate ;\n" + 
						"	snvoc:hasCreator ?replier .\n" + 
						"\n" + 
						"?replier snvoc:id ?replierId ;\n" + 
						"	snvoc:firstName ?firstName ;\n" + 
						"	snvoc:lastName ?lastName .\n" + 
						"}\n" + 
						"UNION\n" + 
						"{\n" + 
						"?person rdf:type snvoc:Person ;\n" + 
						"	snvoc:id \"" + id + "\"^^xsd:long .\n" + 
						"\n" + 
						"?message rdf:type snvoc:Comment ;\n" + 
						"         snvoc:hasCreator ?person .\n" + 
						"\n" + 
						"?comment rdf:type snvoc:Comment ;\n" + 
						"	snvoc:replyOf ?message ;\n" + 
						"	snvoc:id ?commentId ;\n" + 
						"	snvoc:content ?content ;\n" + 
						"	snvoc:creationDate ?commentDate ;\n" + 
						"	snvoc:hasCreator ?replier .\n" + 
						"\n" + 
						"?replier snvoc:id ?replierId ;\n" + 
						"	snvoc:firstName ?firstName ;\n" + 
						"	snvoc:lastName ?lastName .\n" + 
						"}\n" + 
						"}\n" + 
						"ORDER BY DESC(?commentDate) ASC(?commentId)\n" + 
						"LIMIT " + limit
						;

		JSONArray jsonBindings = ryaClient.executeReadQuery(query);


		resultsCount = jsonBindings.length();

		for(int i = 0; i < resultsCount; i++) {
			JSONObject jsonObject = jsonBindings.getJSONObject(i);

			long commentDate = 0;
			try {
				commentDate = creationDateFormat.parse(jsonObject.getJSONObject("commentDate").getString("value")).getTime();
			} catch (JSONException e) {
				e.printStackTrace();
			} catch (ParseException e) {
				e.printStackTrace();
			}
			long replierId = jsonObject.getJSONObject("replierId").getLong("value");
			long commentId = jsonObject.getJSONObject("commentId").getLong("value");
			String firstName = jsonObject.getJSONObject("firstName").getString("value");
			String lastName = jsonObject.getJSONObject("lastName").getString("value");
			String commentContent = jsonObject.getJSONObject("content").getString("value");

			resultList.add(new LdbcQuery8Result(replierId, firstName, lastName, commentDate, commentId, commentContent));

		}
		resultReporter.report(resultsCount, resultList, ldbcQuery8);		
	}
}
