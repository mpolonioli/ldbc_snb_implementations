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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageReplies;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageRepliesResult;

import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaClient;
import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaConnectionState;

public class LdbcShortQuery7MessageRepliesHandler implements OperationHandler<LdbcShortQuery7MessageReplies, DbConnectionState> {
	
	private static DateFormat creationDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");

	public void executeOperation(
			LdbcShortQuery7MessageReplies ldbcShortQuery7MessageReplies,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		RyaClient ryaClient = (((RyaConnectionState) dbConnectionState).getClient());

		List<LdbcShortQuery7MessageRepliesResult> resultList = new ArrayList<LdbcShortQuery7MessageRepliesResult>();
		int resultsCount = 0;

		long id = ldbcShortQuery7MessageReplies.messageId();

		String query =
				"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" + 
						"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
						"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
						"SELECT ?commentId ?commentContent ?commentCreationDate ?personId ?firstName ?lastName ?boolean\n" + 
						"WHERE {\n" + 
						"{\n" + 
						"?message snvoc:id \"" + id + "\"^^xsd:long  ;\n" + 
						"  rdf:type snvoc:Post ;\n" + 
						"  snvoc:hasCreator ?author .\n" + 
						"\n" + 
						"?comment snvoc:replyOf ?message ;\n" + 
						"  snvoc:id ?commentId ;\n" + 
						"  snvoc:content ?commentContent ;\n" + 
						"  snvoc:hasCreator ?person ;\n" + 
						"  snvoc:creationDate ?commentCreationDate .\n" + 
						"\n" + 
						"  ?person snvoc:knows ?knowObject .\n" + 
						"\n" + 
						"?person snvoc:id ?personId ;\n" + 
						"  snvoc:firstName ?firstName ;\n" + 
						"  snvoc:lastName ?lastName .\n" + 
						"\n" + 
						"BIND(IF(EXISTS{?knowObject snvoc:hasPerson ?author} ,true ,false) AS ?boolean)\n" + 
						"}\n" + 
						"UNION\n" + 
						"{\n" + 
						"?message snvoc:id \"" + id + "\"^^xsd:long  ;\n" + 
						"  rdf:type snvoc:Comment ;\n" + 
						"  snvoc:hasCreator ?author .\n" + 
						"\n" + 
						"?comment snvoc:replyOf ?message ;\n" + 
						"  snvoc:id ?commentId ;\n" + 
						"  snvoc:content ?commentContent ;\n" + 
						"  snvoc:hasCreator ?person ;\n" + 
						"  snvoc:creationDate ?commentCreationDate .\n" + 
						" \n" + 
						"?person snvoc:knows ?knowObject . \n" + 
						"\n" + 
						"?person snvoc:id ?personId ;\n" + 
						"  snvoc:firstName ?firstName ;\n" + 
						"  snvoc:lastName ?lastName .\n" + 
						"\n" + 
						"BIND(IF(EXISTS{?knowObject snvoc:hasPerson ?author} ,true ,false) AS ?boolean)\n" + 
						"}\n" + 
						"}\n" + 
						"ORDER BY DESC(?creationDate) ASC(?personId)"
						;

		JSONArray jsonBindings = ryaClient.executeReadQuery(query);

		resultsCount = jsonBindings.length();

		for (int i = 0; i < resultsCount; i++) {
			JSONObject jsonObject = jsonBindings.getJSONObject(i);

			String firstName = jsonObject.getJSONObject("firstName").getString("value");
			String lastName = jsonObject.getJSONObject("lastName").getString("value");
			long personId = jsonObject.getJSONObject("personId").getLong("value");
			long commentId = jsonObject.getJSONObject("commentId").getLong("value");
			String content = jsonObject.getJSONObject("commentContent").getString("value");
			boolean flag = jsonObject.getJSONObject("boolean").getBoolean("value");

			long commentDate = 0;
			try {
				commentDate = creationDateFormat.parse(jsonObject.getJSONObject("commentCreationDate").getString("value")).getTime();
			} catch (JSONException e) {
				e.printStackTrace();
			} catch (ParseException e) {
				e.printStackTrace();
			}

			resultList.add(new LdbcShortQuery7MessageRepliesResult(commentId, content, commentDate, personId, firstName, lastName, flag));
		}

		resultReporter.report(resultsCount, resultList, ldbcShortQuery7MessageReplies);
	}
}
