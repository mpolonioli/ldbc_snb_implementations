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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPosts;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPostsResult;

import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaClient;
import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaConnectionState;

public class LdbcShortQuery2PersonPostsHandler implements OperationHandler<LdbcShortQuery2PersonPosts, DbConnectionState> {

	private static DateFormat creationDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");

	public void executeOperation(
			LdbcShortQuery2PersonPosts ldbcShortQuery2PersonPosts,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		RyaClient ryaClient = (((RyaConnectionState) dbConnectionState).getClient());

		List<LdbcShortQuery2PersonPostsResult> resultList = new ArrayList<LdbcShortQuery2PersonPostsResult>();
		int resultsCount = 0;

		long id = ldbcShortQuery2PersonPosts.personId();
		int limit = ldbcShortQuery2PersonPosts.limit();

		String query =
				"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" + 
						"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
						"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
						"SELECT ?messageId ?content ?creationDate ?postId ?personId ?firstName ?lastName\n" + 
						"WHERE {\n" + 
						"{\n" + 
						"?person rdf:type snvoc:Person ;\n" + 
						"	snvoc:id \"" + id + "\"^^xsd:long .\n" + 
						"\n" + 
						"?post snvoc:hasCreator ?person ;\n" + 
						"	rdf:type snvoc:Post ;\n" + 
						"	snvoc:id ?messageId ;\n" + 
						"	snvoc:content | snvoc:imageFile ?content ;\n" + 
						"	snvoc:creationDate ?creationDate ;\n" + 
						"	snvoc:id ?postId .\n" + 
						"\n" + 
						"?person snvoc:id ?personId ;\n" + 
						"	snvoc:firstName ?firstName ;\n" + 
						"	snvoc:lastName ?lastName .\n" + 
						"}\n" + 
						"UNION\n" + 
						"{\n" + 
						"?person rdf:type snvoc:Person ;\n" + 
						"	snvoc:id \"" + id + "\"^^xsd:long .\n" + 
						"\n" + 
						"?comment snvoc:hasCreator ?person ;\n" + 
						"	rdf:type snvoc:Comment ;\n" + 
						"	snvoc:id ?messageId ;\n" + 
						"	snvoc:content ?content ;\n" + 
						"	snvoc:creationDate ?creationDate .\n" + 
						"\n" + 
						"?comment snvoc:replyOf ?originalMessage .\n" + 
						"\n" + 
						"?originalMessage snvoc:id ?postId ;\n" + 
						"	snvoc:hasCreator ?originalMessagePerson .\n" + 
						"\n" + 
						"?originalMessagePerson snvoc:id ?personId ;\n" + 
						"	snvoc:firstName ?firstName ;\n" + 
						"	snvoc:lastName ?lastName .\n" + 
						"}\n" + 
						"}\n" + 
						"ORDER BY DESC(?creationDate) DESC(?messageId)\n" + 
						"LIMIT " + limit
						;

		JSONArray jsonBindings = ryaClient.executeReadQuery(query);

		resultsCount = jsonBindings.length();

		for (int i = 0; i < resultsCount; i++) {
			JSONObject jsonObject = jsonBindings.getJSONObject(i);

			String firstName = jsonObject.getJSONObject("firstName").getString("value");
			String lastName = jsonObject.getJSONObject("lastName").getString("value");
			long personId = jsonObject.getJSONObject("personId").getLong("value");
			long postId = jsonObject.getJSONObject("postId").getLong("value");
			String content = jsonObject.getJSONObject("content").getString("value");
			long messageId = jsonObject.getJSONObject("messageId").getLong("value");

			long messageDate = 0;
			try {
				messageDate = creationDateFormat.parse(jsonObject.getJSONObject("creationDate").getString("value")).getTime();
			} catch (JSONException e) {
				e.printStackTrace();
			} catch (ParseException e) {
				e.printStackTrace();
			}

			resultList.add(new LdbcShortQuery2PersonPostsResult(messageId, content, messageDate, postId, personId, firstName, lastName));
		}
		resultReporter.report(resultsCount, resultList, ldbcShortQuery2PersonPosts);
	}
}
