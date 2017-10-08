package net.mpolonioli.ldbcimpls.incubator.rya.interactive.queries;

import org.json.JSONArray;
import org.json.JSONObject;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreator;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreatorResult;

import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaClient;
import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaConnectionState;

public class LdbcShortQuery5MessageCreatorHandler implements OperationHandler<LdbcShortQuery5MessageCreator, DbConnectionState> {

	public void executeOperation(
			LdbcShortQuery5MessageCreator ldbcShortQuery5MessageCreator,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		RyaClient ryaClient = (((RyaConnectionState) dbConnectionState).getClient());

		LdbcShortQuery5MessageCreatorResult result = null;

		long id = ldbcShortQuery5MessageCreator.messageId();

		String query =
				"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" + 
						"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
						"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
						"SELECT ?personId ?firstName ?lastName\n" + 
						"WHERE {\n" + 
						"  {\n" + 
						"    ?message snvoc:id \"" + id + "\"^^xsd:long ;\n" + 
						"      rdf:type snvoc:Post ;\n" + 
						"      snvoc:hasCreator ?person .\n" + 
						"\n" + 
						"    ?person snvoc:id ?personId ;\n" + 
						"      snvoc:firstName ?firstName ;\n" + 
						"      snvoc:lastName ?lastName .\n" + 
						"  }\n" + 
						"  UNION\n" + 
						"  {\n" + 
						"    ?message snvoc:id \"" + id + "\"^^xsd:long ;\n" + 
						"      rdf:type snvoc:Comment ;\n" + 
						"      snvoc:hasCreator ?person .\n" + 
						"\n" + 
						"    ?person snvoc:id ?personId ;\n" + 
						"      snvoc:firstName ?firstName ;\n" + 
						"      snvoc:lastName ?lastName .\n" + 
						"  }\n" + 
						"}"
						;

		JSONArray jsonBindings = ryaClient.executeReadQuery(query);

		if (jsonBindings.length() == 1)
		{
			JSONObject jsonObject = jsonBindings.getJSONObject(0);

			long personId = jsonObject.getJSONObject("personId").getLong("value");
			String firstName = jsonObject.getJSONObject("firstName").getString("value");
			String lastName = jsonObject.getJSONObject("lastName").getString("value");

			result = new LdbcShortQuery5MessageCreatorResult(personId, firstName, lastName);
			resultReporter.report(1, result, ldbcShortQuery5MessageCreator);
		}
		else
		{
			result = new LdbcShortQuery5MessageCreatorResult(-1, "", "");
			resultReporter.report(-1, result, ldbcShortQuery5MessageCreator);
		}
	}
}
