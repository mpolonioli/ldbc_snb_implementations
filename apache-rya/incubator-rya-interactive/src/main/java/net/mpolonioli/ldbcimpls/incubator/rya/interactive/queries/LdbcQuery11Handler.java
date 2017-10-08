package net.mpolonioli.ldbcimpls.incubator.rya.interactive.queries;

import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11Result;

import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaClient;
import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaConnectionState;

public class LdbcQuery11Handler implements OperationHandler<LdbcQuery11, DbConnectionState> {

	public void executeOperation(
			LdbcQuery11 ldbcQuery11,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		RyaClient ryaClient = (((RyaConnectionState) dbConnectionState).getClient());

		List<LdbcQuery11Result> resultList = new ArrayList<LdbcQuery11Result>();
		int resultsCount = 0;

		long id = ldbcQuery11.personId();
		int limit = ldbcQuery11.limit();
		int workFromYear = ldbcQuery11.workFromYear();
		String countryName = ldbcQuery11.countryName();

		String query =
				"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" + 
						"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
						"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
						"PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n" + 
						"\n" + 
						"SELECT DISTINCT ?personId ?firstName ?lastName ?companyName ?classYear\n" + 
						"WHERE {\n" + 
						"{\n" + 
						"?person rdf:type snvoc:Person ;\n" + 
						"	snvoc:id \"" + id + "\"^^xsd:long ;\n" + 
						"	snvoc:knows ?knowObject .\n" + 
						"\n" + 
						"?knowObject snvoc:hasPerson ?friend .\n" + 
						"\n" + 
						"?friend snvoc:id ?personId ;\n" + 
						"	snvoc:workAt ?workObject ;\n" + 
						"	snvoc:firstName ?firstName ;\n" + 
						"	snvoc:lastName ?lastName .\n" + 
						"\n" + 
						"?workObject snvoc:hasOrganisation ?company ;\n" + 
						"	snvoc:workFrom ?classYear .\n" + 
						"\n" + 
						"?company snvoc:isLocatedIn ?country ;\n" + 
						"	foaf:name ?companyName .\n" + 
						"\n" + 
						"?country foaf:name \"" + countryName + "\" .\n" + 
						"\n" + 
						"FILTER(?classYear < " + workFromYear + ")\n" + 
						"}\n" + 
						"UNION\n" + 
						"{\n" + 
						"?person rdf:type snvoc:Person ;\n" + 
						"	snvoc:id \"" + id + "\"^^xsd:long ;\n" + 
						"	snvoc:knows ?knowObject1 .\n" + 
						"\n" + 
						"?knowObject1 snvoc:hasPerson ?friend1 .\n" + 
						"\n" + 
						"?friend1 snvoc:knows ?knowObject2 .\n" + 
						"\n" + 
						"?knowObject2 snvoc:hasPerson ?friend2 .\n" + 
						"\n" + 
						"?friend2 snvoc:id ?personId ;\n" + 
						"	snvoc:workAt ?workObject ;\n" + 
						"	snvoc:firstName ?firstName ;\n" + 
						"	snvoc:lastName ?lastName .\n" + 
						"\n" + 
						"?workObject snvoc:hasOrganisation ?company ;\n" + 
						"	snvoc:workFrom ?classYear .\n" + 
						"\n" + 
						"?company snvoc:isLocatedIn ?country ;\n" + 
						"	foaf:name ?companyName .\n" + 
						"\n" + 
						"?country foaf:name \"" + countryName + "\" .\n" + 
						"\n" + 
						"FILTER(?friend2 != ?person)\n" + 
						"FILTER(?classYear < " + workFromYear + ")\n" + 
						"}\n" + 
						"}\n" + 
						"ORDER BY ASC(?classYear) ASC(?personId) DESC(?companyName)\n" + 
						"LIMIT " + limit
						;

		JSONArray jsonBindings = ryaClient.executeReadQuery(query);

		resultsCount = jsonBindings.length();

		for(int i = 0; i < resultsCount; i++) {
			JSONObject jsonObject = jsonBindings.getJSONObject(i);

			long personId = jsonObject.getJSONObject("personId").getLong("value");
			String firstName = jsonObject.getJSONObject("firstName").getString("value");
			String lastName = jsonObject.getJSONObject("lastName").getString("value");
			String organizationName = jsonObject.getJSONObject("companyName").getString("value");
			int organizationWorkFromYear = jsonObject.getJSONObject("classYear").getInt("value");

			resultList.add(new LdbcQuery11Result(personId, firstName, lastName, organizationName, organizationWorkFromYear));
		}
		resultReporter.report(resultsCount, resultList, ldbcQuery11);
	}
}
