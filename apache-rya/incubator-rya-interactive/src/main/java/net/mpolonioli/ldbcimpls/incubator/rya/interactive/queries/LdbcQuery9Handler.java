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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9Result;

import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaClient;
import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaConnectionState;

public class LdbcQuery9Handler implements OperationHandler<LdbcQuery9, DbConnectionState> {
	
	private static DateFormat creationDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");

	public void executeOperation(
			LdbcQuery9 ldbcQuery9,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		RyaClient ryaClient = (((RyaConnectionState) dbConnectionState).getClient());

		List<LdbcQuery9Result> resultList = new ArrayList<LdbcQuery9Result>();
		int resultsCount = 0;

		long id = ldbcQuery9.personId();
		int limit = ldbcQuery9.limit();
		String maxDate = creationDateFormat.format(ldbcQuery9.maxDate()) + ":00";

		String query =
				"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" + 
						"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
						"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
						"\n" + 
						"SELECT ?personId ?firstName ?lastName ?messageId ?content ?messageDate\n" + 
						"WHERE {\n" + 
						"{\n" + 
						"SELECT DISTINCT ?personId ?firstName ?lastName ?messageId ?content ?messageDate\n" + 
						"WHERE {\n" + 
						"{\n" + 
						"?person rdf:type snvoc:Person ;\n" + 
						"	snvoc:id \"" + id + "\"^^xsd:long ;\n" + 
						"	snvoc:knows ?knowObject .\n" + 
						"\n" + 
						"?knowObject snvoc:hasPerson ?friend .\n" + 
						"\n" + 
						"?friend snvoc:id ?personId ;\n" + 
						"	snvoc:firstName ?firstName ;\n" + 
						"	snvoc:lastName ?lastName .\n" + 
						"\n" + 
						"?message snvoc:hasCreator ?friend ;\n" + 
						"	rdf:type snvoc:Post ;\n" + 
						"	snvoc:creationDate ?messageDate ;\n" + 
						"	snvoc:id ?messageId ;\n" + 
						"	snvoc:imageFile | snvoc:content ?content .\n" + 
						"\n" + 
						"}\n" + 
						"UNION\n" + 
						"{\n" + 
						"?person rdf:type snvoc:Person ;\n" + 
						"	snvoc:id \"" + id + "\"^^xsd:long ;\n" + 
						"	snvoc:knows ?knowObject .\n" + 
						"\n" + 
						"?knowObject snvoc:hasPerson ?friend .\n" + 
						"\n" + 
						"?friend snvoc:id ?personId ;\n" + 
						"	snvoc:firstName ?firstName ;\n" + 
						"	snvoc:lastName ?lastName .\n" + 
						"\n" + 
						"?message snvoc:hasCreator ?friend ;\n" + 
						"	rdf:type snvoc:Comment ;\n" + 
						"	snvoc:creationDate ?messageDate ;\n" + 
						"	snvoc:id ?messageId ;\n" + 
						"	snvoc:imageFile | snvoc:content ?content .\n" + 
						"\n" + 
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
						"	snvoc:firstName ?firstName ;\n" + 
						"	snvoc:lastName ?lastName .\n" + 
						"\n" + 
						"?message snvoc:hasCreator ?friend2 ;\n" + 
						"	rdf:type snvoc:Post ;\n" + 
						"	snvoc:creationDate ?messageDate ;\n" + 
						"	snvoc:id ?messageId ;\n" + 
						"	snvoc:imageFile | snvoc:content ?content .\n" + 
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
						"	snvoc:firstName ?firstName ;\n" + 
						"	snvoc:lastName ?lastName .\n" + 
						"\n" + 
						"?message snvoc:hasCreator ?friend2 ;\n" + 
						"	rdf:type snvoc:Comment ;\n" + 
						"	snvoc:creationDate ?messageDate ;\n" + 
						"	snvoc:id ?messageId ;\n" + 
						"	snvoc:imageFile | snvoc:content ?content .\n" + 
						"}\n" + 
						"}\n" + 
						"}\n" + 
						"FILTER(?personId != \"" + id + "\"^^xsd:long)\n" + 
						"FILTER(?messageDate < \"" + maxDate + "\"^^xsd:dateTime)\n" + 
						"}\n" + 
						"ORDER BY DESC(?creationDate) ASC(?messageId)\n" + 
						"LIMIT " + limit
						;

		JSONArray jsonBindings = ryaClient.executeReadQuery(query);

		resultsCount = jsonBindings.length();

		for(int i = 0; i < resultsCount; i++) {
			JSONObject jsonObject = jsonBindings.getJSONObject(i);

			long messageDate = 0;
			try {
				messageDate = creationDateFormat.parse(jsonObject.getJSONObject("messageDate").getString("value")).getTime();
			} catch (JSONException e) {
				e.printStackTrace();
			} catch (ParseException e) {
				e.printStackTrace();
			}
			long personId = jsonObject.getJSONObject("personId").getLong("value");
			long messageId = jsonObject.getJSONObject("messageId").getLong("value");
			String firstName = jsonObject.getJSONObject("firstName").getString("value");
			String lastName = jsonObject.getJSONObject("lastName").getString("value");
			String content = jsonObject.getJSONObject("content").getString("value");

			resultList.add(new LdbcQuery9Result(personId, firstName, lastName, messageId, content, messageDate));
		}
		resultReporter.report(resultsCount, resultList, ldbcQuery9);
	}
}