package net.mpolonioli.ldbcimpls.incubator.rya.interactive.queries;

import java.text.DateFormat;
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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5Result;

import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaClient;
import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaConnectionState;

public class LdbcQuery5Handler implements OperationHandler<LdbcQuery5, DbConnectionState> {
	
	private static DateFormat creationDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
	
	public void executeOperation(
			LdbcQuery5 ldbcQuery5,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		RyaClient ryaClient = (((RyaConnectionState) dbConnectionState).getClient());

		List<LdbcQuery5Result> resultList = new ArrayList<LdbcQuery5Result>();
		int resultsCount = 0;

		long id = ldbcQuery5.personId();
		int limit = ldbcQuery5.limit();
		Date minDate = ldbcQuery5.minDate();

		String query =
				"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" + 
						"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
						"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
						"PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n" + 
						"\n" + 
						"SELECT DISTINCT ?title ?count\n" + 
						"WHERE {\n" + 
						"{\n" + 
						"SELECT DISTINCT ?title (COUNT(?post) AS ?count)\n" + 
						"WHERE {\n" + 
						"	?person rdf:type snvoc:Person ;\n" + 
						"		snvoc:id \"" + id + "\"^^xsd:long ;\n" + 
						"		snvoc:knows ?knowObject .\n" + 
						"\n" + 
						"	?knowObject snvoc:hasPerson ?friend .\n" + 
						"\n" + 
						"	?forum rdf:type snvoc:Forum ;\n" + 
						"		snvoc:hasMember ?memberObject ;\n" + 
						"		snvoc:id ?id ;\n" + 
						"		snvoc:title ?title .\n" + 
						"\n" + 
						"	?memberObject snvoc:hasPerson ?friend ;\n" + 
						"		snvoc:joinDate ?joinDate .\n" + 
						"\n" + 
						"	FILTER(?joinDate > \"" + creationDateFormat.format(minDate) + ":00\"^^xsd:dateTime)\n" + 
						"\n" + 
						"	?forum snvoc:containerOf ?post .\n" + 
						"\n" + 
						"	?post rdf:type snvoc:Post ;\n" + 
						"		snvoc:hasCreator ?friend .\n" + 
						"	}\n" + 
						"GROUP BY ?title\n" + 
						"}\n" + 
						"UNION\n" + 
						"{\n" + 
						"SELECT DISTINCT ?title (COUNT(?post) AS ?count)\n" + 
						"WHERE {\n" + 
						"	?person rdf:type snvoc:Person ;\n" + 
						"		snvoc:id \"" + id + "\"^^xsd:long ;\n" + 
						"		snvoc:knows ?knowObject1 .\n" + 
						"\n" + 
						"	?knowObject1 snvoc:hasPerson ?friend1 .\n" + 
						"\n" + 
						"	?friend1 snvoc:knows ?knowObject2 .\n" + 
						"\n" + 
						"	?knowObject2 snvoc:hasPerson ?friend2 .\n" + 
						"\n" + 
						"        ?friend2 snvoc:id ?friend2Id .\n" + 
						"\n" + 
						"        FILTER(?friend2Id != \"" + id + "\"^^xsd:long)\n" + 
						"\n" + 
						"	?forum rdf:type snvoc:Forum ;\n" + 
						"		snvoc:hasMember ?memberObject ;\n" + 
						"		snvoc:id ?id ;\n" + 
						"		snvoc:title ?title .\n" + 
						"\n" + 
						"	?memberObject snvoc:hasPerson ?friend2 ;\n" + 
						"		snvoc:joinDate ?joinDate .\n" + 
						"\n" + 
						"	FILTER(?joinDate > \"" + creationDateFormat.format(minDate) + ":00\"^^xsd:dateTime)\n" + 
						"\n" + 
						"	?forum snvoc:containerOf ?post .\n" + 
						"\n" + 
						"	?post rdf:type snvoc:Post ;\n" + 
						"		snvoc:hasCreator ?friend2 .\n" + 
						"	}\n" + 
						"GROUP BY ?title\n" + 
						"}\n" + 
						"}\n" + 
						"ORDER BY DESC(?count) ASC(?id)\n" + 
						"LIMIT " + limit
						;

		JSONArray jsonBindings = ryaClient.executeReadQuery(query);

		resultsCount = jsonBindings.length();
		if(resultsCount  == 1)
		{
			try
			{
				JSONObject jsonObject = jsonBindings.getJSONObject(0);

				String title = jsonObject.getJSONObject("title").getString("value");
				int count = jsonObject.getJSONObject("count").getInt("value");

				resultList.add(new LdbcQuery5Result(title, count));
			}catch(JSONException e)
			{
				resultReporter.report(0, resultList, ldbcQuery5);
			}
		}else
			for(int i = 0; i < resultsCount; i++) {
				JSONObject jsonObject = jsonBindings.getJSONObject(i);

				String title = jsonObject.getJSONObject("title").getString("value");
				int count = jsonObject.getJSONObject("count").getInt("value");

				resultList.add(new LdbcQuery5Result(title, count));
			}

		resultReporter.report(resultsCount, resultList, ldbcQuery5);
	}
}