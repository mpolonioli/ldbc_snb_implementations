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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4Result;

import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaClient;
import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaConnectionState;

public class LdbcQuery4Handler implements OperationHandler<LdbcQuery4, DbConnectionState> {
	
	private static DateFormat creationDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
	
	public void executeOperation(
			LdbcQuery4 ldbcQuery4,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		RyaClient ryaClient = (((RyaConnectionState) dbConnectionState).getClient());

		List<LdbcQuery4Result> resultList = new ArrayList<LdbcQuery4Result>();
		int resultsCount = 0;

		long id = ldbcQuery4.personId();
		int limit = ldbcQuery4.limit();
		Date startDate = ldbcQuery4.startDate();
		int durationDays = ldbcQuery4.durationDays();
		long durationDaysInMilliseconds = (long) durationDays * 24 * 60 * 60 * 1000;
		Date endDate = new Date(startDate.getTime() + durationDaysInMilliseconds);

		String query = 
				"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" + 
						"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
						"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
						"PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n" + 
						"\n" + 
						"SELECT DISTINCT ?tagName (COUNT(?tagName) AS ?count)\n" + 
						"WHERE\n" + 
						"{\n" + 
						"?person rdf:type snvoc:Person ;\n" + 
						"	snvoc:id \"" + id + "\"^^xsd:long ;\n" + 
						"	snvoc:knows ?knowObject .\n" + 
						"\n" + 
						"?knowObject snvoc:hasPerson ?friend .\n" + 
						"\n" + 
						"?post rdf:type snvoc:Post ;\n" + 
						"	snvoc:hasCreator ?friend ;\n" + 
						"	snvoc:creationDate ?date ;\n" + 
						"	snvoc:hasTag ?tag .\n" + 
						"\n" + 
						"?tag foaf:name ?tagName .\n" + 
						"\n" + 
						"FILTER(?date < \"" + creationDateFormat.format(endDate) + ":00\"^^xsd:dateTime)\n" + 
						"FILTER(?date > \"" + creationDateFormat.format(startDate) + ":00\"^^xsd:dateTime)\n" + 
						"}\n" + 
						"GROUP BY ?tagName\n" + 
						"ORDER BY DESC(?count) ASC(?tagName)\n" + 
						"LIMIT " + limit
						;

		JSONArray jsonBindings = ryaClient.executeReadQuery(query);

		resultsCount = jsonBindings.length();
		if(resultsCount  == 1)
		{
			try
			{
				JSONObject jsonObject = jsonBindings.getJSONObject(0);

				String tagName = jsonObject.getJSONObject("tagName").getString("value");
				int count = jsonObject.getJSONObject("count").getInt("value");

				resultList.add(new LdbcQuery4Result(tagName, count));
			}catch(JSONException e)
			{
				resultReporter.report(0, resultList, ldbcQuery4);
			}
		}else
		{
			for(int i = 0; i < resultsCount; i++) {
				JSONObject jsonObject = jsonBindings.getJSONObject(i);

				String tagName = jsonObject.getJSONObject("tagName").getString("value");
				int count = jsonObject.getJSONObject("count").getInt("value");

				resultList.add(new LdbcQuery4Result(tagName, count));
			}
		}		
		resultReporter.report(resultsCount, resultList, ldbcQuery4);
	}
}
