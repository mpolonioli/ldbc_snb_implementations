package net.mpolonioli.ldbcimpls.incubator.rya.interactive.queries;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContent;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContentResult;

import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaClient;
import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaConnectionState;

public class LdbcShortQuery4MessageContentHandler implements OperationHandler<LdbcShortQuery4MessageContent, DbConnectionState> {

	private static DateFormat creationDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");

	public void executeOperation(
			LdbcShortQuery4MessageContent ldbcShortQuery4MessageContent,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		RyaClient ryaClient = (((RyaConnectionState) dbConnectionState).getClient());

		LdbcShortQuery4MessageContentResult result = null;

		long id = ldbcShortQuery4MessageContent.messageId();

		String query =
				"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" + 
						"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
						"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
						"\n" + 
						"SELECT ?creationDate ?content\n" + 
						"WHERE {\n" + 
						"{\n" + 
						"?message snvoc:id \"" + id + "\"^^xsd:long ;\n" + 
						"	rdf:type snvoc:Post ;\n" + 
						"	snvoc:content | snvoc:imageFile ?content;\n" + 
						"	snvoc:creationDate ?creationDate .\n" + 
						"}\n" + 
						"UNION\n" + 
						"{\n" + 
						"?message snvoc:id \"" + id + "\"^^xsd:long ;\n" + 
						"	rdf:type snvoc:Comment ;\n" + 
						"	snvoc:content | snvoc:imageFile ?content;\n" + 
						"	snvoc:creationDate ?creationDate .\n" + 
						"}\n" + 
						"}"
						;

		JSONArray jsonBindings = ryaClient.executeReadQuery(query);

		if (jsonBindings.length() == 1)
		{
			JSONObject jsonObject = jsonBindings.getJSONObject(0);

			String content = jsonObject.getJSONObject("content").getString("value");
			long messageDate = 0;
			try {
				messageDate = creationDateFormat.parse(jsonObject.getJSONObject("creationDate").getString("value")).getTime();
			} catch (JSONException e) {
				e.printStackTrace();
			} catch (ParseException e) {
				e.printStackTrace();
			}

			result = new LdbcShortQuery4MessageContentResult(content, messageDate);
			resultReporter.report(1, result, ldbcShortQuery4MessageContent);	
		}
		else
		{
			result = new LdbcShortQuery4MessageContentResult("none", -1);
			resultReporter.report(-1, result, ldbcShortQuery4MessageContent);
		}
	}
}
