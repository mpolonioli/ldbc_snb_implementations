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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriends;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriendsResult;

import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaClient;
import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaConnectionState;

public class LdbcShortQuery3PersonFriendsHandler
implements OperationHandler<LdbcShortQuery3PersonFriends, DbConnectionState> {

	private static DateFormat creationDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");

	public void executeOperation(LdbcShortQuery3PersonFriends ldbcShortQuery3PersonFriends,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		RyaClient client = (((RyaConnectionState) dbConnectionState).getClient());

		List<LdbcShortQuery3PersonFriendsResult> resultList = new ArrayList<LdbcShortQuery3PersonFriendsResult>();
		int resultCount = 0;


		long personId = ldbcShortQuery3PersonFriends.personId();

		String query = 
				"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" + 
						"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
						"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
						"\n" + 
						"SELECT ?personId ?personFirstName ?personLastName ?knowCreationDate\n" + 
						"WHERE {\n" + 
						"	?person rdf:type snvoc:Person ;\n" + 
						"		snvoc:id \"" + personId + "\"^^xsd:long ;\n" + 
						"		snvoc:knows ?know .\n" + 
						"\n" + 
						"	?know snvoc:hasPerson ?friend ;\n" + 
						"		snvoc:creationDate ?knowCreationDate .\n" + 
						"\n" + 
						"	?friend snvoc:id ?personId ;\n" + 
						"		snvoc:firstName ?personFirstName ;\n" + 
						"		snvoc:lastName ?personLastName .\n" + 
						"}\n" + 
						"ORDER BY DESC(?creationDate) ASC(?personId)";

		JSONArray jsonBindings = client.executeReadQuery(query);

		resultCount = jsonBindings.length();

		for(int i = 0; i < resultCount; i++) {

			JSONObject jsonObject = jsonBindings.getJSONObject(i);

			long friendId = jsonObject.getJSONObject("personId").getLong("value");
			String friendFirstName = jsonObject.getJSONObject("personFirstName").getString("value");
			String friendLastName = jsonObject.getJSONObject("personLastName").getString("value");

			long friendshipCreationDate = 0;
			try {
				friendshipCreationDate = creationDateFormat.parse(jsonObject.getJSONObject("knowCreationDate").getString("value")).getTime();
			} catch (JSONException e) {
				e.printStackTrace();
			} catch (ParseException e) {
				e.printStackTrace();
			}

			resultList.add( new LdbcShortQuery3PersonFriendsResult(friendId, friendFirstName, friendLastName, friendshipCreationDate));
		}
		resultReporter.report(resultCount, resultList, ldbcShortQuery3PersonFriends);
	}
}
