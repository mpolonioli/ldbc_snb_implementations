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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfile;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfileResult;

import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaClient;
import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaConnectionState;

public class LdbcShortQuery1PersonProfileHandler implements OperationHandler<LdbcShortQuery1PersonProfile, DbConnectionState> {
	
	private static DateFormat creationDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
	private static DateFormat birthdateDateFormat = new SimpleDateFormat("yyyy-MM-dd");
	
	public void executeOperation(
			LdbcShortQuery1PersonProfile ldbcShortQuery1PersonProfile,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		RyaClient ryaClient = (((RyaConnectionState) dbConnectionState).getClient());

		LdbcShortQuery1PersonProfileResult result = null;

		long id = ldbcShortQuery1PersonProfile.personId();

		String query =
				"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" + 
						"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
						"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
						"\n" + 
						"SELECT ?firstName ?lastName ?birthday ?locationIp ?browserUsed ?placeId ?gender ?creationDate \n" + 
						"WHERE {\n" + 
						"?person rdf:type snvoc:Person ;\n" + 
						"	snvoc:id \"" + id + "\"^^xsd:long ;\n" + 
						"	snvoc:firstName ?firstName ;\n" + 
						"	snvoc:lastName ?lastName ;\n" + 
						"	snvoc:birthday ?birthday ;\n" + 
						"	snvoc:locationIP ?locationIp ;\n" + 
						"	snvoc:browserUsed ?browserUsed ;\n" + 
						"	snvoc:gender ?gender ;\n" + 
						"	snvoc:creationDate ?creationDate ;\n" + 
						"	snvoc:isLocatedIn ?place .\n" + 
						"\n" + 
						"?place snvoc:id ?placeId .\n" + 
						"}"
						;


		JSONArray jsonBindings = ryaClient.executeReadQuery(query);

		if (jsonBindings.length() == 1)
		{
			JSONObject jsonObject = jsonBindings.getJSONObject(0);

			String firstName = jsonObject.getJSONObject("firstName").getString("value");
			String lastName = jsonObject.getJSONObject("lastName").getString("value");
			String locationIp = jsonObject.getJSONObject("locationIp").getString("value");
			String browserUsed = jsonObject.getJSONObject("browserUsed").getString("value");
			String gender = jsonObject.getJSONObject("gender").getString("value");
			long placeId = jsonObject.getJSONObject("placeId").getLong("value");

			long birthday = 0;
			try {
				birthday = birthdateDateFormat.parse(jsonObject.getJSONObject("birthday").getString("value")).getTime();
			} catch (JSONException e) {
				e.printStackTrace();
			} catch (ParseException e) {
				e.printStackTrace();
			}
			long creationDate = 0;
			try {
				creationDate = creationDateFormat.parse(jsonObject.getJSONObject("creationDate").getString("value")).getTime();
			} catch (JSONException e) {
				e.printStackTrace();
			} catch (ParseException e) {
				e.printStackTrace();
			}

			result = new LdbcShortQuery1PersonProfileResult(firstName, lastName, birthday, locationIp, browserUsed, placeId, gender, creationDate);
			resultReporter.report(1, result, ldbcShortQuery1PersonProfile);
		}
		else
		{
			result = new LdbcShortQuery1PersonProfileResult("none", "none", -1, "none", "none", -1, "none", -1);
			resultReporter.report(-1, result, ldbcShortQuery1PersonProfile);
		}
	}
}
