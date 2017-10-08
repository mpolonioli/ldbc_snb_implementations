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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1Result;

import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaClient;
import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaConnectionState;

public class LdbcQuery1Handler implements OperationHandler<LdbcQuery1, DbConnectionState> {

	private static DateFormat creationDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
	private static DateFormat birthdateDateFormat = new SimpleDateFormat("yyyy-MM-dd");
	
	public void executeOperation(
			LdbcQuery1 ldbcQuery1,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		RyaClient ryaClient = (((RyaConnectionState) dbConnectionState).getClient());

		List<LdbcQuery1Result> resultList = new ArrayList<LdbcQuery1Result>();
		int resultsCount = 0;

		int limit = ldbcQuery1.limit();
		String firstName = ldbcQuery1.firstName();
		long id = ldbcQuery1.personId();

		String query = 
				"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" + 
						"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
						"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
						"PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n" + 
						"\n" + 
						"SELECT DISTINCT ?id ?lastName ?birthday ?creationDate ?gender ?browserUsed ?locationIp ?cityName ?dist\n" + 
						"WHERE{\n" + 
						"{\n" + 
						"?person rdf:type snvoc:Person ;\n" + 
						"	snvoc:id \"" + id + "\"^^xsd:long .\n" + 
						"\n" + 
						"?person snvoc:knows ?knowObject1 .\n" + 
						"\n" + 
						"?knowObject1 snvoc:hasPerson ?friend1 .\n" + 
						"\n" + 
						"?friend1 snvoc:firstName \"" + firstName + "\" ;\n" + 
						"	snvoc:id ?id ;\n" + 
						"	snvoc:lastName ?lastName ;\n" + 
						"	snvoc:birthday ?birthday ;\n" + 
						"	snvoc:creationDate ?creationDate ;\n" + 
						"	snvoc:gender ?gender ;\n" + 
						"	snvoc:browserUsed ?browserUsed ;\n" + 
						"	snvoc:locationIP ?locationIp ;\n" + 
						"	snvoc:isLocatedIn ?city .\n" + 
						"\n" + 
						"?city foaf:name ?cityName .\n" + 
						"\n" + 
						"BIND(1 AS ?dist)\n" + 
						"}\n" + 
						"UNION\n" + 
						"{\n" + 
						"?person rdf:type snvoc:Person ;\n" + 
						"	snvoc:id \"" + id + "\"^^xsd:long .\n" + 
						"\n" + 
						"?person snvoc:knows ?knowObject1 .\n" + 
						"\n" + 
						"?knowObject1 snvoc:hasPerson ?friend1 .\n" + 
						"\n" + 
						"?friend1 snvoc:knows ?knowObject2 .\n" + 
						"\n" + 
						"?knowObject2 snvoc:hasPerson ?friend2 .\n" + 
						"\n" + 
						"?friend2 snvoc:firstName \"" + firstName + "\" ;\n" + 
						"	snvoc:id ?id ;\n" + 
						"	snvoc:lastName ?lastName ;\n" + 
						"	snvoc:birthday ?birthday ;\n" + 
						"	snvoc:creationDate ?creationDate ;\n" + 
						"	snvoc:gender ?gender ;\n" + 
						"	snvoc:browserUsed ?browserUsed ;\n" + 
						"	snvoc:locationIP ?locationIp ;\n" + 
						"	snvoc:isLocatedIn ?city .\n" + 
						"\n" + 
						"?city foaf:name ?cityName .\n" + 
						"\n" + 
						"BIND(2 AS ?dist)\n" + 
						"}\n" + 
						"UNION\n" + 
						"{\n" + 
						"?person rdf:type snvoc:Person ;\n" + 
						"	snvoc:id \"" + id + "\"^^xsd:long .\n" + 
						"\n" + 
						"?person snvoc:knows ?knowObject1 .\n" + 
						"\n" + 
						"?knowObject1 snvoc:hasPerson ?friend1 .\n" + 
						"\n" + 
						"?friend1 snvoc:knows ?knowObject2 .\n" + 
						"\n" + 
						"?knowObject2 snvoc:hasPerson ?friend2 .\n" + 
						"\n" + 
						"?friend2 snvoc:knows ?knowObject3 .\n" + 
						"\n" + 
						"?knowObject3 snvoc:hasPerson ?friend3 .\n" + 
						"\n" + 
						"?friend3 snvoc:firstName \"" + firstName + "\" ;\n" + 
						"	snvoc:id ?id ;\n" + 
						"	snvoc:lastName ?lastName ;\n" + 
						"	snvoc:birthday ?birthday ;\n" + 
						"	snvoc:creationDate ?creationDate ;\n" + 
						"	snvoc:gender ?gender ;\n" + 
						"	snvoc:browserUsed ?browserUsed ;\n" + 
						"	snvoc:locationIP ?locationIp ;\n" + 
						"	snvoc:isLocatedIn ?city .\n" + 
						"\n" + 
						"?city foaf:name ?cityName .\n" + 
						"\n" + 
						"BIND(3 AS ?dist)\n" + 
						"}\n" + 
						"}\n" + 
						"ORDER BY ASC(?dist) ASC(?lastName) ASC(?id)\n" + 
						"LIMIT " + limit
						;
		JSONArray jsonBindings = ryaClient.executeReadQuery(query);	

		resultsCount = jsonBindings.length();
		for(int i = 0; i < resultsCount; i++) {
			JSONObject jsonObject = jsonBindings.getJSONObject(i);
			long friendId = jsonObject.getJSONObject("id").getLong("value");
			String lastName = jsonObject.getJSONObject("lastName").getString("value");
			long birthdate = 0;
			try {
				birthdate = birthdateDateFormat.parse(jsonObject.getJSONObject("birthday").getString("value")).getTime();
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
			String gender = jsonObject.getJSONObject("gender").getString("value");
			String browserUsed = jsonObject.getJSONObject("browserUsed").getString("value");
			String locationIP = jsonObject.getJSONObject("locationIp").getString("value");
			int distance = jsonObject.getJSONObject("dist").getInt("value");
			String cityName = jsonObject.getJSONObject("cityName").getString("value");

			String queryEmails =
					"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" + 
							"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
							"SELECT ?email\n" + 
							"WHERE {\n" + 
							"?person rdf:type snvoc:Person ;\n" + 
							"	snvoc:id \"" + friendId + "\"^^xsd:long ;\n" + 
							"	snvoc:email ?email .\n" + 
							"}"
							;
			JSONArray emailsBindings = ryaClient.executeReadQuery(queryEmails);
			List<String> emails = new ArrayList<String>();
			for(int j = 0; j < emailsBindings.length(); j++) {
				emails.add(emailsBindings.getJSONObject(j).getJSONObject("email").getString("value"));
			}

			String queryLanguages =
					"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" + 
							"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
							"SELECT ?language\n" + 
							"WHERE {\n" + 
							"?person rdf:type snvoc:Person ;\n" + 
							"	snvoc:id \"" + friendId + "\"^^xsd:long ;\n" + 
							"	snvoc:speaks ?language .\n" + 
							"}"
							;
			JSONArray languagesBindings = ryaClient.executeReadQuery(queryLanguages);
			List<String> languages = new ArrayList<String>();
			for(int j = 0; j < languagesBindings.length(); j++) {
				languages.add(languagesBindings.getJSONObject(j).getJSONObject("language").getString("value"));
			}

			String queryUniversities =
					"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" + 
							"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
							"PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n" + 
							"SELECT ?universityName ?classYear ?cityName\n" + 
							"WHERE {\n" + 
							"?person rdf:type snvoc:Person ;\n" + 
							"	snvoc:id \"" + friendId + "\"^^xsd:long ;\n" + 
							"	snvoc:studyAt ?studyObject .\n" + 
							"\n" + 
							"?studyObject snvoc:classYear ?classYear ;\n" + 
							"	snvoc:hasOrganisation ?university .\n" + 
							"\n" + 
							"?university foaf:name ?universityName ;\n" + 
							"	snvoc:isLocatedIn ?city .\n" + 
							"\n" + 
							"?city foaf:name ?cityName . \n" + 
							"}"
							;
			JSONArray universitiesBindings = ryaClient.executeReadQuery(queryUniversities);
			List<List<Object>> universities = new ArrayList<List<Object>>();
			for (int j = 0; j < universitiesBindings.length(); j ++) {
				List<Object> list = new ArrayList<Object>(3);
				list.add(0, universitiesBindings.getJSONObject(j).getJSONObject("universityName").getString("value"));
				list.add(1, universitiesBindings.getJSONObject(j).getJSONObject("classYear").getInt("value"));
				list.add(2, universitiesBindings.getJSONObject(j).getJSONObject("cityName").getString("value"));
				universities.add(list);
			}


			String queryCompanies =
					"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" + 
							"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
							"PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n" + 
							"\n" + 
							"SELECT ?companyName ?workFrom ?countryName\n" + 
							"WHERE {\n" + 
							"?person rdf:type snvoc:Person ;\n" + 
							"  snvoc:id \"" + friendId + "\"^^xsd:long ;\n" + 
							"  snvoc:workAt ?workObject .\n" + 
							"\n" + 
							"?workObject snvoc:workFrom ?workFrom ;\n" + 
							"  snvoc:hasOrganisation ?company .\n" + 
							"\n" + 
							"?company foaf:name ?companyName ;\n" + 
							"  snvoc:isLocatedIn ?country .\n" + 
							"\n" + 
							"?country foaf:name ?countryName . \n" + 
							"}"
							;
			JSONArray companiesBindings = ryaClient.executeReadQuery(queryCompanies);
			List<List<Object>> companies = new ArrayList<List<Object>>();
			for (int j = 0; j < companiesBindings.length(); j ++) {
				List<Object> list = new ArrayList<Object>(3);
				list.add(0, companiesBindings.getJSONObject(j).getJSONObject("companyName").getString("value"));
				list.add(1, companiesBindings.getJSONObject(j).getJSONObject("workFrom").getInt("value"));
				list.add(2, companiesBindings.getJSONObject(j).getJSONObject("countryName").getString("value"));
				companies.add(list);
			}

			resultList.add(new LdbcQuery1Result(friendId, lastName, distance, birthdate, creationDate, gender, browserUsed, locationIP, emails, languages, cityName, universities, companies));
		}

		resultReporter.report(resultsCount, resultList, ldbcQuery1);
	}
}