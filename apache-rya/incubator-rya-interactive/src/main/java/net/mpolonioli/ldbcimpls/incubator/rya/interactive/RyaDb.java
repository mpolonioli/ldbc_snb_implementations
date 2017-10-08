package net.mpolonioli.ldbcimpls.incubator.rya.interactive;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.workloads.ldbc.snb.interactive.*;

public class RyaDb extends Db{

	private DbConnectionState dbConnectionState = null;
	private static DateFormat creationDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
	private static DateFormat birthdateDateFormat = new SimpleDateFormat("yyyy-MM-dd");
	
	@Override
	protected DbConnectionState getConnectionState() throws DbException {
		return dbConnectionState;	
	}

	@Override
	protected void onClose() throws IOException {
		dbConnectionState.close();
	}

	@Override
	protected void onInit(Map<String, String> ryaDbProperties, LoggingService log) throws DbException {
		dbConnectionState = new RyaConnectionState("http://siti-rack.siti.disco.unimib.it:8080/web.rya/queryrdf", "http://siti-rack.siti.disco.unimib.it:8080/web.rya/loadrdf");
		registerOperationHandler(LdbcQuery1.class, LdbcQuery1Handler.class);
		registerOperationHandler(LdbcQuery2.class, LdbcQuery2Handler.class);
		registerOperationHandler(LdbcQuery3.class, LdbcQuery3Handler.class);
		registerOperationHandler(LdbcQuery4.class, LdbcQuery4Handler.class);
		registerOperationHandler(LdbcQuery5.class, LdbcQuery5Handler.class);
		registerOperationHandler(LdbcQuery6.class, LdbcQuery6Handler.class);
		registerOperationHandler(LdbcQuery7.class, LdbcQuery7Handler.class);
		registerOperationHandler(LdbcQuery8.class, LdbcQuery8Handler.class);
		registerOperationHandler(LdbcQuery9.class, LdbcQuery9Handler.class);
		registerOperationHandler(LdbcQuery10.class, LdbcQuery10Handler.class);
		registerOperationHandler(LdbcQuery11.class, LdbcQuery11Handler.class);
		registerOperationHandler(LdbcQuery12.class, LdbcQuery12Handler.class);
		registerOperationHandler(LdbcQuery13.class, LdbcQuery13Handler.class);
		registerOperationHandler(LdbcQuery14.class, LdbcQuery14Handler.class);
		registerOperationHandler(LdbcShortQuery1PersonProfile.class, LdbcShortQuery1PersonProfileHandler.class);
		registerOperationHandler(LdbcShortQuery2PersonPosts.class, LdbcShortQuery2PersonPostsHandler.class);
		registerOperationHandler(LdbcShortQuery3PersonFriends.class, LdbcShortQuery3PersonFriendsHandler.class);
		registerOperationHandler(LdbcShortQuery4MessageContent.class, LdbcShortQuery4MessageContentHandler.class);
		registerOperationHandler(LdbcShortQuery5MessageCreator.class, LdbcShortQuery5MessageCreatorHandler.class);
		registerOperationHandler(LdbcShortQuery6MessageForum.class, LdbcShortQuery6MessageForumHandler.class);
		registerOperationHandler(LdbcShortQuery7MessageReplies.class, LdbcShortQuery7MessageRepliesHandler.class);
		registerOperationHandler(LdbcUpdate1AddPerson.class, LdbcUpdate1AddPersonHandler.class);
		registerOperationHandler(LdbcUpdate2AddPostLike.class, LdbcUpdate2AddPostLikeHandler.class);
		registerOperationHandler(LdbcUpdate3AddCommentLike.class, LdbcUpdate3AddCommentLikeHandler.class);
		registerOperationHandler(LdbcUpdate4AddForum.class, LdbcUpdate4AddForumHandler.class);
		registerOperationHandler(LdbcUpdate5AddForumMembership.class, LdbcUpdate5AddForumMembershipHandler.class);
		registerOperationHandler(LdbcUpdate6AddPost.class, LdbcUpdate6AddPostHandler.class);
		registerOperationHandler(LdbcUpdate7AddComment.class, LdbcUpdate7AddCommentHandler.class);
		registerOperationHandler(LdbcUpdate8AddFriendship.class, LdbcUpdate8AddFriendshipHandler.class);
	}

	
	/*
	 * COMPLEX QUERIES
	 */
	
	public static class LdbcQuery1Handler implements OperationHandler<LdbcQuery1, DbConnectionState> {

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
	
	public static class LdbcQuery2Handler implements OperationHandler<LdbcQuery2, DbConnectionState> {

		
		public void executeOperation(
				LdbcQuery2 ldbcQuery2,
				DbConnectionState dbConnectionState,
				ResultReporter resultReporter) throws DbException {
			
			RyaClient ryaClient = (((RyaConnectionState) dbConnectionState).getClient());
			
			List<LdbcQuery2Result> resultList = new ArrayList<LdbcQuery2Result>();
			int resultsCount = 0;
			
			long id = ldbcQuery2.personId();
			Date maxDate = ldbcQuery2.maxDate();
			int limit = ldbcQuery2.limit();
			
			String query = 
					"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" + 
					"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
					"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
					"\n" + 
					"SELECT ?friendId ?firstName ?lastName ?messageId ?content ?creationDate\n" + 
					"WHERE {\n" + 
					"{\n" + 
					"?person rdf:type snvoc:Person ;\n" + 
					"	snvoc:id \"" + id + "\"^^xsd:long .\n" + 
					"\n" + 
					"?person snvoc:knows ?knowObject .\n" + 
					"\n" + 
					"?knowObject snvoc:hasPerson ?friend .\n" + 
					"\n" + 
					"?post rdf:type snvoc:Post ;\n" + 
					"        snvoc:id ?messageId ;\n" + 
					"	snvoc:hasCreator ?friend ;\n" + 
					"	snvoc:creationDate ?creationDate ;\n" + 
					"	snvoc:imageFile | snvoc:content ?content .\n" + 
					"\n" + 
					"?friend snvoc:id ?friendId ;\n" + 
					"	snvoc:firstName ?firstName;\n" + 
					"	snvoc:lastName ?lastName .\n" + 
					"\n" + 
					"FILTER(?creationDate <= \"" + creationDateFormat.format(maxDate) + ":00\"^^xsd:dateTime)\n" + 
					"}\n" + 
					"UNION\n" + 
					"{\n" + 
					"?person rdf:type snvoc:Person ;\n" + 
					"	snvoc:id \"" + id + "\"^^xsd:long .\n" + 
					"\n" + 
					"?person snvoc:knows ?knowObject .\n" + 
					"\n" + 
					"?knowObject snvoc:hasPerson ?friend .\n" + 
					"\n" + 
					"?comment rdf:type snvoc:Comment ;\n" + 
					"snvoc:id ?messageId ;\n" + 
					"	snvoc:hasCreator ?friend ;\n" + 
					"	snvoc:creationDate ?creationDate ;\n" + 
					"	snvoc:content ?content .\n" + 
					"\n" + 
					"?friend snvoc:id ?friendId ;\n" + 
					"	snvoc:firstName ?firstName;\n" + 
					"	snvoc:lastName ?lastName .\n" + 
					"\n" + 
					"FILTER(?creationDate <= \"" + creationDateFormat.format(maxDate) + ":00\"^^xsd:dateTime)\n" + 
					"}\n" + 
					"}\n" + 
					"ORDER BY DESC(?creationDate) ASC(?id)\n" + 
					"LIMIT " + limit
					;
			
			JSONArray jsonBindings = ryaClient.executeReadQuery(query);

			resultsCount = jsonBindings.length();
			
			for(int i = 0; i < resultsCount; i++) {
				JSONObject jsonObject = jsonBindings.getJSONObject(i);
				long friendId = jsonObject.getJSONObject("friendId").getLong("value");
				String firstName = jsonObject.getJSONObject("firstName").getString("value");
				String lastName = jsonObject.getJSONObject("lastName").getString("value");
				long messageId = jsonObject.getJSONObject("messageId").getLong("value");
				String content = jsonObject.getJSONObject("content").getString("value");
				long creationDate = 0;
				try {
					creationDate = creationDateFormat.parse(jsonObject.getJSONObject("creationDate").getString("value")).getTime();
				} catch (JSONException e) {
					e.printStackTrace();
				} catch (ParseException e) {
					e.printStackTrace();
				}
				resultList.add(new LdbcQuery2Result(friendId, firstName, lastName, messageId, content, creationDate));
			}
						
			resultReporter.report(resultsCount, resultList, ldbcQuery2);
		}
	}
	
	// TODO 
	public static class LdbcQuery3Handler implements OperationHandler<LdbcQuery3, DbConnectionState> {
		
		
		public void executeOperation(
				LdbcQuery3 ldbcQuery3,
				DbConnectionState dbConnectionState,
				ResultReporter resultReporter) throws DbException {

			// RyaClient client = (((RyaConnectionState) dbConnectionState).getClient());
			
			List<LdbcQuery3Result> resultList = new ArrayList<LdbcQuery3Result>();
			
			// String query = "";
			
			//JSONArray rd = client.executeQuery(query);
			
			resultReporter.report(0, resultList, ldbcQuery3);
		}
	}
	
	public static class LdbcQuery4Handler implements OperationHandler<LdbcQuery4, DbConnectionState> {
		
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

	public static class LdbcQuery5Handler implements OperationHandler<LdbcQuery5, DbConnectionState> {
		
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
	
	// TODO
	public static class LdbcQuery6Handler implements OperationHandler<LdbcQuery6, DbConnectionState> {
		
		public void executeOperation(
				LdbcQuery6 ldbcQuery6,
				DbConnectionState dbConnectionState,
				ResultReporter resultReporter) throws DbException {
 
			//RyaClient client = (((RyaConnectionState) dbConnectionState).getClient());
			
			List<LdbcQuery6Result> resultList = new ArrayList<LdbcQuery6Result>();
			
			//String query = "";
			
			//JSONArray rd = client.executeQuery(query);
			
			resultReporter.report(0, resultList, ldbcQuery6);
		}
	}
	
	public static class LdbcQuery7Handler implements OperationHandler<LdbcQuery7, DbConnectionState> {
		
		public void executeOperation(
				LdbcQuery7 ldbcQuery7,
				DbConnectionState dbConnectionState,
				ResultReporter resultReporter) throws DbException {

			RyaClient ryaClient = (((RyaConnectionState) dbConnectionState).getClient());
			
			List<LdbcQuery7Result> resultList = new ArrayList<LdbcQuery7Result>();
			int resultsCount = 0;
			
			long id = ldbcQuery7.personId();
			int limit = ldbcQuery7.limit();
			
			String query =
					"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" + 
					"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
					"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
					"\n" + 
					"SELECT DISTINCT ?likerId ?firstName ?lastName ?likeDate ?messageId ?content ?messageDate ?isNew\n" + 
					"WHERE {\n" + 
					"{\n" + 
					"SELECT DISTINCT ?likerId ?firstName ?lastName ?likeDate ?messageId ?content ?messageDate ?isNew\n" + 
					"WHERE {\n" + 
					"\n" + 
					"?person rdf:type snvoc:Person ;\n" + 
					"	snvoc:id \""+ id +"\"^^xsd:long .\n" + 
					"\n" + 
					"?post snvoc:hasCreator ?person ;\n" + 
					"	rdf:type snvoc:Post ;\n" + 
					"	snvoc:creationDate ?messageDate ;\n" + 
					"	snvoc:content | snvoc:imageFile ?content ;\n" + 
					"	snvoc:id ?messageId .\n" + 
					"\n" + 
					"?like snvoc:hasPost ?post ;\n" + 
					"	snvoc:creationDate ?likeDate .\n" + 
					"\n" + 
					"?liker snvoc:likes ?like ;\n" + 
					"	snvoc:id ?likerId ;\n" + 
					"	snvoc:firstName ?firstName ;\n" + 
					"	snvoc:lastName ?lastName .\n" + 
					"\n" + 
					"BIND(EXISTS{?liker snvoc:knows ?knowObject . ?knowObject snvoc:hasPerson ?person .} AS ?isNew)\n" + 
					"\n" + 
					"}\n" + 
					"}\n" + 
					"UNION\n" + 
					"{\n" + 
					"SELECT DISTINCT ?likerId ?firstName ?lastName ?likeDate ?messageId ?content ?messageDate ?isNew\n" + 
					"WHERE {\n" + 
					"?person rdf:type snvoc:Person ;\n" + 
					"	snvoc:id \"" + id + "\"^^xsd:long .\n" + 
					"\n" + 
					"?comment snvoc:hasCreator ?person ;\n" + 
					"	rdf:type snvoc:Comment ;\n" + 
					"	snvoc:creationDate ?messageDate ;\n" + 
					"	snvoc:content ?content ;\n" + 
					"	snvoc:id ?messageId .\n" + 
					"\n" + 
					"?like snvoc:hasPost ?comment ;\n" + 
					"	snvoc:creationDate ?likeDate .\n" + 
					"\n" + 
					"?liker snvoc:likes ?like ;\n" + 
					"	snvoc:id ?likerId ;\n" + 
					"	snvoc:firstName ?firstName ;\n" + 
					"	snvoc:lastName ?lastName .\n" + 
					"\n" + 
					"BIND(EXISTS{?liker snvoc:knows ?knowObject . ?knowObject snvoc:hasPerson ?person .} AS ?isNew)\n" + 
					"}\n" + 
					"}\n" + 
					"}\n" + 
					"ORDER BY DESC(?likeDate) ASC(?likerId)\n" + 
					"LIMIT " + limit
					;
			
			JSONArray jsonBindings = ryaClient.executeReadQuery(query);


			resultsCount = jsonBindings.length();
			
			for(int i = 0; i < resultsCount; i++) {
				JSONObject jsonObject = jsonBindings.getJSONObject(i);
				
				long likeDate = 0;
				long messageDate = 0;
				try {
					likeDate = creationDateFormat.parse(jsonObject.getJSONObject("likeDate").getString("value")).getTime();
					messageDate = creationDateFormat.parse(jsonObject.getJSONObject("messageDate").getString("value")).getTime();
				} catch (JSONException e) {
					e.printStackTrace();
				} catch (ParseException e) {
					e.printStackTrace();
				}
				int latency =(int)((likeDate - messageDate) / (1000 * 60));
				long likerId = jsonObject.getJSONObject("likerId").getLong("value");
				long messageId = jsonObject.getJSONObject("messageId").getLong("value");
				String firstName = jsonObject.getJSONObject("firstName").getString("value");
				String lastName = jsonObject.getJSONObject("lastName").getString("value");
				String content = jsonObject.getJSONObject("content").getString("value");
				boolean isNew = jsonObject.getJSONObject("isNew").getBoolean("value");
				
				resultList.add(new LdbcQuery7Result(likerId, firstName, lastName, likeDate, messageId, content, latency, isNew));
			}
			
			resultReporter.report(resultsCount, resultList, ldbcQuery7);		
		}
	}
	
	public static class LdbcQuery8Handler implements OperationHandler<LdbcQuery8, DbConnectionState> {
		
		public void executeOperation(
				LdbcQuery8 ldbcQuery8,
				DbConnectionState dbConnectionState,
				ResultReporter resultReporter) throws DbException {
 
			RyaClient ryaClient = (((RyaConnectionState) dbConnectionState).getClient());
			
			List<LdbcQuery8Result> resultList = new ArrayList<LdbcQuery8Result>();
			int resultsCount = 0;
			
			long id = ldbcQuery8.personId();
			int limit = ldbcQuery8.limit();
			
			String query =
					"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" + 
					"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
					"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
					"\n" + 
					"SELECT DISTINCT ?replierId ?firstName ?lastName ?commentDate ?commentId ?content\n" + 
					"WHERE {\n" + 
					"{\n" + 
					"?person rdf:type snvoc:Person ;\n" + 
					"	snvoc:id \"" + id + "\"^^xsd:long .\n" + 
					"\n" + 
					"?message rdf:type snvoc:Post ;\n" + 
					"	 snvoc:hasCreator ?person .\n" + 
					"\n" + 
					"\n" + 
					"?comment rdf:type snvoc:Comment ;\n" + 
					"	snvoc:replyOf ?message ;\n" + 
					"	snvoc:id ?commentId ;\n" + 
					"	snvoc:content ?content ;\n" + 
					"	snvoc:creationDate ?commentDate ;\n" + 
					"	snvoc:hasCreator ?replier .\n" + 
					"\n" + 
					"?replier snvoc:id ?replierId ;\n" + 
					"	snvoc:firstName ?firstName ;\n" + 
					"	snvoc:lastName ?lastName .\n" + 
					"}\n" + 
					"UNION\n" + 
					"{\n" + 
					"?person rdf:type snvoc:Person ;\n" + 
					"	snvoc:id \"" + id + "\"^^xsd:long .\n" + 
					"\n" + 
					"?message rdf:type snvoc:Comment ;\n" + 
					"         snvoc:hasCreator ?person .\n" + 
					"\n" + 
					"?comment rdf:type snvoc:Comment ;\n" + 
					"	snvoc:replyOf ?message ;\n" + 
					"	snvoc:id ?commentId ;\n" + 
					"	snvoc:content ?content ;\n" + 
					"	snvoc:creationDate ?commentDate ;\n" + 
					"	snvoc:hasCreator ?replier .\n" + 
					"\n" + 
					"?replier snvoc:id ?replierId ;\n" + 
					"	snvoc:firstName ?firstName ;\n" + 
					"	snvoc:lastName ?lastName .\n" + 
					"}\n" + 
					"}\n" + 
					"ORDER BY DESC(?commentDate) ASC(?commentId)\n" + 
					"LIMIT " + limit
					;
			
			JSONArray jsonBindings = ryaClient.executeReadQuery(query);


			resultsCount = jsonBindings.length();
			
			for(int i = 0; i < resultsCount; i++) {
				JSONObject jsonObject = jsonBindings.getJSONObject(i);
				
				long commentDate = 0;
				try {
					commentDate = creationDateFormat.parse(jsonObject.getJSONObject("commentDate").getString("value")).getTime();
				} catch (JSONException e) {
					e.printStackTrace();
				} catch (ParseException e) {
					e.printStackTrace();
				}
				long replierId = jsonObject.getJSONObject("replierId").getLong("value");
				long commentId = jsonObject.getJSONObject("commentId").getLong("value");
				String firstName = jsonObject.getJSONObject("firstName").getString("value");
				String lastName = jsonObject.getJSONObject("lastName").getString("value");
				String commentContent = jsonObject.getJSONObject("content").getString("value");
				
				resultList.add(new LdbcQuery8Result(replierId, firstName, lastName, commentDate, commentId, commentContent));
				
			}
			resultReporter.report(resultsCount, resultList, ldbcQuery8);		
		}
	}
	
	public static class LdbcQuery9Handler implements OperationHandler<LdbcQuery9, DbConnectionState> {
		
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
	
	//TODO
	public static class LdbcQuery10Handler implements OperationHandler<LdbcQuery10, DbConnectionState> {
		
		
		public void executeOperation(
				LdbcQuery10 ldbcQuery10,
				DbConnectionState dbConnectionState,
				ResultReporter resultReporter) throws DbException {

			//RyaClient client = (((RyaConnectionState) dbConnectionState).getClient());
			
			List<LdbcQuery10Result> resultList = new ArrayList<LdbcQuery10Result>();
			
			//String query = "";
			
			//JSONArray rd = client.executeQuery(query);
			
			resultReporter.report(0, resultList, ldbcQuery10);
		}
	}
	
	public static class LdbcQuery11Handler implements OperationHandler<LdbcQuery11, DbConnectionState> {
		
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
	
	// TODO
	public static class LdbcQuery12Handler implements OperationHandler<LdbcQuery12, DbConnectionState> {
		
		public void executeOperation(
				LdbcQuery12 ldbcQuery12,
				DbConnectionState dbConnectionState,
				ResultReporter resultReporter) throws DbException {

			// RyaClient client = (((RyaConnectionState) dbConnectionState).getClient());
			
			List<LdbcQuery12Result> resultList = new ArrayList<LdbcQuery12Result>();
			
			// String query = "";
			
			//JSONArray rd = client.executeQuery(query);
			
			resultReporter.report(0, resultList, ldbcQuery12);
		}
	}
	
	// TODO
	public static class LdbcQuery13Handler implements OperationHandler<LdbcQuery13, DbConnectionState> {
		
		public void executeOperation(
				LdbcQuery13 ldbcQuery13,
				DbConnectionState dbConnectionState,
				ResultReporter resultReporter) throws DbException {
 
			// RyaClient client = (((RyaConnectionState) dbConnectionState).getClient());
			
			LdbcQuery13Result result = new LdbcQuery13Result(-1);
			
			// String query = "";
			
			//JSONArray rd = client.executeQuery(query);
			
			resultReporter.report(0, result, ldbcQuery13);
		}
	}
	
	// TODO
	public static class LdbcQuery14Handler implements OperationHandler<LdbcQuery14, DbConnectionState> {
		
		public void executeOperation(
				LdbcQuery14 ldbcQuery14,
				DbConnectionState dbConnectionState,
				ResultReporter resultReporter) throws DbException {
			
			// RyaClient client = (((RyaConnectionState) dbConnectionState).getClient());
			
			List<LdbcQuery14Result> resultList = new ArrayList<LdbcQuery14Result>();
			
			// String query = "";
			
			//JSONArray rd = client.executeQuery(query);
			
			resultReporter.report(0, resultList, ldbcQuery14);
		}
	}
	
	
	/*
	 * SHORT QUERIES
	 */
	public static class LdbcShortQuery1PersonProfileHandler implements OperationHandler<LdbcShortQuery1PersonProfile, DbConnectionState> {
		
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
	
	public static class LdbcShortQuery2PersonPostsHandler implements OperationHandler<LdbcShortQuery2PersonPosts, DbConnectionState> {
		
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
	
	public static class LdbcShortQuery3PersonFriendsHandler
	implements OperationHandler<LdbcShortQuery3PersonFriends, DbConnectionState> {

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
	
	public static class LdbcShortQuery4MessageContentHandler implements OperationHandler<LdbcShortQuery4MessageContent, DbConnectionState> {
		
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

	public static class LdbcShortQuery5MessageCreatorHandler implements OperationHandler<LdbcShortQuery5MessageCreator, DbConnectionState> {
		
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
	
	//TODO
	public static class LdbcShortQuery6MessageForumHandler implements OperationHandler<LdbcShortQuery6MessageForum, DbConnectionState> {
		
		public void executeOperation(
				LdbcShortQuery6MessageForum ldbcShortQuery6MessageForum,
				DbConnectionState dbConnectionState,
				ResultReporter resultReporter) throws DbException {

			// RyaClient client = (((RyaConnectionState) dbConnectionState).getClient());
			
			LdbcShortQuery6MessageForumResult result = null;
			
			// String query = "";
			
			//JSONArray rd = client.executeQuery(query);
			result = new LdbcShortQuery6MessageForumResult(-1, "none", -1, "none", "none");
			resultReporter.report(-1, result, ldbcShortQuery6MessageForum);
		}
	}
	
	public static class LdbcShortQuery7MessageRepliesHandler implements OperationHandler<LdbcShortQuery7MessageReplies, DbConnectionState> {
		
		public void executeOperation(
				LdbcShortQuery7MessageReplies ldbcShortQuery7MessageReplies,
				DbConnectionState dbConnectionState,
				ResultReporter resultReporter) throws DbException {

			RyaClient ryaClient = (((RyaConnectionState) dbConnectionState).getClient());
			
			List<LdbcShortQuery7MessageRepliesResult> resultList = new ArrayList<LdbcShortQuery7MessageRepliesResult>();
			int resultsCount = 0;
			
			long id = ldbcShortQuery7MessageReplies.messageId();
			
			String query =
					"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" + 
					"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
					"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
					"SELECT ?commentId ?commentContent ?commentCreationDate ?personId ?firstName ?lastName ?boolean\n" + 
					"WHERE {\n" + 
					"{\n" + 
					"?message snvoc:id \"" + id + "\"^^xsd:long  ;\n" + 
					"  rdf:type snvoc:Post ;\n" + 
					"  snvoc:hasCreator ?author .\n" + 
					"\n" + 
					"?comment snvoc:replyOf ?message ;\n" + 
					"  snvoc:id ?commentId ;\n" + 
					"  snvoc:content ?commentContent ;\n" + 
					"  snvoc:hasCreator ?person ;\n" + 
					"  snvoc:creationDate ?commentCreationDate .\n" + 
					"\n" + 
					"  ?person snvoc:knows ?knowObject .\n" + 
					"\n" + 
					"?person snvoc:id ?personId ;\n" + 
					"  snvoc:firstName ?firstName ;\n" + 
					"  snvoc:lastName ?lastName .\n" + 
					"\n" + 
					"BIND(IF(EXISTS{?knowObject snvoc:hasPerson ?author} ,true ,false) AS ?boolean)\n" + 
					"}\n" + 
					"UNION\n" + 
					"{\n" + 
					"?message snvoc:id \"" + id + "\"^^xsd:long  ;\n" + 
					"  rdf:type snvoc:Comment ;\n" + 
					"  snvoc:hasCreator ?author .\n" + 
					"\n" + 
					"?comment snvoc:replyOf ?message ;\n" + 
					"  snvoc:id ?commentId ;\n" + 
					"  snvoc:content ?commentContent ;\n" + 
					"  snvoc:hasCreator ?person ;\n" + 
					"  snvoc:creationDate ?commentCreationDate .\n" + 
					" \n" + 
					"?person snvoc:knows ?knowObject . \n" + 
					"\n" + 
					"?person snvoc:id ?personId ;\n" + 
					"  snvoc:firstName ?firstName ;\n" + 
					"  snvoc:lastName ?lastName .\n" + 
					"\n" + 
					"BIND(IF(EXISTS{?knowObject snvoc:hasPerson ?author} ,true ,false) AS ?boolean)\n" + 
					"}\n" + 
					"}\n" + 
					"ORDER BY DESC(?creationDate) ASC(?personId)"
					;

			JSONArray jsonBindings = ryaClient.executeReadQuery(query);

			resultsCount = jsonBindings.length();

			for (int i = 0; i < resultsCount; i++) {
				JSONObject jsonObject = jsonBindings.getJSONObject(i);

				String firstName = jsonObject.getJSONObject("firstName").getString("value");
				String lastName = jsonObject.getJSONObject("lastName").getString("value");
				long personId = jsonObject.getJSONObject("personId").getLong("value");
				long commentId = jsonObject.getJSONObject("commentId").getLong("value");
				String content = jsonObject.getJSONObject("commentContent").getString("value");
				boolean flag = jsonObject.getJSONObject("boolean").getBoolean("value");

				long commentDate = 0;
				try {
					commentDate = creationDateFormat.parse(jsonObject.getJSONObject("commentCreationDate").getString("value")).getTime();
				} catch (JSONException e) {
					e.printStackTrace();
				} catch (ParseException e) {
					e.printStackTrace();
				}

				resultList.add(new LdbcShortQuery7MessageRepliesResult(commentId, content, commentDate, personId, firstName, lastName, flag));
			}

			resultReporter.report(resultsCount, resultList, ldbcShortQuery7MessageReplies);
		}
	}

	
	/*
	 * UPDATE QUERIES
	 */
	public static class LdbcUpdate1AddPersonHandler implements
	OperationHandler<LdbcUpdate1AddPerson, DbConnectionState> {
		
		public void executeOperation(
				LdbcUpdate1AddPerson ldbcUpdate1AddPerson, 
				DbConnectionState dbConnectionState, 
				ResultReporter resultReporter)
						throws DbException {

			// get the client able to execute the query
			RyaClient client = (((RyaConnectionState) dbConnectionState).getClient());

			// prepare the update query
			String insertClause = "";
			String whereClause = "";
			String query = ""; 
			
			
			// add study object
			List<String> studyBNodes = new ArrayList<String>();
			for(int i = 0; i < ldbcUpdate1AddPerson.studyAt().size(); i++) 
			{ 
				studyBNodes.add("_:study" + i);
				insertClause += 
						studyBNodes.get(i) + " snvoc:hasOrganization ?univeristy" + i + " ;\n" + 
						"	snvoc:classYear \"" + ldbcUpdate1AddPerson.studyAt().get(i).year() + "\"^^xsd:integer .\n\n"
						;
				whereClause +=
						"?university" + i + " rdf:type dbpedia-owl:University ;\n" + 
						"	snvoc:id \"" + ldbcUpdate1AddPerson.studyAt().get(i).organizationId() + "\"^^xsd:int .\n\n"
						;
			}
			
			// add works object
			List<String> workBNodes = new ArrayList<String>();
			for(int i = 0; i < ldbcUpdate1AddPerson.workAt().size(); i++) 
			{
				workBNodes.add("_:work" + i);
				insertClause += 
						workBNodes.get(i) + " snvoc:hasOrganization ?company" + i + " ;\n" + 
						"	snvoc:workFrom \"" + ldbcUpdate1AddPerson.workAt().get(i).year() + "\"^^xsd:integer .\n\n"
						;
				whereClause +=
						"?company" + i + " rdf:type dbpedia-owl:Company ;\n" + 
						"	snvoc:id \"" + ldbcUpdate1AddPerson.workAt().get(i).organizationId() + "\"^^xsd:int .\n\n"
						;
			}
			
			// add first part of person
			insertClause +=
					"sn:pers" + ldbcUpdate1AddPerson.personId() + " rdf:type snvoc:Person ;\n"
					;
			
			// add study relationships
			for(int i = 0; i < studyBNodes.size(); i++) 
			{
				 insertClause += 
						 "snvoc:studyAt " + studyBNodes.get(i) + " ;\n"
						 ;
			}
			
			// add work relationships
			for(int i = 0; i < workBNodes.size(); i++) 
			{
				 insertClause += 
						 "snvoc:workAt " + workBNodes.get(i) + " ;\n"
						 ;
			}
			
			// add languages
			if(ldbcUpdate1AddPerson.languages().size() > 0) 
			{
			insertClause += "snvoc:speaks ";
			}
			for(int i = 0; i < ldbcUpdate1AddPerson.languages().size(); i++) 
			{
				if(i == ldbcUpdate1AddPerson.languages().size() - 1)
				{
					insertClause +=
							"\"" + ldbcUpdate1AddPerson.languages().get(i) + "\" ;\n";
							;
				} else
				{
				insertClause +=
						"\"" + ldbcUpdate1AddPerson.languages().get(i) + "\" , ";
						;
				}
			}
			
			// add emails
			if(ldbcUpdate1AddPerson.emails().size() > 0)
			{
			insertClause += "snvoc:email ";
			}
			for(int i = 0; i < ldbcUpdate1AddPerson.emails().size(); i++) 
			{
				if(i == ldbcUpdate1AddPerson.emails().size() - 1)
				{
					insertClause +=
							"\"" + ldbcUpdate1AddPerson.emails().get(i) + "\" ;\n";
					;
				} else
				{
					insertClause +=
							"\"" + ldbcUpdate1AddPerson.emails().get(i) + "\" , ";
					;
				}
			}
			
			// add tags
			if(ldbcUpdate1AddPerson.tagIds().size() > 0)
			{
			whereClause += "?tagClass rdf:type snvoc:TagClass .\n\n" ;
			insertClause += "snvoc:hasInterest ";
			}
			for(int i = 0; i < ldbcUpdate1AddPerson.tagIds().size(); i++)
			{
				whereClause += 
						"?tag" + i + " rdf:type ?tagClass ;\n" + 
						"	snvoc:id \"" + ldbcUpdate1AddPerson.tagIds().get(i) + "\"^^xsd:int .\n\n"
						;
				if(i == ldbcUpdate1AddPerson.tagIds().size() -1)
				{
				insertClause += "?tag" + i + " ;\n";
				} else 
				{
				insertClause += "?tag" + i + " , ";
				}
			}
			
			// add city and remaining parameters
			whereClause += 
					"?city rdf:type dbpedia-owl:City ;\n" + 
					"	snvoc:id \"" + ldbcUpdate1AddPerson.cityId() + "\"^^xsd:int ."
					;
			insertClause += 
					"snvoc:isLocatedIn ?city ;\n" + 
					"snvoc:id \"" + ldbcUpdate1AddPerson.personId() + "\"^^xsd:long ;\n" + 
					"snvoc:firstName \"" + ldbcUpdate1AddPerson.personFirstName() +"\" ;\n" + 
					"snvoc:lastName \"" + ldbcUpdate1AddPerson.personLastName() + "\" ;\n" + 
					"snvoc:gender \"" + ldbcUpdate1AddPerson.gender() + "\" ;\n" + 
					"snvoc:birthday \"" + birthdateDateFormat.format(ldbcUpdate1AddPerson.birthday()) + "\"^^xsd:date ;\n" + 
					"snvoc:locationIp \"" + ldbcUpdate1AddPerson.locationIp() + "\" ;\n" + 
					"snvoc:browserUsed \"" + ldbcUpdate1AddPerson.browserUsed() + "\" ;\n" + 
					"snvoc:creationDate \"" + creationDateFormat.format(ldbcUpdate1AddPerson.creationDate()) + ":00\"^^xsd:dateTime ."
					;
			
			query = 
					"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" + 
					"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
					"PREFIX sn:<http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" + 
					"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
					"PREFIX dbpedia-owl: <http://dbpedia.org/ontology/>\n" + 
					"\n" + 
					"INSERT\n" + 
					"{\n" +
					insertClause + 
					"\n" +
					"}\n" +
					"WHERE\n" +
					"{\n" +
					whereClause +
					"\n" +
					"}";
			 
			
			// execute the update query
			client.executeUpdateQuery(query);

			// report the result
			resultReporter.report(0, LdbcNoResult.INSTANCE, ldbcUpdate1AddPerson);
		}
	}
	
	public static class LdbcUpdate2AddPostLikeHandler implements
	OperationHandler<LdbcUpdate2AddPostLike, DbConnectionState> {

		public void executeOperation(
				LdbcUpdate2AddPostLike ldbcUpdate2AddPostLike,
				DbConnectionState dbConnectionState,
				ResultReporter resultReporter) throws DbException {
			RyaClient ryaClient = (((RyaConnectionState) dbConnectionState).getClient());

			long postId = ldbcUpdate2AddPostLike.postId();
			long personId = ldbcUpdate2AddPostLike.personId();
			String creationDate = creationDateFormat.format(ldbcUpdate2AddPostLike.creationDate()) + ":00";
			String query = 
					"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" + 
					"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
					"PREFIX sn:<http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" + 
					"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
					"\n" + 
					"INSERT\n" + 
					"{\n" + 
					"?person snvoc:likes _:like .\n" + 
					"\n" + 
					"_:like snvoc:hasPost ?post ;\n" + 
					"	snvoc:creationDate \"" + creationDate + "\"^^xsd:dateTime .\n" + 
					"}\n" + 
					"WHERE\n" + 
					"{\n" + 
					"?person rdf:type snvoc:Person ;\n" + 
					"	snvoc:id \"" + personId + "\"^^xsd:long .\n" + 
					"\n" + 
					"?post rdf:type snvoc:Post ;\n" + 
					"	snvoc:id \"" + postId + "\"^^xsd:long .\n" + 
					"}"
					;

			ryaClient.executeUpdateQuery(query);
			
			resultReporter.report(0, LdbcNoResult.INSTANCE, ldbcUpdate2AddPostLike);
		}
	}
	
	public static class LdbcUpdate3AddCommentLikeHandler implements
	OperationHandler<LdbcUpdate3AddCommentLike, DbConnectionState> {

		public void executeOperation(
				LdbcUpdate3AddCommentLike ldbcUpdate3AddCommentLike,
				DbConnectionState dbConnectionState,
				ResultReporter resultReporter) throws DbException {

			RyaClient ryaClient = (((RyaConnectionState) dbConnectionState).getClient());

			long commentId = ldbcUpdate3AddCommentLike.commentId();
			long personId = ldbcUpdate3AddCommentLike.personId();
			String creationDate = creationDateFormat.format(ldbcUpdate3AddCommentLike.creationDate()) + ":00";
			
			String query = 
					"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" + 
					"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
					"PREFIX sn:<http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" + 
					"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
					"\n" + 
					"INSERT\n" + 
					"{\n" + 
					"?person snvoc:likes _:like .\n" + 
					"\n" + 
					"_:like snvoc:hasComment ?comment ;\n" + 
					"	snvoc:creationDate \"" + creationDate + "\"^^xsd:dateTime .\n" + 
					"}\n" + 
					"WHERE\n" + 
					"{\n" + 
					"?person rdf:type snvoc:Person ;\n" + 
					"	snvoc:id \"" + personId + "\"^^xsd:long .\n" + 
					"\n" + 
					"?comment rdf:type snvoc:Comment ;\n" + 
					"	snvoc:id \"" + commentId + "\"^^xsd:long .\n" + 
					"}"
					;

			ryaClient.executeUpdateQuery(query);			
			
			resultReporter.report(0, LdbcNoResult.INSTANCE, ldbcUpdate3AddCommentLike);
		}
	}
	
	public static class LdbcUpdate4AddForumHandler implements
	OperationHandler<LdbcUpdate4AddForum, DbConnectionState> {

		public void executeOperation(
				LdbcUpdate4AddForum ldbcUpdate4AddForum,
				DbConnectionState dbConnectionState,
				ResultReporter resultReporter) throws DbException {
			
			RyaClient ryaClient = (((RyaConnectionState) dbConnectionState).getClient());

			List<Long> tagIds = ldbcUpdate4AddForum.tagIds();
			long forumId = ldbcUpdate4AddForum.forumId();
			String forumTitle = ldbcUpdate4AddForum.forumTitle();
			String creationDate = creationDateFormat.format(ldbcUpdate4AddForum.creationDate()) + ":00";
			long moderatorId = ldbcUpdate4AddForum.moderatorPersonId();
			
			String insertClause = 
					"sn:forum" + forumId + " rdf:type snvoc:Forum ;\n" + 
					"	snvoc:id \"" + forumId + "\"^^xsd:long ;\n" + 
					"	snvoc:title \"" + forumTitle + "\" ;\n" + 
					"	snvoc:creationDate \"" + creationDate + "\"^^xsd:dateTime ;\n" + 
					"	snvoc:hasModerator ?moderator .\n" 
					;
			String whereClause =
					"?moderator snvoc:id \"" + moderatorId + "\"^^xsd:long ; \n" + 
					"	rdf:type snvoc:Person .\n" + 
					"\n" + 
					"?tagClass rdf:type snvoc:TagClass .\n" + 
					"\n" + 
					""
					;
			
			for(int i = 0; i < tagIds.size(); i++) {
				insertClause += "sn:forum" + forumId + " snvoc:hasTag ?tag" + i + " .\n";
				
				whereClause += 
						"?tag" + i + " rdf:type ?tagClass ;\n" + 
						"	snvoc:id \"" + tagIds.get(i) + "\"^^xsd:int .\n" 
						;
			}
			
			String query = 
					"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" + 
					"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
					"PREFIX sn:<http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" + 
					"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
					"PREFIX dbpedia-owl: <http://dbpedia.org/ontology/>\n" + 
					"\n" + 
					"INSERT\n" + 
					"{\n" +
					insertClause + 
					"}\n" +
					"WHERE\n" +
					"{\n" +
					whereClause +
					"}";

			ryaClient.executeUpdateQuery(query);
			
			resultReporter.report(0, LdbcNoResult.INSTANCE, ldbcUpdate4AddForum);
		}
	}
	
	public static class LdbcUpdate5AddForumMembershipHandler implements
	OperationHandler<LdbcUpdate5AddForumMembership, DbConnectionState> {

		public void executeOperation(
				LdbcUpdate5AddForumMembership ldbcUpdate5AddForumMembership,
				DbConnectionState dbConnectionState,
				ResultReporter resultReporter) throws DbException {
			
			RyaClient ryaClient = (((RyaConnectionState) dbConnectionState).getClient());

			long personId = ldbcUpdate5AddForumMembership.personId();
			long forumId = ldbcUpdate5AddForumMembership.forumId();
			String joinDate = creationDateFormat.format(ldbcUpdate5AddForumMembership.joinDate()) + ":00";
			
			String query =
			"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" + 
			"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
			"PREFIX sn:<http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" + 
			"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
			"INSERT\n" + 
			"{\n" + 
			"_:mbs snvoc:hasPerson ?person ;\n" + 
			"	snvoc:joinDate \"" + joinDate + "\"^^xsd:dateTime .\n" + 
			"\n" + 
			"?forum snvoc:hasMember _:mbs .	\n" + 
			"}\n" + 
			"WHERE\n" + 
			"{\n" + 
			"?person snvoc:id \"" + personId + "\"^^xsd:long ;\n" + 
			"	rdf:type snvoc:Person .\n" + 
			"\n" + 
			"?forum snvoc:id \"" + forumId + "\"^^xsd:long ;\n" + 
			"	rdf:type snvoc:Forum .\n" +
			"}"
			;

			ryaClient.executeUpdateQuery(query);
			
			resultReporter.report(0, LdbcNoResult.INSTANCE, ldbcUpdate5AddForumMembership);
		}
	}
	
	public static class LdbcUpdate6AddPostHandler implements
	OperationHandler<LdbcUpdate6AddPost, DbConnectionState> {

		public void executeOperation(
				LdbcUpdate6AddPost ldbcUpdate6AddPost,
				DbConnectionState dbConnectionState,
				ResultReporter resultReporter) throws DbException {
			
			RyaClient ryaClient = (((RyaConnectionState) dbConnectionState).getClient());

			long personId = ldbcUpdate6AddPost.authorPersonId();
			long forumId = ldbcUpdate6AddPost.forumId();
			long postId = ldbcUpdate6AddPost.postId();
			long countryId = ldbcUpdate6AddPost.countryId();
			String imageFile = ldbcUpdate6AddPost.imageFile();
			String content = ldbcUpdate6AddPost.content();
			String creationDate = creationDateFormat.format(ldbcUpdate6AddPost.creationDate()) + ":00";
			String locationIp = ldbcUpdate6AddPost.locationIp();
			String browserUsed = ldbcUpdate6AddPost.browserUsed();
			String language = ldbcUpdate6AddPost.language();
			int postLength = ldbcUpdate6AddPost.length();
			
			List<Long> tagIds = ldbcUpdate6AddPost.tagIds();
			
			String insertClause =
					"sn:post" + postId + " rdf:type snvoc:Post ;\n" + 
					"	snvoc:id \"" + postId + "\"^^xsd:long ;\n" + 
					"	snvoc:imageFile \"" + imageFile + "\" ;\n" + 
					"	snvoc:creationDate \"" + creationDate + "\"^^xsd:dateTime ;\n" + 
					"	snvoc:locationIp \"" + locationIp + "\" ;\n" + 
					"	snvoc:browserUsed \"" + browserUsed + "\" ;\n" + 
					"	snvoc:language \"" + language + "\" ;\n" + 
					"	snvoc:content \"" + content + "\" ;\n" + 
					"	snvoc:length \"" + postLength + "\"^^xsd:int ;\n" + 
					"	snvoc:hasCreator ?person ;\n" + 
					"	snvoc:isLocatedIn ?country ;\n" + 
					"\n" + 
					"?forum snvoc:containerOf sn:post" + postId + " .\n\n"
					;
			String whereClause = 
					"?person rdf:type snvoc:Person ;\n" + 
					"	snvoc:id \"" + personId + "\"^^xsd:long .\n" + 
					"\n" + 
					"?forum rdf:type snvoc:Forum ;\n" + 
					"	snvoc:id \"" + forumId + "\"^^xsd:long .\n" + 
					"\n" + 
					"?country dbpedia-owl:type snvoc:Country ;\n" + 
					"	snvoc:id \"" + countryId + "\"^^xsd:int .\n" + 
					"\n" + 
					"?tagClass rdf:type snvoc:TagClass .\n\n"
					;
			
			for(int i = 0; i < tagIds.size(); i++) {
				insertClause += "sn:post" + postId + " snvoc:hasTag ?tag" + i + " .\n";
				
				whereClause += 
						"?tag" + i + " rdf:type ?tagClass ;\n" + 
						"	snvoc:id \"" + tagIds.get(i) + "\"^^xsd:int .\n" 
						;
			}
			
			String query =
					"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" + 
					"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
					"PREFIX sn:<http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" + 
					"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
					"PREFIX dbpedia-owl: <http://dbpedia.org/ontology/>\n" +
					"\n" + 
					"INSERT\n" + 
					"{\n" +
					insertClause + 
					"\n" +
					"}\n" +
					"WHERE\n" +
					"{\n" +
					whereClause +
					"\n" +
					"}";

			ryaClient.executeUpdateQuery(query);
			
			resultReporter.report(0, LdbcNoResult.INSTANCE, ldbcUpdate6AddPost);
		}
	}
	
	public static class LdbcUpdate7AddCommentHandler implements
	OperationHandler<LdbcUpdate7AddComment, DbConnectionState> {

		public void executeOperation(
				LdbcUpdate7AddComment ldbcUpdate7AddComment,
				DbConnectionState dbConnectionState,
				ResultReporter resultReporter) throws DbException {
			
			RyaClient ryaClient = (((RyaConnectionState) dbConnectionState).getClient());

			long personId = ldbcUpdate7AddComment.authorPersonId();
			long replyToId;
			if(ldbcUpdate7AddComment.replyToCommentId() == -1)
			{
				replyToId = ldbcUpdate7AddComment.replyToPostId();
			} else
			{
				replyToId = ldbcUpdate7AddComment.replyToCommentId();
			}
			long commentId = ldbcUpdate7AddComment.commentId();
			long countryId = ldbcUpdate7AddComment.countryId();
			String content = ldbcUpdate7AddComment.content();
			String creationDate = creationDateFormat.format(ldbcUpdate7AddComment.creationDate()) + ":00";
			String locationIp = ldbcUpdate7AddComment.locationIp();
			String browserUsed = ldbcUpdate7AddComment.browserUsed();
			int commentLength = ldbcUpdate7AddComment.length();
			
			List<Long> tagIds = ldbcUpdate7AddComment.tagIds();
			
			String insertClause =
					"sn:comm" + commentId + " rdf:type snvoc:Comment ;\n" + 
					"	snvoc:id \"" + commentId + "\"^^xsd:long ;\n" + 
					"	snvoc:creationDate \"" + creationDate + "\"^^xsd:dateTime ;\n" + 
					"	snvoc:locationIp \"" + locationIp + "\" ;\n" + 
					"	snvoc:browserUsed \"" + browserUsed + "\" ;\n" + 
					"	snvoc:content \"" + content + "\" ;\n" + 
					"	snvoc:length \"" + commentLength + "\"^^xsd:int ;\n" + 
					"	snvoc:hasCreator ?person ;\n" + 
					"	snvoc:isLocatedIn ?country ;\n" + 
					"	snvoc:replyOf ?commentOrPost .\n" + 
					"\n" + 
					"?forum snvoc:containerOf sn:comm" + commentId + " .\n" + 
					""
					;
			String whereClause = 
					"?person rdf:type snvoc:Person ;\n" + 
					"	snvoc:id \"" + personId + "\"^^xsd:long .\n" + 
					"\n" + 
					"?country dbpedia-owl:type snvoc:Country ;\n" + 
					"	snvoc:id \"" + countryId + "\"^^xsd:int .\n" + 
					"\n" + 
					"?commentOrPost rdf:type ?type ;\n" + 
					"	snvoc:id \"" + replyToId  + "\"^^xsd:long .\n" + 
					"\n" + 
					"FILTER(?type = snvoc:Post || ?type = snvoc:Comment)\n" + 
					"\n" + 
					"?forum snvoc:containerOf ?commentOrPost .\n" + 
					"\n" + 
					"?tagClass rdf:type snvoc:TagClass ."
					;
			
			for(int i = 0; i < tagIds.size(); i++) {
				insertClause += "sn:comment" + commentId + " snvoc:hasTag ?tag" + i + " .\n";
				
				whereClause += 
						"?tag" + i + " rdf:type ?tagClass ;\n" + 
						"	snvoc:id \"" + tagIds.get(i) + "\"^^xsd:int .\n" 
						;
			}
			
			String query =
					"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" + 
					"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
					"PREFIX sn:<http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" + 
					"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
					"PREFIX dbpedia-owl: <http://dbpedia.org/ontology/>\n" +
					"\n" + 
					"INSERT\n" + 
					"{\n" +
					insertClause + 
					"\n" +
					"}\n" +
					"WHERE\n" +
					"{\n" +
					whereClause +
					"\n" +
					"}";

			ryaClient.executeUpdateQuery(query);
			
			resultReporter.report(0, LdbcNoResult.INSTANCE, ldbcUpdate7AddComment);
		}
	}
	
	public static class LdbcUpdate8AddFriendshipHandler implements
	OperationHandler<LdbcUpdate8AddFriendship, DbConnectionState> {

		public void executeOperation(
				LdbcUpdate8AddFriendship ldbcUpdate8AddFriendship,
				DbConnectionState dbConnectionState,
				ResultReporter resultReporter) throws DbException {
			
			RyaClient ryaClient = (((RyaConnectionState) dbConnectionState).getClient());

			long person1Id = ldbcUpdate8AddFriendship.person1Id();
			long person2Id = ldbcUpdate8AddFriendship.person2Id();
			String creationDate = creationDateFormat.format(ldbcUpdate8AddFriendship.creationDate()) + ":00";
			
			String query =
					"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" + 
					"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
					"PREFIX sn:<http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" + 
					"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
					"INSERT\n" + 
					"{\n" + 
					"_:know snvoc:hasPerson ?person2 ;\n" + 
					"	snvoc:creationDate \"" + creationDate + "\"^^xsd:dateTime .\n" + 
					"\n" + 
					"?person1 snvoc:knows _:know .\n" + 
					"}\n" + 
					"WHERE\n" + 
					"{\n" + 
					"?person1 rdf:type snvoc:Person ;\n" + 
					"	snvoc:id \"" + person1Id + "\"^^xsd:long .\n" + 
					"\n" + 
					"?person2 rdf:type snvoc:Person ;\n" + 
					"	snvoc:id \"" + person2Id + "\"^^xsd:long .\n" + 
					"}"
					;

			ryaClient.executeUpdateQuery(query);
			
			resultReporter.report(0, LdbcNoResult.INSTANCE, ldbcUpdate8AddFriendship);
		}
	}

}
