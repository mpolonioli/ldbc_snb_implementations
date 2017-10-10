package net.mpolonioli.ldbcimpls.incubator.rya.interactive.queries;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1Result;

import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaConnectionState;

public class LdbcQuery1Handler implements OperationHandler<LdbcQuery1, DbConnectionState> {

	private static DateFormat creationDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
	private static DateFormat birthdateDateFormat = new SimpleDateFormat("yyyy-MM-dd");
	
	public void executeOperation(
			LdbcQuery1 ldbcQuery1,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		RepositoryConnection conn = (((RyaConnectionState) dbConnectionState).getClient());

		List<LdbcQuery1Result> result = new ArrayList<LdbcQuery1Result>();

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
				"WHERE\n" + 
				"{\n" + 
				"?person snvoc:id \"" + id + "\"^^xsd:long ;\n" + 
				"	rdf:type snvoc:Person .\n" + 
				"\n" + 
				"?person \n" + 
				"	(snvoc:knows/snvoc:hasPerson)|\n" + 
				"	(snvoc:knows/snvoc:hasPerson/snvoc:knows/snvoc:hasPerson)|\n" + 
				"	(snvoc:knows/snvoc:hasPerson/snvoc:knows/snvoc:hasPerson/snvoc:knows/snvoc:hasPerson) \n" + 
				"		?friend .\n" + 
				"FILTER ( ?person != ?friend )\n" + 
				"\n" + 
				"BIND( \n" + 
				"	IF ( EXISTS { ?person (snvoc:knows/snvoc:hasPerson) ?friend },\n" + 
				"		1 ,\n" + 
				"		IF ( EXISTS { ?person (snvoc:knows/snvoc:hasPerson/snvoc:knows/snvoc:hasPerson) ?friend },\n" + 
				"			2,\n" + 
				"				3\n" + 
				"		)\n" + 
				"	)\n" + 
				"	AS ?dist\n" + 
				")\n" + 
				"\n" + 
				"?friend snvoc:firstName \""+ firstName +"\" ;\n" + 
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
				"}\n" + 
				"ORDER BY ASC(?dist) ASC(?lastName) ASC(?id)\n" + 
				"LIMIT " + limit
				;
		
		TupleQuery tupleQuery = null;
		try {
			tupleQuery = conn.prepareTupleQuery(
			        QueryLanguage.SPARQL, query);
		} catch (RepositoryException | MalformedQueryException e) {
			e.printStackTrace();
		}
		
		tupleQuery.setMaxQueryTime(1800);
		
		TupleQueryResult tupleQueryResult = null;
		try {
			tupleQueryResult = tupleQuery.evaluate();;
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}
		
		try {
			while(tupleQueryResult.hasNext())
			{
				BindingSet bindingSet = tupleQueryResult.next();
				
				long friendId = Long.parseLong(bindingSet.getValue("id").stringValue());
				String friendLastName = bindingSet.getValue("lastName").stringValue();
				int distanceFromPerson = Integer.parseInt(bindingSet.getValue("dist").stringValue());
				long friendBirthday = birthdateDateFormat.parse(bindingSet.getValue("birthday").stringValue()).getTime();
				long friendCreationDate = creationDateFormat.parse(bindingSet.getValue("creationDate").stringValue()).getTime();
				String friendGender = bindingSet.getValue("gender").stringValue();
				String friendBrowserUsed = bindingSet.getValue("browserUsed").stringValue();
				String friendLocationIp = bindingSet.getValue("locationIp").stringValue();
				List<String> friendEmails = new ArrayList<>();
				List<String> friendLanguages = new ArrayList<>();
				String friendCityName = bindingSet.getValue("cityName").stringValue();
				List<List<Object>> friendUniversities = new ArrayList<>();
				List<List<Object>> friendCompanies = new ArrayList<>();
				
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
				
				TupleQuery tupleQueryEmails = conn.prepareTupleQuery(
				        QueryLanguage.SPARQL, queryEmails);
				
				TupleQueryResult tupleQueryEmailsResult = tupleQueryEmails.evaluate();
				
				while (tupleQueryEmailsResult.hasNext())
				{
					BindingSet bindingSetEmail = tupleQueryEmailsResult.next();
					friendEmails.add(bindingSetEmail.getValue("email").stringValue());
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
				
				TupleQuery tupleQueryLanguages = conn.prepareTupleQuery(
				        QueryLanguage.SPARQL, queryLanguages);
				
				TupleQueryResult tupleQueryLanguagesResult = tupleQueryLanguages.evaluate();
				
				while (tupleQueryLanguagesResult.hasNext())
				{
					BindingSet bindingSetlanguage = tupleQueryLanguagesResult.next();
					friendLanguages.add(bindingSetlanguage.getValue("language").stringValue());
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
				
				TupleQuery tupleQueryUniversities = conn.prepareTupleQuery(
				        QueryLanguage.SPARQL, queryUniversities);
				
				TupleQueryResult tupleQueryUniversitiesResult = tupleQueryUniversities.evaluate();
				
				while(tupleQueryUniversitiesResult.hasNext())
				{
					BindingSet bindingSetUniversity = tupleQueryUniversitiesResult.next();
					List<Object> universityProperties = new ArrayList<>();
					universityProperties.add(bindingSetUniversity.getValue("universityName").stringValue());
					universityProperties.add(Integer.parseInt(bindingSetUniversity.getValue("classYear").stringValue()));
					universityProperties.add(bindingSetUniversity.getValue("cityName").stringValue());
					friendUniversities.add(universityProperties);
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
				
				TupleQuery tupleQueryCompanies = conn.prepareTupleQuery(
				        QueryLanguage.SPARQL, queryCompanies);
				
				TupleQueryResult tupleQueryCompaniesResult = tupleQueryCompanies.evaluate();
				
				while(tupleQueryCompaniesResult.hasNext())
				{
					BindingSet bindingSetCompany = tupleQueryCompaniesResult.next();
					List<Object> companyProperties = new ArrayList<>();
					companyProperties.add(bindingSetCompany.getValue("companyName").stringValue());
					companyProperties.add(Integer.parseInt(bindingSetCompany.getValue("workFrom").stringValue()));
					companyProperties.add(bindingSetCompany.getValue("countryName").stringValue());
					friendUniversities.add(companyProperties);
				}
				
				result.add(new LdbcQuery1Result(friendId, friendLastName, distanceFromPerson, friendBirthday, friendCreationDate, friendGender, friendBrowserUsed, friendLocationIp, friendEmails, friendLanguages, friendCityName, friendUniversities, friendCompanies));
			}
		} catch (QueryEvaluationException | RepositoryException | MalformedQueryException | ParseException e) {
			e.printStackTrace();
		}

		resultReporter.report(result.size(), result, ldbcQuery1);
	}
}