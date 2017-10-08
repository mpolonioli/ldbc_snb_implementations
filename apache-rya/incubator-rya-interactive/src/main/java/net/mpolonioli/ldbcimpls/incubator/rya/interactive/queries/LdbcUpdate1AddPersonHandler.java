package net.mpolonioli.ldbcimpls.incubator.rya.interactive.queries;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate1AddPerson;

import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaClient;
import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaConnectionState;

public class LdbcUpdate1AddPersonHandler implements
OperationHandler<LdbcUpdate1AddPerson, DbConnectionState> {

	private static DateFormat creationDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
	private static DateFormat birthdateDateFormat = new SimpleDateFormat("yyyy-MM-dd");

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
