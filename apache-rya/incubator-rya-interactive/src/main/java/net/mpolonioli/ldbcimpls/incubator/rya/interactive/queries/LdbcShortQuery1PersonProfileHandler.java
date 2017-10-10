package net.mpolonioli.ldbcimpls.incubator.rya.interactive.queries;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfile;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfileResult;

import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaConnectionState;

public class LdbcShortQuery1PersonProfileHandler implements OperationHandler<LdbcShortQuery1PersonProfile, DbConnectionState> {
	
	private static DateFormat creationDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
	private static DateFormat birthdateDateFormat = new SimpleDateFormat("yyyy-MM-dd");
	
	public void executeOperation(
			LdbcShortQuery1PersonProfile ldbcShortQuery1PersonProfile,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		RepositoryConnection conn = (((RyaConnectionState) dbConnectionState).getClient());

		LdbcShortQuery1PersonProfileResult result = null;

		long id = ldbcShortQuery1PersonProfile.personId();

		String query =
				"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" + 
				"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
				"\n" + 
				"SELECT ?firstName ?lastName ?birthday ?locationIp ?browserUsed ?placeId ?gender ?creationDate \n" + 
				"WHERE {\n" + 
				"?person snvoc:id \"" + id  + "\"^^xsd:long ; \n" + 
				"	rdf:type snvoc:Person ;\n" + 
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

		TupleQuery tupleQuery = null;
		try {
			tupleQuery = conn.prepareTupleQuery(
					QueryLanguage.SPARQL, query);
		} catch (RepositoryException | MalformedQueryException e) {
			e.printStackTrace();
		}
		
		tupleQuery.setMaxQueryTime(2100);

		TupleQueryResult tupleQueryResult = null;
		try {
			tupleQueryResult = tupleQuery.evaluate();
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}

		try {
			if(tupleQueryResult.hasNext())
			{
				BindingSet bindingSet = tupleQueryResult.next();
				
				String firstName = bindingSet.getValue("firstName").stringValue();
				String lastName = bindingSet.getValue("lastName").stringValue();
				long birthday = birthdateDateFormat.parse(bindingSet.getValue("birthday").stringValue()).getTime();
				long creationDate = creationDateFormat.parse(bindingSet.getValue("creationDate").stringValue()).getTime();
				String locationIp = bindingSet.getValue("locationIp").stringValue();
				String browserUsed = bindingSet.getValue("browserUsed").stringValue();
				String gender = bindingSet.getValue("gender").stringValue();
				long placeId = Long.parseLong(bindingSet.getValue("placeId").stringValue());
				
				result = new LdbcShortQuery1PersonProfileResult(firstName, lastName, birthday, locationIp, browserUsed, placeId, gender, creationDate);
			}else
			{
				result = new LdbcShortQuery1PersonProfileResult("null", "null", 0, "null", "null", 0, "null", 0);
			}
		}catch (QueryEvaluationException | ParseException e) {
			e.printStackTrace();
			result = new LdbcShortQuery1PersonProfileResult("null", "null", 0, "null", "null", 0, "null", 0);
		}
		resultReporter.report(1, result, ldbcShortQuery1PersonProfile);
	}
}
