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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriends;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriendsResult;

import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaConnectionState;

public class LdbcShortQuery3PersonFriendsHandler
implements OperationHandler<LdbcShortQuery3PersonFriends, DbConnectionState> {

	private static DateFormat creationDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");

	public void executeOperation(LdbcShortQuery3PersonFriends ldbcShortQuery3PersonFriends,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		RepositoryConnection conn = (((RyaConnectionState) dbConnectionState).getClient());

		List<LdbcShortQuery3PersonFriendsResult> result = new ArrayList<LdbcShortQuery3PersonFriendsResult>();

		long id = ldbcShortQuery3PersonFriends.personId();

		String query = 
				"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" + 
				"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
				"\n" + 
				"SELECT ?personId ?personFirstname ?personLastName ?knowCreationDate\n" + 
				"WHERE \n" + 
				"{\n" + 
				"?person snvoc:id \"" + id + "\"^^xsd:long \n" + 
				"	rdf:type snvoc:Person .\n" + 
				"\n" + 
				"?person snvoc:knows ?knowObject .\n" + 
				"?knowObject snvoc:hasPerson ?friend ;\n" + 
				"	snvoc:creationDate ?knowCreationDate .\n" + 
				"\n" + 
				"?friend snvoc:id ?personId ;\n" + 
				"	snvoc:firstName ?personFirstName ;\n" + 
				"	snvoc:lastName ?personLastName .\n" + 
				"}\n" + 
				"ORDER BY DESC(?creationDate) ASC(?personId)"
				;

		TupleQuery tupleQuery = null;

		try {
			tupleQuery = conn.prepareTupleQuery(
					QueryLanguage.SPARQL, query);
		} catch (RepositoryException | MalformedQueryException e) {
			e.printStackTrace();
		}

		TupleQueryResult tupleQueryResult = null;
		try {
			tupleQueryResult = tupleQuery.evaluate();
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}

		try {
			while(tupleQueryResult.hasNext())
			{
				BindingSet bindingSet = tupleQueryResult.next();
				
				String firstName = bindingSet.getValue("personFirstname").stringValue();
				String lastName = bindingSet.getValue("personLastName").stringValue();
				long personId = Long.parseLong(bindingSet.getValue("personId").stringValue());
				long friendshipCreationDate = creationDateFormat.parse(bindingSet.getValue("knowCreationDate").stringValue()).getTime();
				
				result.add(new LdbcShortQuery3PersonFriendsResult(personId, firstName, lastName, friendshipCreationDate));
			}
		}catch (QueryEvaluationException | ParseException e) {
			e.printStackTrace();
		}
		
		resultReporter.report(result.size(), result, ldbcShortQuery3PersonFriends);
	}
}
