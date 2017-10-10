package net.mpolonioli.ldbcimpls.incubator.rya.interactive.queries;

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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreator;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreatorResult;

import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaConnectionState;

public class LdbcShortQuery5MessageCreatorHandler implements OperationHandler<LdbcShortQuery5MessageCreator, DbConnectionState> {

	public void executeOperation(
			LdbcShortQuery5MessageCreator ldbcShortQuery5MessageCreator,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		RepositoryConnection conn = (((RyaConnectionState) dbConnectionState).getClient());

		LdbcShortQuery5MessageCreatorResult result = null;

		long id = ldbcShortQuery5MessageCreator.messageId();

		String query =
				"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" + 
				"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
				"SELECT ?personId ?firstName ?lastName\n" + 
				"WHERE\n" + 
				"{\n" + 
				"VALUES (?messageType) { ( snvoc:Post ) ( snvoc:Comment ) }\n" + 
				"?message snvoc:id \"" + id + "\"^^xsd:long ;\n" + 
				"	rdf:type ?messageType ;\n" + 
				"	snvoc:hasCreator ?person .\n" + 
				"\n" + 
				"?person snvoc:id ?personId ;\n" + 
				"	snvoc:firstName ?firstName ;\n" + 
				"	snvoc:lastName ?lastName .\n" + 
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

				long personId = Long.parseLong(bindingSet.getValue("personId").stringValue());
				String firstName = bindingSet.getValue("firstName").stringValue();
				String lastName = bindingSet.getValue("lastName").stringValue();

				result = new LdbcShortQuery5MessageCreatorResult(personId, firstName, lastName);
			} else
			{
				result = new LdbcShortQuery5MessageCreatorResult(0, "null", "null");			
			}
		}catch (QueryEvaluationException e) {
			e.printStackTrace();
			result = new LdbcShortQuery5MessageCreatorResult(0, "null", "null");		
		}
		resultReporter.report(1, result, ldbcShortQuery5MessageCreator);
	}
}
