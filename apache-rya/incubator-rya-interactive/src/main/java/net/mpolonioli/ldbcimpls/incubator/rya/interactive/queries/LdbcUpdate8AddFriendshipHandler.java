package net.mpolonioli.ldbcimpls.incubator.rya.interactive.queries;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate8AddFriendship;

import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaConnectionState;

public class LdbcUpdate8AddFriendshipHandler implements
OperationHandler<LdbcUpdate8AddFriendship, DbConnectionState> {

	private static DateFormat creationDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");

	public void executeOperation(
			LdbcUpdate8AddFriendship ldbcUpdate8AddFriendship,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		RepositoryConnection conn = (((RyaConnectionState) dbConnectionState).getClient());

		long person1Id = ldbcUpdate8AddFriendship.person1Id();
		long person2Id = ldbcUpdate8AddFriendship.person2Id();
		String creationDate = creationDateFormat.format(ldbcUpdate8AddFriendship.creationDate());

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

		try {
			TupleQuery tupleQuery = conn.prepareTupleQuery(
					QueryLanguage.SPARQL, query);
			tupleQuery.evaluate();
		} catch (RepositoryException | MalformedQueryException | QueryEvaluationException e) {
			e.printStackTrace();
		}
		resultReporter.report(0, LdbcNoResult.INSTANCE, ldbcUpdate8AddFriendship);
	}
}