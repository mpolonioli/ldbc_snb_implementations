package net.mpolonioli.ldbcimpls.incubator.rya.interactive.queries;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.Update;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate2AddPostLike;

import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaConnectionState;

public class LdbcUpdate2AddPostLikeHandler implements
OperationHandler<LdbcUpdate2AddPostLike, DbConnectionState> {

	private static DateFormat creationDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");

	public void executeOperation(
			LdbcUpdate2AddPostLike ldbcUpdate2AddPostLike,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {
		RepositoryConnection conn = (((RyaConnectionState) dbConnectionState).getClient());

		long postId = ldbcUpdate2AddPostLike.postId();
		long personId = ldbcUpdate2AddPostLike.personId();
		String creationDate = creationDateFormat.format(ldbcUpdate2AddPostLike.creationDate());
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

		try {
			Update update = conn.prepareUpdate(QueryLanguage.SPARQL, query);
			update.execute();
		} catch (RepositoryException | MalformedQueryException | UpdateExecutionException e) {
			e.printStackTrace();
		}

		resultReporter.report(0, LdbcNoResult.INSTANCE, ldbcUpdate2AddPostLike);
	}
}
