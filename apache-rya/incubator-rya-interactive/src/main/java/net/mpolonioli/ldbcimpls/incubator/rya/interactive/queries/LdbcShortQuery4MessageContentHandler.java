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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContent;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContentResult;

import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaConnectionState;

public class LdbcShortQuery4MessageContentHandler implements OperationHandler<LdbcShortQuery4MessageContent, DbConnectionState> {

	private static DateFormat creationDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");

	public void executeOperation(
			LdbcShortQuery4MessageContent ldbcShortQuery4MessageContent,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		RepositoryConnection conn = (((RyaConnectionState) dbConnectionState).getClient());

		LdbcShortQuery4MessageContentResult result = null;

		long id = ldbcShortQuery4MessageContent.messageId();

		String query =
				"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" + 
				"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
				"\n" + 
				"SELECT ?creationDate ?content\n" + 
				"WHERE\n" + 
				"{\n" + 
				"VALUES (?messageType) { ( snvoc:Post ) ( snvoc:Comment ) }\n" + 
				"?message snvoc:id \"" + id + "\"^^xsd:long ;\n" + 
				"	rdf:type ?messageType ;\n" + 
				"	snvoc:content | snvoc:imageFile ?content;\n" + 
				"	snvoc:creationDate ?creationDate .\n" + 
				"}"
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
			if(tupleQueryResult.hasNext())
			{
				BindingSet bindingSet = tupleQueryResult.next();

				String messageContent = bindingSet.getValue("content").stringValue();
				long messageCreationDate = creationDateFormat.parse(bindingSet.getValue("creationDate").stringValue()).getTime();

				result = new LdbcShortQuery4MessageContentResult(messageContent, messageCreationDate);
			} else
			{
				result = new LdbcShortQuery4MessageContentResult("null", 0);
			}
		}catch (QueryEvaluationException | ParseException e) {
			e.printStackTrace();
			result = new LdbcShortQuery4MessageContentResult("null", 0);
		}
		resultReporter.report(1, result, ldbcShortQuery4MessageContent);
	}
}
