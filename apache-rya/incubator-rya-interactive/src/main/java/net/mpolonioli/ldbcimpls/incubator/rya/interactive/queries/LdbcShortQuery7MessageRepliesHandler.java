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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageReplies;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageRepliesResult;

import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaConnectionState;

public class LdbcShortQuery7MessageRepliesHandler implements OperationHandler<LdbcShortQuery7MessageReplies, DbConnectionState> {
	
	private static DateFormat creationDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");

	public void executeOperation(
			LdbcShortQuery7MessageReplies ldbcShortQuery7MessageReplies,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		RepositoryConnection conn = (((RyaConnectionState) dbConnectionState).getClient());

		List<LdbcShortQuery7MessageRepliesResult> result = new ArrayList<LdbcShortQuery7MessageRepliesResult>();

		long id = ldbcShortQuery7MessageReplies.messageId();

		String query =
				"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" + 
				"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
				"SELECT ?commentId ?commentContent ?commentCreationDate ?personId ?firstName ?lastName ?boolean\n" + 
				"WHERE {\n" + 
				"VALUES (?messageType) { ( snvoc:Post ) ( snvoc:Comment ) }\n" + 
				"?message snvoc:id \"" + id + "\"^^xsd:long  ;\n" + 
				"	rdf:type ?messageType ;\n" + 
				"	snvoc:hasCreator ?author .\n" + 
				"\n" + 
				"?comment snvoc:replyOf ?message ;\n" + 
				"	snvoc:id ?commentId ;\n" + 
				"	snvoc:content ?commentContent ;\n" + 
				"	snvoc:hasCreator ?person ;\n" + 
				"	snvoc:creationDate ?commentCreationDate .\n" + 
				"\n" + 
				"?person snvoc:knows ?knowObject .\n" + 
				"\n" + 
				"?person snvoc:id ?personId ;\n" + 
				"	snvoc:firstName ?firstName ;\n" + 
				"	snvoc:lastName ?lastName .\n" + 
				"\n" + 
				"BIND(\n" + 
				"	EXISTS{?knowObject snvoc:hasPerson ?author}\n" + 
				"	AS ?boolean\n" + 
				")\n" + 
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
				
				String replyAuthorFirstName = bindingSet.getValue("firstName").stringValue();
				String replyAuthorLastName = bindingSet.getValue("lastname").stringValue();
				long replyAuthorId = Long.parseLong(bindingSet.getValue("personId").stringValue());
				long commentId = Long.parseLong(bindingSet.getValue("commentId").stringValue());
				String commentContent = bindingSet.getValue("commentContent").stringValue();
				boolean replyAuthorKnowsOriginalMessageAuthor = Boolean.parseBoolean(bindingSet.getValue("boolean").stringValue());
				long commentCreationDate = creationDateFormat.parse(bindingSet.getValue("commentCreationDate").stringValue()).getTime();

				result.add(new LdbcShortQuery7MessageRepliesResult(commentId, commentContent, commentCreationDate, replyAuthorId, replyAuthorFirstName, replyAuthorLastName, replyAuthorKnowsOriginalMessageAuthor));
			} else
			{
			}
		}catch (QueryEvaluationException | ParseException e) {
			e.printStackTrace();
		}
		resultReporter.report(result.size(), result, ldbcShortQuery7MessageReplies);
	}
}
