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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPosts;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPostsResult;

import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaConnectionState;

public class LdbcShortQuery2PersonPostsHandler implements OperationHandler<LdbcShortQuery2PersonPosts, DbConnectionState> {

	private static DateFormat creationDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");

	public void executeOperation(
			LdbcShortQuery2PersonPosts ldbcShortQuery2PersonPosts,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		RepositoryConnection conn = (((RyaConnectionState) dbConnectionState).getClient());

		List<LdbcShortQuery2PersonPostsResult> result = new ArrayList<LdbcShortQuery2PersonPostsResult>();

		long id = ldbcShortQuery2PersonPosts.personId();
		int limit = ldbcShortQuery2PersonPosts.limit();

		String query =
				"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" + 
				"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
				"SELECT DISTINCT ?messageId ?content ?creationDate ?postId ?personId ?firstName ?lastName\n" + 
				"WHERE {\n" + 
				"{\n" + 
				"?person snvoc:id \"" + id + "\"^^xsd:long ; \n" + 
				"	rdf:type snvoc:Person .\n" + 
				"\n" + 
				"?message snvoc:hasCreator ?person ;\n" + 
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
				"?person snvoc:id \"" + id + "\"^^xsd:long ; \n" + 
				"	rdf:type snvoc:Person .\n" + 
				"\n" + 
				"?comment snvoc:hasCreator ?person ;\n" + 
				"	rdf:type snvoc:Comment ;\n" + 
				"	snvoc:id ?messageId ;\n" + 
				"	snvoc:content ?content ;\n" + 
				"	snvoc:creationDate ?creationDate .\n" + 
				"\n" + 
				"?comment (snvoc:replyOf)+ ?originalMessage .\n" + 
				"\n" + 
				"?originalMessage rdf:type snvoc:Post ;\n" + 
				"	snvoc:id ?postId ;\n" + 
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
			while(tupleQueryResult.hasNext())
			{
				BindingSet bindingSet = tupleQueryResult.next();
				
				String firstName = bindingSet.getValue("firstName").stringValue();
				String lastName = bindingSet.getValue("lastName").stringValue();
				long personId = Long.parseLong(bindingSet.getValue("personId").stringValue());
				long postId = Long.parseLong(bindingSet.getValue("postId").stringValue());
				String content = bindingSet.getValue("content").stringValue();
				long messageId = Long.parseLong(bindingSet.getValue("messageId").stringValue());
				long messageDate = creationDateFormat.parse(bindingSet.getValue("creationDate").stringValue()).getTime();
				
				result.add(new LdbcShortQuery2PersonPostsResult(messageId, content, messageDate, postId, personId, firstName, lastName));
			}
		}catch (QueryEvaluationException | ParseException e) {
			e.printStackTrace();
		}

		resultReporter.report(result.size(), result, ldbcShortQuery2PersonPosts);
	}
}
