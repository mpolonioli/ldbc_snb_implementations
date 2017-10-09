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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForumResult;

import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaConnectionState;

public class LdbcShortQuery6MessageForumHandler implements OperationHandler<LdbcShortQuery6MessageForum, DbConnectionState> {
	
	public void executeOperation(
			LdbcShortQuery6MessageForum ldbcShortQuery6MessageForum,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {
		
		RepositoryConnection conn = (((RyaConnectionState) dbConnectionState).getClient());
		
		LdbcShortQuery6MessageForumResult result = null;
		
		long id = ldbcShortQuery6MessageForum.messageId();

		String query =
				"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" + 
				"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
				"SELECT ?forumId ?forumTitle ?personId ?firstName ?lastName\n" + 
				"WHERE {\n" + 
				"{\n" + 
				"?post snvoc:id \"" + id + "\"^^xsd:long ;\n" + 
				"	rdf:type snvoc:Post .\n" + 
				"\n" + 
				"?forum snvoc:containerOf ?post ;\n" + 
				"	snvoc:id ?forumId ;\n" + 
				"	snvoc:title ?forumTitle ;\n" + 
				"	snvoc:hasModerator ?person .\n" + 
				"\n" + 
				"?person snvoc:id ?personId ;\n" + 
				"	snvoc:firstName ?firstName ;\n" + 
				"	snvoc:lastName ?lastName .\n" + 
				"}\n" + 
				"UNION\n" + 
				"{\n" + 
				"?comment snvoc:id \"" + id + "\"^^xsd:long ;\n" + 
				"	rdf:type snvoc:Comment .\n" + 
				"\n" + 
				"?comment (snvoc:replyOf)+ ?post .\n" + 
				"?originalPost rdf:type snvoc:Post .\n" + 
				"\n" + 
				"?forum snvoc:containerOf ?post ;\n" + 
				"	snvoc:id ?forumId ;\n" + 
				"	snvoc:title ?forumTitle ;\n" + 
				"	snvoc:hasModerator ?person .\n" + 
				"\n" + 
				"?person snvoc:id ?personId ;\n" + 
				"	snvoc:firstName ?firstName ;\n" + 
				"	snvoc:lastName ?lastName .\n" + 
				"}\n" + 
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
				
				String moderatorFirstName = bindingSet.getValue("firstName").stringValue();
				String moderatorLastName = bindingSet.getValue("lastName").stringValue();
				long forumId = Long.parseLong(bindingSet.getValue("forumId").stringValue());
				long moderatorId = Long.parseLong(bindingSet.getValue("personId").stringValue());
				String forumTitle = bindingSet.getValue("forumTitle").stringValue();
				
				result = new LdbcShortQuery6MessageForumResult(forumId, forumTitle, moderatorId, moderatorFirstName, moderatorLastName);
			} else
			{
				result = new LdbcShortQuery6MessageForumResult(0, "null", 0, "null", "null");
			}
		}catch (QueryEvaluationException e) {
			result = new LdbcShortQuery6MessageForumResult(0, "null", 0, "null", "null");
			e.printStackTrace();
		}
		resultReporter.report(1, result, ldbcShortQuery6MessageForum);
	}
}
