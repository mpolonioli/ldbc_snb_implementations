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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8Result;

import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaConnectionState;

public class LdbcQuery8Handler implements OperationHandler<LdbcQuery8, DbConnectionState> {
	
	private static DateFormat creationDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");

	public void executeOperation(
			LdbcQuery8 ldbcQuery8,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		RepositoryConnection conn = (((RyaConnectionState) dbConnectionState).getClient());

		List<LdbcQuery8Result> result = new ArrayList<LdbcQuery8Result>();
		
		long id = ldbcQuery8.personId();
		int limit = ldbcQuery8.limit();

		String query =
				"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" + 
						"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
						"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
						"\n" + 
						"SELECT DISTINCT ?replierId ?firstName ?lastName ?commentDate ?commentId ?content\n" + 
						"WHERE \n" + 
						"{\n" + 
						"?person snvoc:id \"" + id + "\"^^xsd:long ; \n" + 
						"	rdf:type snvoc:Person .\n" + 
						"	 .\n" + 
						"\n" + 
						"VALUES (?messageType) { ( snvoc:Post ) ( snvoc:Comment ) }\n" + 
						"?message snvoc:hasCreator ?person ;\n" + 
						"	rdf:type ?messageType .\n" + 
						"	  .\n" + 
						"?comment snvoc:replyOf ?message ; \n" + 
						"	rdf:type snvoc:Comment ;\n" + 
						"	snvoc:id ?commentId ;\n" + 
						"	snvoc:content ?content ;\n" + 
						"	snvoc:creationDate ?commentDate ;\n" + 
						"	snvoc:hasCreator ?replier .\n" + 
						"\n" + 
						"?replier snvoc:id ?replierId ;\n" + 
						"	snvoc:firstName ?firstName ;\n" + 
						"	snvoc:lastName ?lastName .\n" + 
						"}\n" + 
						"ORDER BY DESC(?commentDate) ASC(?commentId)\n" + 
						"LIMIT " + limit
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
				
				long replierId = Long.parseLong(bindingSet.getValue("replierId").stringValue());
				long commentId = Long.parseLong(bindingSet.getValue("commentId").stringValue());
				long commentDate = creationDateFormat.parse(bindingSet.getValue("commentDate").stringValue()).getTime();
				String firstName = bindingSet.getValue("firstName").stringValue();
				String lastName = bindingSet.getValue("lastName").stringValue();
				String commentContent = bindingSet.getValue("content").stringValue();
				
				result.add(new LdbcQuery8Result(replierId, firstName, lastName, commentDate, commentId, commentContent));
			}
		}catch (QueryEvaluationException | ParseException e) {
			e.printStackTrace();
		}
		resultReporter.report(result.size(), result, ldbcQuery8);		
	}
}
