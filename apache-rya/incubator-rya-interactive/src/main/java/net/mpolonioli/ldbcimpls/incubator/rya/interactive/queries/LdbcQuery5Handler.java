package net.mpolonioli.ldbcimpls.incubator.rya.interactive.queries;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5Result;

import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaConnectionState;

public class LdbcQuery5Handler implements OperationHandler<LdbcQuery5, DbConnectionState> {
	
	private static DateFormat creationDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
	
	public void executeOperation(
			LdbcQuery5 ldbcQuery5,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		RepositoryConnection conn = (((RyaConnectionState) dbConnectionState).getClient());
		
		List<LdbcQuery5Result> resultList = new ArrayList<LdbcQuery5Result>();
		int resultsCount = 0;

		long id = ldbcQuery5.personId();
		int limit = ldbcQuery5.limit();
		Date minDate = ldbcQuery5.minDate();

		String query =
				"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" + 
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
				"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
				"PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n" + 
				"\n" + 
				"SELECT DISTINCT ?title (COUNT(?post) AS ?count)\n" + 
				"WHERE \n" + 
				"{\n" + 
				"?person snvoc:id \"" + id + "\"^^xsd:long ;\n" + 
				"	rdf:type snvoc:Person ;\n" + 
				"	snvoc:knows ?knowObject .\n" + 
				"\n" + 
				"?person \n" + 
				"	(snvoc:knows/snvoc:hasPerson)|\n" + 
				"	(snvoc:knows/snvoc:hasPerson/snvoc:knows/snvoc:hasPerson)|\n" + 
				"		?friend .\n" + 
				"FILTER ( ?person != ?friend )\n" + 
				"\n" + 
				"?forum (snvoc:hasMember/snvoc:hasPerson) ?friend .\n" + 
				"?forum rdf:type snvoc:Forum ;\n" + 
				"	snvoc:hasMember ?memberObject ;\n" + 
				"	snvoc:id ?id ;\n" + 
				"	snvoc:title ?title .\n" + 
				"?memberObject snvoc:joinDate ?joinDate .\n" + 
				"FILTER(?joinDate > \"" + creationDateFormat.format(minDate) + "\"^^xsd:dateTime)\n" + 
				"\n" + 
				"?forum snvoc:containerOf ?post .\n" + 
				"\n" + 
				"?post snvoc:hasCreator ?friend ; \n" + 
				"	rdf:type snvoc:Post .\n" + 
				"}\n" + 
				"GROUP BY ?title\n" + 
				"ORDER BY DESC(?count) ASC(?id)\n" + 
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
				try
				{
					BindingSet bindingSet = tupleQueryResult.next();
					String forumTitle = bindingSet.getValue("title").stringValue();
					int postCount = Integer.parseInt(bindingSet.getValue("count").stringValue());
					resultList.add(new LdbcQuery5Result(forumTitle, postCount));
				}catch(NullPointerException e)
				{
				}
			}
		}catch (QueryEvaluationException e) {
			e.printStackTrace();
		}

		resultReporter.report(resultsCount, resultList, ldbcQuery5);
	}
}