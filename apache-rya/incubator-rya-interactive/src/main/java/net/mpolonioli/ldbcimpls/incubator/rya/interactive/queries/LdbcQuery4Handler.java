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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4Result;

import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaConnectionState;

public class LdbcQuery4Handler implements OperationHandler<LdbcQuery4, DbConnectionState> {
	
	private static DateFormat creationDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
	
	public void executeOperation(
			LdbcQuery4 ldbcQuery4,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		RepositoryConnection conn = (((RyaConnectionState) dbConnectionState).getClient());

		List<LdbcQuery4Result> resultList = new ArrayList<LdbcQuery4Result>();

		long id = ldbcQuery4.personId();
		int limit = ldbcQuery4.limit();
		Date startDate = ldbcQuery4.startDate();
		int durationDays = ldbcQuery4.durationDays();
		long durationDaysInMilliseconds = (long) durationDays * 24 * 60 * 60 * 1000;
		Date endDate = new Date(startDate.getTime() + durationDaysInMilliseconds);

		String query = 
				"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" + 
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
				"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
				"PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n" + 
				"\n" + 
				"SELECT DISTINCT ?tagName (COUNT(?tagName) AS ?count)\n" + 
				"WHERE\n" + 
				"{\n" + 
				"?person snvoc:id \"" + id + "\"^^xsd:long ;\n" + 
				"	 rdf:type snvoc:Person .\n" + 
				"\n" + 
				"?person (snvoc:knows/snvoc:hasPerson) ?friend .\n" + 
				"\n" + 
				"?post snvoc:hasCreator ?friend ; \n" + 
				"	rdf:type snvoc:Post ;\n" + 
				"	snvoc:creationDate ?date ;\n" + 
				"	snvoc:hasTag ?tag .\n" + 
				"\n" + 
				"?tag foaf:name ?tagName .\n" + 
				"\n" + 
				"FILTER(?date < \"" + creationDateFormat.format(startDate) + "\"^^xsd:dateTime)\n" + 
				"FILTER(?date > \"" + creationDateFormat.format(endDate) + "\"^^xsd:dateTime)\n" + 
				"}\n" + 
				"GROUP BY ?tagName\n" + 
				"ORDER BY DESC(?count) ASC(?tagName)\n" + 
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
				try {
					String tagName = bindingSet.getValue("tagName").stringValue();
					int postCount = Integer.parseInt(bindingSet.getValue("count").stringValue());
					resultList.add(new LdbcQuery4Result(tagName, postCount));
				}catch(NullPointerException e)
				{
				}
			}
		}catch (QueryEvaluationException e) {
			e.printStackTrace();
		}

		resultReporter.report(resultList.size(), resultList, ldbcQuery4);
	}
}
