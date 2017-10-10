package net.mpolonioli.ldbcimpls.incubator.rya.interactive.queries;

import java.text.DateFormat;
import java.text.ParseException;
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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2Result;

import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaConnectionState;

public class LdbcQuery2Handler implements OperationHandler<LdbcQuery2, DbConnectionState> {

	private static DateFormat creationDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");

	public void executeOperation(
			LdbcQuery2 ldbcQuery2,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		RepositoryConnection conn = (((RyaConnectionState) dbConnectionState).getClient());

		List<LdbcQuery2Result> resultList = new ArrayList<LdbcQuery2Result>();

		long id = ldbcQuery2.personId();
		Date maxDate = ldbcQuery2.maxDate();
		int limit = ldbcQuery2.limit();

		String query = 
				"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" + 
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
				"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
				"\n" + 
				"SELECT ?friendId ?firstName ?lastName ?messageId ?content ?creationDate\n" + 
				"WHERE \n" + 
				"{\n" + 
				"?person snvoc:id \"" + id + "\"^^xsd:long ;\n" + 
				"	 rdf:type snvoc:Person .\n" + 
				"\n" + 
				"?person (snvoc:knows/snvoc:hasPerson) ?friend .\n" + 
				"\n" + 
				"VALUES (?messageType) { ( snvoc:Post ) ( snvoc:Comment ) }\n" + 
				"?post rdf:type ?messageType ;\n" + 
				"        snvoc:id ?messageId ;\n" + 
				"	snvoc:hasCreator ?friend ;\n" + 
				"	snvoc:creationDate ?creationDate ;\n" + 
				"	snvoc:imageFile | snvoc:content ?content .\n" + 
				"\n" + 
				"?friend snvoc:id ?friendId ;\n" + 
				"	snvoc:firstName ?firstName;\n" + 
				"	snvoc:lastName ?lastName .\n" + 
				"\n" + 
				"FILTER(?creationDate <= \"" + creationDateFormat.format(maxDate) + "\"^^xsd:dateTime)\n" + 
				"}\n" + 
				"ORDER BY DESC(?creationDate) ASC(?id)\n" + 
				"LIMIT " + limit
				;

		TupleQuery tupleQuery = null;
		try {
			tupleQuery = conn.prepareTupleQuery(
					QueryLanguage.SPARQL, query);
		} catch (RepositoryException | MalformedQueryException e) {
			e.printStackTrace();
		}

		tupleQuery.setMaxQueryTime(1800);
		
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
				long friendId = Long.parseLong(bindingSet.getValue("friendId").stringValue());
				String firstName = bindingSet.getValue("firstName").stringValue();
				String lastName = bindingSet.getValue("lastName").stringValue();
				long messageId = Long.parseLong(bindingSet.getValue("messageId").stringValue());
				String content = bindingSet.getValue("content").stringValue();
				long creationDate = creationDateFormat.parse(bindingSet.getValue("creationDate").stringValue()).getTime();
				
				resultList.add(new LdbcQuery2Result(friendId, firstName, lastName, messageId, content, creationDate));
			}
		} catch (QueryEvaluationException | ParseException e) {
			e.printStackTrace();
		}

		resultReporter.report(resultList.size(), resultList, ldbcQuery2);
	}
}
