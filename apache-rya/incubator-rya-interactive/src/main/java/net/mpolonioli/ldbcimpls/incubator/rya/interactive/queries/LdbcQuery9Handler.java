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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9Result;

import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaConnectionState;

public class LdbcQuery9Handler implements OperationHandler<LdbcQuery9, DbConnectionState> {
	
	private static DateFormat creationDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");

	public void executeOperation(
			LdbcQuery9 ldbcQuery9,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		RepositoryConnection conn = (((RyaConnectionState) dbConnectionState).getClient());

		List<LdbcQuery9Result> result = new ArrayList<LdbcQuery9Result>();

		long id = ldbcQuery9.personId();
		int limit = ldbcQuery9.limit();
		String maxDate = creationDateFormat.format(ldbcQuery9.maxDate());

		String query =
						"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" + 
						"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
						"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
						"SELECT DISTINCT ?personId ?firstName ?lastName ?messageId ?content ?messageDate\n" + 
						"WHERE\n" + 
						"{\n" + 
						"?person snvoc:id \"" + id + "\"^^xsd:long ; \n" + 
						"	rdf:type snvoc:Person .\n" + 
						"\n" + 
						"?person \n" + 
						"	(snvoc:knows/snvoc:hasPerson)|\n" + 
						"	(snvoc:knows/snvoc:hasPerson/snvoc:knows/snvoc:hasPerson) \n" + 
						"		?friend .\n" + 
						"FILTER ( ?person != ?friend )\n" + 
						"\n" + 
						"?friend snvoc:id ?personId ;\n" + 
						"	snvoc:firstName ?firstName ;\n" + 
						"	snvoc:lastName ?lastName .\n" + 
						"\n" + 
						"VALUES (?messageType) { ( snvoc:Post ) ( snvoc:Comment ) }\n" + 
						"?message snvoc:hasCreator ?friend ;\n" + 
						"	rdf:type ?messageType ;\n" + 
						"	snvoc:creationDate ?messageDate ;\n" + 
						"	snvoc:id ?messageId ;\n" + 
						"	snvoc:imageFile | snvoc:Content ?content .\n" + 
						"\n" + 
						"FILTER(?messageDate < \"" + maxDate + "\"^^xsd:dateTime)\n" + 
						"}\n" + 
						"ORDER BY DESC(?creationDate) ASC(?messageId)\n" + 
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
				
				long personId = Long.parseLong(bindingSet.getValue("personId").stringValue());
				long messageId = Long.parseLong(bindingSet.getValue("messageId").stringValue());
				long messageDate = creationDateFormat.parse(bindingSet.getValue("messageDate").stringValue()).getTime();
				String firstName = bindingSet.getValue("firstName").stringValue();
				String lastName = bindingSet.getValue("lastName").stringValue();
				String content = bindingSet.getValue("content").stringValue();
				
				result.add(new LdbcQuery9Result(personId, firstName, lastName, messageId, content, messageDate));
			}
		}catch (QueryEvaluationException | ParseException e) {
			e.printStackTrace();
		}
		resultReporter.report(result.size(), result, ldbcQuery9);
	}
}