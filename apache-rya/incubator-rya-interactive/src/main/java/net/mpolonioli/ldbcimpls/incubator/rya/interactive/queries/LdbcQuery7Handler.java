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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7Result;

import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaConnectionState;

public class LdbcQuery7Handler implements OperationHandler<LdbcQuery7, DbConnectionState> {
	
	private static DateFormat creationDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");

	public void executeOperation(
			LdbcQuery7 ldbcQuery7,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		RepositoryConnection conn = (((RyaConnectionState) dbConnectionState).getClient());

		List<LdbcQuery7Result> result = new ArrayList<LdbcQuery7Result>();

		long id = ldbcQuery7.personId();
		int limit = ldbcQuery7.limit();

		String query =
				"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" + 
						"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
						"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
						"\n" + 
						"SELECT DISTINCT ?likerId ?firstName ?lastName ?likeDate ?messageId ?content ?messageDate ?isNew\n" + 
						"WHERE \n" + 
						"{\n" + 
						"?person snvoc:id \"" + id + "\"^^xsd:long ; \n" + 
						"	rdf:type snvoc:Person .\n" + 
						"	 .\n" + 
						"\n" + 
						"VALUES (?messageType) { ( snvoc:Post ) ( snvoc:Comment ) }\n" + 
						"?post snvoc:hasCreator ?person ;\n" + 
						"	rdf:type ?messageType ;\n" + 
						"	snvoc:creationDate ?messageDate ;\n" + 
						"	snvoc:Content | snvoc:imageFile ?content ;\n" + 
						"	snvoc:id ?messageId .\n" + 
						"\n" + 
						"?like snvoc:hasPost ?post ;\n" + 
						"	snvoc:creationDate ?likeDate .\n" + 
						"\n" + 
						"?liker snvoc:likes ?like ;\n" + 
						"	snvoc:id ?likerId ;\n" + 
						"	snvoc:firstName ?firstName ;\n" + 
						"	snvoc:lastName ?lastName .\n" + 
						"\n" + 
						"BIND(\n" + 
						"	EXISTS { ?liker (snvoc:knows/snvoc:hasPerson) ?person } \n" + 
						"	AS ?isNew\n" + 
						")\n" + 
						"ORDER BY DESC(?likeDate) ASC(?likerId)\n" + 
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
				long likeCreationDate = creationDateFormat.parse(bindingSet.getValue("likeDate").stringValue()).getTime();
				long messageDate = creationDateFormat.parse(bindingSet.getValue("messageDate").stringValue()).getTime();
				int minutesLatency = (int)((likeCreationDate - messageDate) / (1000 * 60));
				long personId = Long.parseLong(bindingSet.getValue("likerId").stringValue());
				long commentOrPostId = Long.parseLong(bindingSet.getValue("messageId").stringValue());
				String personFirstName = bindingSet.getValue("firstName").stringValue();
				String personLastName = bindingSet.getValue("lastName").stringValue();
				String commentOrPostContent = bindingSet.getValue("content").stringValue();
				boolean isNew = Boolean.parseBoolean(bindingSet.getValue("isNew").stringValue());
				result.add(new LdbcQuery7Result(personId, personFirstName, personLastName, likeCreationDate, commentOrPostId, commentOrPostContent, minutesLatency, isNew));
			}
		}catch (QueryEvaluationException | ParseException e) {
			e.printStackTrace();
		}

		resultReporter.report(result.size(), result, ldbcQuery7);		
	}
}
