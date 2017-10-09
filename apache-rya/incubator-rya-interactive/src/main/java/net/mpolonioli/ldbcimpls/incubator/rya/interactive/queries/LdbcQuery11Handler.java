package net.mpolonioli.ldbcimpls.incubator.rya.interactive.queries;

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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11Result;

import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaConnectionState;

public class LdbcQuery11Handler implements OperationHandler<LdbcQuery11, DbConnectionState> {

	public void executeOperation(
			LdbcQuery11 ldbcQuery11,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		RepositoryConnection conn = (((RyaConnectionState) dbConnectionState).getClient());

		List<LdbcQuery11Result> result = new ArrayList<LdbcQuery11Result>();

		long id = ldbcQuery11.personId();
		int limit = ldbcQuery11.limit();
		int workFromYear = ldbcQuery11.workFromYear();
		String countryName = ldbcQuery11.countryName();

		String query =
				"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" + 
						"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
						"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
						"PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n" + 
						"\n" + 
						"SELECT DISTINCT ?personId ?firstName ?lastName ?companyName ?classYear\n" + 
						"WHERE\n" + 
						"{\n" + 
						"?person snvoc:id \"" + id + "\"^^xsd:long ;\n" + 
						"	rdf:type snvoc:Person .\n" + 
						"\n" + 
						"?person \n" + 
						"	(snvoc:knows/snvoc:hasPerson)|\n" + 
						"	(snvoc:knows/snvoc:hasPerson/snvoc:knows/snvoc:hasPerson) \n" + 
						"		?friend .\n" + 
						"FILTER ( ?person != ?friend )\n" + 
						"\n" + 
						"?friend snvoc:id ?personId ;\n" + 
						"	snvoc:workAt ?workObject ;\n" + 
						"	snvoc:firstName ?firstName ;\n" + 
						"	snvoc:lastName ?lastName .\n" + 
						"\n" + 
						"?workObject snvoc:hasOrganisation ?company ;\n" + 
						"	snvoc:workFrom ?classYear .\n" + 
						"FILTER(?classYear < " + workFromYear + ")\n" + 
						"\n" + 
						"?company snvoc:isLocatedIn ?country ;\n" + 
						"	foaf:name ?companyName .\n" + 
						"\n" + 
						"?country foaf:name \"" + countryName + "\" .\n" + 
						"}\n" + 
						"ORDER BY ASC(?classYear) ASC(?personId) DESC(?companyName)\n" + 
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
				
				long personId = Long.parseLong(bindingSet.getValue("personId").stringValue());
				String firstName = bindingSet.getValue("firstName").stringValue();
				String lastName = bindingSet.getValue("lastName").stringValue();
				String organizationName = bindingSet.getValue("companyName").stringValue();
				int organizationWorkFromYear = Integer.parseInt(bindingSet.getValue("classYear").stringValue());
				
				result.add(new LdbcQuery11Result(personId, firstName, lastName, organizationName, organizationWorkFromYear));
			}
		}catch (QueryEvaluationException e) {
			e.printStackTrace();
		}
		resultReporter.report(result.size(), result, ldbcQuery11);
	}
}
