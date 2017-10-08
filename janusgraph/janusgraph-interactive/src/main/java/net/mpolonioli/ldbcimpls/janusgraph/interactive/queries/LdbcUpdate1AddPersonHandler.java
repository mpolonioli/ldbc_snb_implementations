package net.mpolonioli.ldbcimpls.janusgraph.interactive.queries;

import java.util.ArrayList;
import java.util.List;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.SchemaViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate1AddPerson;

import net.mpolonioli.ldbcimpls.janusgraph.interactive.JanusGraphDbConnectionState;

/**
 * Add a Person to the social network. [1]
 */
public class LdbcUpdate1AddPersonHandler implements
OperationHandler<LdbcUpdate1AddPerson, DbConnectionState> {

	final static Logger logger =
			LoggerFactory.getLogger(LdbcUpdate1AddPersonHandler.class);

	@Override
	public void executeOperation(LdbcUpdate1AddPerson operation,
			DbConnectionState dbConnectionState,
			ResultReporter reporter) throws DbException {
		Graph client = ((JanusGraphDbConnectionState) dbConnectionState).getClient();
		GraphTraversalSource g = client.traversal();
		try
		{
			// Build key value properties array
			List<Object> personKeyValues =
					new ArrayList<>(18 + 2 * operation.languages().size()
							+ 2 * operation.emails().size());
			personKeyValues.add("personId");
			personKeyValues.add(operation.personId());
			personKeyValues.add(T.label);
			personKeyValues.add("person");
			personKeyValues.add("firstName");
			personKeyValues.add(operation.personFirstName());
			personKeyValues.add("lastName");
			personKeyValues.add(operation.personLastName());
			personKeyValues.add("gender");
			personKeyValues.add(operation.gender());
			personKeyValues.add("birthday");
			personKeyValues.add(String.valueOf(operation.birthday().getTime()));
			personKeyValues.add("creationDate");
			personKeyValues.add(String.valueOf(operation.creationDate().getTime()));
			personKeyValues.add("locationIP");
			personKeyValues.add(operation.locationIp());
			personKeyValues.add("browserUsed");
			personKeyValues.add(operation.browserUsed());

			for (String language : operation.languages()) {
				personKeyValues.add("language");
				personKeyValues.add(language);
			}

			for (String email : operation.emails()) {
				personKeyValues.add("email");
				personKeyValues.add(email);
			}

			// Add person
			Vertex person = client.addVertex(personKeyValues.toArray());

			// Add edge to place
			Vertex place = g.V().has("placeId", operation.cityId()).next();
			person.addEdge("isLocatedIn", place);

			// Add edges to tags
			for (long tagId : operation.tagIds()) {
				Vertex tag = g.V().has("tagId", tagId).next();
				person.addEdge("hasInterest", tag);
			}

			// Add edges to universities
			List<Object> studiedAtKeyValues = new ArrayList<>(2);
			for (LdbcUpdate1AddPerson.Organization org : operation.studyAt()) {
				studiedAtKeyValues.clear();
				studiedAtKeyValues.add("classYear");
				studiedAtKeyValues.add(org.year());
				Vertex orgV = g.V().has("organisationId", org.organizationId()).next();
				person.addEdge("studyAt", orgV, studiedAtKeyValues.toArray());
			}

			// Add edges to companies
			List<Object> workedAtKeyValues = new ArrayList<>(2);
			for (LdbcUpdate1AddPerson.Organization org : operation.workAt()) {
				workedAtKeyValues.clear();
				workedAtKeyValues.add("workFrom");
				workedAtKeyValues.add(String.valueOf(org.year()));
				Vertex orgV = g.V().has("organisationId", org.organizationId()).next();
				person.addEdge("workAt", orgV, workedAtKeyValues.toArray());
			}

			client.tx().commit();
		}catch(SchemaViolationException e)
		{

		}
		reporter.report(0, LdbcNoResult.INSTANCE, operation);
	}
}
