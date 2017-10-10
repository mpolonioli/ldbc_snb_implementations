package net.mpolonioli.ldbcimpls.janusgraph.interactive.queries;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11Result;

import net.mpolonioli.ldbcimpls.janusgraph.interactive.JanusGraphDbConnectionState;

/**
 * Given a start Person, find that Person’s friends and friends of friends
 * (excluding start Person) who started Working in some Company in a given
 * Country, before a given date (year). Return top 10 Persons, the Company
 * they worked at, and the year they started working at that Company. Sort
 * results ascending by the start date, then ascending by Person identifier,
 * and lastly by Organization name descending.[1]
 */
public class LdbcQuery11Handler
implements OperationHandler<LdbcQuery11, DbConnectionState> {

	final static Logger logger =
			LoggerFactory.getLogger(LdbcQuery11Handler.class);

	@Override
	public void executeOperation(final LdbcQuery11 operation,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		JanusGraph graph = ((JanusGraphDbConnectionState) dbConnectionState).getClient();
		GraphTraversalSource g = graph.traversal();

		long startPersonId = operation.personId();
		String countryName = operation.countryName();
		int year = operation.workFromYear();
		int limit = operation.limit();

		List<LdbcQuery11Result> result = new ArrayList<>();

		try
		{

			List<Map<String, Object>> resultSet =  
					g.V().has("personId", startPersonId).repeat(__.out("knows")).emit().times(2).has("personId", P.neq(startPersonId))
					.as("person")
					.outE("workAt")
					.where(__.values("workFrom").is(P.lte(year)))
					.as("workAt")
					.inV()
					.as("organization")
					.out("isLocatedIn")
					.has("name", countryName)
					.select("person")
					.order()
					.by(__.outE("workAt").values("workFrom"), Order.incr)
					.by("personId", Order.incr)
					.by(__.out("workAt").values("name"), Order.decr)
					.dedup()
					.limit(limit)
					.select("person", "organization", "workAt")
					.toList();

			for(int i = 0; i < resultSet.size(); i++)
			{
				long personId = 0;
				String personFirstName = null;
				String personLastName = null;
				String organizationName = null;
				int organizationWorkFromYear = 0;
				for (Map.Entry<String, Object> entry : resultSet.get(i).entrySet()) {
					String key = entry.getKey();
					if(key.equals("person"))
					{
						Vertex person = (Vertex) entry.getValue();
						personId = person.value("personId");
						personFirstName = person.value("firstName");
						personLastName = person.value("lastName");
					} else if(key.equals("organization"))
					{
						Vertex organization = (Vertex) entry.getValue();
						organizationName = organization.value("name");
					} else if (key.equals("workAt"))
					{
						Edge workAt = (Edge) entry.getValue();
						organizationWorkFromYear = workAt.value("workFrom");
					}
				}
				result.add(new LdbcQuery11Result(personId, personFirstName, personLastName, organizationName, organizationWorkFromYear));
			}
		}catch(Exception e)
		{
			e.printStackTrace();
		}
		resultReporter.report(result.size(), result, operation);
	}

}
