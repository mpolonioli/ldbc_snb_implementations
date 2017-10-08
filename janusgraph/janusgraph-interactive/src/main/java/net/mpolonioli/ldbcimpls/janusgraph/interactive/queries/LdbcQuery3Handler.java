package net.mpolonioli.ldbcimpls.janusgraph.interactive.queries;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3Result;

import net.mpolonioli.ldbcimpls.janusgraph.interactive.JanusGraphDbConnectionState;


/**
 * Given a start Person, find Persons that are their friends and friends of
 * friends (excluding start Person) that have made Posts/Comments in both of
 * the given Countries, X and Y, within a given period. Only Persons that are
 * foreign to Countries X and Y are considered, that is Persons whose
 * Location is not Country X or Country Y. Return top 20 Persons, and their
 * Post/Comment counts, in the given countries and period. Sort results
 * descending by total number of Posts/Comments, and then ascending by Person
 * identifier.[1]
 */
public class LdbcQuery3Handler
implements OperationHandler<LdbcQuery3, DbConnectionState> {

	final static Logger logger =
			LoggerFactory.getLogger(LdbcQuery3Handler.class);

	@Override
	public void executeOperation(final LdbcQuery3 operation,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		Graph graph = ((JanusGraphDbConnectionState) dbConnectionState).getClient();
		GraphTraversalSource g = graph.traversal();

		long personId = operation.personId();
		String countryX = operation.countryXName();
		String countryY = operation.countryYName();
		long startDate = operation.startDate().getTime();
		long durationDays = (long) operation.durationDays();
		long endDate = startDate + (durationDays * 24 * 60 * 60 * 1000);
		int limit = operation.limit();

		List<LdbcQuery3Result> result = new ArrayList<>();

		try
		{
			List<Map<String, Object>> resultSet = g.V().has("personId", personId)
					.repeat(__.out("knows")).emit().times(2)
					.has("personId", P.neq(personId))
					.dedup()
					.where(
							__.out("isLocatedIn")
							.out("isPartOf")
							.and(
									__.has("name", P.neq(countryX)), 
									__.has("name", P.neq(countryY))
									)
							)
					.as("person", "xCount", "yCount")
					.select("person", "xCount", "yCount")
					.by()
					.by(
							__.in("hasCreator")
							.where(
									__.and(
											__.has(("creationDate"), P.gte(startDate)), 
											__.has(("creationDate"), P.lte(endDate))
											)
									)
							.out("isLocatedIn")
							.has("name", countryX).count())
					.by(
							__.in("hasCreator")
							.where(
									__.and(
											__.has(("creationDate"), P.gte(startDate)), 
											__.has(("creationDate"), P.lte(endDate))
											)
									)
							.out("isLocatedIn")
							.has("name", countryY).count())
					.limit(limit)
					.toList();

			for(int i = 0; i < resultSet.size(); i++)
			{
				Vertex person = null;
				long countX = 0;
				long countY = 0;
				for (Map.Entry<String, Object> entry : resultSet.get(i).entrySet()) {
					String key = entry.getKey();
					if (key.equals("person"))
					{
						person = (Vertex) entry.getValue();
					} else if (key.equals("xCount"))
					{
						countX = (long) entry.getValue();
					} else if (key.equals("yCount"))
					{
						countY = (long) entry.getValue();
					}
				}
				result.add(new LdbcQuery3Result(person.value("personId"), person.value("firstName"), person.value("lastName"), countX, countY, countX + countY));
			}
		}catch(Exception e )
		{
			e.printStackTrace();
		}

  	resultReporter.report(result.size(), result, operation);
  }

}
