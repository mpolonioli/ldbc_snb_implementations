package net.mpolonioli.ldbcimpls.janusgraph.interactive.queries;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10Result;

import net.mpolonioli.ldbcimpls.janusgraph.interactive.JanusGraphDbConnectionState;

/**
 * Given a start Person, find that Person’s friends of friends (excluding
 * start Person, and immediate friends).
 * Calculate the similarity between each of these Persons and start Person,
 * where similarity for any Person is defined as follows:
 * <ul>
 * <li>common = number of Posts created by that Person, such that the Post
 * has a Tag that start Person is Interested in</li>
 * <li>uncommon = number of Posts created by that Person, such that the Post
 * has no Tag that start Person is Interested in</li>
 * <li>similarity = common - uncommon</li>
 * </ul>
 * Return top 10 Persons, their Place, and their similarity score. Sort
 * results descending by similarity score, and then ascending by Person
 * identifier.[1]
 */
public class LdbcQuery10Handler
implements OperationHandler<LdbcQuery10, DbConnectionState> {

	final static Logger logger =
			LoggerFactory.getLogger(LdbcQuery10Handler.class);

	@Override
	public void executeOperation(final LdbcQuery10 operation,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		JanusGraph graph = ((JanusGraphDbConnectionState) dbConnectionState).getClient();
		GraphTraversalSource g = graph.traversal();

		long startPersonId = operation.personId();
		int limit = operation.limit();

		List<LdbcQuery10Result> result = new ArrayList<>();

		try
		{
			List<Map<String, Object>> resultSet = g.V().has("personId", startPersonId).repeat(__.out("knows")).times(2).has("personId", P.neq(startPersonId))
					.dedup()
					.order()
					.by("personId", Order.incr)
					.limit(limit)
					.as("person")
					.select("person")
					.out("isLocatedIn")
					.as("place")
					.select("person")
					.as("common", "uncommon")
					.select("common", "uncommon")
					.by(__.in("hasCreator").where(__.out("hasTag").in("hasInterest").has("personId", startPersonId)).count())
					.by(__.in("hasCreator").where(__.out("hasTag").in("hasInterest").has("personId", P.neq(startPersonId))).count())
					.select("person", "place", "common", "uncommon")
					.toList();

			for(int i = 0; i < resultSet.size(); i++)
			{
				long personId = 0;
				String firstName = null;
				String lastName = null;
				String cityName = null;
				String gender = null;
				Long common = 0L;
				Long uncommon = 0L;
				for (Map.Entry<String, Object> entry : resultSet.get(i).entrySet()) {
					String key = entry.getKey();
					if(key.equals("person"))
					{
						Vertex person = (Vertex) entry.getValue();
						personId = person.value("personId");
						firstName = person.value("firstName");
						lastName = person.value("lastName");
					} else if(key.equals("place"))
					{
						Vertex city = (Vertex) entry.getValue();
						cityName = city.value("name");
					} else if(key.equals("common"))
					{
						common = (Long) entry.getValue();
					} else if(key.equals("uncommon"))
					{
						uncommon = (Long) entry.getValue();
					}

				}
				Long commonInterestScore = common - uncommon;
				result.add(new LdbcQuery10Result(personId, firstName, lastName, commonInterestScore.intValue(), gender, cityName));
			}
		}catch(Exception e)
		{
			e.printStackTrace();
		}

		resultReporter.report(result.size(), result, operation);

	}

}
