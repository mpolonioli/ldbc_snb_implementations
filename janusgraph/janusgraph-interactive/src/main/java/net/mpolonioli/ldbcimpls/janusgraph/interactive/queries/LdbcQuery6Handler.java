package net.mpolonioli.ldbcimpls.janusgraph.interactive.queries;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.janusgraph.core.JanusGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6Result;

import net.mpolonioli.ldbcimpls.janusgraph.interactive.JanusGraphDbConnectionState;

/**
 * Given a start Person and some Tag, find the other Tags that occur together
 * with this Tag on Posts that were created by start Person’s friends and
 * friends of friends (excluding start Person). Return top 10 Tags, and the
 * count of Posts that were created by these Persons, which contain both this
 * Tag and the given Tag. Sort results descending by count, and then
 * ascending by Tag name.[1]
 */
public class LdbcQuery6Handler
    implements OperationHandler<LdbcQuery6, DbConnectionState> {

	final static Logger logger =
			LoggerFactory.getLogger(LdbcQuery6Handler.class);

	@Override
	public void executeOperation(final LdbcQuery6 operation,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		JanusGraph graph = ((JanusGraphDbConnectionState) dbConnectionState).getClient();
		GraphTraversalSource g = graph.traversal();

		String tag = operation.tagName();
		long personId = operation.personId();
		int limit = operation.limit();

		List<LdbcQuery6Result> result = new ArrayList<>();

		try
		{
			List<Map<String, Object>> resultSet = g.V().has("personId", personId)
					.repeat(__.out("knows")).emit().times(2).has("personId", P.neq(personId))
					.dedup()
					.in("hasCreator").hasLabel("post")
					.filter(__.out("hasTag").values("name").is(tag))
					.out("hasTag")
					.has("name", P.neq(tag))
					.dedup()
					.order()
						.by(__.in("hasTag").count(), Order.decr)
						.by("name", Order.incr)
					.limit(limit)
					.as("tagName", "count")
					.select("tagName", "count")
						.by(__.values("name"))
						.by(__.in("hasTag").count())
					.toList();

			for(int i = 0; i < resultSet.size(); i++)
			{
				Long postCount = 0L;
				String tagName = null;
				for (Map.Entry<String, Object> entry : resultSet.get(i).entrySet()) {
					String key = entry.getKey();
					if(key.equals("tagName"))
					{
						tagName = (String) entry.getValue();
					} else
					{
						postCount = (Long) entry.getValue();
					}

				}
				result.add(new LdbcQuery6Result(tagName, postCount.intValue()));
			}
		} catch(Exception e)
		{
			e.printStackTrace();
			System.out.println("*\n*\n*" + operation + "\n*\n*\n*");
		}

		resultReporter.report(result.size(), result, operation);
	}

}
