package net.mpolonioli.ldbcimpls.janusgraph.interactive.queries;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4Result;

import net.mpolonioli.ldbcimpls.janusgraph.interactive.JanusGraphDbConnectionState;

/**
 * Given a start Person, find Tags that are attached to Posts that were
 * created by that Person’s friends. Only include Tags that were attached to
 * friends’ Posts created within a given time interval, and that were never
 * attached to friends’ Posts created before this interval. Return top 10
 * Tags, and the count of Posts, which were created within the given time
 * interval, that this Tag was attached to. Sort results descending by Post
 * count, and then ascending by Tag name.[1]
 */
public class LdbcQuery4Handler
implements OperationHandler<LdbcQuery4, DbConnectionState> {

	final static Logger logger =
			LoggerFactory.getLogger(LdbcQuery4Handler.class);

	@Override
	public void executeOperation(final LdbcQuery4 operation,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		JanusGraph graph = ((JanusGraphDbConnectionState) dbConnectionState).getClient();
		GraphTraversalSource g = graph.traversal();
		
		long personId = operation.personId();
		long startDate = operation.startDate().getTime();
		long durationDays = (long) operation.durationDays();
		long endDate = startDate + (durationDays * 24 * 60 * 60 * 1000);
		int limit = operation.limit();
		
		List<LdbcQuery4Result> result = new ArrayList<>();
		
		try {
			List<Map<Object, Long>> resultSet = g.V().has("personId", personId)
					.out("knows")
					.in("hasCreator")
					.where(
							__.and(
									__.has(("creationDate"), P.gte(startDate)), 
									__.has(("creationDate"), P.lte(endDate))
									)
							)
					.hasLabel("post")
					.out("hasTag")
					.groupCount()
					.toList();

			Iterator<Entry<Object, Long>> entryList = resultSet.get(0).entrySet().iterator();
			for (int i  = 0; entryList.hasNext() && i < limit; i++) {
				Entry<Object, Long> entry = entryList.next();
				String tagName = ((Vertex) entry.getKey()).value("name");
				int count = entry.getValue().intValue();
				result.add(new LdbcQuery4Result(tagName, count));
			}
		}catch(Exception e)
		{
			e.printStackTrace();
		}

		resultReporter.report(result.size(), result, operation);

	}

}
