package net.mpolonioli.ldbcimpls.janusgraph.interactive.queries;

import java.util.ArrayList;
import java.util.List;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.SchemaViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate4AddForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate8AddFriendship;

import net.mpolonioli.ldbcimpls.janusgraph.interactive.JanusGraphDbConnectionState;

/**
 * Add a friendship relation to the social network.[1]
 */
public class LdbcUpdate8AddFriendshipHandler implements
OperationHandler<LdbcUpdate8AddFriendship, DbConnectionState> {

	final static Logger logger =
			LoggerFactory.getLogger(LdbcUpdate4AddForum.class);

	@Override
	public void executeOperation(LdbcUpdate8AddFriendship operation,
			DbConnectionState dbConnectionState,
			ResultReporter reporter) throws DbException {
		Graph client = ((JanusGraphDbConnectionState) dbConnectionState).getClient();
		GraphTraversalSource g = client.traversal();

		try
		{
			List<Object> knowsEdgeKeyValues = new ArrayList<>(2);
			knowsEdgeKeyValues.add("creationDate");
			knowsEdgeKeyValues.add(operation.creationDate().getTime());

			Vertex person1 = g.V().has("personId", operation.person1Id()).next();
			Vertex person2 = g.V().has("personId", operation.person2Id()).next();

			person1.addEdge("knows", person2, knowsEdgeKeyValues.toArray());
			person2.addEdge("knows", person1, knowsEdgeKeyValues.toArray());

			client.tx().commit();
		}catch(SchemaViolationException e)
		{

		}
		reporter.report(0, LdbcNoResult.INSTANCE, operation);
	}
}
