package net.mpolonioli.ldbcimpls.janusgraph.interactive.queries;

import java.util.ArrayList;
import java.util.List;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.SchemaViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate4AddForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate5AddForumMembership;

import net.mpolonioli.ldbcimpls.janusgraph.interactive.JanusGraphDbConnectionState;

/**
 * Add a Forum membership to the social network.[1]
 */
public class LdbcUpdate5AddForumMembershipHandler implements
OperationHandler<LdbcUpdate5AddForumMembership, DbConnectionState> {

	final static Logger logger =
			LoggerFactory.getLogger(LdbcUpdate4AddForum.class);

	@Override
	public void executeOperation(LdbcUpdate5AddForumMembership operation,
			DbConnectionState dbConnectionState,
			ResultReporter reporter) throws DbException {
		JanusGraph client = ((JanusGraphDbConnectionState) dbConnectionState).getClient();
		GraphTraversalSource g = client.traversal();
		try
		{
			Vertex forum = g.V().has("forumId", operation.forumId()).next();
			Vertex member = g.V().has("personId", operation.personId()).next();

			List<Object> edgeKeyValues = new ArrayList<>(2);
			edgeKeyValues.add("joinDate");
			edgeKeyValues.add(operation.joinDate().getTime());

			forum.addEdge("hasMember", member, edgeKeyValues.toArray());

			client.tx().commit();
		}catch(SchemaViolationException e)
		{

		}
		reporter.report(0, LdbcNoResult.INSTANCE, operation);
	}
}
