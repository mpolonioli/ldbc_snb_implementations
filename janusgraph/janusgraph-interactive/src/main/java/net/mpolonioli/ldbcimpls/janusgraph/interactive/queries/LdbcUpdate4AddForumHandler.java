package net.mpolonioli.ldbcimpls.janusgraph.interactive.queries;

import static org.apache.tinkerpop.gremlin.process.traversal.P.within;

import java.util.ArrayList;
import java.util.List;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
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

import net.mpolonioli.ldbcimpls.janusgraph.interactive.JanusGraphDbConnectionState;

/**
 * Add a Like to a Comment of the social network.[1]
 */
public class LdbcUpdate4AddForumHandler implements
OperationHandler<LdbcUpdate4AddForum, DbConnectionState> {

	final static Logger logger =
			LoggerFactory.getLogger(LdbcUpdate4AddForum.class);

	@Override
	public void executeOperation(LdbcUpdate4AddForum operation,
			DbConnectionState dbConnectionState,
			ResultReporter reporter) throws DbException {
		JanusGraph client = ((JanusGraphDbConnectionState) dbConnectionState).getClient();
		GraphTraversalSource g = client.traversal();

		try
		{
			List<Object> forumKeyValues = new ArrayList<>(8);
			forumKeyValues.add("forumId");
			forumKeyValues.add(operation.forumId());
			forumKeyValues.add(T.label);
			forumKeyValues.add("forum");
			forumKeyValues.add("title");
			forumKeyValues.add(operation.forumTitle());
			forumKeyValues.add("creationDate");
			forumKeyValues.add(operation.creationDate().getTime());

			Vertex forum = client.addVertex(forumKeyValues.toArray());

			List<Long> tagIds = new ArrayList<>(operation.tagIds().size());
			operation.tagIds().forEach((id) -> {
				tagIds.add(id);
			});

			g.V().has("tagId", within(tagIds)).forEachRemaining((v) -> {
				forum.addEdge("hasTag", v);
			});

			Vertex moderator = g.V().has("personId", operation.moderatorPersonId()).next();
			forum.addEdge("hasModerator", moderator);

			client.tx().commit();
		}catch(SchemaViolationException e)
		{
			e.printStackTrace();
			System.out.println("*\n*\n*" + operation + "\n*\n*\n*");
		}
		reporter.report(0, LdbcNoResult.INSTANCE, operation);
	}
}