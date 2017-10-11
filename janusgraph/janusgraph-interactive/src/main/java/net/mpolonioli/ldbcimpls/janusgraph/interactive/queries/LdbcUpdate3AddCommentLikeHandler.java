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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate3AddCommentLike;

import net.mpolonioli.ldbcimpls.janusgraph.interactive.JanusGraphDbConnectionState;

/**
 * Add a Like to a Comment of the social network.[1]
 */
public class LdbcUpdate3AddCommentLikeHandler implements
OperationHandler<LdbcUpdate3AddCommentLike, DbConnectionState> {

	final static Logger logger =
			LoggerFactory.getLogger(LdbcUpdate3AddCommentLikeHandler.class);

	@Override
	public void executeOperation(LdbcUpdate3AddCommentLike operation,
			DbConnectionState dbConnectionState,
			ResultReporter reporter) throws DbException {
		JanusGraph client = ((JanusGraphDbConnectionState) dbConnectionState).getClient();
		GraphTraversalSource g = client.traversal();
		try
		{
			Vertex person = g.V().has("personId", operation.personId()).next();
			Vertex comment = g.V().has("messageId", operation.commentId()).next();
			List<Object> keyValues = new ArrayList<>(2);
			keyValues.add("creationDate");
			keyValues.add(operation.creationDate().getTime());
			person.addEdge("likes", comment, keyValues.toArray());

			client.tx().commit();
		}catch(SchemaViolationException e)
		{
			e.printStackTrace();
			System.out.println("*\n*\n*" + operation + "\n*\n*\n*");
		}
		reporter.report(0, LdbcNoResult.INSTANCE, operation);
	}
}
