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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate7AddComment;

import net.mpolonioli.ldbcimpls.janusgraph.interactive.JanusGraphDbConnectionState;

/**
 * Add a Comment replying to a Post/Comment to the social network.[1]
 */
public class LdbcUpdate7AddCommentHandler implements
OperationHandler<LdbcUpdate7AddComment, DbConnectionState> {

	final static Logger logger =
			LoggerFactory.getLogger(LdbcUpdate4AddForum.class);

	@Override
	public void executeOperation(LdbcUpdate7AddComment operation,
			DbConnectionState dbConnectionState,
			ResultReporter reporter) throws DbException {
		JanusGraph client = ((JanusGraphDbConnectionState) dbConnectionState).getClient();
		GraphTraversalSource g = client.traversal();

		try
		{
			List<Object> commentKeyValues = new ArrayList<>(14);
			commentKeyValues.add("messageId");
			commentKeyValues.add(operation.commentId());
			commentKeyValues.add(T.label);
			commentKeyValues.add("comment");
			commentKeyValues.add("creationDate");
			commentKeyValues.add(operation.creationDate().getTime());
			commentKeyValues.add("locationIP");
			commentKeyValues.add(operation.locationIp());
			commentKeyValues.add("browserUsed");
			commentKeyValues.add(operation.browserUsed());
			commentKeyValues.add("content");
			commentKeyValues.add(operation.content());
			commentKeyValues.add("length");
			commentKeyValues.add(operation.length());

			Vertex comment = client.addVertex(commentKeyValues.toArray());

			List<Long> tagIds = new ArrayList<>(operation.tagIds().size());
			operation.tagIds().forEach((id) -> {
				tagIds.add(id);
			});
			Vertex author = g.V().has("personId", operation.authorPersonId()).next();
			Vertex place = g.V().has("placeId", operation.countryId()).next();
			Vertex originalMessage = null;
			if (operation.replyToCommentId() != -1) {
				originalMessage = g.V().has("messageId", operation.replyToCommentId()).next();
			}
			if (operation.replyToPostId() != -1) {
				originalMessage = g.V().has("messageId", operation.replyToPostId()).next();
			}

			g.V().has("tagId", within(tagIds)).forEachRemaining((v) -> {
				comment.addEdge("hasTag", v);
			});
			comment.addEdge("hasCreator", author);
			comment.addEdge("isLocatedIn", place);
			comment.addEdge("replyOf", originalMessage);

			client.tx().commit();
		}catch(SchemaViolationException e)
		{
			e.printStackTrace();
			System.out.println("*\n*\n*" + operation + "\n*\n*\n*");
		}
		reporter.report(0, LdbcNoResult.INSTANCE, operation);
	}
}
