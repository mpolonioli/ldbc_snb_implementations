package net.mpolonioli.ldbcimpls.janusgraph.interactive.queries;

import java.util.ArrayList;
import java.util.List;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageReplies;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageRepliesResult;

import net.mpolonioli.ldbcimpls.janusgraph.interactive.JanusGraphDbConnectionState;

/**
 * Given a Message (Post or Comment), retrieve the (1-hop) Comments that
 * reply to it. In addition, return a boolean flag indicating if the author
 * of the reply knows the author of the original message. If author is same
 * as original author, return false for "knows" flag. Order results
 * descending by creation date, then ascending by author identifier.[1]
 */
public class LdbcShortQuery7MessageRepliesHandler implements
OperationHandler<LdbcShortQuery7MessageReplies, DbConnectionState> {

	final static Logger logger =
			LoggerFactory.getLogger(LdbcShortQuery7MessageRepliesHandler.class);

	@Override
	public void executeOperation(final LdbcShortQuery7MessageReplies operation,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		Graph graph = ((JanusGraphDbConnectionState) dbConnectionState).getClient();
		GraphTraversalSource g = graph.traversal();
		
		long messageId = operation.messageId();
		
		List<LdbcShortQuery7MessageRepliesResult> result = new ArrayList<>();		
		try 
		{
			Vertex originalMessage = g.V().has("messageId", messageId).next();
			Vertex originalMessageAuthor = g.V(originalMessage).out("hasCreator").next();
			List<Vertex> replies = g.V(originalMessage)
					.in("replyOf")
					.order().by("creationDate", Order.decr)
					.toList();
			
			for(int i = 0; i < replies.size(); i++)
			{
				Vertex comment = replies.get(i);
				
				long commentId = comment.value("messageId");
				String commentContent;
				try {
					commentContent = comment.value("content");
				}catch(IllegalStateException e) {
					commentContent = comment.value("imageFile");
				}
				long commentCreationDate = comment.value("creationDate");
				
				Vertex replyAuthor = g.V(comment).out("hasCreator").next();
				
				long replyAuthorId = replyAuthor.value("personId");
				String replyAuthorFirstName = replyAuthor.value("firstName");
				String replyAuthorLastName = replyAuthor.value("lastName"); 
				
				boolean replyAuthorKnowsOriginalMessageAuthor = g.V(replyAuthor).out("knows").is(originalMessageAuthor).hasNext();
				result.add(new LdbcShortQuery7MessageRepliesResult(commentId, commentContent, commentCreationDate, replyAuthorId, replyAuthorFirstName, replyAuthorLastName, replyAuthorKnowsOriginalMessageAuthor));

			}
			
		}catch(Exception e)
		{
			e.printStackTrace();
		}
		
		resultReporter.report(result.size(), result, operation);

	}

}