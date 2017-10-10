package net.mpolonioli.ldbcimpls.janusgraph.interactive.queries;

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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForumResult;

import net.mpolonioli.ldbcimpls.janusgraph.interactive.JanusGraphDbConnectionState;

/**
 * Given a Message (Post or Comment), retrieve the Forum that contains it and
 * the Person that moderates that forum. Since comments are not directly
 * contained in forums, for comments, return the forum containing the
 * original post in the thread which the comment is replying to.[1]
 */
public class LdbcShortQuery6MessageForumHandler implements
OperationHandler<LdbcShortQuery6MessageForum, DbConnectionState> {

	final static Logger logger =
			LoggerFactory.getLogger(LdbcShortQuery6MessageForumHandler.class);

	@Override
	public void executeOperation(final LdbcShortQuery6MessageForum operation,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		JanusGraph graph = ((JanusGraphDbConnectionState) dbConnectionState).getClient();
		GraphTraversalSource g = graph.traversal();
		
		long messageId = operation.messageId();

		LdbcShortQuery6MessageForumResult result = null;
		
		try 
		{
			Vertex message = g.V().has("messageId", messageId).next();
			if(message.label().equals("comment"))
			{
				message = g.V(message).repeat(__.out("replyOf")).until(__.hasLabel("post")).next();
			}
			Vertex forum = g.V(message).in("containerOf").next();
			Vertex moderator = g.V(forum).out("hasModerator").next();
			
			long forumId = forum.value("forumId");
			String forumTitle = forum.value("title");
			long moderatorId = moderator.value("personId");
			String moderatorFirstName = moderator.value("firstName");
			String moderatorLastName = moderator.value("lastName");
			
			result = new LdbcShortQuery6MessageForumResult(forumId, forumTitle, moderatorId, moderatorFirstName, moderatorLastName);
		}catch(Exception e)
		{
			result = new LdbcShortQuery6MessageForumResult(0, "null", 0, "null", "null");
			e.printStackTrace();
		}
		
		resultReporter.report(1, result, operation);
		
	}

}
