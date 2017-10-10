package net.mpolonioli.ldbcimpls.janusgraph.interactive.queries;

import java.util.ArrayList;
import java.util.List;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPosts;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPostsResult;

import net.mpolonioli.ldbcimpls.janusgraph.interactive.JanusGraphDbConnectionState;

/**
 * Given a start Person, retrieve the last 10 Messages (Posts or Comments)
 * created by that user. For each message, return that message, the original
 * post in its conversation, and the author of that post. If any of the
 * Messages is a Post, then the original Post will be the same Message, i.e.,
 * that Message will appear twice in that result. Order results descending by
 * message creation date, then descending by message identifier.[1]
 */
public  class LdbcShortQuery2PersonPostsHandler implements
OperationHandler<LdbcShortQuery2PersonPosts, DbConnectionState> {

	final static Logger logger =
			LoggerFactory.getLogger(LdbcShortQuery2PersonPostsHandler.class);

	@Override
	public void executeOperation(final LdbcShortQuery2PersonPosts operation,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		JanusGraph graph = ((JanusGraphDbConnectionState) dbConnectionState).getClient();
		GraphTraversalSource g = graph.traversal();

		long personId = operation.personId();
		int limit = operation.limit();

		List<LdbcShortQuery2PersonPostsResult> result = new ArrayList<>();

		try 
		{
			Vertex startPerson = g.V().has("personId", personId).next();
			List<Vertex> messageList = 
					g.V(startPerson).
					in("hasCreator")
					.order().by("creationDate", Order.decr).by("messageId", Order.decr)
					.limit(limit)
					.toList();

			String startPersonFirstName = startPerson.value("firstName");
			String startPersonLastName = startPerson.value("lastName");

			for(int i = 0; i < messageList.size(); i++)
			{
				Vertex message = messageList.get(i);

				long messageId = message.value("messageId");
				long creationDate = message.value("creationDate");
				String content;
				try {
					content = message.value("content");
				}catch(IllegalStateException e) {
					content = message.value("imageFile");
				}

				if(message.label().equals("post"))
				{
					result.add(new LdbcShortQuery2PersonPostsResult(messageId, content, creationDate, messageId, personId, startPersonFirstName, startPersonLastName));
				}else
				{
					Vertex originalPost = g.V(message).repeat(__.out("replyOf")).until(__.hasLabel("post")).next();
					long originalPostId = originalPost.value("messageId");

					Vertex originalPostCreator = g.V(originalPost).out("hasCreator").next();
					long originalPostCreatorId = originalPostCreator.value("personId");
					String originalPostCreatorFirstName = originalPostCreator.value("firstName");
					String originalPostCreatorLastName = originalPostCreator.value("lastName");

					result.add(new LdbcShortQuery2PersonPostsResult(messageId, content, creationDate, originalPostId, originalPostCreatorId, originalPostCreatorFirstName, originalPostCreatorLastName));
				}
			}
		}catch(Exception e)
		{
			e.printStackTrace();
		}

		resultReporter.report(result.size(), result, operation);

	}
}