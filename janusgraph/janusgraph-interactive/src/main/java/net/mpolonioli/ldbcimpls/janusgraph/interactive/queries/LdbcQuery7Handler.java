package net.mpolonioli.ldbcimpls.janusgraph.interactive.queries;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7Result;

import net.mpolonioli.ldbcimpls.janusgraph.interactive.JanusGraphDbConnectionState;

/**
 * Given a start Person, find (most recent) Likes on any of start Person’s
 * Posts/Comments. Return top 20 Persons that Liked any of start Person’s
 * Posts/Comments, the Post/Comment they liked most recently, creation date
 * of that Like, and the latency (in minutes) between creation of
 * Post/Comment and Like. Additionally, return a flag indicating whether the
 * liker is a friend of start Person. In the case that a Person Liked
 * multiple Posts/Comments at the same time, return the Post/Comment with
 * lowest identifier. Sort results descending by creation time of Like, then
 * ascending by Person identifier of liker.[1]
 */
public class LdbcQuery7Handler
implements OperationHandler<LdbcQuery7, DbConnectionState> {

	final static Logger logger =
			LoggerFactory.getLogger(LdbcQuery5Handler.class);

	@Override
	public void executeOperation(final LdbcQuery7 operation,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		JanusGraph graph = ((JanusGraphDbConnectionState) dbConnectionState).getClient();
		GraphTraversalSource g = graph.traversal();

		long personId = operation.personId();
		int limit = operation.limit();

		List<LdbcQuery7Result> result = new ArrayList<>();

		try 
		{
			List<Map<String, Object>> resultSet = g.V().has("personId", personId)
					.in("hasCreator").as("message")
					.inE("likes").as("like")
					.order().by("creationDate", Order.decr).by(__.outV().values("personId"), Order.decr)
					.limit(limit)
					.select("like")
					.as("likeDate", "messageDate", "person", "isNew")
					.select("likeDate", "messageDate", "person", "isNew")
					.by(__.values("creationDate"))
					.by(__.inV().values("creationDate"))
					.by(__.outV())
					.by(__.outV().choose(__.or(__.in("knows").has("personId", personId), __.out("knows").has("personId", personId)), __.constant(true), __.constant(false)))
					.select("message", "messageDate", "likeDate", "person", "isNew")
					.toList()
					;

			for(int i = 0; i < resultSet.size(); i++)
			{
				long likerId = 0;
				String firstName = null;
				String lastName = null;
				long likeDate = 0;
				long messageId = 0;
				String content = null;
				long messageDate = 0;
				boolean isNew = false;
				for (Map.Entry<String, Object> entry : resultSet.get(i).entrySet()) {
					String key = entry.getKey();
					if(key.equals("message"))
					{
						Vertex message = (Vertex) entry.getValue();
						messageId = message.value("messageId");
						try {
							content = message.value("content");
						}catch(IllegalStateException e) {
							content = message.value("imageFile");
						}
					}else if(key.equals("messageDate"))
					{
						messageDate = (long) entry.getValue();
					}else if(key.equals("likeDate"))
					{
						likeDate = (long) entry.getValue();
					}else if(key.equals("person"))
					{
						Vertex person = (Vertex) entry.getValue();
						likerId = person.value("personId");
						firstName = person.value("firstName");
						lastName = person.value("lastName");
					}else if(key.equals("isNew"))
					{
						isNew = (boolean) entry.getValue();
					}
				}
				Long latency = (likeDate - messageDate) / 1000 / 60;
				result.add(new LdbcQuery7Result(likerId, firstName, lastName, likeDate, messageId, content, latency.intValue(), isNew));
			}
		}catch(Exception e)
		{
			e.printStackTrace();
			System.out.println("*\n*\n*" + operation + "\n*\n*\n*");
		}

		resultReporter.report(result.size(), result, operation);

	}

}
