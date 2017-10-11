package net.mpolonioli.ldbcimpls.janusgraph.interactive.queries;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9Result;

import net.mpolonioli.ldbcimpls.janusgraph.interactive.JanusGraphDbConnectionState;

/**
 * Given a start Person, find the (most recent) Posts/Comments created by
 * that Person’s friends or friends of friends (excluding start Person). Only
 * consider the Posts/Comments created before a given date (excluding that
 * date). Return the top 20 Posts/Comments, and the Person that created each
 * of those Posts/Comments. Sort results descending by creation date of
 * Post/Comment, and then ascending by Post/Comment identifier.[1]
 */
public class LdbcQuery9Handler
implements OperationHandler<LdbcQuery9, DbConnectionState> {

	final static Logger logger =
			LoggerFactory.getLogger(LdbcQuery9Handler.class);

	@Override
	public void executeOperation(final LdbcQuery9 operation,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		JanusGraph graph = ((JanusGraphDbConnectionState) dbConnectionState).getClient();
		GraphTraversalSource g = graph.traversal();

		long startPersonId = operation.personId();
		int limit = operation.limit();

		List<LdbcQuery9Result> result = new ArrayList<>();

		try
		{
			List<Map<String, Object>> resultSet = g.V().has("personId", startPersonId)
					.repeat(__.out("knows")).emit().times(2).has("personId", P.neq(startPersonId))
					.dedup()
					.as("person")
					.in("hasCreator").as("message")
					.order()
					.by("creationDate", Order.decr)
					.by("messageId", Order.incr)
					.limit(limit)
					.select("message", "person")
					.toList();

			for(int i = 0; i < resultSet.size(); i++)
			{
				long personId = 0;
				String personFirstName = null;
				String personLastName = null;
				long commentOrPostCreationDate = 0;
				long commentOrPostId = 0;
				String commentOrPostContent = null;
				for (Map.Entry<String, Object> entry : resultSet.get(i).entrySet()) {
					String key = entry.getKey();
					if(key.equals("message"))
					{
						Vertex comment = (Vertex) entry.getValue();
						commentOrPostId = comment.value("messageId");
						commentOrPostCreationDate = comment.value("creationDate");
						try
						{
							commentOrPostContent = comment.value("content");
						}catch(IllegalStateException e)
						{
							commentOrPostContent = comment.value("imageFile");
						}
					}else if(key.endsWith("person"))
					{
						Vertex person = (Vertex) entry.getValue();
						personId = person.value("personId");
						personFirstName = person.value("firstName");
						personLastName = person.value("lastName");
					}
				}
				result.add(new LdbcQuery9Result(personId, personFirstName, personLastName, commentOrPostId, commentOrPostContent, commentOrPostCreationDate));
			}
		}catch(Exception e)
		{
			e.printStackTrace();
			System.out.println("*\n*\n*" + operation + "\n*\n*\n*");
		}
		resultReporter.report(result.size(), result, operation);
  }
}
