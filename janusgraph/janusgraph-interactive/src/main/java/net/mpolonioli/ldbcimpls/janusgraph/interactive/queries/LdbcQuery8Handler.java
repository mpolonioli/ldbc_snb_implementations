package net.mpolonioli.ldbcimpls.janusgraph.interactive.queries;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8Result;

import net.mpolonioli.ldbcimpls.janusgraph.interactive.JanusGraphDbConnectionState;

/**
 * Given a start Person, find (most recent) Comments that are replies to
 * Posts/Comments of the start Person. Only consider immediate (1-hop)
 * replies, not the transitive (multi-hop) case. Return the top 20 reply
 * Comments, and the Person that created each reply Comment. Sort results
 * descending by creation date of reply Comment, and then ascending by
 * identifier of reply Comment.[1]
 */
public class LdbcQuery8Handler
implements OperationHandler<LdbcQuery8, DbConnectionState> {

	final static Logger logger =
			LoggerFactory.getLogger(LdbcQuery8Handler.class);

	@Override
	public void executeOperation(final LdbcQuery8 operation,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		Graph graph = ((JanusGraphDbConnectionState) dbConnectionState).getClient();
		GraphTraversalSource g = graph.traversal();

		long startPersonId = operation.personId();
		int limit = operation.limit();

		List<LdbcQuery8Result> result = new ArrayList<>();

		try
		{
			List<Map<String, Object>> resultSet = g.V().has("personId", startPersonId)
					.in("hasCreator")
					.in("replyOf").as("comment")
					.order()
					.by("creationDate", Order.decr)
					.by("messageId", Order.incr)
					.limit(limit)
					.out("hasCreator").as("person")
					.select("comment", "person")
					.toList();

			for(int i = 0; i < resultSet.size(); i++)
			{
				long personId = 0;
				String personFirstName = null;
				String personLastName = null;
				long commentCreationDate = 0;
				long commentId = 0;
				String commentContent = null;
				for (Map.Entry<String, Object> entry : resultSet.get(i).entrySet()) {
					String key = entry.getKey();
					if(key.equals("comment"))
					{
						Vertex comment = (Vertex) entry.getValue();
						commentId = comment.value("messageId");
						commentCreationDate = comment.value("creationDate");
						try
						{
							commentContent = comment.value("content");
						}catch(IllegalStateException e)
						{
							commentContent = comment.value("imageFile");
						}
					}else if(key.endsWith("person"))
					{
						Vertex person = (Vertex) entry.getValue();
						personId = person.value("personId");
						personFirstName = person.value("firstName");
						personLastName = person.value("lastName");
					}
				}
				result.add(new LdbcQuery8Result(personId, personFirstName, personLastName, commentCreationDate, commentId, commentContent));
			}
		}catch(Exception e)
		{
			e.printStackTrace();
		}
		resultReporter.report(result.size(), result, operation);
	}
}
