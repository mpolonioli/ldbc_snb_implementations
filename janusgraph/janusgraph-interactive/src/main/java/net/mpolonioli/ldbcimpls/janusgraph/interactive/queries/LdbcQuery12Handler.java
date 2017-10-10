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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12Result;

import net.mpolonioli.ldbcimpls.janusgraph.interactive.JanusGraphDbConnectionState;

/**
 * Given a start Person, find the Comments that this Person’s friends made in
 * reply to Posts, considering only those Comments that are immediate (1-hop)
 * replies to Posts, not the transitive (multi-hop) case. Only consider Posts
 * with a Tag in a given TagClass or in a descendent of that TagClass. Count
 * the number of these reply Comments, and collect the Tags (with valid tag
 * class) that were attached to the Posts they replied to. Return top 20
 * Persons with at least one reply, the reply count, and the collection of
 * Tags. Sort results descending by Comment count, and then ascending by
 * Person identifier.[1]
 */
public class LdbcQuery12Handler
    implements OperationHandler<LdbcQuery12, DbConnectionState> {

  final static Logger logger =
      LoggerFactory.getLogger(LdbcQuery12Handler.class);

  @Override
  public void executeOperation(final LdbcQuery12 operation,
      DbConnectionState dbConnectionState,
      ResultReporter resultReporter) throws DbException {
	  
		JanusGraph graph = ((JanusGraphDbConnectionState) dbConnectionState).getClient();
		GraphTraversalSource g = graph.traversal();
		
		long startPersonId = operation.personId();
		String tagClass = operation.tagClassName();
		int limit = operation.limit();
		
		List<LdbcQuery12Result> result = new ArrayList<>();
		
		try 
		{
			
			List<Map<String, Object>> resultSet = 
					g.V().has("personId", startPersonId)
					.out("knows")
					.order()
					.by(__.in("hasCreator").as("comment").out("replyOf").out("hasTag").out("hasType")
							.where(
									__.or(
											__.has("name", tagClass),
											__.repeat(__.out("isSubclassOf")).has("name", tagClass)))
							.select("comment")
							.count(), Order.decr)
					.by("personId", Order.incr)
					.limit(limit)
					.as("person", "count")
					.select("person", "count")
					.by()
					.by(__.in("hasCreator").as("comment").out("replyOf").out("hasTag").out("hasType")
							.where(
									__.or(
											__.has("name", tagClass),
											__.repeat(__.out("isSubclassOf")).has("name", tagClass)))
							.select("comment").count())
					.toList();

			for(int i = 0; i < resultSet.size(); i++)
			{
				for (Map.Entry<String, Object> entry : resultSet.get(i).entrySet()) {
					System.out.println("Key : " + entry.getKey() + " Value : " + entry.getValue());
				}
			}
			
			for(int i = 0; i < resultSet.size(); i++)
			{
				long personId = 0;
				String personFirstName = null;
				String personLastName = null;
				List<String> tagNames = new ArrayList<>();
				int replyCount = 0;
				for (Map.Entry<String, Object> entry : resultSet.get(i).entrySet()) {
					String key = entry.getKey();
					if(key.equals("person"))
					{
						Vertex person = (Vertex) entry.getValue();
						personId = person.value("personId");
						personFirstName = person.value("firstName");
						personLastName = person.value("lastName");
						try
						{
						List<Object> tags = g.V(person).in("hasCreator").out("replyOf").out("hasTag").as("tags").out("hasType")
								.where(
										__.or(
												__.has("name", tagClass),
												__.repeat(__.out("isSubclassOf")).has("name", tagClass)))
								.dedup()
								.select("tags")
								.toList();
								;
						for(int j = 0; j < tags.size(); j++)
						{
							Vertex tag = (Vertex) tags.get(j);
							tagNames.add(tag.value("name"));
						}
						}catch(Exception e)
						{
							e.printStackTrace();
						}			
					} else if(key.equals("count"))
					{
						Long countLong = (Long) entry.getValue();
						replyCount = countLong.intValue();
					}
				}
				if(replyCount > 0)
				{
				result.add(new LdbcQuery12Result(personId, personFirstName, personLastName, tagNames, replyCount));
				}
			}
		}catch(Exception e)
		{
			e.printStackTrace();
		}

		resultReporter.report(result.size(), result, operation);
  }

}
