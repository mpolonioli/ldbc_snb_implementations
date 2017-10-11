package net.mpolonioli.ldbcimpls.janusgraph.interactive.queries;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.janusgraph.core.JanusGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5Result;

import net.mpolonioli.ldbcimpls.janusgraph.interactive.JanusGraphDbConnectionState;

/**
 * Given a start Person, find the Forums which that Person’s friends and
 * friends of friends (excluding start Person) became Members of after a
 * given date. Return top 20 Forums, and the number of Posts in each Forum
 * that was Created by any of these Persons. For each Forum consider only
 * those Persons which joined that particular Forum after the given date.
 * Sort results descending by the count of Posts, and then ascending by Forum
 * identifier.[1]
 */
public class LdbcQuery5Handler
    implements OperationHandler<LdbcQuery5, DbConnectionState> {

  final static Logger logger =
      LoggerFactory.getLogger(LdbcQuery5Handler.class);

  @Override
  public void executeOperation(final LdbcQuery5 operation,
      DbConnectionState dbConnectionState,
      ResultReporter resultReporter) throws DbException {
	  
		JanusGraph graph = ((JanusGraphDbConnectionState) dbConnectionState).getClient();
		GraphTraversalSource g = graph.traversal();
				
		long personId = operation.personId();
		long startDate = operation.minDate().getTime();
		int limit = operation.limit();
		
		List<LdbcQuery5Result> result = new ArrayList<>();
		
		try 
		{
			List<Map<String, Object>> resultSet = g.V().has("personId", personId)
					.repeat(__.out("knows")).emit().times(2).has("personId", P.neq(personId))
					.dedup()
					.inE("hasMember").has("joinDate", P.gte(startDate)).outV()
					.dedup()
					.order().by("forumId", Order.incr)
					.limit(limit)
					.as("forum", "count")
					.select("forum", "count")
					.by(__.values("title"))
					.by(__.out("containerOf").count())
					.toList();

			for(int i = 0; i < resultSet.size(); i++)
			{
				Long postCount = 0L;
				String forumTitle = null;
				for (Map.Entry<String, Object> entry : resultSet.get(i).entrySet()) {
					String key = entry.getKey();
					if(key.equals("forum"))
					{
						forumTitle = (String) entry.getValue();
					} else
					{
						postCount = (Long) entry.getValue();
					}

				}
				result.add(new LdbcQuery5Result(forumTitle, postCount.intValue()));
			}
		}catch(Exception e)
		{
			e.printStackTrace();
			System.out.println("*\n*\n*" + operation + "\n*\n*\n*");
		}
		
		resultReporter.report(result.size(), result, operation);
  }

}
