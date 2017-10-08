package net.mpolonioli.ldbcimpls.janusgraph.interactive.queries;

import java.util.ArrayList;
import java.util.List;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriends;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriendsResult;

import net.mpolonioli.ldbcimpls.janusgraph.interactive.JanusGraphDbConnectionState;

/**
 * Given a start Person, retrieve all of their friends, and the date at which
 * they became friends. Order results descending by friendship creation date,
 * then ascending by friend identifier.[1]
 */
public class LdbcShortQuery3PersonFriendsHandler implements
OperationHandler<LdbcShortQuery3PersonFriends, DbConnectionState> {

	final static Logger logger =
			LoggerFactory.getLogger(LdbcShortQuery3PersonFriendsHandler.class);

	@Override
	public void executeOperation(final LdbcShortQuery3PersonFriends operation,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		Graph graph = ((JanusGraphDbConnectionState) dbConnectionState).getClient();
		GraphTraversalSource g = graph.traversal();
		
		long personId = operation.personId();
		
		List<LdbcShortQuery3PersonFriendsResult> result = new ArrayList<>();
		
		try 
		{
			Vertex startPerson = g.V().has("personId", personId).next();
			
			List<Edge> knowEdges = 
					g.V(startPerson)
					.out("knows")
					.order().by("personId", Order.incr)
					.inE("knows")
					.where(__.outV().is(startPerson))
					.order().by("creationDate", Order.decr)
					.toList();
			
			for(int i = 0; i < knowEdges.size(); i++)
			{
				Edge know = knowEdges.get(i);
				long creationDate = know.value("creationDate");
				
				Vertex friend = know.inVertex();
				long friendId = friend.value("personId");
				String firstName = friend.value("firstName");
				String lastName = friend.value("lastName");
				
				result.add(new LdbcShortQuery3PersonFriendsResult(friendId, firstName, lastName, creationDate));
			}
		}catch(Exception e)
    	{
    		e.printStackTrace();
    	}
		
		resultReporter.report(result.size(), result, operation);
	}

}