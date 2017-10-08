package net.mpolonioli.ldbcimpls.janusgraph.interactive.queries;

import java.util.ArrayList;
import java.util.List;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2Result;

import net.mpolonioli.ldbcimpls.janusgraph.interactive.JanusGraphDbConnectionState;

/**
 * Given a start Person, find (most recent) Posts and Comments from all of
 * that Person’s friends, that were created before (and including) a given
 * date. Return the top 20 Posts/Comments, and the Person that created each
 * of them. Sort results descending by creation date, and then ascending by
 * Post identifier.[1]
 */
public class LdbcQuery2Handler
implements OperationHandler<LdbcQuery2, DbConnectionState> {

final static Logger logger =
  LoggerFactory.getLogger(LdbcQuery2Handler.class);

@Override
public void executeOperation(final LdbcQuery2 operation,
  DbConnectionState dbConnectionState,
  ResultReporter resultReporter) throws DbException {
	
	Graph graph = ((JanusGraphDbConnectionState) dbConnectionState).getClient();
	GraphTraversalSource g = graph.traversal();
	
	long maxDate = operation.maxDate().getTime();
	int limit = operation.limit();
	long personId = operation.personId();
	
	List<LdbcQuery2Result> result = new ArrayList<>();
	
	// execute the query
	List<Vertex> messageList = new ArrayList<>();
	try {
		messageList = 
				g.V().has("personId", personId)
				.out("knows")
				.in("hasCreator")
				.has("creationDate", P.lte(maxDate))
				.order().by("creationDate", Order.decr).by("messageId", Order.incr)
				.limit(limit)
				.toList();
	}catch(Exception e) {
		e.printStackTrace();
	}
	
	for(int i = 0; i < messageList.size(); i++)
	{
		Vertex message = messageList.get(i);
		long creationDate = (long) message.value("creationDate");

		Vertex creator = message.vertices(Direction.OUT, "hasCreator").next();
		long creatorId =  (long) creator.value("personId");
		String firstName = (String) creator.value("firstName");
		String lastName = (String) creator.value("lastName");
		long messageId = (long) message.value("messageId");
		String content;
		try {
			content = (String) message.value("content");
		}catch(IllegalStateException e) {
			content = (String) message.value("imageFile");
		}
		result.add(new LdbcQuery2Result(creatorId, firstName, lastName, messageId, content, creationDate));

    }
	resultReporter.report(result.size(), result, operation);

}

}
