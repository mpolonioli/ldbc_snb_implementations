package net.mpolonioli.ldbcimpls.janusgraph.interactive.queries;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreator;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreatorResult;

import net.mpolonioli.ldbcimpls.janusgraph.interactive.JanusGraphDbConnectionState;

/**
 * Given a Message (Post or Comment), retrieve its author.[1]
 */
public class LdbcShortQuery5MessageCreatorHandler implements
OperationHandler<LdbcShortQuery5MessageCreator, DbConnectionState> {

	final static Logger logger =
			LoggerFactory.getLogger(LdbcShortQuery5MessageCreatorHandler.class);

	@Override
	public void executeOperation(final LdbcShortQuery5MessageCreator operation,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		Graph graph = ((JanusGraphDbConnectionState) dbConnectionState).getClient();
		GraphTraversalSource g = graph.traversal();

		long messageId = operation.messageId();

		LdbcShortQuery5MessageCreatorResult result = null;

		try 
		{
			Vertex messageCreator = g.V().has("messageId", messageId).out("hasCreator").next();

			long messageCreatorId = messageCreator.value("personId");
			String firstName = messageCreator.value("firstName");
			String lastName = messageCreator.value("lastName");

			result = new LdbcShortQuery5MessageCreatorResult(messageCreatorId,firstName, lastName);
		}catch(Exception e)
		{
			result = new LdbcShortQuery5MessageCreatorResult(0, "null", "null");
			e.printStackTrace();
		}

		resultReporter.report(1, result, operation);
	}
}