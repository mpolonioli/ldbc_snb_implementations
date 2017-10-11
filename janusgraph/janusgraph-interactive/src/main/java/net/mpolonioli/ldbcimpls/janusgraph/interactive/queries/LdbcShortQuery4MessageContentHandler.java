package net.mpolonioli.ldbcimpls.janusgraph.interactive.queries;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContent;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContentResult;

import net.mpolonioli.ldbcimpls.janusgraph.interactive.JanusGraphDbConnectionState;

/**
 * Given a Message (Post or Comment), retrieve its content and creation
 * date.[1]
 */
public class LdbcShortQuery4MessageContentHandler implements
OperationHandler<LdbcShortQuery4MessageContent, DbConnectionState> {

	final static Logger logger =
			LoggerFactory.getLogger(LdbcShortQuery4MessageContentHandler.class);

	@Override
	public void executeOperation(final LdbcShortQuery4MessageContent operation,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {
		
		JanusGraph graph = ((JanusGraphDbConnectionState) dbConnectionState).getClient();
		GraphTraversalSource g = graph.traversal();
		
		long messageId = operation.messageId();

		LdbcShortQuery4MessageContentResult result = null;

		try 
		{
			Vertex message = g.V().has("messageId", messageId).next();

			long creationDate = message.value("creationDate");
			String content;
			try {
				content = message.value("content");
			}catch(IllegalStateException e) {
				content = message.value("imageFile");
			}

			result = new LdbcShortQuery4MessageContentResult(content, creationDate);
		}catch(Exception e)
		{
			result = new LdbcShortQuery4MessageContentResult("null", 0);
			e.printStackTrace();
			System.out.println("*\n*\n*" + operation + "\n*\n*\n*");
		}
		
		resultReporter.report(1, result, operation);

	}
}