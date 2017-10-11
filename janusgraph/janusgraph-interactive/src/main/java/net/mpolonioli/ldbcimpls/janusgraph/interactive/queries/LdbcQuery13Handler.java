package net.mpolonioli.ldbcimpls.janusgraph.interactive.queries;

import org.apache.tinkerpop.gremlin.process.traversal.Scope;
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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13Result;

import net.mpolonioli.ldbcimpls.janusgraph.interactive.JanusGraphDbConnectionState;

/**
 * Given two Persons, find the shortest path between these two Persons in the
 * subgraph induced by the Knows relationships. Return the length of this
 * path. -1 should be returned if no path is found, and 0 should be returned
 * if the start person is the same as the end person.[1]
 */
public class LdbcQuery13Handler
    implements OperationHandler<LdbcQuery13, DbConnectionState> {

  final static Logger logger =
      LoggerFactory.getLogger(LdbcQuery13Handler.class);

  @Override
  public void executeOperation(final LdbcQuery13 operation,
      DbConnectionState dbConnectionState,
      ResultReporter resultReporter) throws DbException {
	  
		JanusGraph graph = ((JanusGraphDbConnectionState) dbConnectionState).getClient();
		GraphTraversalSource g = graph.traversal();
		
		long personId1 = operation.person1Id();
		long personId2 = operation.person2Id();
		
		int maxLoops = 15;
		int length = -1;
		try
		{
			Vertex person1 = g.V().has("personId", personId1).next();
			Vertex person2 = g.V().has("personId", personId2).next();
			if(person1.value("personId").equals(person2.value("personId")))
			{
				length = 0;
			}else
			{
				length = g.V().has("personId", personId1)
						.repeat(__.out("knows").simplePath())
							.until(__.or(__.loops().is(maxLoops), __.has("personId", personId2)))
						.path()
						.limit(1)
						.count(Scope.local)
						.next()
						.intValue();
			}
			if(length >= maxLoops + 1)
			{
				length = -1;
			}
		}catch(Exception e)
		{
			e.printStackTrace();
			System.out.println("*\n*\n*" + operation + "\n*\n*\n*");
		}
		LdbcQuery13Result result = new LdbcQuery13Result(length);
		
		resultReporter.report(0, result, operation);
  }

}
