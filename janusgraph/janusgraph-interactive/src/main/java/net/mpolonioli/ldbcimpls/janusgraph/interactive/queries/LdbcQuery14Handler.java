package net.mpolonioli.ldbcimpls.janusgraph.interactive.queries;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14Result;

//TODO
/**
 * Given two Persons, find all (unweighted) shortest paths between these two
 * Persons, in the subgraph induced by the Knows relationship. Then, for each
 * path calculate a weight. The nodes in the path are Persons, and the weight
 * of a path is the sum of weights between every pair of consecutive Person
 * nodes in the path. The weight for a pair of Persons is calculated such
 * that every reply (by one of the Persons) to a Post (by the other Person)
 * contributes 1.0, and every reply (by ones of the Persons) to a Comment (by
 * the other Person) contributes 0.5. Return all the paths with shortest
 * length, and their weights. Sort results descending by path weight. The
 * order of paths with the same weight is unspecified.[1]
 */
public class LdbcQuery14Handler
    implements OperationHandler<LdbcQuery14, DbConnectionState> {

  final static Logger logger =
      LoggerFactory.getLogger(LdbcQuery14Handler.class);

  @Override
  public void executeOperation(final LdbcQuery14 operation,
      DbConnectionState dbConnectionState,
      ResultReporter resultReporter) throws DbException {
	  
	  List<LdbcQuery14Result> result = new ArrayList<>();
	  resultReporter.report(0, result, operation);

  }

}