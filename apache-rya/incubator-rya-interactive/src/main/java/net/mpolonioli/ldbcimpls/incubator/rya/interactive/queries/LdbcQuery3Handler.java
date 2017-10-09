package net.mpolonioli.ldbcimpls.incubator.rya.interactive.queries;

import java.util.ArrayList;
import java.util.List;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3Result;

// TODO 
public class LdbcQuery3Handler implements OperationHandler<LdbcQuery3, DbConnectionState> {

	public void executeOperation(
			LdbcQuery3 ldbcQuery3,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		List<LdbcQuery3Result> resultList = new ArrayList<LdbcQuery3Result>();

		resultReporter.report(0, resultList, ldbcQuery3);
	}
}
