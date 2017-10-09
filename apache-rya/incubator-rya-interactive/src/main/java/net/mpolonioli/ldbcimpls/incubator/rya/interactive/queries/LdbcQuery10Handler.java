package net.mpolonioli.ldbcimpls.incubator.rya.interactive.queries;

import java.util.ArrayList;
import java.util.List;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10Result;

//TODO
public class LdbcQuery10Handler implements OperationHandler<LdbcQuery10, DbConnectionState> {


	public void executeOperation(
			LdbcQuery10 ldbcQuery10,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		List<LdbcQuery10Result> resultList = new ArrayList<LdbcQuery10Result>();

		resultReporter.report(0, resultList, ldbcQuery10);
	}
}
