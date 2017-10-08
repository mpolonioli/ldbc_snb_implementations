package net.mpolonioli.ldbcimpls.incubator.rya.interactive.queries;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13Result;

// TODO
public class LdbcQuery13Handler implements OperationHandler<LdbcQuery13, DbConnectionState> {

	public void executeOperation(
			LdbcQuery13 ldbcQuery13,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		// RyaClient client = (((RyaConnectionState) dbConnectionState).getClient());

		LdbcQuery13Result result = new LdbcQuery13Result(-1);

		// String query = "";

		//JSONArray rd = client.executeQuery(query);

		resultReporter.report(0, result, ldbcQuery13);
	}
}
