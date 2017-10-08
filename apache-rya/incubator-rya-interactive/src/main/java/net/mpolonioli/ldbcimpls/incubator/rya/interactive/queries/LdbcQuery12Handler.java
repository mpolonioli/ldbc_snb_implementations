package net.mpolonioli.ldbcimpls.incubator.rya.interactive.queries;

import java.util.ArrayList;
import java.util.List;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12Result;

// TODO
public class LdbcQuery12Handler implements OperationHandler<LdbcQuery12, DbConnectionState> {

	public void executeOperation(
			LdbcQuery12 ldbcQuery12,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		// RyaClient client = (((RyaConnectionState) dbConnectionState).getClient());

		List<LdbcQuery12Result> resultList = new ArrayList<LdbcQuery12Result>();

		// String query = "";

		//JSONArray rd = client.executeQuery(query);

		resultReporter.report(0, resultList, ldbcQuery12);
	}
}
