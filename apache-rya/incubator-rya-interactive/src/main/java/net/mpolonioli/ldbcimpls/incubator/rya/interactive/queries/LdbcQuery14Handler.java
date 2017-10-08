package net.mpolonioli.ldbcimpls.incubator.rya.interactive.queries;

import java.util.ArrayList;
import java.util.List;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14Result;

// TODO
public class LdbcQuery14Handler implements OperationHandler<LdbcQuery14, DbConnectionState> {
	
	public void executeOperation(
			LdbcQuery14 ldbcQuery14,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		// RyaClient client = (((RyaConnectionState) dbConnectionState).getClient());

		List<LdbcQuery14Result> resultList = new ArrayList<LdbcQuery14Result>();

		// String query = "";

		//JSONArray rd = client.executeQuery(query);

		resultReporter.report(0, resultList, ldbcQuery14);
	}
}
