package net.mpolonioli.ldbcimpls.incubator.rya.interactive.queries;

import java.util.ArrayList;
import java.util.List;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6Result;

//TODO
public class LdbcQuery6Handler implements OperationHandler<LdbcQuery6, DbConnectionState> {

	public void executeOperation(
			LdbcQuery6 ldbcQuery6,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		//RyaClient client = (((RyaConnectionState) dbConnectionState).getClient());

		List<LdbcQuery6Result> resultList = new ArrayList<LdbcQuery6Result>();

		//String query = "";

		//JSONArray rd = client.executeQuery(query);

		resultReporter.report(0, resultList, ldbcQuery6);
	}
}
