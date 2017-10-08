package net.mpolonioli.ldbcimpls.incubator.rya.interactive.queries;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForumResult;

//TODO
public class LdbcShortQuery6MessageForumHandler implements OperationHandler<LdbcShortQuery6MessageForum, DbConnectionState> {
	
	public void executeOperation(
			LdbcShortQuery6MessageForum ldbcShortQuery6MessageForum,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		// RyaClient client = (((RyaConnectionState) dbConnectionState).getClient());
		
		LdbcShortQuery6MessageForumResult result = null;
		
		// String query = "";
		
		//JSONArray rd = client.executeQuery(query);
		result = new LdbcShortQuery6MessageForumResult(-1, "none", -1, "none", "none");
		resultReporter.report(-1, result, ldbcShortQuery6MessageForum);
	}
}
