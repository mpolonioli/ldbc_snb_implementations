package net.mpolonioli.ldbcimpls.incubator.rya.interactive.queries;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate5AddForumMembership;

import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaClient;
import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaConnectionState;

public class LdbcUpdate5AddForumMembershipHandler implements
OperationHandler<LdbcUpdate5AddForumMembership, DbConnectionState> {

	private static DateFormat creationDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");

	public void executeOperation(
			LdbcUpdate5AddForumMembership ldbcUpdate5AddForumMembership,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		RyaClient ryaClient = (((RyaConnectionState) dbConnectionState).getClient());

		long personId = ldbcUpdate5AddForumMembership.personId();
		long forumId = ldbcUpdate5AddForumMembership.forumId();
		String joinDate = creationDateFormat.format(ldbcUpdate5AddForumMembership.joinDate()) + ":00";

		String query =
				"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" + 
						"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
						"PREFIX sn:<http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" + 
						"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
						"INSERT\n" + 
						"{\n" + 
						"_:mbs snvoc:hasPerson ?person ;\n" + 
						"	snvoc:joinDate \"" + joinDate + "\"^^xsd:dateTime .\n" + 
						"\n" + 
						"?forum snvoc:hasMember _:mbs .	\n" + 
						"}\n" + 
						"WHERE\n" + 
						"{\n" + 
						"?person snvoc:id \"" + personId + "\"^^xsd:long ;\n" + 
						"	rdf:type snvoc:Person .\n" + 
						"\n" + 
						"?forum snvoc:id \"" + forumId + "\"^^xsd:long ;\n" + 
						"	rdf:type snvoc:Forum .\n" +
						"}"
						;

		ryaClient.executeUpdateQuery(query);

		resultReporter.report(0, LdbcNoResult.INSTANCE, ldbcUpdate5AddForumMembership);
	}
}
