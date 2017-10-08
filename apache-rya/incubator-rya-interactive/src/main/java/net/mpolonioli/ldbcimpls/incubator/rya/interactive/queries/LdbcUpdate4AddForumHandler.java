package net.mpolonioli.ldbcimpls.incubator.rya.interactive.queries;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate4AddForum;

import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaClient;
import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaConnectionState;

public class LdbcUpdate4AddForumHandler implements
OperationHandler<LdbcUpdate4AddForum, DbConnectionState> {

	private static DateFormat creationDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");

	public void executeOperation(
			LdbcUpdate4AddForum ldbcUpdate4AddForum,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		RyaClient ryaClient = (((RyaConnectionState) dbConnectionState).getClient());

		List<Long> tagIds = ldbcUpdate4AddForum.tagIds();
		long forumId = ldbcUpdate4AddForum.forumId();
		String forumTitle = ldbcUpdate4AddForum.forumTitle();
		String creationDate = creationDateFormat.format(ldbcUpdate4AddForum.creationDate()) + ":00";
		long moderatorId = ldbcUpdate4AddForum.moderatorPersonId();

		String insertClause = 
				"sn:forum" + forumId + " rdf:type snvoc:Forum ;\n" + 
						"	snvoc:id \"" + forumId + "\"^^xsd:long ;\n" + 
						"	snvoc:title \"" + forumTitle + "\" ;\n" + 
						"	snvoc:creationDate \"" + creationDate + "\"^^xsd:dateTime ;\n" + 
						"	snvoc:hasModerator ?moderator .\n" 
						;
		String whereClause =
				"?moderator snvoc:id \"" + moderatorId + "\"^^xsd:long ; \n" + 
						"	rdf:type snvoc:Person .\n" + 
						"\n" + 
						"?tagClass rdf:type snvoc:TagClass .\n" + 
						"\n" + 
						""
						;

		for(int i = 0; i < tagIds.size(); i++) {
			insertClause += "sn:forum" + forumId + " snvoc:hasTag ?tag" + i + " .\n";

			whereClause += 
					"?tag" + i + " rdf:type ?tagClass ;\n" + 
							"	snvoc:id \"" + tagIds.get(i) + "\"^^xsd:int .\n" 
							;
		}

		String query = 
				"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" + 
						"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
						"PREFIX sn:<http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" + 
						"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
						"PREFIX dbpedia-owl: <http://dbpedia.org/ontology/>\n" + 
						"\n" + 
						"INSERT\n" + 
						"{\n" +
						insertClause + 
						"}\n" +
						"WHERE\n" +
						"{\n" +
						whereClause +
						"}";

		ryaClient.executeUpdateQuery(query);

		resultReporter.report(0, LdbcNoResult.INSTANCE, ldbcUpdate4AddForum);
	}
}

