package net.mpolonioli.ldbcimpls.incubator.rya.interactive.queries;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.Update;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate7AddComment;

import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaConnectionState;

public class LdbcUpdate7AddCommentHandler implements
OperationHandler<LdbcUpdate7AddComment, DbConnectionState> {

	private static DateFormat creationDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");

	public void executeOperation(
			LdbcUpdate7AddComment ldbcUpdate7AddComment,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		RepositoryConnection conn = (((RyaConnectionState) dbConnectionState).getClient());

		long personId = ldbcUpdate7AddComment.authorPersonId();
		long replyToId;
		if(ldbcUpdate7AddComment.replyToCommentId() == -1)
		{
			replyToId = ldbcUpdate7AddComment.replyToPostId();
		} else
		{
			replyToId = ldbcUpdate7AddComment.replyToCommentId();
		}
		long commentId = ldbcUpdate7AddComment.commentId();
		long countryId = ldbcUpdate7AddComment.countryId();
		String content = ldbcUpdate7AddComment.content();
		String creationDate = creationDateFormat.format(ldbcUpdate7AddComment.creationDate());
		String locationIp = ldbcUpdate7AddComment.locationIp();
		String browserUsed = ldbcUpdate7AddComment.browserUsed();
		int commentLength = ldbcUpdate7AddComment.length();

		List<Long> tagIds = ldbcUpdate7AddComment.tagIds();

		String insertClause =
				"sn:comm" + commentId + " rdf:type snvoc:Comment ;\n" + 
						"	snvoc:id \"" + commentId + "\"^^xsd:long ;\n" + 
						"	snvoc:creationDate \"" + creationDate + "\"^^xsd:dateTime ;\n" + 
						"	snvoc:locationIp \"" + locationIp + "\" ;\n" + 
						"	snvoc:browserUsed \"" + browserUsed + "\" ;\n" + 
						"	snvoc:content \"" + content + "\" ;\n" + 
						"	snvoc:length \"" + commentLength + "\"^^xsd:int ;\n" + 
						"	snvoc:hasCreator ?person ;\n" + 
						"	snvoc:isLocatedIn ?country ;\n" + 
						"	snvoc:replyOf ?commentOrPost .\n" + 
						"\n" + 
						"?forum snvoc:containerOf sn:comm" + commentId + " .\n" + 
						""
						;
		String whereClause = 
				"?person rdf:type snvoc:Person ;\n" + 
						"	snvoc:id \"" + personId + "\"^^xsd:long .\n" + 
						"\n" + 
						"?country dbpedia-owl:type snvoc:Country ;\n" + 
						"	snvoc:id \"" + countryId + "\"^^xsd:int .\n" + 
						"\n" + 
						"?commentOrPost rdf:type ?type ;\n" + 
						"	snvoc:id \"" + replyToId  + "\"^^xsd:long .\n" + 
						"\n" + 
						"FILTER(?type = snvoc:Post || ?type = snvoc:Comment)\n" + 
						"\n" + 
						"?forum snvoc:containerOf ?commentOrPost .\n" + 
						"\n" + 
						"?tagClass rdf:type snvoc:TagClass ."
						;

		for(int i = 0; i < tagIds.size(); i++) {
			insertClause += "sn:comment" + commentId + " snvoc:hasTag ?tag" + i + " .\n";

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
						"\n" +
						"}\n" +
						"WHERE\n" +
						"{\n" +
						whereClause +
						"\n" +
						"}";

		try {
			Update update = conn.prepareUpdate(QueryLanguage.SPARQL, query);
			update.execute();
		} catch (RepositoryException | MalformedQueryException | UpdateExecutionException e) {
			e.printStackTrace();
		}
		resultReporter.report(0, LdbcNoResult.INSTANCE, ldbcUpdate7AddComment);
	}
}
