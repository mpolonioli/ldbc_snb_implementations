package net.mpolonioli.ldbcimpls.incubator.rya.interactive.queries;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate6AddPost;

import net.mpolonioli.ldbcimpls.incubator.rya.interactive.RyaConnectionState;

public class LdbcUpdate6AddPostHandler implements
OperationHandler<LdbcUpdate6AddPost, DbConnectionState> {

	private static DateFormat creationDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");

	public void executeOperation(
			LdbcUpdate6AddPost ldbcUpdate6AddPost,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		RepositoryConnection conn = (((RyaConnectionState) dbConnectionState).getClient());

		long personId = ldbcUpdate6AddPost.authorPersonId();
		long forumId = ldbcUpdate6AddPost.forumId();
		long postId = ldbcUpdate6AddPost.postId();
		long countryId = ldbcUpdate6AddPost.countryId();
		String imageFile = ldbcUpdate6AddPost.imageFile();
		String content = ldbcUpdate6AddPost.content();
		String creationDate = creationDateFormat.format(ldbcUpdate6AddPost.creationDate());
		String locationIp = ldbcUpdate6AddPost.locationIp();
		String browserUsed = ldbcUpdate6AddPost.browserUsed();
		String language = ldbcUpdate6AddPost.language();
		int postLength = ldbcUpdate6AddPost.length();

		List<Long> tagIds = ldbcUpdate6AddPost.tagIds();

		String insertClause =
				"sn:post" + postId + " rdf:type snvoc:Post ;\n" + 
						"	snvoc:id \"" + postId + "\"^^xsd:long ;\n" + 
						"	snvoc:imageFile \"" + imageFile + "\" ;\n" + 
						"	snvoc:creationDate \"" + creationDate + "\"^^xsd:dateTime ;\n" + 
						"	snvoc:locationIp \"" + locationIp + "\" ;\n" + 
						"	snvoc:browserUsed \"" + browserUsed + "\" ;\n" + 
						"	snvoc:language \"" + language + "\" ;\n" + 
						"	snvoc:content \"" + content + "\" ;\n" + 
						"	snvoc:length \"" + postLength + "\"^^xsd:int ;\n" + 
						"	snvoc:hasCreator ?person ;\n" + 
						"	snvoc:isLocatedIn ?country ;\n" + 
						"\n" + 
						"?forum snvoc:containerOf sn:post" + postId + " .\n\n"
						;
		String whereClause = 
				"?person rdf:type snvoc:Person ;\n" + 
						"	snvoc:id \"" + personId + "\"^^xsd:long .\n" + 
						"\n" + 
						"?forum rdf:type snvoc:Forum ;\n" + 
						"	snvoc:id \"" + forumId + "\"^^xsd:long .\n" + 
						"\n" + 
						"?country dbpedia-owl:type snvoc:Country ;\n" + 
						"	snvoc:id \"" + countryId + "\"^^xsd:int .\n" + 
						"\n" + 
						"?tagClass rdf:type snvoc:TagClass .\n\n"
						;

		for(int i = 0; i < tagIds.size(); i++) {
			insertClause += "sn:post" + postId + " snvoc:hasTag ?tag" + i + " .\n";

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
			TupleQuery tupleQuery = conn.prepareTupleQuery(
					QueryLanguage.SPARQL, query);
			tupleQuery.evaluate();
		} catch (RepositoryException | MalformedQueryException | QueryEvaluationException e) {
			e.printStackTrace();
		}

		resultReporter.report(0, LdbcNoResult.INSTANCE, ldbcUpdate6AddPost);
	}
}
