package net.mpolonioli.ldbcimpls.janusgraph.interactive.queries;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfile;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfileResult;

import net.mpolonioli.ldbcimpls.janusgraph.interactive.JanusGraphDbConnectionState;


/**
 * Given a start Person, retrieve their first name, last name, birthday, IP
 * address, browser, and city of residence.[1]
 */
public class LdbcShortQuery1PersonProfileHandler implements
OperationHandler<LdbcShortQuery1PersonProfile, DbConnectionState> {

	final static Logger logger =
			LoggerFactory.getLogger(LdbcShortQuery1PersonProfileHandler.class);

	@Override
	public void executeOperation(final LdbcShortQuery1PersonProfile operation,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		Graph graph = ((JanusGraphDbConnectionState) dbConnectionState).getClient();
		GraphTraversalSource g = graph.traversal();

		long personId = operation.personId();;

		LdbcShortQuery1PersonProfileResult result = null;

		try 
		{
			Vertex person = g.V().has("personId", personId).next();
			Vertex place = g.V(person).out("isLocatedIn").next();

			String firstName = person.value("firstName");
			String lastName = person.value("lastName");
			long birthday = person.value("birthday");
			String locationIp = person.value("locationIP");
			String browserUsed = person.value("browserUsed");
			long cityId = place.value("placeId");
			String gender = person.value("gender");
			long creationDate = person.value("creationDate");

			result = new LdbcShortQuery1PersonProfileResult(firstName, lastName, birthday, locationIp, browserUsed, cityId, gender, creationDate);
		} catch (Exception e)
		{
			result = new LdbcShortQuery1PersonProfileResult("null", "null", 0, "null", "null", 0, "null", 0);
			e.printStackTrace();	
		}

		resultReporter.report(1, result, operation);
	}
}
