package net.mpolonioli.ldbcimpls.janusgraph.interactive.queries;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1Result;

import net.mpolonioli.ldbcimpls.janusgraph.interactive.JanusGraphDbConnectionState;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Given a start Person, find up to 20 Persons with a given first name that
 * the start Person is connected to (excluding start Person) by at most 3
 * steps via Knows relationships. Return Persons, including summaries of the
 * Persons workplaces and places of study. Sort results ascending by their
 * distance from the start Person, for Persons within the same distance sort
 * ascending by their last name, and for Persons with same last name
 * ascending by their identifier.[1]
 */
public class LdbcQuery1Handler
implements OperationHandler<LdbcQuery1, DbConnectionState> {

	final static Logger logger =
			LoggerFactory.getLogger(LdbcQuery1Handler.class);

	@Override
	public void executeOperation(final LdbcQuery1 operation,
			DbConnectionState dbConnectionState,
			ResultReporter resultReporter) throws DbException {

		
		JanusGraph graph = ((JanusGraphDbConnectionState) dbConnectionState).getClient();
		GraphTraversalSource g = graph.traversal();
		
		long personId = operation.personId();
		int limit = operation.limit();
		String firstName = operation.firstName();
		
		int i = 0;
		List<LdbcQuery1Result> result = new ArrayList<>();
    	try 
    	{
    		Vertex startPerson = g.V().has("personId", personId).next();

    		List<Vertex> firstLevelFriendList = new ArrayList<>();
    		firstLevelFriendList =
    				g.V(startPerson)
    				.out("knows")
    				.has("firstName", firstName)
    				.order().by("lastName", Order.incr).by("personId", Order.incr)
    				.limit(limit)
    				.toList();

    		List<Vertex> secondLevelFriendList = new ArrayList<>();
    		secondLevelFriendList =
    				g.V(startPerson)
    				.out("knows")
    				.out("knows")
    				.has("firstName", firstName)
    				.has("personId", P.neq(startPerson.value("personId")))
    				.order().by("lastName", Order.incr).by("personId", Order.incr)
    				.dedup()
    				.limit(limit)
    				.toList();

    		List<Vertex> thirdLevelFriendList = new ArrayList<>();
    		thirdLevelFriendList =
    				g.V(startPerson)
    				.out("knows")
    				.out("knows")
    				.out("knows")
    				.has("firstName", firstName)
    				.dedup()
    				.has("personId", P.neq(startPerson.value("personId")))
    				.order().by("lastName", Order.incr).by("personId", Order.incr)
    				.limit(limit)
    				.toList();

    		int k = 0;
    		while(i < limit && k < firstLevelFriendList.size())
    		{	
    			Vertex friend = firstLevelFriendList.get(k);
    			long friendId = (long) friend.value("personId");
    			String lastName = (String) friend.value("lastName");
    			long birthday = (long) friend.value("birthday");
    			long creationDate = (long) friend.value("creationDate");
    			String gender = (String) friend.value("gender");
    			String browserUsed = (String) friend.value("browserUsed");
    			String locationIp = (String) friend.value("locationIP");
    			Iterator<Object> emailsObj = friend.values("email");
    			Iterator<Object> languagesObj = friend.values("language");

    			List<String> emails = new ArrayList<>();
    			while(emailsObj.hasNext())
    			{
    				emails.add((String) emailsObj.next());
    			}
    			List<String> languages = new ArrayList<>();
    			while(languagesObj.hasNext())
    			{
    				languages.add((String) languagesObj.next());
    			}
    			
    			Vertex place = g.V(friend).out("isLocatedIn").next();
    			String placeName = (String) place.value("name");

    			List<List<Object>> universities = new ArrayList<>();    		
    			List<Edge> studyAts = g.V(friend).outE("studyAt").toList();
    			for(int j = 0; j < studyAts.size(); j++)
    			{
    				Edge studyAt = studyAts.get(j);
    				int classYear = (int) studyAt.value("classYear");
    				Vertex university = g.E(studyAt).inV().next();
    				String universityName = (String) university.value("name");
    				Vertex universityCity = g.V(university).out("isLocatedIn").next();
    				String universityCityName = (String) universityCity.value("name");

    				List<Object> universityInfo = new ArrayList<>();
    				universityInfo.add(universityName);
    				universityInfo.add(classYear);
    				universityInfo.add(universityCityName);

    				universities.add(universityInfo);
    			}

    			List<List<Object>> companies = new ArrayList<>();    		
    			List<Edge> workAts = g.V(friend).outE("workAt").toList();
    			for(int j = 0; j < workAts.size(); j++)
    			{
    				Edge workAt = workAts.get(j);
    				int workFrom = (int) workAt.value("workFrom");
    				Vertex company = g.E(workAt).inV().next();
    				String companyName = (String) company.value("name");
    				Vertex companyCity = g.V(company).out("isLocatedIn").next();
    				String companyCityName = (String) companyCity.value("name");

    				List<Object> companyInfo = new ArrayList<>();
    				companyInfo.add(companyName);
    				companyInfo.add(workFrom);
    				companyInfo.add(companyCityName);

    				companies.add(companyInfo);
    			}
    			result.add(new LdbcQuery1Result(friendId, lastName, 1, birthday, creationDate, gender, browserUsed, locationIp, emails, languages, placeName, universities, companies));
    			i++;
    			k++;
    		}

    		k = 0;
    		while(i < limit && k < secondLevelFriendList.size())
    		{	
    			Vertex friend = secondLevelFriendList.get(k);
    			long friendId = (long) friend.value("personId");
    			String lastName = (String) friend.value("lastName");
    			long birthday = (long) friend.value("birthday");
    			long creationDate = (long) friend.value("creationDate");
    			String gender = (String) friend.value("gender");
    			String browserUsed = (String) friend.value("browserUsed");
    			String locationIp = (String) friend.value("locationIP");
    			Iterator<Object> emailsObj = friend.values("email");
    			Iterator<Object> languagesObj = friend.values("language");

    			List<String> emails = new ArrayList<>();
    			while(emailsObj.hasNext())
    			{
    				emails.add((String) emailsObj.next());
    			}
    			List<String> languages = new ArrayList<>();
    			while(languagesObj.hasNext())
    			{
    				languages.add((String) languagesObj.next());
    			}

    			Vertex place = g.V(friend).out("isLocatedIn").next();
    			String placeName = (String) place.value("name");

    			List<List<Object>> universities = new ArrayList<>();    		
    			List<Edge> studyAts = g.V(friend).outE("studyAt").toList();
    			for(int j = 0; j < studyAts.size(); j++)
    			{
    				Edge studyAt = studyAts.get(j);
    				int classYear = (int) studyAt.value("classYear");
    				Vertex university = g.E(studyAt).inV().next();
    				String universityName = (String) university.value("name");
    				Vertex universityCity = g.V(university).out("isLocatedIn").next();
    				String universityCityName = (String) universityCity.value("name");

    				List<Object> universityInfo = new ArrayList<>();
    				universityInfo.add(universityName);
    				universityInfo.add(classYear);
    				universityInfo.add(universityCityName);

    				universities.add(universityInfo);
    			}

    			List<List<Object>> companies = new ArrayList<>();    		
    			List<Edge> workAts = g.V(friend).outE("workAt").toList();
    			for(int j = 0; j < workAts.size(); j++)
    			{
    				Edge workAt = workAts.get(j);
    				int workFrom = (int) workAt.value("workFrom");
    				Vertex company = g.E(workAt).inV().next();
    				String companyName = (String) company.value("name");
    				Vertex companyCity = g.V(company).out("isLocatedIn").next();
    				String companyCityName = (String) companyCity.value("name");

    				List<Object> companyInfo = new ArrayList<>();
    				companyInfo.add(companyName);
    				companyInfo.add(workFrom);
    				companyInfo.add(companyCityName);

    				companies.add(companyInfo);
    			}
    			result.add(new LdbcQuery1Result(friendId, lastName, 2, birthday, creationDate, gender, browserUsed, locationIp, emails, languages, placeName, universities, companies));
    			i++;
    			k++;
    		}

    		k = 0;
    		while(i < limit && k < thirdLevelFriendList.size())
    		{    			
    			Vertex friend = thirdLevelFriendList.get(k);
    			long friendId = (long) friend.value("personId");
    			String lastName = (String) friend.value("lastName");
    			long birthday = (long) friend.value("birthday");
    			long creationDate = (long) friend.value("creationDate");
    			String gender = (String) friend.value("gender");
    			String browserUsed = (String) friend.value("browserUsed");
    			String locationIp = (String) friend.value("locationIP");
    			Iterator<Object> emailsObj = friend.values("email");
    			Iterator<Object> languagesObj = friend.values("language");

    			List<String> emails = new ArrayList<>();
    			while(emailsObj.hasNext())
    			{
    				emails.add((String) emailsObj.next());
    			}
    			List<String> languages = new ArrayList<>();
    			while(languagesObj.hasNext())
    			{
    				languages.add((String) languagesObj.next());
    			}

    			Vertex place = g.V(friend).out("isLocatedIn").next();
    			String placeName = (String) place.value("name");

    			List<List<Object>> universities = new ArrayList<>();    		
    			List<Edge> studyAts = g.V(friend).outE("studyAt").toList();
    			for(int j = 0; j < studyAts.size(); j++)
    			{
    				Edge studyAt = studyAts.get(j);
    				int classYear = (int) studyAt.value("classYear");
    				Vertex university = g.E(studyAt).inV().next();
    				String universityName = (String) university.value("name");
    				Vertex universityCity = g.V(university).out("isLocatedIn").next();
    				String universityCityName = (String) universityCity.value("name");

    				List<Object> universityInfo = new ArrayList<>();
    				universityInfo.add(universityName);
    				universityInfo.add(classYear);
    				universityInfo.add(universityCityName);

    				universities.add(universityInfo);
    			}

    			List<List<Object>> companies = new ArrayList<>();    		
    			List<Edge> workAts = g.V(friend).outE("workAt").toList();
    			for(int j = 0; j < workAts.size(); j++)
    			{
    				Edge workAt = workAts.get(j);
    				int workFrom = (int) workAt.value("workFrom");
    				Vertex company = g.E(workAt).inV().next();
    				String companyName = (String) company.value("name");
    				Vertex companyCity = g.V(company).out("isLocatedIn").next();
    				String companyCityName = (String) companyCity.value("name");

    				List<Object> companyInfo = new ArrayList<>();
    				companyInfo.add(companyName);
    				companyInfo.add(workFrom);
    				companyInfo.add(companyCityName);

    				companies.add(companyInfo);
    			}
    			result.add(new LdbcQuery1Result(friendId, lastName, 3, birthday, creationDate, gender, browserUsed, locationIp, emails, languages, placeName, universities, companies));
    			i++;
    			k++;	
    		}
    	}catch(Exception e)
    	{
    		e.printStackTrace();
    		System.out.println("*\n*\n*" + operation + "\n*\n*\n*");
    	}
    	resultReporter.report(result.size(), result, operation);
	}

}
