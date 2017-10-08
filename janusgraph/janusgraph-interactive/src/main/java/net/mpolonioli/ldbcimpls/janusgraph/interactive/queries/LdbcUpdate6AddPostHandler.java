package net.mpolonioli.ldbcimpls.janusgraph.interactive.queries;

import static org.apache.tinkerpop.gremlin.process.traversal.P.within;

import java.util.ArrayList;
import java.util.List;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.SchemaViolationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate4AddForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate6AddPost;

import net.mpolonioli.ldbcimpls.janusgraph.interactive.JanusGraphDbConnectionState;

/**
 * Add a Post to the social network.[1]
 */
public class LdbcUpdate6AddPostHandler implements
    OperationHandler<LdbcUpdate6AddPost, DbConnectionState> {

  final static Logger logger =
      LoggerFactory.getLogger(LdbcUpdate4AddForum.class);

  @Override
  public void executeOperation(LdbcUpdate6AddPost operation,
      DbConnectionState dbConnectionState,
      ResultReporter reporter) throws DbException {
    Graph client = ((JanusGraphDbConnectionState) dbConnectionState).getClient();
    GraphTraversalSource g = client.traversal();

    try
    {
    List<Object> postKeyValues = new ArrayList<>(18);
    postKeyValues.add("messageId");
    postKeyValues.add(operation.postId());
    postKeyValues.add(T.label);
    postKeyValues.add("post");
    postKeyValues.add("imageFile");
    postKeyValues.add(operation.imageFile());
    postKeyValues.add("creationDate");
    postKeyValues.add(operation.creationDate().getTime());
    postKeyValues.add("locationIP");
    postKeyValues.add(operation.locationIp());
    postKeyValues.add("browserUsed");
    postKeyValues.add(operation.browserUsed());
    postKeyValues.add("language");
    postKeyValues.add(operation.language());
    postKeyValues.add("content");
    postKeyValues.add(operation.content());
    postKeyValues.add("length");
    postKeyValues.add(operation.length());

    Vertex post = client.addVertex(postKeyValues.toArray());

    List<Long> tagIds = new ArrayList<>(operation.tagIds().size());
    operation.tagIds().forEach((id) -> {
      tagIds.add(id);
    });

    g.V().has("tagId", within(tagIds)).forEachRemaining((v) -> {
  	  post.addEdge("hasTag", v);
    });
    Vertex creator = g.V().has("personId", operation.authorPersonId()).next();
    post.addEdge("hasCreator", creator);
    Vertex forum = g.V().has("forumId", operation.forumId()).next();
    forum.addEdge("containerOf", post);
    Vertex place = g.V().has("placeId", operation.countryId()).next();
    post.addEdge("isLocatedIn", place);

    client.tx().commit();
    }catch(SchemaViolationException e)
    {
    	
    }
    reporter.report(0, LdbcNoResult.INSTANCE, operation);
  }
}
