package net.mpolonioli.ldbcimpls.janusgraph.interactive;

import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfile;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPosts;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriends;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContent;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreator;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageReplies;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate1AddPerson;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate2AddPostLike;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate3AddCommentLike;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate4AddForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate5AddForumMembership;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate6AddPost;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate7AddComment;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate8AddFriendship;

import net.mpolonioli.ldbcimpls.janusgraph.interactive.queries.*;

import java.io.IOException;
import java.util.Map;

/**
 * An implementation of the LDBC SNB interactive workload[1] for JanusGraphDB.
 * Queries are executed against a running JanuGraphDB-HBase cluster.
 */
public class JanusGraphDb extends Db{

	private JanusGraphDbConnectionState connectionState = null;

	@Override
	protected void onClose() throws IOException {
		connectionState.close();
	}

	@Override
	protected DbConnectionState getConnectionState() throws DbException {
		return connectionState;
	}

	@Override
	protected void onInit(Map<String, String> properties,
			LoggingService loggingService) throws DbException {

		connectionState = new JanusGraphDbConnectionState(/*properties*/);
		
		/*
		 * Register operation handlers with the benchmark.
		 */
		registerOperationHandler(LdbcQuery1.class, 
				LdbcQuery1Handler.class);
		registerOperationHandler(LdbcQuery2.class, 
				LdbcQuery2Handler.class);
		registerOperationHandler(LdbcQuery3.class,
				LdbcQuery3Handler.class);
		registerOperationHandler(LdbcQuery4.class, 
				LdbcQuery4Handler.class);
		registerOperationHandler(LdbcQuery5.class, 
				LdbcQuery5Handler.class);
		registerOperationHandler(LdbcQuery6.class,
				LdbcQuery6Handler.class);
		registerOperationHandler(LdbcQuery7.class, 
				LdbcQuery7Handler.class);
		registerOperationHandler(LdbcQuery8.class, 
				LdbcQuery8Handler.class);
		registerOperationHandler(LdbcQuery9.class,
				LdbcQuery9Handler.class);
		registerOperationHandler(LdbcQuery10.class, 
				LdbcQuery10Handler.class);
		registerOperationHandler(LdbcQuery11.class,
				LdbcQuery11Handler.class);
		registerOperationHandler(LdbcQuery12.class, 
				LdbcQuery12Handler.class);
		registerOperationHandler(LdbcQuery13.class,
				LdbcQuery13Handler.class);
		registerOperationHandler(LdbcQuery14.class,
				LdbcQuery14Handler.class);
		
		registerOperationHandler(LdbcShortQuery1PersonProfile.class,
				LdbcShortQuery1PersonProfileHandler.class);
		registerOperationHandler(LdbcShortQuery2PersonPosts.class,
				LdbcShortQuery2PersonPostsHandler.class);
		registerOperationHandler(LdbcShortQuery3PersonFriends.class,
				LdbcShortQuery3PersonFriendsHandler.class);
		registerOperationHandler(LdbcShortQuery4MessageContent.class,
				LdbcShortQuery4MessageContentHandler.class);
		registerOperationHandler(LdbcShortQuery5MessageCreator.class,
				LdbcShortQuery5MessageCreatorHandler.class);
		registerOperationHandler(LdbcShortQuery6MessageForum.class,
				LdbcShortQuery6MessageForumHandler.class);
		registerOperationHandler(LdbcShortQuery7MessageReplies.class,
				LdbcShortQuery7MessageRepliesHandler.class);
				
		registerOperationHandler(LdbcUpdate1AddPerson.class,
				LdbcUpdate1AddPersonHandler.class);
		registerOperationHandler(LdbcUpdate2AddPostLike.class,
				LdbcUpdate2AddPostLikeHandler.class);
		registerOperationHandler(LdbcUpdate3AddCommentLike.class,
				LdbcUpdate3AddCommentLikeHandler.class);
		registerOperationHandler(LdbcUpdate4AddForum.class,
				LdbcUpdate4AddForumHandler.class);
		registerOperationHandler(LdbcUpdate5AddForumMembership.class,
				LdbcUpdate5AddForumMembershipHandler.class);
		registerOperationHandler(LdbcUpdate6AddPost.class,
				LdbcUpdate6AddPostHandler.class);
		registerOperationHandler(LdbcUpdate7AddComment.class,
				LdbcUpdate7AddCommentHandler.class);
		registerOperationHandler(LdbcUpdate8AddFriendship.class,
				LdbcUpdate8AddFriendshipHandler.class);
	}
}
