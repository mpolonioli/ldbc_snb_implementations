package net.mpolonioli.ldbcimpls.incubator.rya.interactive;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.openrdf.repository.RepositoryException;

import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;

import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.workloads.ldbc.snb.interactive.*;

import net.mpolonioli.ldbcimpls.incubator.rya.interactive.queries.*;

public class RyaDb extends Db{

	public DbConnectionState dbConnectionState = null;
	
	@Override
	protected DbConnectionState getConnectionState() throws DbException {
		return dbConnectionState;	
	}

	@Override
	protected void onClose() throws IOException {
		dbConnectionState.close();
	}

	@Override
	protected void onInit(Map<String, String> ryaDbProperties, LoggingService log) throws DbException {
		try {
			dbConnectionState = new RyaConnectionState();
		} catch (RepositoryException | AccumuloException | AccumuloSecurityException e) {
			e.printStackTrace();
		}
		registerOperationHandler(LdbcQuery1.class, LdbcQuery1Handler.class);
		registerOperationHandler(LdbcQuery2.class, LdbcQuery2Handler.class);
		registerOperationHandler(LdbcQuery3.class, LdbcQuery3Handler.class);
		registerOperationHandler(LdbcQuery4.class, LdbcQuery4Handler.class);
		registerOperationHandler(LdbcQuery5.class, LdbcQuery5Handler.class);
		registerOperationHandler(LdbcQuery6.class, LdbcQuery6Handler.class);
		registerOperationHandler(LdbcQuery7.class, LdbcQuery7Handler.class);
		registerOperationHandler(LdbcQuery8.class, LdbcQuery8Handler.class);
		registerOperationHandler(LdbcQuery9.class, LdbcQuery9Handler.class);
		registerOperationHandler(LdbcQuery10.class, LdbcQuery10Handler.class);
		registerOperationHandler(LdbcQuery11.class, LdbcQuery11Handler.class);
		registerOperationHandler(LdbcQuery12.class, LdbcQuery12Handler.class);
		registerOperationHandler(LdbcQuery13.class, LdbcQuery13Handler.class);
		registerOperationHandler(LdbcQuery14.class, LdbcQuery14Handler.class);
		registerOperationHandler(LdbcShortQuery1PersonProfile.class, LdbcShortQuery1PersonProfileHandler.class);
		registerOperationHandler(LdbcShortQuery2PersonPosts.class, LdbcShortQuery2PersonPostsHandler.class);
		registerOperationHandler(LdbcShortQuery3PersonFriends.class, LdbcShortQuery3PersonFriendsHandler.class);
		registerOperationHandler(LdbcShortQuery4MessageContent.class, LdbcShortQuery4MessageContentHandler.class);
		registerOperationHandler(LdbcShortQuery5MessageCreator.class, LdbcShortQuery5MessageCreatorHandler.class);
		registerOperationHandler(LdbcShortQuery6MessageForum.class, LdbcShortQuery6MessageForumHandler.class);
		registerOperationHandler(LdbcShortQuery7MessageReplies.class, LdbcShortQuery7MessageRepliesHandler.class);
		registerOperationHandler(LdbcUpdate1AddPerson.class, LdbcUpdate1AddPersonHandler.class);
		registerOperationHandler(LdbcUpdate2AddPostLike.class, LdbcUpdate2AddPostLikeHandler.class);
		registerOperationHandler(LdbcUpdate3AddCommentLike.class, LdbcUpdate3AddCommentLikeHandler.class);
		registerOperationHandler(LdbcUpdate4AddForum.class, LdbcUpdate4AddForumHandler.class);
		registerOperationHandler(LdbcUpdate5AddForumMembership.class, LdbcUpdate5AddForumMembershipHandler.class);
		registerOperationHandler(LdbcUpdate6AddPost.class, LdbcUpdate6AddPostHandler.class);
		registerOperationHandler(LdbcUpdate7AddComment.class, LdbcUpdate7AddCommentHandler.class);
		registerOperationHandler(LdbcUpdate8AddFriendship.class, LdbcUpdate8AddFriendshipHandler.class);
	}
}
