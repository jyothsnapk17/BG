package src;

import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import org.ejdb.bson.BSONObject;
import org.ejdb.driver.EJDB;
import org.ejdb.driver.EJDBCollection;
import org.ejdb.driver.EJDBException;
import org.ejdb.driver.EJDBQueryBuilder;
import org.ejdb.driver.EJDBResultSet;

import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.DB;
import edu.usc.bg.base.DBException;
import edu.usc.bg.base.ObjectByteIterator;

public class EJDBClient extends DB {

	private static EJDB ejdb ;
//	EJDBCollection users, resources, manipulations, friendships ;

	@Override
	public boolean init() throws DBException{

		System.out.println("WITHIN INIT");
		ejdb = new EJDB() ;
		try {
			ejdb.open("BG_DB", EJDB.JBOREADER) ;
			System.out.println("PATH : " + ejdb.getPath());

			if (!ejdb.isOpen()) {
				System.out.println("(log)Could not open EJDB Connection to BG_DB");
				return false;
	        }

		} catch(EJDBException e) {
			System.out.println("(log) " + e.getCode());
			e.printStackTrace() ;
			return false;
		}

		return true;
	}


	@Override
	public int insertEntity(String entitySet, String entityPK, HashMap<String, ByteIterator> values,
			boolean insertImage) {
		// TODO Auto-generated method stub
		try {
			BSONObject newObj = new BSONObject() ;

			if(entitySet.equalsIgnoreCase("users")) {
				System.out.println("inserting user");
				newObj.append("userid", entityPK);
			}
			else {
				System.out.println("inserting resource");
				newObj.append("rid", entityPK);
			}

			for(String key:values.keySet()) {
				if(!(key.toString().equalsIgnoreCase("pic") || key.toString().equalsIgnoreCase("tpic")))
					newObj.append(key, values.get(key).toString());
			}

			//insert image
			if (entitySet.equalsIgnoreCase("users") && insertImage) {
				byte[] profileImage = ((ObjectByteIterator)values.get("pic")).toArray() ;
				byte[] tprofileImage = ((ObjectByteIterator)values.get("tpic")).toArray() ;

				newObj.append("pic", profileImage).append("tpic", tprofileImage) ;
			}

			EJDBCollection coll = ejdb.getCollection(entitySet.toLowerCase()) ;
			coll.save(newObj) ;
		} catch (Exception e) {
			System.out.println("could not create/save entity") ;
			e.printStackTrace();
			return -1 ;
		}
		return 0;
	}

	@Override
	public int viewProfile(int requesterID, int profileOwnerID, HashMap<String,
			ByteIterator> result,
			boolean insertImage, boolean testMode) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int listFriends(int requesterID, int profileOwnerID,
			Set<String> fields,
			Vector<HashMap<String, ByteIterator>> result, boolean insertImage,
			boolean testMode) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int viewFriendReq(int profileOwnerID, Vector<HashMap<String,
			ByteIterator>> results, boolean insertImage,
			boolean testMode) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int acceptFriend(int inviterID, int inviteeID) {
		EJDBCollection friends = ejdb.getCollection("friends");

		//change the status of the result from pending to confirmed
		EJDBQueryBuilder qb = new EJDBQueryBuilder();
        EJDBResultSet rs;

        qb.field("userid1", inviterID);
        qb.field("userid2", inviteeID);
        rs = friends.createQuery(qb).find();
        BSONObject friendRec = rs.get(0);
        if(((String)friendRec.get("status")).equalsIgnoreCase("pending")) {
        	friendRec.put("status", "confirmed");
//        	BSONObject newFriendRec = new BSONObject("userid1", friendid2)
//        			.append("userid2", friendid1)
//        			.append("status", "confirmed");

        	friends.save(friendRec);
//        	friends.save(newFriendRec);
        }
        return 0;

	}

	@Override
	public int rejectFriend(int inviterID, int inviteeID) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int inviteFriend(int inviterID, int inviteeID) {
		// TODO Auto-generated method stub
		BSONObject newFriendReq = new BSONObject("userid1", inviterID)
				.append("userid2", inviteeID)
				.append("status", "pending");

		EJDBCollection friends = ejdb.getCollection("friends");
		friends.save(newFriendReq);
		return 0;
	}

	@Override
	public int viewTopKResources(int requesterID, int profileOwnerID, int k,
			Vector<HashMap<String, ByteIterator>> result) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getCreatedResources(int creatorID, Vector<HashMap<String,
			ByteIterator>> result) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int viewCommentOnResource(int requesterID, int profileOwnerID,
			int resourceID,
			Vector<HashMap<String, ByteIterator>> result) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int postCommentOnResource(int commentCreatorID, int resourceCreatorID,
			int resourceID,
			HashMap<String, ByteIterator> values) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int delCommentOnResource(int resourceCreatorID, int resourceID,
			int manipulationID) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int thawFriendship(int friendid1, int friendid2) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public HashMap<String, String> getInitialStats() {

		HashMap<String, String> stats = new HashMap<String, String>();

		//usercount
		EJDBCollection users = ejdb.getCollection("users");
		EJDBQueryBuilder qb = new EJDBQueryBuilder() ;
		EJDBResultSet rs = users.createQuery(qb).find();
		int numUsers = rs.length() ;
		stats.put("usercount", Integer.toString(numUsers));

		//resourcesperuser
		EJDBCollection resources = ejdb.getCollection("resources");
		int avgRes = 0 ;
		EJDBResultSet resR = resources.createQuery(qb).find();
		if(numUsers > 0) {
			avgRes = resR.length()/numUsers;
		}
		stats.put("resourcesperuser", Integer.toString(avgRes)) ;


		//averagefriendsperuser
		EJDBCollection friends = ejdb.getCollection("friends");
		EJDBQueryBuilder confirmedQ = new EJDBQueryBuilder() ;
		confirmedQ.field("status", "confirmed");
		resR = friends.createQuery(confirmedQ).find() ;
		int avgFriendsPerUser = 0 ;
		if(numUsers > 0) {
			avgFriendsPerUser = (resR.length()*2)/rs.length() ;
		}
		stats.put("averagefriendsperuser", Integer.toString(avgFriendsPerUser));

		//averagependingperuser
		EJDBQueryBuilder pendingQ = new EJDBQueryBuilder() ;
		confirmedQ.field("status", "pending");
		resR = friends.createQuery(confirmedQ).find() ;
		int avgPendingPerUser = 0 ;
		if(numUsers > 0) {
			avgPendingPerUser = (resR.length()*2)/rs.length() ;
		}
		stats.put("averagependingsperuser", Integer.toString(avgPendingPerUser));

		return stats;
	}

	@Override
	public int CreateFriendship(int friendid1, int friendid2) {
		return acceptFriend(friendid1, friendid2);
	}

	@Override
	public void createSchema(Properties props) {

		System.out.println("(log) Creating Schema") ;
		ejdb.ensureCollection("users") ;
		ejdb.ensureCollection("resources");
		ejdb.ensureCollection("manipulations") ;
		ejdb.ensureCollection("friends") ;
	}

	@Override
	public int queryPendingFriendshipIds(int memberID, Vector<Integer> pendingIds) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int queryConfirmedFriendshipIds(int memberID, Vector<Integer> confirmedIds) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void cleanup(boolean warmup) throws DBException
	{
		if(ejdb.isOpen()) {
			System.out.println("(log) Closing EJDB Connection");
			ejdb.close();
		}
	}
}
