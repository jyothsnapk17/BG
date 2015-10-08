package src;

import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.ejdb.bson.BSONObject;
import org.ejdb.bson.types.ObjectId;
import org.ejdb.driver.EJDB;
import org.ejdb.driver.EJDBCollection;
import org.ejdb.driver.EJDBException;
import org.ejdb.driver.EJDBQueryBuilder;
import org.ejdb.driver.EJDBResultSet;

import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.DB;
import edu.usc.bg.base.DBException;
import edu.usc.bg.base.StringByteIterator;

public class EJDBClient extends DB {

	private static EJDB ejdb ;
	private static AtomicInteger NumThreads = null;
	private static Semaphore semaphore = new Semaphore(1, true);
	private static Properties props;
	private static EJDBCollection users, resources, manipulations, friends ;

	private static int incrementNumThreads() {
		int v;
		do {
			v = NumThreads.get();
		} while (!NumThreads.compareAndSet(v, v + 1));
		return v + 1;
	}

	private static int decrementNumThreads() {
		int v;
		do {
			v = NumThreads.get();
		} while (!NumThreads.compareAndSet(v, v - 1));
		return v - 1;
	}

	@Override
	public boolean init() throws DBException{

		props = getProperties() ;
		System.out.println("Properties - " + props.toString());
		try {
				semaphore.acquire();

				if(ejdb == null) {
					ejdb = new EJDB() ;
				}

				if (NumThreads == null) {
					NumThreads = new AtomicInteger();
					NumThreads.set(0);

//					System.out.println("\tThread - " + NumThreads.get());
					if(!ejdb.isOpen())
						ejdb.open(EJDBClientProperties.DB_NAME,EJDB.JBOREADER | EJDB.JBOWRITER | EJDB.JBOCREAT | EJDB.JBOTRUNC) ;

					System.out.println("\tPATH : " + ejdb.getPath());

					System.out.println("Creating Schema") ;
					EJDBCollection.Options collectionOpt = new EJDBCollection.Options(false,true,65535,0) ;
					users = ejdb.ensureCollection(EJDBClientProperties.DB_COLLECTION_USERS, collectionOpt) ;
					resources = ejdb.ensureCollection(EJDBClientProperties.DB_COLLECTION_RESOURCES, collectionOpt);
					manipulations = ejdb.ensureCollection(EJDBClientProperties.DB_COLLECTION_MANIPULATIONS, collectionOpt) ;
					friends = ejdb.ensureCollection(EJDBClientProperties.DB_COLLECTION_FRIENDS, collectionOpt) ;

					if (!ejdb.isOpen()) {
						System.out.println("(log) Thread " +
					NumThreads.get() + ", Could not open EJDB Connection to BG_DB");
						return false;
			        }
				}
				incrementNumThreads();
		}catch (EJDBException e) {
				System.out.println("EJDB init failed to open DB.");
				e.printStackTrace(System.out);
		}catch (Exception e) {
				System.out.println("failed to acquire lock");
				e.printStackTrace(System.out);
		}
		finally {
				semaphore.release();
		}
		if (!ejdb.isOpen()) {
			System.out.println("Init - Connection to EJDB not open!");
			return false;
		}
		return true;
	}


	@Override
	public int insertEntity(String entitySet, String entityPK,
			HashMap<String, ByteIterator> values, boolean insertImage) {

		try {
			if(ejdb.isOpen()) {
//				System.out.println("Connection to EJDB Open - " + ejdb.getPath()) ;
			}
			else {
//				System.out.println("No open EJDB connection available. aborting!");
				return -1 ;
			}
			BSONObject entity ;
//			ObjectId pk = new ObjectId(entityPK);
//			newObj.append("_id", Integer.parseInt(entityPK)) ;

//			System.out.println("RECORD ID : " + entityPK);
			if(entitySet.equalsIgnoreCase("users")) {
//				System.out.println("received object of type : " + entitySet);
				entity = new BSONObject("userid", entityPK);
			}
			else {
//				System.out.println("received object of type : " + entitySet);
				entity = new BSONObject("rid", entityPK);
			}

			HashMap<String, String> strIter = StringByteIterator.getStringMap(values);
//			System.out.println("Creating object for insertion -");
			for(String key:strIter.keySet()) {
				String key_val = strIter.get(key) ;
				if(!(key_val.equalsIgnoreCase("pic") || key_val.equalsIgnoreCase("tpic"))) {
//					System.out.println(key + " : " + key_val);
//					System.out.println(key_val.length());
					if(key_val.length() > 19)
						entity.append(key, key_val.substring(0, key_val.length()/4));
					else
						entity.append(key, key_val);
				}
			}

//			//insert image
//			if (entitySet.equalsIgnoreCase("users") && insertImage) {
//				byte[] profileImage = ((ObjectByteIterator)values.get("pic")).toArray() ;
//				byte[] tprofileImage = ((ObjectByteIterator)values.get("tpic")).toArray() ;
//
//				newObj.append("pic", profileImage).append("tpic", tprofileImage) ;
//			}
			if(ejdb.isOpen()) {
//				System.out.println("Connection Open");
/*				//check if collection exists
//				ArrayList<EJDBCollection> collections = new ArrayList<EJDBCollection>(ejdb.getCollections()) ;
//				EJDBCollection usersL = null, resourcesL = null;
//				System.out.print("CollectionNames - ");
//				for(EJDBCollection key:collections) {
//					System.out.print("\t" + key.getName());
//					if(key.getName().equalsIgnoreCase("users"))
//						usersL = key ;
//					else if(key.getName().equalsIgnoreCase("resources"))
//						resourcesL = key;
//				}
				System.out.println();
*/
//				EJDBCollection usersL = ejdb.getCollection("users", true) ;
				if(entitySet.equalsIgnoreCase("users")) {
					if(users.isExists()) {
						ObjectId userID = users.save(entity);
//						System.out.println("\nInserting into USERS - " + userID.toString());
					}
					else {
//						System.out.println("Users collection does not exist");
					}
				}
//				EJDBCollection resourcesL = ejdb.getCollection("resources", true) ;
				if(entitySet.equalsIgnoreCase("resources")) {
					if(resources.isExists()) {
//						System.out.println("Inserting into RESOURCES");
						ObjectId resID = resources.save(entity);
					}
					else {
//						System.out.println("Resources collection does not exist");
					}
				}
			}
			else {
				System.out.println("Connection to EJDB not open");
			}
		} catch (EJDBException e) {
			System.out.println("could not create/save entity. " + e.getLocalizedMessage()) ;
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
		EJDBCollection friends = ejdb.getCollection(EJDBClientProperties.DB_COLLECTION_FRIENDS);

		//change the status of the result from pending to confirmed
		EJDBQueryBuilder qb = new EJDBQueryBuilder();
        EJDBResultSet rs;

        qb.field("userid1", String.valueOf(inviterID));
        qb.field("userid2", String.valueOf(inviteeID));
        rs = friends.createQuery(qb).find();
        BSONObject friendRec = rs.get(0);
        if(((String)friendRec.get("status")).equalsIgnoreCase(EJDBClientProperties.FRIEND_PENDING)) {
        	friendRec.put("status", EJDBClientProperties.FRIEND_CONFIRMED);
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
		BSONObject newFriendReq = new BSONObject("userid1", String.valueOf(inviterID))
				.append("userid2", String.valueOf(inviteeID))
				.append("status", EJDBClientProperties.FRIEND_PENDING);

		EJDBCollection friends = ejdb.getCollection(EJDBClientProperties.DB_COLLECTION_FRIENDS);
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

		System.out.println("GET INITIAL STATS");
		HashMap<String, String> stats = new HashMap<String, String>();

		//usercount
		EJDBCollection users = ejdb.getCollection(EJDBClientProperties.DB_COLLECTION_USERS);
		EJDBQueryBuilder qb = new EJDBQueryBuilder() ;
		EJDBResultSet rs = users.createQuery(qb).find();
		int numUsers = rs.length() ;
		System.out.println("USER COUNT - " + numUsers);
		stats.put("usercount", Integer.toString(numUsers));

		//resourcesperuser
		EJDBCollection resources = ejdb.getCollection(EJDBClientProperties.DB_COLLECTION_RESOURCES);
		int avgRes = 0 ;
		EJDBResultSet resR = resources.createQuery(qb).find();
		if(numUsers > 0) {
			avgRes = resR.length()/numUsers;
		}
		System.out.println("ResourcesCount - " + resR.length());
		System.out.println("resourcesperuser - " + avgRes);
		stats.put("resourcesperuser", Integer.toString(avgRes)) ;


		//averagefriendsperuser
		EJDBCollection friends = ejdb.getCollection(EJDBClientProperties.DB_COLLECTION_FRIENDS);
		EJDBQueryBuilder confirmedQ = new EJDBQueryBuilder() ;
		confirmedQ.field("status", EJDBClientProperties.FRIEND_CONFIRMED);
		resR = friends.createQuery(confirmedQ).find() ;
		int avgFriendsPerUser = 0 ;
		if(numUsers > 0) {
			avgFriendsPerUser = (resR.length()*2)/rs.length() ;
		}
		System.out.println("ConfirmedFriendsCount - " + resR.length()*2);
		System.out.println("avgfriendsperuser - " + avgFriendsPerUser);
		stats.put("avgfriendsperuser", Integer.toString(avgFriendsPerUser));

		//averagependingperuser
		EJDBQueryBuilder pendingQ = new EJDBQueryBuilder() ;
		pendingQ.field("status", EJDBClientProperties.FRIEND_PENDING);
		resR = friends.createQuery(pendingQ).find() ;
		int avgPendingPerUser = 0 ;
		if(numUsers > 0) {
			avgPendingPerUser = (resR.length()*2)/rs.length() ;
		}
		System.out.println("PendingFriendsCount - " + resR.length()*2);
		System.out.println("avgfriendsperuser - " + avgPendingPerUser);
		stats.put("avgpendingperuser", Integer.toString(avgPendingPerUser));

		return stats;
	}

	@Override
	public int CreateFriendship(int friendid1, int friendid2) {

		//insert friendship
		BSONObject friendRecord = new BSONObject("userid1", String.valueOf(friendid1)) ;
		friendRecord.append("userid2", String.valueOf(friendid2))
			.append("status", EJDBClientProperties.FRIEND_CONFIRMED) ;

		if(friends.isExists()) {
			//insert record
			System.out.println(friendRecord.toString());
			System.out.println(friends.save(friendRecord)) ;
			return 0;
		}
		else
			return -1 ;
//		return acceptFriend(friendid1, friendid2);
	}

	@Override
	public void createSchema(Properties props) {

//		System.out.println("Creating Schema") ;
//		EJDBCollection.Options collectionOpt = new EJDBCollection.Options(false,true,65535,0) ;
//		users = ejdb.ensureCollection("users", collectionOpt) ;
//		resources = ejdb.ensureCollection("resources", collectionOpt);
//		manipulations = ejdb.ensureCollection("manipulations", collectionOpt) ;
//		friends = ejdb.ensureCollection("friends", collectionOpt) ;

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
		if(!warmup){
			try {
				semaphore.acquire();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			decrementNumThreads();
			// add instance to vector of connections
			if (NumThreads.get() > 0) {
				semaphore.release();
				return;
			} else {
				// close all connections in vector
				try {
					Thread.sleep(6000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				if(ejdb!= null) {
					System.out.println("connection to EJDB closed");
					ejdb.close();
				}
				semaphore.release();
			}
		}
	}
}
