package src;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
import edu.usc.bg.base.ObjectByteIterator;
import edu.usc.bg.base.StringByteIterator;

public class EJDBClient extends DB {

	private static EJDB ejdb ;
	private static AtomicInteger NumThreads = null;
	private static Semaphore semaphore = new Semaphore(1, true);
	private static Properties props;
	private static EJDBCollection users, resources, manipulations, friends, images, thumbnails ;

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
//		System.out.println("Properties - " + props.toString());
		try {
				semaphore.acquire();

				if(ejdb == null) {
					ejdb = new EJDB() ;
				}

				if (NumThreads == null) {
					NumThreads = new AtomicInteger();
					NumThreads.set(0);

//					System.out.println("\tThread - " + NumThreads.get());
					if(!ejdb.isOpen()) {
						if(props.containsKey("db.create"))
							ejdb.open(EJDBClientProperties.DB_NAME,EJDB.JBOREADER | EJDB.JBOWRITER | EJDB.JBOCREAT | EJDB.JBOTRUNC) ;
						else
							ejdb.open(EJDBClientProperties.DB_NAME,EJDB.JBOREADER | EJDB.JBOWRITER | EJDB.JBOCREAT) ;
					}
					System.out.println("\tPATH : " + ejdb.getPath());

//					System.out.println("Creating Schema") ;
					EJDBCollection.Options collectionOpt = new EJDBCollection.Options(false,true,65535,0) ;
					users = ejdb.ensureCollection(EJDBClientProperties.DB_COLLECTION_USERS, collectionOpt) ;
					resources = ejdb.ensureCollection(EJDBClientProperties.DB_COLLECTION_RESOURCES, collectionOpt);
					manipulations = ejdb.ensureCollection(EJDBClientProperties.DB_COLLECTION_MANIPULATIONS, collectionOpt) ;
					friends = ejdb.ensureCollection(EJDBClientProperties.DB_COLLECTION_FRIENDS, collectionOpt) ;
					images = ejdb.ensureCollection(EJDBClientProperties.DB_COLLECTION_IMAGES, collectionOpt);
					thumbnails = ejdb.ensureCollection(EJDBClientProperties.DB_COLLECTION_THUMBNAILS, collectionOpt);

//					users = ejdb.getCollection(EJDBClientProperties.DB_COLLECTION_USERS, false);
//					resources = ejdb.getCollection(EJDBClientProperties.DB_COLLECTION_RESOURCES, false);
//					manipulations = ejdb.getCollection(EJDBClientProperties.DB_COLLECTION_MANIPULATIONS, false) ;
//					friends = ejdb.getCollection(EJDBClientProperties.DB_COLLECTION_FRIENDS, false) ;
//					images = ejdb.getCollection(EJDBClientProperties.DB_COLLECTION_IMAGES, false);
//					thumbnails = ejdb.getCollection(EJDBClientProperties.DB_COLLECTION_THUMBNAILS, false);

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
//			if(insertImage) {
//				System.out.println("Inserting Images!");
//			}
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

			if (entitySet.equalsIgnoreCase("users") && insertImage) {
				byte[] profileImage = ((ObjectByteIterator)values.get("pic")).toArray();
				byte[] thumbnail = ((ObjectByteIterator)values.get("tpic")).toArray();

				String imagePath = EJDBClientProperties.IMAGE_PATH + entityPK + ".img";
				String thumbPath = EJDBClientProperties.THUMBNAIL_PATH + entityPK + ".thumbnail";
				FileOutputStream fs = new FileOutputStream(imagePath) ;
				fs.write(profileImage);
				fs.close();
				fs = new FileOutputStream(thumbPath) ;
				fs.write(profileImage);
				fs.close();
				entity.append("pic", imagePath).append("tpic", thumbPath);
			}

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
//				EJDBCollection users = ejdb.getCollection(EJDBClientProperties.DB_COLLECTION_USERS) ;
				if(entitySet.equalsIgnoreCase("users")) {
					if(users.isExists()) {
						ObjectId userID = users.save(entity);
						users.commitTransaction();
//						System.out.println("\nInserting into USERS - " + userID.toString());
					}
					else {
//						System.out.println("Users collection does not exist");
					}
				}
//				EJDBCollection resources = ejdb.getCollection("resources") ;
				if(entitySet.equalsIgnoreCase("resources")) {
					if(resources.isExists()) {
//						System.out.println("Inserting into RESOURCES");
						ObjectId resID = resources.save(entity);
						resources.commitTransaction();
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
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}

	@Override
	public int viewProfile(int requesterID, int profileOwnerID, HashMap<String,
			ByteIterator> result,
			boolean insertImage, boolean testMode) {

		int retVal = 0 ;
		if(profileOwnerID < 0 || requesterID < 0) {
			return -1 ;
		}

//		EJDBCollection users = ejdb.getCollection(EJDBClientProperties.DB_COLLECTION_USERS);
//		System.out.println("Printing Properties - " + EJDBClientProperties.DB_COLLECTION_USERS);
		EJDBQueryBuilder qb = new EJDBQueryBuilder();
		qb.field("userid", String.valueOf(profileOwnerID));

        BSONObject userProfile = users.createQuery(qb).findOne();
//        System.out.println(userProfile.toString());

        //find number of confirmed
//        EJDBCollection friends = ejdb.getCollection(EJDBClientProperties.DB_COLLECTION_FRIENDS) ;
        qb = new EJDBQueryBuilder() ;
        qb.field("userid1",profileOwnerID) ;
        qb.field("status", EJDBClientProperties.FRIEND_CONFIRMED);
        int confirmedFriends = friends.createQuery(qb).count();

        qb = new EJDBQueryBuilder();
        qb.field("userid2", profileOwnerID);
        qb.field("status",EJDBClientProperties.FRIEND_CONFIRMED);
        confirmedFriends += friends.createQuery(qb).count();

        //if the user requests his/her profile, also retrieve pending friends
        int pendingFriends = -1 ;
        if(requesterID == profileOwnerID) {
        	qb = new EJDBQueryBuilder();
        	qb.field("userid2",profileOwnerID);
        	qb.field("status",EJDBClientProperties.FRIEND_PENDING);
        	pendingFriends = friends.createQuery(qb).count();
        }

        //get resources for user
//        EJDBCollection resources = ejdb.getCollection(EJDBClientProperties.DB_COLLECTION_RESOURCES);
        qb = new EJDBQueryBuilder();
        qb.field("walluserid", Integer.toString(profileOwnerID));
        int ownedResources = resources.createQuery(qb).count();

        userProfile.append("friendcount", Integer.toString(confirmedFriends));
        if(requesterID == profileOwnerID){
        	userProfile.append("pendingcount", Integer.toString(pendingFriends));
        }
        userProfile.append("resourcecount", Integer.toString(ownedResources));

        if(userProfile != null) {
        	for(String key:userProfile.fields()) {
//        		System.out.println(key);
        		if(key.equals("_id"))
        			result.put(key, new ObjectByteIterator(userProfile.getId().toByteArray()));
        		else
        			result.put(key, new ObjectByteIterator(((String)userProfile.get(key)).getBytes())) ;
        	}
        }

        //insert image
        try {
        	if(insertImage){
        		if(testMode){
//        			//Save loaded image from database into new image file
        			String imagePath = EJDBClientProperties.IMAGE_PATH + profileOwnerID + ".img";
    				String thumbPath = EJDBClientProperties.THUMBNAIL_PATH + profileOwnerID + ".thumbnail";
        			FileInputStream profileImage = new FileInputStream(imagePath);
        			profileImage.close();
//        			outputImage.write(((ObjectByteIterator)userProfile.get("pic")).toArray());
//        			outputImage.close();
        		}
        	}
        } catch(Exception e) {
        	System.out.println("Could not open file to store image");
        	retVal = -1;
        }

        return retVal;
	}

	@Override
	public int listFriends(int requesterID, int profileOwnerID,
			Set<String> fields,
			Vector<HashMap<String, ByteIterator>> result, boolean insertImage,
			boolean testMode) {

		if(requesterID < 0 || profileOwnerID < 0)
			return -1 ;

		int confirmedFriends = 0 ;

		try {
			EJDBQueryBuilder qb = new EJDBQueryBuilder();
			qb.field("userid1", String.valueOf(profileOwnerID));
			qb.field("status", EJDBClientProperties.FRIEND_CONFIRMED);

			EJDBResultSet res = friends.createQuery(qb).find() ;
			for(BSONObject friend:res) {
				confirmedFriends+=1;
				int profileID = (int) friend.get("userid2") ;
				qb = new EJDBQueryBuilder();
				qb.field("userid", profileID) ;
				BSONObject userProfile = users.createQuery(qb).findOne();

				for(String key: userProfile.fields()){
					HashMap<String, ByteIterator> profile = new HashMap<String, ByteIterator>();
					if(key.equals("_id"))
						profile.put(key, new ObjectByteIterator(userProfile.getId().toByteArray()));
	        		else
	        			profile.put(key, new ObjectByteIterator(((String)userProfile.get(key)).getBytes())) ;
					result.add(profile);
				}
			}
//			confirmedFriends = friends.createQuery(qb).count();

			qb = new EJDBQueryBuilder();
			qb.field("userid2", String.valueOf(profileOwnerID));
			qb.field("status", EJDBClientProperties.FRIEND_CONFIRMED);

			res = friends.createQuery(qb).find();
			for(BSONObject friend:res) {
				confirmedFriends+=1;

				int profileID = (int) friend.get("userid1") ;
				qb = new EJDBQueryBuilder();
				qb.field("userid", profileID) ;
				BSONObject userProfile = users.createQuery(qb).findOne();

				for(String key: userProfile.fields()){
					HashMap<String, ByteIterator> profile = new HashMap<String, ByteIterator>();
					if(key.equals("_id"))
						profile.put(key, new ObjectByteIterator(userProfile.getId().toByteArray()));
	        		else
	        			profile.put(key, new ObjectByteIterator(((String)userProfile.get(key)).getBytes())) ;
					result.add(profile);
				}

			}
		}
		catch (EJDBException e) {
			return -1 ;
		}

		return confirmedFriends ;
	}

	@Override
	public int viewFriendReq(int profileOwnerID, Vector<HashMap<String,
			ByteIterator>> results, boolean insertImage,
			boolean testMode) {


		if(profileOwnerID < 0)
			return -1 ;

		int pendingFriends = 0 ;

		try {
			EJDBQueryBuilder qb = new EJDBQueryBuilder();
			qb.field("userid1", String.valueOf(profileOwnerID));
			qb.field("status", EJDBClientProperties.FRIEND_PENDING);

			EJDBResultSet res = friends.createQuery(qb).find();

			for(BSONObject friend:res) {
				pendingFriends++ ;

				int profileID = (int) friend.get("userid2") ;
				qb = new EJDBQueryBuilder();
				qb.field("userid", profileID) ;
				BSONObject userProfile = users.createQuery(qb).findOne();

				for(String key: userProfile.fields()){
					HashMap<String, ByteIterator> profile = new HashMap<String, ByteIterator>();
					if(key.equals("_id"))
						profile.put(key, new ObjectByteIterator(userProfile.getId().toByteArray()));
	        		else
	        			profile.put(key, new ObjectByteIterator(((String)userProfile.get(key)).getBytes())) ;
					results.add(profile);
				}
			}

			qb = new EJDBQueryBuilder();
			qb.field("userid2", String.valueOf(profileOwnerID));
			qb.field("status", EJDBClientProperties.FRIEND_PENDING);

			res = friends.createQuery(qb).find();

			for(BSONObject friend:res) {
				pendingFriends++ ;

				int profileID = (int) friend.get("userid1") ;
				qb = new EJDBQueryBuilder();
				qb.field("userid", profileID) ;
				BSONObject userProfile = users.createQuery(qb).findOne();

				for(String key: userProfile.fields()){
					HashMap<String, ByteIterator> profile = new HashMap<String, ByteIterator>();
					if(key.equals("_id"))
						profile.put(key, new ObjectByteIterator(userProfile.getId().toByteArray()));
	        		else
	        			profile.put(key, new ObjectByteIterator(((String)userProfile.get(key)).getBytes())) ;
					results.add(profile);
				}
			}
		}
		catch (EJDBException e) {
			return -1 ;
		}

		return pendingFriends ;
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
        	friends.commitTransaction();
//        	friends.save(newFriendRec);
        }
        return 0;

	}

	@Override
	public int rejectFriend(int inviterID, int inviteeID) {
		//remove entry from friends collection

		if(inviterID < 0 || inviteeID < 0) {
			return  -1;
		}

		EJDBQueryBuilder qb = new EJDBQueryBuilder();
		qb.field("userid1", inviterID);
		qb.field("userid2", inviteeID);
		qb.field("status", EJDBClientProperties.FRIEND_PENDING) ;

		BSONObject res = friends.createQuery(qb).findOne();
		ObjectId recordID = res.getId() ;
		friends.remove(recordID);
		friends.commitTransaction();
		return 0;
	}

	@Override
	public int inviteFriend(int inviterID, int inviteeID) {
		BSONObject newFriendReq = new BSONObject("userid1", String.valueOf(inviterID))
				.append("userid2", String.valueOf(inviteeID))
				.append("status", EJDBClientProperties.FRIEND_PENDING);

		EJDBCollection friends = ejdb.getCollection(EJDBClientProperties.DB_COLLECTION_FRIENDS);
		friends.save(newFriendReq);
		friends.commitTransaction();
		return 0;
	}

	@Override
	public int viewTopKResources(int requesterID, int profileOwnerID, int k,
			Vector<HashMap<String, ByteIterator>> result) {
		// TODO Auto-generated method stub

		if(requesterID < 0 || profileOwnerID < 0) {
			return -1;
		}

		EJDBQueryBuilder qb = new EJDBQueryBuilder() ;
		qb.field("walluserid", profileOwnerID);

		EJDBResultSet res = resources.createQuery(qb).find();
		if (res == null || res.length() < k){
			return -1 ;
		}
		else {
			int i=0;
			for(BSONObject resource: res) {
				if(i==k)
					break;
				HashMap<String, ByteIterator> obj = new HashMap<String, ByteIterator>() ;
				obj.put("rid", new ObjectByteIterator(Integer.toString((int)resource.get("rid")).getBytes())) ;
				obj.put("walluserid", new ObjectByteIterator(Integer.toString((int)resource.get("walluserid")).getBytes()));
				obj.put("creatorid", new ObjectByteIterator(Integer.toString((int)resource.get("creatorid")).getBytes()));
				result.add(obj);
				i++;
			}
		}

		return 0;
	}

	@Override
	public int getCreatedResources(int creatorID, Vector<HashMap<String,
			ByteIterator>> result) {
		if(creatorID < 0) {
			return -1;
		}

		EJDBQueryBuilder qb = new EJDBQueryBuilder() ;
		qb.field("creatorID", creatorID);

		EJDBResultSet res = resources.createQuery(qb).find();
		if (res == null ){
			return -1 ;
		}
		else {
			for(BSONObject resource: res) {
				HashMap<String, ByteIterator> obj = new HashMap<String, ByteIterator>() ;
				obj.put("rid", new ObjectByteIterator(Integer.toString((int)resource.get("rid")).getBytes())) ;
				obj.put("creatorid", new ObjectByteIterator(Integer.toString((int)resource.get("creatorid")).getBytes()));
				result.add(obj);
			}
		}
		return 0;
	}

	@Override
	public int viewCommentOnResource(int requesterID, int profileOwnerID,
			int resourceID,
			Vector<HashMap<String, ByteIterator>> result) {

		if(requesterID < 0 || profileOwnerID < 0 || resourceID < 0) {
			return -1 ;
		}

		EJDBQueryBuilder qb = new EJDBQueryBuilder() ;
		qb.field("rid", new ObjectByteIterator(Integer.toString(resourceID).getBytes()));

		EJDBResultSet res = manipulations.createQuery(qb).find();
//		Iterator<BSONObject> iter = res.iterator() ;
		for(BSONObject comment:res) {
			HashMap<String, ByteIterator> obj = new HashMap<String, ByteIterator>() ;
			for(String key: comment.fields()) {
				obj.put(key, (ObjectByteIterator)comment.get(key));
			}
			result.add(obj);
		}
		return 0;
	}

	@Override
	public int postCommentOnResource(int commentCreatorID, int resourceCreatorID,
			int resourceID,
			HashMap<String, ByteIterator> values) {

		if(commentCreatorID < 0 || resourceCreatorID < 0 || resourceID < 0) {
			return -1 ;
		}

		BSONObject comment = new BSONObject();
		comment.append("mid", values.get("mid"));
		comment.append("creatorid", new ObjectByteIterator(Integer
				.toString(resourceCreatorID).getBytes()));
		comment.append("rid", new ObjectByteIterator(Integer
				.toString(resourceID).getBytes())) ;
		comment.append("modifierid", new ObjectByteIterator(Integer
				.toString(commentCreatorID).getBytes()));
		comment.append("timestamp", values.get("timestamp"));
		comment.append("type", values.get("type"));
		comment.append("content", values.get("content"));

		manipulations.save(comment);
		manipulations.commitTransaction();

		return 0;
	}

	@Override
	public int delCommentOnResource(int resourceCreatorID, int resourceID,
			int manipulationID) {
		// TODO Auto-generated method stub

		EJDBQueryBuilder qb = new EJDBQueryBuilder() ;
		qb.field("creatorid",  new ObjectByteIterator(Integer
				.toString(resourceCreatorID).getBytes()));
		qb.field("rid", new ObjectByteIterator(Integer
				.toString(resourceID).getBytes()));
		qb.field("mid", new ObjectByteIterator(Integer
				.toString(manipulationID).getBytes()));

		BSONObject res = manipulations.createQuery(qb).findOne();
		if(res!=null) {
			ObjectId recordID = res.getId() ;
			manipulations.remove(recordID);
			manipulations.commitTransaction();
		}


		return 0;
	}

	@Override
	public int thawFriendship(int friendid1, int friendid2) {

		if(friendid1 < 0 || friendid2 < 0) {
			return  -1;
		}

		EJDBQueryBuilder qb = new EJDBQueryBuilder();
		qb.field("userid1", friendid1);
		qb.field("userid2", friendid2);
		qb.field("status", EJDBClientProperties.FRIEND_CONFIRMED) ;

		BSONObject res = friends.createQuery(qb).findOne();
		if(res == null) {
			qb = new EJDBQueryBuilder() ;
			qb.field("userid1", friendid2);
			qb.field("userid2", friendid1);
			qb.field("status", EJDBClientProperties.FRIEND_CONFIRMED) ;
			res = friends.createQuery(qb).findOne();
		}
		if(res == null) {
			//no friendship exists
			return -1 ;
		}

		ObjectId recordID = res.getId() ;
		friends.remove(recordID);
		friends.commitTransaction();
		return 0;

	}

	@Override
	public HashMap<String, String> getInitialStats() {

		System.out.println("GET INITIAL STATS");
		HashMap<String, String> stats = new HashMap<String, String>();

		//usercount
//		EJDBCollection users = ejdb.getCollection(EJDBClientProperties.DB_COLLECTION_USERS);
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

//		EJDBCollection friends = ejdb.getCollection(EJDBClientProperties.DB_COLLECTION_FRIENDS);
		ObjectId recordID = friends.save(friendRecord) ;
		friends.commitTransaction();

		return 0 ;
	}

	@Override
	public void createSchema(Properties props) {

		ejdb.dropCollection(EJDBClientProperties.DB_COLLECTION_USERS, true);
		ejdb.dropCollection(EJDBClientProperties.DB_COLLECTION_RESOURCES, true);
		ejdb.dropCollection(EJDBClientProperties.DB_COLLECTION_MANIPULATIONS, true);
		ejdb.dropCollection(EJDBClientProperties.DB_COLLECTION_FRIENDS, true);
		ejdb.dropCollection(EJDBClientProperties.DB_COLLECTION_IMAGES, true);
		ejdb.dropCollection(EJDBClientProperties.DB_COLLECTION_THUMBNAILS, true);
		System.out.println("Creating Schema - dropping collections if present and recreating");
		EJDBCollection.Options collectionOpt = new EJDBCollection.Options(false,true,65535,0) ;
		 users = ejdb.ensureCollection(EJDBClientProperties.DB_COLLECTION_USERS, collectionOpt) ;
		 resources = ejdb.ensureCollection(EJDBClientProperties.DB_COLLECTION_RESOURCES, collectionOpt);
		 manipulations = ejdb.ensureCollection(EJDBClientProperties.DB_COLLECTION_MANIPULATIONS, collectionOpt) ;
		 friends = ejdb.ensureCollection(EJDBClientProperties.DB_COLLECTION_FRIENDS, collectionOpt) ;
		 images = ejdb.ensureCollection(EJDBClientProperties.DB_COLLECTION_IMAGES, collectionOpt);
		 thumbnails = ejdb.ensureCollection(EJDBClientProperties.DB_COLLECTION_THUMBNAILS, collectionOpt);

		 //create images and thumbnails directories -
		 File imagePath = new File(EJDBClientProperties.IMAGE_PATH);
		 File thumbnailPath = new File(EJDBClientProperties.THUMBNAIL_PATH);

		 imagePath.mkdir();
		 thumbnailPath.mkdir() ;

	}

	@Override
	public int queryPendingFriendshipIds(int memberID, Vector<Integer> pendingIds) {
		// TODO Auto-generated method stub

		if(memberID < 0) {
			return -1;
		}
		EJDBQueryBuilder qb = new EJDBQueryBuilder();
		qb.field("userid2", memberID);
		qb.field("status", EJDBClientProperties.FRIEND_PENDING) ;

		EJDBResultSet resultSet = friends.createQuery(qb).find() ;
		for(BSONObject friend:resultSet) {
			pendingIds.add((int)friend.get("userid1"));
		}

		return 0;
	}

	@Override
	public int queryConfirmedFriendshipIds(int memberID, Vector<Integer> confirmedIds) {
		// TODO Auto-generated method stub

		if(memberID < 0) {
			return -1;
		}
		EJDBQueryBuilder qb = new EJDBQueryBuilder();
		qb.field("userid2", memberID);
		qb.field("status", EJDBClientProperties.FRIEND_CONFIRMED) ;

		EJDBResultSet resultSet = friends.createQuery(qb).find() ;
		for(BSONObject friend:resultSet) {
			confirmedIds.add((int)friend.get("userid1"));
		}

		qb = new EJDBQueryBuilder();
		qb.field("userid1", memberID);
		qb.field("status", EJDBClientProperties.FRIEND_CONFIRMED) ;

		resultSet = friends.createQuery(qb).find() ;
		for(BSONObject friend:resultSet) {
			confirmedIds.add((int)friend.get("userid2"));
		}

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

	public static byte[] serialize(Object obj) throws IOException {
	    ByteArrayOutputStream out = new ByteArrayOutputStream();
	    ObjectOutputStream os = new ObjectOutputStream(out);
	    os.writeObject(obj);
	    return out.toByteArray();
	}
	public static Object deserialize(byte[] data) throws IOException, ClassNotFoundException {
	    ByteArrayInputStream in = new ByteArrayInputStream(data);
	    ObjectInputStream is = new ObjectInputStream(in);
	    return is.readObject();
	}
}
