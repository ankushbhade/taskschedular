package com.qbi.db;

import java.util.Hashtable;
import java.util.Set;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.WriteResult;
import com.mongodb.util.JSON;
import com.qbi.common.dataclasses.Criteria;
import java.util.ArrayList;
import javax.management.Query;

public class DBUtil {

    static private DB dbConnection;
    static ArrayList<DBObject> docList;
    private static final Logger logger = LoggerFactory.getLogger(DBUtil.class);

    static {
        try {
            MongoClient mongoClient = new MongoClient("localhost", 3001);
            dbConnection = mongoClient.getDB("meteor");
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    public static DBObject queryByID(String collectionName, String dbID) {

        BasicDBList orList = new BasicDBList();
        ObjectId objectId = null;

        try {

            objectId = new ObjectId(dbID);

        } catch (Exception ex) {

            ex.printStackTrace();
        }

        orList.add(new BasicDBObject("_id", dbID));

        if (objectId != null) {
            orList.add(new BasicDBObject("_id", objectId));
        }

        BasicDBObject query = new BasicDBObject("$or", orList);

        DBCollection collectionObj = dbConnection.getCollection(collectionName);

        DBObject doc = collectionObj.findOne(query);

        return doc;
    }

    public static void insertAsJson(String collectionName, String data) {
        DBObject dbObject = null;
        DBCollection collectionObj = dbConnection.getCollection(collectionName);

        dbObject = (DBObject) JSON.parse(data);
        collectionObj.insert(dbObject);

        return;
    }

    public static void insert(String collectionName, BasicDBObjectBuilder dbObjectBuilder) {
        DBObject dbObject = null;
        DBCollection collectionObj = dbConnection.getCollection(collectionName);

        collectionObj.insert(dbObjectBuilder.get());

        return;
    }

    public static DBCursor queryByKey(String collectionName, String key, String value) {

        BasicDBObject query = new BasicDBObject(key, value);

        DBCollection collectionObj = dbConnection.getCollection(collectionName);
        DBCursor cursor = collectionObj.find(query);

        return cursor;
    }

    public static DBCursor queryByKeyValue(String collectionName, Hashtable<String, String> keyValueHash) {

        BasicDBObject query = new BasicDBObject();

        DBCollection collectionObj = dbConnection.getCollection(collectionName);

        Set<String> keys = keyValueHash.keySet();

        for (String key : keys) {
            query.append(key, keyValueHash.get(key));

        }

        DBCursor cursor = collectionObj.find(query);

        return cursor;
    }

    public static void removeByKeyValue(String collectionName, Hashtable<String, String> keyValueHash) {

        BasicDBObject query = new BasicDBObject();

        DBCollection collectionObj = dbConnection.getCollection(collectionName);

        Set<String> keys = keyValueHash.keySet();

        for (String key : keys) {
            query.append(key, keyValueHash.get(key));

        }

        collectionObj.remove(query);

        return;
    }

    public static void upsertDB(String collectionName, Hashtable queryHash, DBObject dataObj) {

        BasicDBObject query = new BasicDBObject();
        DBCollection collectionObj = dbConnection.getCollection(collectionName);

        Set<String> keys = queryHash.keySet();

        for (String key : keys) {
            query.append(key, queryHash.get(key));

        }

        BasicDBObject newDoc = new BasicDBObject();
        newDoc.append("$set", dataObj);

        WriteResult objResult = collectionObj.update(query, newDoc, true, false);
        logger.info("Write Resut" + objResult.toString());
    }

    
    
    public static ArrayList<DBObject>
            ScheduleTaskFindByCurrentTimeAndCurrentDay(
                    String collectionName, String currentTime, String currentDay) {

                BasicDBObject query = new BasicDBObject();

                DBObject dailyObject = new BasicDBObject("taskPattern", "Daily");
                DBObject dayObject = new BasicDBObject("taskPattern", currentDay);

                BasicDBList orQuery = new BasicDBList();
                orQuery.add(dailyObject);
                orQuery.add(dayObject);

                DBObject queryDay = new BasicDBObject("$or", orQuery);
                DBObject queryTime = new BasicDBObject("scheduledTime", currentTime);

                BasicDBList andQuery = new BasicDBList();
                andQuery.add(queryDay);
                andQuery.add(queryTime);

                query.put("$and", andQuery);

//                logger.info("final db query = " + query);
                DBCollection collectionObj = dbConnection.getCollection(collectionName);

                DBCursor cursor = collectionObj.find(query);

//                logger.info(cursor.toString());
                docList = new ArrayList<DBObject>();

                while (cursor.hasNext()) {

                    DBObject doc = cursor.next();
                    docList.add(doc);
                }

                return docList;

            }

}
