#!/usr/bin/env groovy

@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
])
import com.mongodb.Mongo
import com.mongodb.MongoURI
import com.mongodb.BasicDBObject

import java.util.concurrent.BrokenBarrierException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

/**
 * MongoCreateIndex
 */

class MongoCreateIndex{
    static Properties props = null;
    static String profilepath = "/empty/crontab/db.properties";

    static getProperties(String key, Object defaultValue) {
        try {
            if (props == null) {
                props = new Properties();
                props.load(new FileInputStream(profilepath));
            }
        } catch (Exception e) {
            println e;
        }
        return props.get(key, defaultValue)
    }

    static mongo  = new Mongo(new MongoURI(getProperties('mongo.uri','mongodb://192.168.31.249:27017/?w=1') as String))
    static locations = mongo.getDB('test').getCollection('locations')
    static void main(String[] args) {
        //createIndex()
        //println "distance: 1000 : " + locations.find(new BasicDBObject('coordinates': [$nearSphere: [121.510294d, 31.238914d], $maxDistance: 0.0001]))
        createIndex();
    }

    static create2DIndex(){
        mongo.getDB('test').getCollection('locations').createIndex(new BasicDBObject(coordinates : "2dsphere"))
        println mongo.getDB('test').getCollection('locations').getIndexInfo()
    }

    static createIndex(){
        /*
        mongo.getDB("xy_user").getCollection("users")
                .ensureIndex(new BasicDBObject(userName: 1), new BasicDBObject(unique:true,dropDups: true,  sparse: true))
        mongo.getDB("xy_user").getCollection("users")
                .ensureIndex(new BasicDBObject(mobile: 1), new BasicDBObject(unique:true,dropDups: true,sparse: true))
        mongo.getDB("xy_user").getCollection("users")
                .ensureIndex(new BasicDBObject(weixin_openid: 1), new BasicDBObject(unique:true,dropDups: true,sparse: true))
        mongo.getDB("xy_user").getCollection("users")
                .ensureIndex(new BasicDBObject(sina_uid: 1), new BasicDBObject(unique:true,dropDups: true,sparse: true))
        mongo.getDB("xy_user").getCollection("users")
                .ensureIndex(new BasicDBObject(token: 1), new BasicDBObject(unique:true,dropDups: true))
        mongo.getDB("xy_user").getCollection("users")
                .ensureIndex(new BasicDBObject(mm_no: 1), new BasicDBObject(unique:true,dropDups: true,sparse: true))

        mongo.getDB("xy_user").getCollection("users")
                .ensureIndex(new BasicDBObject(userName: 1, mobile: 1, qq_openid: 1, sina_uid: 1))


        mongo.getDB("xy_user").getCollection("users")
                .ensureIndex(new BasicDBObject(weixin_unionid: 1), new BasicDBObject(unique:true,dropDups: true,sparse: true))
        */
        mongo.getDB("xy_user").getCollection("users")
                .createIndex(new BasicDBObject(qq_unionid: 1), new BasicDBObject(unique:true,dropDups:false,sparse: true))
    }
}




