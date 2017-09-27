#!/usr/bin/env groovy

@GrabResolver(name = 'restlet', root = 'http://192.168.31.253:8081/nexus/content/groups/public')
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
        @Grab('com.ttpod:https-util:1.0'),
])
import com.mongodb.Mongo
import com.mongodb.MongoURI
import com.mongodb.BasicDBObject
import groovy.json.JsonBuilder
import groovy.json.JsonOutput

import java.security.MessageDigest
import groovy.json.JsonSlurper
import org.apache.commons.lang.math.RandomUtils
import redis.clients.jedis.Jedis
import com.https.HttpsUtil
import com.mongodb.DBObject

/**
 * 抓取数据
 */

class GrabHttpsData{
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

    static mongo  = new Mongo(new MongoURI(getProperties('mongo.uri','mongodb://192.168.31.246:27017/?w=1') as String))
    static api_url  = getProperties('api.domain','http://localhost:8080/')

    static final String jedis_host = getProperties("main_jedis_host", "192.168.31.246")

    static final Integer main_jedis_port = getProperties("main_jedis_port",6379) as Integer

    static mainRedis = new Jedis(jedis_host, main_jedis_port, 50000)

    static grab_data_log = mongo.getDB('xylog').getCollection('grab_data_log')

    static def jsonSlurper = new JsonSlurper()
    static String uuid = "889f0675-54ce-4b3f-9118-cc873d802663"
    static String userId = "233419"
    static void main(String[] args) {
        Long cur = System.currentTimeMillis()
/*
        def roomlist = 'https://service.wawa.rgbvr.com/wawaServer/rest/roomList/parentRoomList'
        def queryRoomList = new JsonBuilder()
        queryRoomList.setContent([
                page: '1',
                pageSize: '50',
                userId:   userId,
                uuid:   uuid
        ])
        println jsonSlurper.parseText(HttpsUtil.postMethod(roomlist, queryRoomList.toString()))

        staticHuanLe('113');
        */
        Map<String, String> counts = mainRedis.hgetAll(redisRoomKey)
        String q119 = getQueryString('119')
        String q113 = getQueryString('113')
        String q115 = getQueryString('115')
        String q105 = getQueryString('105')
        60.times {
            staticHuanLe(counts,'119', q119);
            staticHuanLe(counts,'113',q113);
            staticHuanLe(counts,'115',q115);
            staticHuanLe(counts,'105',q105);
            Thread.sleep(900l);
        }
        counts.each {String roomId, String uid ->
            mainRedis.hset(redisRoomKey, roomId, uid.toString())
        }
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')} ".toString()
    }

    static String redisRoomKey = "huanle:room:live"
    static def roomLastUserUrl = 'https://service.wawa.rgbvr.com/wawaServer/rest/room/enter/check'
    static void staticHuanLe(Map<String, String> counts, String roomId, String query){
        try{
            def response =  jsonSlurper.parseText(HttpsUtil.postMethod(roomLastUserUrl, query)) as Map
            def room = (response.get("data") as Map).get("roomInfo") as Map
            Integer inUsingUserId = room.get("inUsingUserId") as Integer
            Integer lastUid = (counts.get(roomId) ?: 0)as Integer
            if(inUsingUserId > 0 && lastUid != inUsingUserId){
                //记录用户
                Long time = System.currentTimeMillis()
                String _id = "${roomId}_${inUsingUserId}_${time}".toString()
                grab_data_log.insert($$(_id:_id, uid:inUsingUserId,room_id:roomId,timestmap:time, date:new Date(time).format('yyyy-MM-dd HH:mm:ss')))
            }
            counts.put(roomId, inUsingUserId.toString())
            println room
        }catch (Exception e){
            println e
        }
    }

    static String getQueryString(String roomId){
        def builder = new JsonBuilder()
        def queryContent = [
                roomId: roomId,
                userId:   userId,
                uuid:   uuid
        ]
        builder.setContent(queryContent)
        return builder.toString()
    }

    public static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    public static BasicDBObject $$(Map map) {
        return new BasicDBObject(map);
    }
}



