#!/usr/bin/env groovy
package tmp

@GrabResolver(name = 'restlet', root = 'http://210.22.151.242:8081/nexus/content/groups/public')
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
        @Grab('com.ttpod:https-util:1.0'),
])
import com.mongodb.*
import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import org.apache.commons.lang.StringUtils
import org.apache.commons.lang.math.NumberUtils
import redis.clients.jedis.Jedis
import com.https.HttpsUtil
import java.text.SimpleDateFormat


import groovy.json.JsonSlurper
import org.apache.commons.lang.StringUtils
import java.security.MessageDigest
import com.https.HttpsUtil
import com.mongodb.BasicDBObject
import com.mongodb.DBObject
import com.mongodb.Mongo
import com.mongodb.MongoURI
import groovy.json.JsonBuilder
class tmp {

    static Properties props = null;
    static String profilepath="/empty/crontab/db.properties";

    static getProperties(String key, Object defaultValue){
        try {
            if(props == null){
                props = new Properties();
                props.load(new FileInputStream(profilepath));
            }
        } catch (Exception e) {
            println e;
        }
        return props.get(key, defaultValue)
    }

    static final String jedis_host = getProperties("main_jedis_host", "192.168.31.246")
    static final String chat_jedis_host = getProperties("chat_jedis_host", "192.168.31.246")
    static final String live_jedis_host = getProperties("live_jedis_host", "192.168.31.246")
    static final String user_jedis_host = getProperties("user_jedis_host", "192.168.31.246")

    static final Integer main_jedis_port = getProperties("main_jedis_port",6379) as Integer
    static final Integer chat_jedis_port = getProperties("chat_jedis_port",6379) as Integer
    static final Integer live_jedis_port = getProperties("live_jedis_port",6379) as Integer
    static final Integer user_jedis_port = getProperties("user_jedis_port",6379) as Integer

    static mongo  = new Mongo(new MongoURI(getProperties('mongo.uri','mongodb://192.168.31.249:27017/?w=1') as String))
    static historyMongo = new Mongo(new MongoURI(getProperties('mongo_history.uri', 'mongodb://192.168.31.246:27017/?w=1') as String))
    static historyDB = historyMongo.getDB('xylog_history')
    static xyHistoryDB = historyMongo.getDB('xy_history')
    static mainRedis = new Jedis(jedis_host,main_jedis_port)
    static userRedis = new Jedis(user_jedis_host,user_jedis_port, 50000)


    def final Long DAY_MILL = 24*3600*1000L
    static DAY_MILLON = 24 * 3600 * 1000L
    static finance_log = mongo.getDB('xy_admin').getCollection('finance_log')
    static users = mongo.getDB('xy').getCollection('users')
    static rooms = mongo.getDB('xy').getCollection('rooms')
    static day_login = mongo.getDB('xylog').getCollection('day_login')
    static lottery_logs = mongo.getDB('xylog').getCollection('lottery_logs')

    static DBCollection room_cost_DB =  mongo.getDB("xylog").getCollection("room_cost")
    static DBCollection order_logs =  mongo.getDB("xylog").getCollection("order_logs")
    static DBCollection room_feather =  mongo.getDB("xylog").getCollection("room_feather")
    static DBCollection week_stars =  mongo.getDB("xyrank").getCollection("week_stars")
    static channels = mongo.getDB('xy_admin').getCollection('channels')

    static long zeroMill = new Date().clearTime().getTime()
    static Long yesTday = zeroMill - DAY_MILLON

    public static BasicDBObject $$(String key,Object value){
        return new BasicDBObject(key,value);
    }

    public static BasicDBObject $$(Map map){
        return new BasicDBObject(map);
    }

    static void recoverFinanceLogToId(){
        def finance_log = mongo.getDB('xy_admin').getCollection('finance_log')
        finance_log.setReadPreference(ReadPreference.secondary())
        def list = finance_log.find($$(via:'weixin_m')) .toArray()
        println "size : ${list.size()}";
        list.each { BasicDBObject obj ->
            def feeType = obj.get('feeType') as String
            def transactionId = obj.get('transactionId') as String
            obj.put('feeType', transactionId)
            obj.put('transactionId', feeType)
            finance_log.save(obj)
        }
    }

    static void main(String[] args){
        def l = System.currentTimeMillis()

        recoverFinanceLogToId();

        println " cost  ${System.currentTimeMillis() -l} ms".toString()
    }

}