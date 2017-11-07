#!/usr/bin/env groovy

import com.mongodb.BasicDBObject
import com.mongodb.*
import com.mongodb.DBCollection
import com.mongodb.DBObject
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
        @Grab('org.apache.httpcomponents:httpclient:4.2.5')
])
import com.mongodb.Mongo
import com.mongodb.MongoURI
import com.mongodb.util.Hash
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import org.apache.commons.lang.StringUtils
import org.apache.http.client.HttpClient
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.params.HttpConnectionParams
import org.apache.http.params.HttpParams
import redis.clients.jedis.Jedis
import redis.clients.jedis.Pipeline
import redis.clients.jedis.Response


/**
 * 数据初始化
 */
class DataInit {

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

    static final String jedis_host = getProperties("main_jedis_host", "192.168.31.236")
    static final String chat_jedis_host = getProperties("chat_jedis_host", "192.168.31.236")
    static final String live_jedis_host = getProperties("live_jedis_host", "192.168.31.236")
    static final String user_jedis_host = getProperties("user_jedis_host", "192.168.31.236")

    static final Integer main_jedis_port = getProperties("main_jedis_port", 6379) as Integer
    static final Integer chat_jedis_port = getProperties("chat_jedis_port", 6379) as Integer
    static final Integer live_jedis_port = getProperties("live_jedis_port", 6379) as Integer
    static final Integer user_jedis_port = getProperties("user_jedis_port", 6380) as Integer

    static redis = new Jedis(jedis_host, main_jedis_port)
    static chatRedis = new Jedis(chat_jedis_host, chat_jedis_port)
    static userRedis = new Jedis(user_jedis_host, user_jedis_port)
    static liveRedis = new Jedis(live_jedis_host, live_jedis_port)

    static mongo = new Mongo(new MongoURI(getProperties('mongo.uri', 'mongodb://192.168.31.246:27017/?w=1') as String))
    static DB xy = mongo.getDB("xy")
    static DB xy_user = mongo.getDB("xy_user")
    static DB xy_admin = mongo.getDB("xy_admin")
    static DB xylog  = mongo.getDB("xylog")
    static DB game_log  = mongo.getDB("game_log")
    static DB xy_family  = mongo.getDB("xy_family")
    static DB xy_friend  = mongo.getDB("xy_friend")
    static DB xyrank  = mongo.getDB("xyrank")

    static initData(){
        //清除用户信息
        xy.getCollection("users").remove($$(via:[$ne:'robot']))
        xy_user.getCollection("users").remove($$(via:[$ne:'robot']))
        xy.getCollection("rooms").remove($$(_id:[$ne:null]))
        //admin 信息
        /*xy_admin.getCollection("finance_log").remove($$(_id:[$ne:null]))
        cleanAllCollections(xy_family)
        cleanAllCollections(xy_friend)
        cleanAllCollections(game_log)
        cleanAllCollections(xylog)
        cleanAllCollections(xyrank)*/

    }

    static initRedis(){
        redis.flushAll()
        liveRedis.flushAll();
        userRedis.flushAll();
    }

    //清楚此库下所有集合
    static cleanAllCollections(DB db){
        println db.getCollectionNames()
        db.getCollectionNames().each {String col ->
            db.getCollection(col).remove($$(_id:[$ne:null]))
        }
    }

    public static BasicDBObject $$(Map map) {
        return new BasicDBObject(map)
    }

    static void main(String[] args) {
        long l = System.currentTimeMillis()
        initData()
        //initRedis();
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   initData, cost  ${System.currentTimeMillis() - l} ms"
    }
}