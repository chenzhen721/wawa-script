#!/usr/bin/env groovy
package crontab.tmp

import com.mongodb.BasicDBObject
import com.mongodb.DBCursor
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
        @Grab(group = 'net.sf.json-lib', module = 'json-lib', version = '2.3', classifier = 'jdk15')
]) import com.mongodb.Mongo
import com.mongodb.MongoURI
import org.bson.types.BSONTimestamp
import redis.clients.jedis.Jedis

import java.text.SimpleDateFormat
import com.mongodb.DBCollection
import com.mongodb.DBObject

/**
 *
 * 数据库日志查询
 * 临时文件
 *
 */
class Oplog{

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

    static final String jedis_host = getProperties("main_jedis_host", "192.168.31.249")
    static final String chat_jedis_host = getProperties("chat_jedis_host", "192.168.31.249")
    static final String live_jedis_host = getProperties("live_jedis_host", "192.168.31.249")

    static final Integer main_jedis_port = getProperties("main_jedis_port",6379) as Integer
    static final Integer chat_jedis_port = getProperties("chat_jedis_port",6379) as Integer
    static final Integer live_jedis_port = getProperties("live_jedis_port",6379) as Integer

    static mongo  = new Mongo(new MongoURI(getProperties('mongo.uri','mongodb://192.168.31.249:27017/?w=1') as String))
    static historyMongo = new Mongo(new MongoURI(getProperties('mongo_history.uri', 'mongodb://192.168.31.246:27017/?w=1') as String))
    static historyDB = historyMongo.getDB('xylog_history')
    static mainRedis = new Jedis(jedis_host,main_jedis_port)


    static query(){
        String outFilePath = "/empty/crontab/oplog.txt" ;
        FileWriter fileWriter = new FileWriter(outFilePath);
        PrintWriter printWriter = new PrintWriter(fileWriter);
        def query = new BasicDBObject(op: 'u')
        query.append('ns', 'xy.users')
        query.append('o2._id', 25801345)
        Iterator logs =  mongo.getDB('local').getCollection('oplog.rs').find(query).batchSize(100).iterator()
        while (logs.hasNext()){
            DBObject log = logs.next();
            def ts = log.get('ts') as BSONTimestamp
            def o = log.get('o')
            def o2 = log.get('o2')
            String str = "${new Date((ts.getTime() * 1000L) as Long).format("yyyy-MM-dd HH:mm:ss SSS")}, ${o}, ${o2}"
            if( !str.contains("finance")){
                println str
            }
            if(str.contains("charge_award_bag") )
                println str;
        }
        /**
        fileWriter.close();
        printWriter.close();*/
    }

    static void main(String[] args)
    {
        long l = System.currentTimeMillis()
        query()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${Oplog.class.getSimpleName()}  cost  ${System.currentTimeMillis() -l} ms"

    }

}