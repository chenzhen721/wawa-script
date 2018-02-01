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
class DomainReplace {

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
    static xy_user = mongo.getDB('xy_user').getCollection('users')
    static channels = mongo.getDB('xy_admin').getCollection('channels')
    static stat_daily = mongo.getDB('xy_admin').getCollection('stat_daily')
    static long zeroMill = new Date().clearTime().getTime()
    static Long yesTday = zeroMill - DAY_MILLON


    static String replace_from = "aiimg.sumeme.";
    static String replace_to = "img.lezhuale.";

    static replaceImg(String db, String colName){
        try{
            def col = mongo.getDB(db).getCollection(colName)
            col.find().toArray().each {DBObject obj ->
                Boolean flag = Boolean.FALSE
                def update = new BasicDBObject();
                obj.keySet().each {String key ->
                    def val = obj.get(key)
                    if(val.toString().contains(replace_from)){
                        flag = Boolean.TRUE
                        String newval = val.toString().replace(replace_from, replace_to)
                        newval = newval.toString().replace(replace_from, replace_to)
                        update.append(key, newval)
                        println "${key} : ${val} new: ${newval}"
                    }
                }
                if(flag){
                    col.update(obj, new BasicDBObject('$set',update))
                }

            }
        }catch (Exception e){
            println "replaceImg Exception :" + e
        }

    }

    static userBatchReplace(){
        DBCursor cur = users.find(new BasicDBObject(), new BasicDBObject('_id':1,'pic':1)).batchSize(100000);
        while (cur.hasNext()){
            DBObject obj = cur.next()
            String pic = obj?.get('pic') as String
            Integer _id = obj.get('_id') as Integer
            if(pic.toString().contains("img.2339.") || pic.toString().contains(".2339.") ){
                String newval = pic.toString().replace(replace_from, replace_to)
                users.update(new BasicDBObject('_id',_id), new BasicDBObject('$set',new BasicDBObject('pic',newval)))
            }
        }
        cur.close();
    }

    static void main(String[] args){
        def l = System.currentTimeMillis()
/*        List dbs = ['xyrank']
        dbs.each {String db ->
          Set<String> collections = mongo.getDB(db).getCollectionNames()
          collections.each {String col ->
              replaceImg(db,col)
          }
       }*/
       replaceImg('xy_catch','catch_room')
       replaceImg('xy_catch','catch_toy')
       replaceImg('xy_admin','shop')
       replaceImg('xy_admin','posters')
       replaceImg('xy_admin','category')

           //userReplace();
        println "DomainReplace cost  ${System.currentTimeMillis() -l} ms".toString()
    }

}
