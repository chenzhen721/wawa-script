#!/usr/bin/env groovy
package tmp.test
import com.mongodb.BasicDBObject
@GrabResolver(name = 'restlet', root = 'http://192.168.31.253:8081/nexus/content/groups/public')
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
])
import com.mongodb.Mongo
import com.mongodb.MongoURI
import groovy.json.JsonSlurper
import redis.clients.jedis.Jedis

/**
 * 家族机器人
 */

class FamilyRobot{
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
    static final String chat_jedis_host = getProperties("chat_jedis_host", "192.168.31.246")
    static final String live_jedis_host = getProperties("live_jedis_host", "192.168.31.246")
    static final String user_jedis_host = getProperties("user_jedis_host", "192.168.31.246")

    static final Integer main_jedis_port = getProperties("main_jedis_port",6379) as Integer
    static final Integer chat_jedis_port = getProperties("chat_jedis_port",6379) as Integer
    static final Integer live_jedis_port = getProperties("live_jedis_port",6379) as Integer
    static final Integer user_jedis_port = getProperties("user_jedis_port",6379) as Integer

    static mainRedis = new Jedis(jedis_host, main_jedis_port, 50000)
    static liveRedis = new Jedis(live_jedis_host, live_jedis_port, 50000)
    static userRedis = new Jedis(user_jedis_host, user_jedis_port, 50000)

    final static String KEY = "wJfSNrsVM1HoU5zx8PY2OzFzr3N8dIyt"
    static users = mongo.getDB("xy").getCollection("users");
    static rooms = mongo.getDB("xy").getCollection("rooms");

    static void main(String[] args) {
        Long cur = System.currentTimeMillis()
        println "yy  static >>>>>>>>>>>>>"
        59.times {
            staticYY();
            Thread.sleep(1000l);
        }
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')} ".toString()
    }

    static String redisRoomKey = "yy:room:live"
    static String redisKey = "${new Date().format('yyyy-MM-dd')}:yy:total".toString()
    static void staticYY(){
        String responseContent = new URL("http://data.3g.yy.com/play/assemble/crane").getText("utf-8")
        def jsonSlurper = new JsonSlurper()
        Map result = jsonSlurper.parseText(responseContent) as Map
        def modules = (result.get("data") as Map).get("modules") as List
        def data =  (modules[0] as Map).get('data') as List
        Map<String, String> counts = mainRedis.hgetAll(redisRoomKey)
        //println counts
        println "mechine size : ${counts.size()}";
        data.each {
            Integer linkMic = it["linkMic"] as Integer
            String uid = it["uid"] as String
            if(linkMic.equals(1)){
                Integer count = (counts.get(uid) ?: 0)as Integer
                if(count == 0){//如果1秒钟之前为0则为新上用户
                    mainRedis.incr(redisKey)
                }
            }
            mainRedis.hset(redisRoomKey, uid.toString(), linkMic.toString())
        }

        println "${new Date().format('yyyy-MM-dd HH:mm')} total : " + mainRedis.get(redisKey)
        println "${new Date().format('yyyy-MM-dd HH:mm')} total : " + mainRedis.get(redisKey)
    }

    public static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    public static BasicDBObject $$(Map map) {
        return new BasicDBObject(map);
    }
}



