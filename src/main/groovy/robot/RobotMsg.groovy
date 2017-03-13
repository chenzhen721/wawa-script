#!/usr/bin/env groovy

import com.mongodb.MongoURI
import org.apache.commons.lang.StringUtils
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0')
])
import redis.clients.jedis.Jedis

import com.mongodb.Mongo
import com.mongodb.BasicDBObject
import com.mongodb.DB
import com.mongodb.DBCollection
import com.mongodb.DBObject

/**
 * 导入聊天消息
 */
class RobotMsg {

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

    static mongo = new Mongo(new MongoURI(getProperties('mongo.uri', 'mongodb://192.168.31.246:27017/?w=1') as String))
    static final String user_jedis_host = getProperties("user_jedis_host", "192.168.31.236")

    static final Integer user_jedis_port = getProperties("user_jedis_port", 6380) as Integer
    static userRedis = new Jedis(user_jedis_host, user_jedis_port)

    static String filePath = "/empty/crontab/robot_msg.txt"

    //注册机器人
    static importMsg(){
        String key = 'chat:msg:list'
        userRedis.del(key)
        new File(filePath).splitEachLine(',') { row ->
            row.each {String msg ->
                println msg
                if(StringUtils.isNotBlank(msg))
                    userRedis.sadd(key, msg)
            }
        }

    }

    static void main(String[] args) {
        long l = System.currentTimeMillis()
        importMsg()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   importMsg, cost  ${System.currentTimeMillis() - l} ms"
    }
}