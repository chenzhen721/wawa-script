#!/usr/bin/env groovy
package tmp.tmp

import com.mongodb.BasicDBObject
import com.mongodb.DBCollection
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
        @Grab('commons-codec:commons-codec:1.6')
])
import com.mongodb.Mongo
import com.mongodb.MongoURI

/**
 * 房间发言数统计
 */
class Tmp1 {
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

    static mongo  = new Mongo(new MongoURI(getProperties('mongo.uri','mongodb://192.168.2.27:10000/?w=1') as String))
    /*static final String jedis_host = getProperties("main_jedis_host", "192.168.31.236")
    static final Integer main_jedis_port = getProperties("main_jedis_port", 6379) as Integer*/
//    static redis = new Jedis(jedis_host, main_jedis_port)

    static DAY_MILLON = 24 * 3600 * 1000L
    static long zeroMill = new Date().clearTime().getTime()
    static Long yesTday = zeroMill - DAY_MILLON

    static statics(int i) {

        def catch_record = mongo.getDB('xy_catch').getCollection('catch_record')

        def gteMill = yesTday - i * DAY_MILLON
        def ltMill = gteMill + DAY_MILLON
        def YMD = new Date(gteMill).format('yyyyMMdd')
        //设置超时、概率
        def catch_room = mongo.getDB('xy_catch').getCollection('catch_room')
        def catch_toy = mongo.getDB('xy_catch').getCollection('catch_toy')
        //补充线上中奖id
        def catch_user = mongo.getDB('xy_catch').getCollection('catch_user')
        def users = mongo.getDB('xy').getCollection('users')
        def finance_log_DB = mongo.getDB('xy_admin').getCollection('finance_log')
        def apply_post_logs = mongo.getDB('xylog').getCollection('apply_post_logs')
        def catch_success_logs = mongo.getDB('xylog').getCollection('catch_success_logs')
        def diamond_cost_logs = mongo.getDB("xylog").getCollection("diamond_cost_logs")
        def diamond_add_logs = mongo.getDB("xylog").getCollection("diamond_add_logs")



    }

    public static final String APP_ID = "984069e5f8edd8ca4411e81863371f16"

    static Integer day = 0

    static void main(String[] args) {
        long l = System.currentTimeMillis()
        statics(0 )
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${this.getSimpleName()},statics cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

    }


    static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    static BasicDBObject $$(Map map) {
        return new BasicDBObject(map)
    }

}