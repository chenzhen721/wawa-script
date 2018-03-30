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
import groovy.json.JsonSlurper
import org.apache.commons.lang.StringUtils
import redis.clients.jedis.Jedis


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

    static final String jedis_host = getProperties("main_jedis_host", "192.168.31.246")
    static final Integer main_jedis_port = getProperties("main_jedis_port",6379) as Integer
    static mainRedis = new Jedis(jedis_host, main_jedis_port, 50000)

    static mongo  = new Mongo(new MongoURI(getProperties('mongo.uri','mongodb://127.0.0.1:27017/?w=1') as String))
    /*static final String jedis_host = getProperties("main_jedis_host", "192.168.31.236")
    static final Integer main_jedis_port = getProperties("main_jedis_port", 6379) as Integer*/
//    static redis = new Jedis(jedis_host, main_jedis_port)

    static DAY_MILLON = 24 * 3600 * 1000L
    static long zeroMill = new Date().clearTime().getTime()
    static Long yesTday = zeroMill - DAY_MILLON

    static DBCollection stat_channels = mongo.getDB('xy_admin').getCollection('stat_channels')

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
        def xy_users = mongo.getDB('xy_user').getCollection('users')
        def finance_log_DB = mongo.getDB('xy_admin').getCollection('finance_log')
        def apply_post_logs = mongo.getDB('xylog').getCollection('apply_post_logs')
        def catch_success_logs = mongo.getDB('xylog').getCollection('catch_success_logs')
        def diamond_cost_logs = mongo.getDB("xylog").getCollection("diamond_cost_logs")
        def diamond_add_logs = mongo.getDB("xylog").getCollection("diamond_add_logs")
        def day_login = mongo.getDB("xylog").getCollection("day_login")
        def impression_logs = mongo.getDB("xylog").getCollection("impression_logs")
        def stat_daily = mongo.getDB('xy_admin').getCollection('stat_daily')
        def goods = mongo.getDB('xy_admin').getCollection('goods')
        def stat_regpay = mongo.getDB('xy_admin').getCollection('stat_regpay')
        def category = mongo.getDB('xy_admin').getCollection('category')
        //def sdf = new SimpleDateFormat('yyyyMMdd')

        /*StringBuffer stringBuffer = new StringBuffer()
        users.find($$(qd: 'quanmincai')).toArray().each {BasicDBObject obj ->
            def _id = obj.get('_id')
            def tuid = obj.get('tuid') as String ?: ''
            stringBuffer.append(_id).append(',').append(tuid).append(System.lineSeparator())
        }
        println stringBuffer.toString()*/

        /*def file = new File('/empty/crontab/wawaid.txt')
        def ids = new HashMap()
        file.readLines().each {String line ->
            if (line != null && line.trim() != '') {
                ids.put(Integer.parseInt(line), 0)
            }
        }
        println ids
        //查询这些id的发货数量
        apply_post_logs.find($$(is_delete: false, post_type: [$in: [2, 3]])).toArray().each {BasicDBObject obj ->
            def toys = obj['toys'] as Set
            toys.each {BasicDBObject toy ->
                if (toy['_id'] != null && ids.containsKey(toy['_id'])) {
                    Integer count = ids.get(toy['_id']) as Integer
                    count = count + 1
                    ids.put(toy['_id'], count)
                }
            }
        }
        println ids*/

        def json = new JsonSlurper()
        /*def file = new File('C:\\Users\\Administrator\\Desktop\\server_src\\db\\xy_user_users.txt')

        Map<String, Integer> map = new HashMap<String, Integer>();*/

        /* 读取数据 */
        /*try {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF-8"));
            String lineTxt = null;
            while ((lineTxt = br.readLine()) != null) {
                xy_users.save($$(json.parseText(lineTxt) as Map))
            }
            br.close();
        } catch (Exception e) {
            System.err.println("read errors :" + e);
        }
        println 'xy_user_users finish.'*/


        def file1 = new File('C:\\Users\\Administrator\\Desktop\\server_src\\db\\xy_users.txt')
        /* 读取数据 */
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file1), "UTF-8"))
            String lineTxt = null;
            while ((lineTxt = br.readLine()) != null) {
                println lineTxt
                users.save($$(json.parseText(lineTxt) as Map))
            }
            br.close();
        } catch (Exception e) {
            System.err.println("read errors :" + e);
        }
        println 'xy_users finish.'
    }

    static getInviteDiamond(def ids, long s, long e) {
        def diamond_logs = mongo.getDB('xylog').getCollection('diamond_add_logs')
        Integer diamond = 0 as Integer
        diamond_logs.find($$(type: 'invite_diamond', user_id: [$in: ids], timestamp: [$gte: s, $lt: e])).toArray().each {BasicDBObject obj ->
            diamond = diamond + (obj['award']['diamond'] as Integer ?: 0)
        }
        diamond
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