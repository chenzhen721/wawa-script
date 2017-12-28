#!/usr/bin/env groovy
package tmp.tmp

import com.mongodb.BasicDBObject
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
        @Grab('commons-codec:commons-codec:1.6')
])
import com.mongodb.Mongo
import com.mongodb.MongoURI
import groovy.json.JsonSlurper
import org.apache.commons.codec.digest.DigestUtils

import javax.persistence.Basic
import java.text.SimpleDateFormat

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
        def catch_success_log = mongo.getDB('xylog').getCollection('catch_success_logs')
        def apply_post_log = mongo.getDB('xylog').getCollection('apply_post_logs')
        //获取goods_id
        /*def file = new File('/empty/crontab/goodsid.txt')
        def ids = new HashMap()
        file.readLines().each {String line ->
            if (!line.isEmpty()) {
                def a = line.split(' ')
                ids.put(Integer.parseInt(a[1]), Integer.parseInt(a[0])) //toyid  goodsId
            }
        }
        println ids
        ids.each {Integer key, Integer value->
            def records = catch_record.find($$('toy._id': key, goods_id: [$ne: value], type: 2, timestamp: [$gte: 1513324800000])).toArray()
            println records.size()
            records.each {BasicDBObject obj->
                catch_record.update($$(_id: obj['_id']), $$($set: [goods_id: value]), false, false)
                //println obj['_id'] + '|' + obj['goods_id']
            }

            def log = catch_success_log.find($$('toy._id': key, goods_id: [$ne: value], timestamp: [$gte: 1513324800000])).toArray()
            println log.size()
            log.each {BasicDBObject obj->
                def s = obj['_id'] + '|' + obj['goods_id']
                catch_success_log.update($$(_id: obj['_id']), $$($set: [goods_id: value]), false, false)
                println s
            }
        }

        def post_log = apply_post_log.find($$(timestamp: [$gte: 1513324800000], is_delete: [$ne: true])).toArray()
        println post_log.size()
        post_log.each { BasicDBObject obj->
            def toys = obj['toys'] as List<BasicDBObject>
            def set = new BasicDBObject()
            def s = ''
            for(int index = 0; index < toys.size(); index++) {
                def toy = toys.get(index)
                def record_id = toy['record_id'] as String
                def goods_id = toy['goods_id'] as Integer
                if (goods_id == null) {
                    continue
                }
                def succ = catch_success_log.findOne($$(_id: record_id))
                //商品id不一致，修改，给出提示
                if (succ['goods_id'] != goods_id) {
                    set.put("toys.${index}.goods_id".toString(), succ['goods_id'])
                    s = s + '|' + toy['record_id'] + '|' + index
                }
            }

            if (!s.isEmpty()) {
                println obj['_id'] + '|' + obj['order_id'] + s
            }
            if (set.size() <= 0) {
                println apply_post_log.update($$(_id: obj['_id']), $$($set: set), false, false)
                return
            }
        }*/

        //设置goods_id
        //设置成功记录

        def catch_user = mongo.getDB('xy_catch').getCollection('catch_user')

        def channels = mongo.getDB('xy_admin').getCollection('channels')

        //统计渠道抓取人数和次数

        //已申请邮寄订单补充 goods_id

        //def channels = mongo.getDB('xy_admin').getCollection('channels')

        def users = mongo.getDB('xy').getCollection('users')
        def finance_log_DB = mongo.getDB('xy_admin').getCollection('finance_log')


        //需要付费的
        //apply_post_log.update(new BasicDBObject(need_postage: [$exists: false]), $$($set: [need_postage: false, channel: 0]))

        /*def uids = finance_log_DB.distinct('user_id', $$(qd: 'wawa_kuailai_gzh', via: [$ne: 'Admin'], timestamp: [$gte: 1511452800000, $lt:1512489600000]))
        //days = days + (((time - start) / DAY_MILLON) as Double).toInteger() + 1
        def map = [:]
        uids.each { Integer uid ->
            def user = users.findOne($$(_id: uid))
            def start = user['timestamp'] as Long
            def fin = finance_log_DB.find($$(user_id: uid, via: [$ne: 'Admin'], timestamp: [$gte: 1511452800000, $lt: 1512489600000])).sort($$(timestamp: -1)).toArray()
            println fin
            def end = fin[0]['timestamp'] as Long
            def days = (((end - start) / DAY_MILLON) as Double).toInteger() + 1
            map.put(uid, [days: days, count: fin.size()])
        }
        //所有用户的天数和次数
        def total_days = 0
        def total_count = 0
        def total_avg = 0 as Double
        for(Integer key : map.keySet()) {
            def val = map.get(key) as Map
            def days = val['days'] as Integer
            def count = val['count'] as Integer
            total_days = total_days + days
            total_count = total_count + count
            total_avg = total_avg + days / count
        }
        println total_days
        println total_count
        println total_avg / map.size()*/


        def uids = users.distinct('_id', $$(qd: 'tashequ'))
        def timestamp = [$gte: gteMill, $lt: ltMill]
        println uids.size()
        println users.count($$(qd: 'tashequ', timestamp: timestamp))
        println catch_record.count($$(user_id: [$in: uids], timestamp: timestamp, is_delete: [$ne: true]))
        println catch_record.count($$(user_id: [$in: uids], timestamp: timestamp, is_delete: [$ne: true], status: true))

        println catch_record.distinct('user_id', $$(user_id: [$in: uids], timestamp: timestamp, is_delete: [$ne: true])).size()
        println catch_record.distinct('user_id', $$(user_id: [$in: uids], timestamp: timestamp, is_delete: [$ne: true], status: true))

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