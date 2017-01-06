#!/usr/bin/env groovy
package crontab.tmp

import com.mongodb.BasicDBObject
import com.mongodb.DBCollection
import com.mongodb.DBObject
@Grapes([
@Grab('org.mongodb:mongo-java-driver:2.14.2'),
@Grab('commons-lang:commons-lang:2.6'),
@Grab('redis.clients:jedis:2.1.0'),
@Grab(group = 'net.sf.json-lib', module = 'json-lib', version = '2.3', classifier = 'jdk15'),
]) import com.mongodb.Mongo
import net.sf.json.JSONObject
import org.apache.commons.lang.StringUtils

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/**
 *
 * 临时测试脚本
 */
class SysEveryHours {

    //static mongo = new Mongo("192.168.31.249", 27017)
    //static mongo = new Mongo("192.168.1.156", 10000)
    static mongo = new Mongo(new com.mongodb.MongoURI('mongodb://192.168.1.36:10000,192.168.1.37:10000,192.168.1.38:10000/?w=1&slaveok=true'))

    static DAY_MILLON = 24 * 3600 * 1000L

    static long zeroMill = new Date().clearTime().getTime()
    static Long yesTday = zeroMill - DAY_MILLON
    static String YMD = new Date(yesTday).format("yyyyMMdd")

    static DBCollection coll = mongo.getDB('xy_admin').getCollection('stat_daily')
    static DBCollection room_cost_DB = mongo.getDB("xylog").getCollection("room_cost")
    static DBCollection finance_log_DB = mongo.getDB('xy_admin').getCollection('finance_log')

    private static final MEDAL_ID = 233
    private static final Integer MAX_COINS = 500000
    static payStaticsAward(){
        println timestamp: [$gte: yesTday, $lte: zeroMill]
        finance_log_DB.aggregate(
                new BasicDBObject('$match', ['via': [$ne: 'Admin'], timestamp: [$gte: yesTday, $lte: zeroMill]]),
                new BasicDBObject('$project', [_id: '$user_id', coin: '$coin']),
                new BasicDBObject('$group', [_id: '$_id', coin_sum: [$sum: '$coin']]),
                new BasicDBObject('$match', new BasicDBObject('coin_sum', [$gte: 500000]))
        ).results().each {
            def obj = new BasicDBObject(it as Map)
            def uid = obj['_id'] as Integer
            println uid
        }
    }

    static void main(String[] args) { //待优化，可以到历史表查询记录
        long l = System.currentTimeMillis()
        //01.送礼日报表
        long begin = l

        //16.用户每日充值5000RMB奖励徽章
        l = System.currentTimeMillis()
        payStaticsAward()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   payStaticsAward, cost  ${System.currentTimeMillis() - l} ms"

        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${SysEveryHours.class.getSimpleName()}, cost  ${System.currentTimeMillis() - begin} ms"
    }


}

