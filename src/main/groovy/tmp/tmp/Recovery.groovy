#!/usr/bin/env groovy
package st
/**
 * Author: monkey 
 * Date: 2017/3/23
 */
import com.mongodb.BasicDBObject
import com.mongodb.DBCollection
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
//        @Grab('org.codehaus.groovy:groovy-all:2.1.3'),
])

import com.mongodb.Mongo
import com.mongodb.MongoURI
import org.apache.commons.lang.StringUtils

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/**
 * 每天统计一份数据
 */
class Recovery {

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

    static mongo = new Mongo(new MongoURI(getProperties('mongo.uri', 'mongodb://192.168.31.231:20000,192.168.31.236:20000,192.168.31.231:20001/?w=1&slaveok=true') as String))


    static DAY_MILLON = 24 * 3600 * 1000L

    static long zeroMill = new Date().clearTime().getTime()
    static Long yesTday = zeroMill - DAY_MILLON

    static DBCollection coll = mongo.getDB('xy_admin').getCollection('stat_daily')
    static DBCollection finance_log_DB = mongo.getDB('xy_admin').getCollection('finance_log')
    static DBCollection users = mongo.getDB('xy').getCollection('users')
    static DBCollection channel_pay_DB = mongo.getDB('xy_admin').getCollection('channel_pay')
    static DBCollection channels = mongo.getDB('xy_admin').getCollection('channels')

    /**
     * 充值统计recovery
     */
    static pay_statics_recovery(int day) {
        for (int i = 0; i < day; i++) {
            financeStatics(i)
        }
    }

    /**
     * 充值统计
     * @return
     */
    static financeStatics(int i) {
        def gteMill = yesTday - i * DAY_MILLON
        def YMD = new Date(gteMill).format("yyyyMMdd")
        def time = [$gte: gteMill, $lt: gteMill + DAY_MILLON]

        def list = mongo.getDB('xy_admin').getCollection('finance_log').find(new BasicDBObject(timestamp: time))
                .toArray()

        def total = new BigDecimal(0)
        def totalCoin = new AtomicLong()

        def pays = MapWithDefault.<String, PayType> newInstance(new HashMap()) { new PayType() }
        Double android_recharge = 0d
        Double ios_recharge = 0d
        Double other_recharge = 0d
        Integer android_recharge_count = 0
        Integer ios_recharge_count = 0
        Integer other_recharge_count = 0
        list.each { obj ->
            def cny = obj.get('cny') as Double
            def payType = pays[obj.via]
            payType.count.incrementAndGet()
            if(payType.user.add(obj.user_id) && cny != null){
                // 新增统计android和ios充值情况
                def userId = obj['user_id'] as Integer
                def user = users.findOne($$('_id': userId), $$('qd': 1))
                if(user == null){
                    return
                }
                def qd = user.containsField('qd') ? user['qd'] : 'aiwan_default'
                def channel = channels.findOne($$('_id': qd), $$('client': 1))
                def client = channel.containsField('client') ? channel['client'] as Integer : 2
                // client = 2 android 4 ios
                if (client == 2) {
                    android_recharge += cny
                    android_recharge_count += 1
                } else if (client == 4) {
                    ios_recharge += cny
                    ios_recharge_count += 1
                    other_recharge_count += 1
                } else {
                    other_recharge += cny
                }
            }
            if (cny != null) {
                cny = new BigDecimal(cny)
                total = total.add(cny)
                payType.cny = payType.cny.add(cny)
            }
            def coin = obj.get('coin') as Long
            if (coin) {
                totalCoin.addAndGet(coin)
                payType.coin.addAndGet(coin)
            }
        }

        def obj = new BasicDBObject(
                _id: "${YMD}_finance".toString(),
                total: total.doubleValue(),
                total_coin: totalCoin,
                type: 'finance',
                android_recharge: android_recharge,
                ios_recharge: ios_recharge,
                other_recharge: other_recharge,
                ios_recharge_count:ios_recharge_count,
                android_recharge_count:android_recharge_count,
                other_recharge_count:other_recharge_count,
                timestamp: gteMill
        )
        pays.each { String key, PayType type -> obj.put(StringUtils.isBlank(key) ? '' : key.toLowerCase(), type.toMap()) }

        coll.save(obj)

    }

    static int day = 30
    static void main(String[] args) {
        // 充值统计recovery
        long l = System.currentTimeMillis()
//        //01.送礼日报表
        long begin = l
        pay_statics_recovery(day)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   pay_statics_recovery, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //落地定时执行的日志
        jobFinish(begin)
    }
    /**
     * 标记任务完成  用于运维监控
     * @return
     */
    private static jobFinish(Long begin) {
        def timerName = 'Recovery'
        Long totalCost = System.currentTimeMillis() - begin
        saveTimerLogs(timerName, totalCost)
        println "${new Date().format('yyyy-MM-dd')}:${Recovery.class.getSimpleName()}:finish  cost  ${System.currentTimeMillis() - begin} ms"
    }

    //落地定时执行的日志
    private static saveTimerLogs(String timerName, Long totalCost) {
        def timerLogsDB = mongo.getDB("xyrank").getCollection("timer_logs")
        def tmp = System.currentTimeMillis()
        def id = timerName + "_" + new Date().format("yyyyMMdd")
        def update = new BasicDBObject(timer_name: timerName, cost_total: totalCost, cat: 'day', unit: 'ms', timestamp: tmp)
        timerLogsDB.findAndModify(new BasicDBObject('_id', id), null, null, false, new BasicDBObject('$set', update), true, true)
    }

    private static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    private static BasicDBObject $$(Map map) {
        return new BasicDBObject(map)
    }

    static class PayType {
        final user = new HashSet(1000)
        final count = new AtomicInteger()
        final coin = new AtomicLong()
        def cny = new BigDecimal(0)

        def toMap() { [user: user.size(), count: count.get(), coin: coin.get(), cny: cny.doubleValue()] }
    }
}