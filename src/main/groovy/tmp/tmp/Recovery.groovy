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
    static DBCollection users = mongo.getDB('xy').getCollection('users')
    static DBCollection channels = mongo.getDB('xy_admin').getCollection('channels')
    static DBCollection diamond_logs = mongo.getDB('xy_admin').getCollection('diamond_logs')
    static DBCollection diamond_cost_logs = mongo.getDB('xy_admin').getCollection('diamond_cost_logs')
    static DBCollection diamond_dailyReport_stat = mongo.getDB('xy_admin').getCollection('diamond_dailyReport_stat')

    /**
     * 充值统计recovery
     */
    static pay_statics_recovery(int day) {
        for (int i = 0; i < day; i++) {
            financeStatics(i)
        }
    }

    /**
     * 钻石统计recovery
     */
    static diamond_statics_recovery(int day) {
        for (int i = 0; i < day; i++) {
            statics_diamond(i)
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
            def cny = obj.containsField('cny') ? obj['cny'] as Double : 0.0d
            def payType = pays[obj.via]
            payType.count.incrementAndGet()
            def userId = obj['user_id'] as Integer
            def user = users.findOne($$('_id': userId), $$('qd': 1))
            if (user == null) {
                return
            }
            def qd = user.containsField('qd') ? user['qd'] : 'aiwan_default'
            // client = 2 android 4 ios
            def channel = channels.findOne($$('_id': qd), $$('client': 1))
            def client = channel.containsField('client') ? channel['client'] as Integer : 2
            def via = obj.containsField('Admin') ? obj['via'] : ''
            if (payType.user.add(obj.user_id) && via != 'Admin') {
                // 统计android和ios的充值人数，去重，如果是admin加币，则不用统计
                if (client == 2) {
                    android_recharge_count += 1
                } else if (client == 4) {
                    ios_recharge_count += 1
                } else {
                    other_recharge_count += 1
                }
            }

            if (cny != null) {
                cny = new BigDecimal(cny)
                total = total.add(cny)
                payType.cny = payType.cny.add(cny)
                // 统计android和ios的充值金额
                if (client == 2) {
                    android_recharge += cny
                } else if (client == 4) {
                    ios_recharge += cny
                } else {
                    other_recharge += cny
                }
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
                ios_recharge_count: ios_recharge_count,
                android_recharge_count: android_recharge_count,
                other_recharge_count: other_recharge_count,
                timestamp: gteMill
        )
        pays.each { String key, PayType type -> obj.put(StringUtils.isBlank(key) ? '' : key.toLowerCase(), type.toMap()) }

        coll.save(obj)
    }

    /**
     * 统计钻石的入账，出账，总账
     */
    private static void statics_diamond(int i) {
        def gteMill = yesTday - i * DAY_MILLON
        def YMD = new Date(gteMill).format("yyyyMMdd")
        def time = [$gte: gteMill, $lt: gteMill + DAY_MILLON]
        //本日期初结余=昨日期末结余
        def begin_surplus = lastDaySurplus(gteMill)
        def inc_total = 0
        def desc_total = 0
        def inc_detail = new HashMap<String, Long>()
        def desc_detail = new HashMap<String, Long>()
        def query = $$('timestamp': time)
        def diamond_cursors = diamond_logs.find(query).batchSize(5000)
        def diamond_cost_cursors = diamond_cost_logs.find(query).batchSize(5000)

        // 加币统计
        while (diamond_cursors.hasNext()) {
            def obj = diamond_cursors.next()
            def diamond_count = obj['diamond_count'] as Long
            def type = obj['type'] as String
            inc_total += diamond_count
            def current_type_add_diamond = inc_detail.containsKey(type) ? inc_detail[type] : 0L
            current_type_add_diamond += diamond_count
            inc_detail.put(type, current_type_add_diamond)
        }

        // 消费统计
        while (diamond_cost_cursors.hasNext()) {
            def obj = diamond_cost_cursors.next()
            def diamond_count = obj['diamond_count'] as Long
            def type = obj['type'] as String
            desc_total += diamond_count
            def current_type_minus_total = desc_detail.containsKey(type) ? desc_detail[type] : 0L
            current_type_minus_total += diamond_count
            desc_detail.put(type, current_type_minus_total)
        }

        def total = inc_total - desc_total
        def myId = "${YMD}_diamond_dailyReport_stat".toString()

        def row = $$('_id': myId, 'inc_total': inc_total, 'desc_total': desc_total, 'total': total, 'timestamp': gteMill,
                'inc_detail': inc_detail, 'desc_detail': desc_detail, 'begin_surplus': begin_surplus, 'end_surplus': total)
        diamond_dailyReport_stat.save($$(row))
    }

    /**
     * 日期末结余
     * @param begin
     * @return
     */
    static Long lastDaySurplus(Long begin) {
        long yesterDay = begin - DAY_MILLON
        String ymd = new Date(yesterDay).format("yyyyMMdd")
        def last_day = diamond_dailyReport_stat.findOne($$(_id: "${ymd}_diamond_dailyReport_stat".toString()))
        return (last_day?.get('end_surplus') ?: 0) as Long;
    }

    static int day = 30

    static void main(String[] args) {
        long l = System.currentTimeMillis()
        long begin = l

        // 充值统计recovery
        pay_statics_recovery(day)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   pay_statics_recovery, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)


        // 钻石出入统计
        diamond_statics_recovery(day)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   diamond_statics_recovery, cost  ${System.currentTimeMillis() - l} ms"
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