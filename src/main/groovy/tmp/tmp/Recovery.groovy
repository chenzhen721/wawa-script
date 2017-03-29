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
    static DBCollection channel_pay_DB = mongo.getDB('xy_admin').getCollection('channel_pay')
    static DBCollection finance_log_DB = mongo.getDB('xy_admin').getCollection('finance_log')

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
     * 按支付渠道统计
     */
    static payStatics_recovery(int day) {
        for (int i = 0; i < day; i++) {
            payStatics(i)
        }
    }
    /**
     * 运营总表recovery
     */
    static staticTotalReport_recovery(int day) {
        for (int i = 0; i < day; i++) {
            staticTotalReport(i)
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
        def android_recharge_set = new HashSet()
        def ios_recharge_set = new HashSet()
        def other_recharge_set = new HashSet()
        list.each { obj ->
            def cny = obj.containsField('cny') ? obj['cny'] as Double : 0.0d
            def payType = pays[obj.via]
            payType.count.incrementAndGet()
            payType.user.add(obj.user_id)
            def userId = obj['user_id'] as Integer
            def user = users.findOne($$('_id': userId), $$('qd': 1))
            if (user == null) {
                return
            }
            def qd = user.containsField('qd') ? user['qd'] : 'aiwan_default'
            // client = 2 android 4 ios
            def channel = channels.findOne($$('_id': qd), $$('client': 1))
            def client = channel.containsField('client') ? channel['client'] as Integer : 2
            def via = obj.containsField('via') ? obj['via']: ''
            if (via != 'Admin') {
                // 统计android和ios的充值人数，去重，如果是admin加币，则不用统计
                if (client == 2) {
                    android_recharge_set.add(userId)
                } else if (client == 4) {
                    ios_recharge_set.add(userId)
                } else {
                    other_recharge_set.add(userId)
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
                ios_recharge_count: ios_recharge_set.size(),
                android_recharge_count: android_recharge_set.size(),
                other_recharge_count: other_recharge_set.size(),
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

    /**
     * 充值统计(充值方式划分)
     * @param i
     * @return
     */
    static payStatics(int i) {
        def gteMill = yesTday - i * DAY_MILLON
        def YMD = new Date(gteMill).format("yyyyMMdd")
        def time = [$gte: gteMill, $lt: gteMill + DAY_MILLON]

        Map<String, Number> old_ids = new HashMap<String, Number>()
        finance_log_DB.aggregate(
                new BasicDBObject('$match', new BasicDBObject('via', [$ne: 'Admin'])),
                new BasicDBObject('$project', [_id: '$user_id', timestamp: '$timestamp']),
                new BasicDBObject('$group', [_id: '$_id', timestamp: [$min: '$timestamp']])
        ).results().each {
            def obj = $$(it as Map)
            old_ids.put(obj.get('_id') as String, obj.get('timestamp') as Number)
        }
        def typeMap = new HashMap<String, PayStat>()
        PayStat total = new PayStat()
        def pc = channel_pay_DB.find($$([client: "1", _id: [$ne: 'Admin']])).toArray()*._id
        def mobile = channel_pay_DB.find($$([client: ['$ne': "1"], _id: [$ne: 'Admin']])).toArray()*._id
        [pc    : pc,
         mobile: mobile,
        ].each { String k, List<String> v ->
            PayStat all = new PayStat()
            PayStat delta = new PayStat()
            def cursor = finance_log_DB.find($$([timestamp: time, via: [$in: v.toArray()]]),
                    $$(user_id: 1, cny: 1, coin: 1, timestamp: 1)).batchSize(50000)
            while (cursor.hasNext()) {
                def obj = cursor.next()
                def user_id = obj['user_id'] as String
                def cny = new BigDecimal(((Number) obj.get('cny')).doubleValue())
                def coin = obj.get('coin') as Long
                all.add(user_id, cny, coin)
                total.add(user_id, cny, coin)
                //该用户之前无充值记录或首冲记录为当天则算为当天新增用户
                if (old_ids.containsKey(user_id)) {
                    def userTimestamp = old_ids.get(user_id) as Long
                    Long day = gteMill
                    Long userday = new Date(userTimestamp).clearTime().getTime()
                    if (day.equals(userday)) {
                        delta.add(user_id, cny, coin)
                    }
                }
            }
            typeMap.put(k + 'all', all)
            typeMap.put(k + 'delta', delta)
        }
        coll.update(new BasicDBObject(_id: YMD + '_allpay'),
                new BasicDBObject(type: 'allpay',
                        user_pay: total.toMap(),
                        user_pay_pc: typeMap.get('pcall').toMap(),
                        user_pay_pc_delta: typeMap.get('pcdelta').toMap(),
                        user_pay_mobile: typeMap.get('mobileall').toMap(),
                        user_pay_mobile_delta: typeMap.get('mobiledelta').toMap(),
                        timestamp: gteMill
                ), true, false)
    }

    /**
     * 统计运营数据总表
     * @param i
     * @return
     */
    static staticTotalReport(int i) {
        long l = System.currentTimeMillis()
        def gteMill = yesTday - i * DAY_MILLON
        def date = new Date(gteMill)//
        def prefix = date.format('yyyyMMdd_')
        //运营统计报表
        def stat_report = mongo.getDB('xy_admin').getCollection('stat_report')
        // 查询充值信息
        def pay = coll.findOne(new BasicDBObject(_id: "${prefix}allpay".toString()))
        // 查询充值柠檬
        def pay_coin = coll.findOne(new BasicDBObject(_id: "${prefix}finance".toString()))
        // 查询注册人数
        def regs = users.count(new BasicDBObject(timestamp: [$gte: gteMill, $lt: gteMill + DAY_MILLON]))
        // 查询消费信息
        def cost = coll.findOne(new BasicDBObject(_id: "${prefix}allcost".toString()))
        def map = new HashMap()
        map.put('type', 'allreport')
        map.put('timestamp', gteMill)
        map.put('pay_coin', (pay_coin?.get('total_coin') ?: 0) as Integer)
        if (pay != null) {
            def user_pay = pay.get('user_pay') as BasicDBObject
            map.put('pay_cny', (user_pay.get('cny') ?: 0) as Double)
            map.put('pay_user', (user_pay.get('user') ?: 0) as Integer)
        }
        map.put('regs', regs)
        if (cost != null) {
            def user_cost = cost.get('user_cost') as BasicDBObject
            def coin = user_cost.get('cost') as Double
            def cost_cny = new BigDecimal(coin / 100).toDouble()
            map.put('cost_cny', cost_cny)
            map.put('cost_user', (user_cost.get('user') ?: 0) as Integer)
        }
        stat_report.update(new BasicDBObject(_id: "${prefix}allreport".toString()), new BasicDBObject(map), true, false)
    }

    static int day = 1

    static void main(String[] args) {
        long l = System.currentTimeMillis()
        long begin = l

        // 充值统计recovery
        pay_statics_recovery(day)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   pay_statics_recovery, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)


        // 钻石出入统计
        l = System.currentTimeMillis()
        diamond_statics_recovery(day)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   diamond_statics_recovery, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)


        // 支付渠道统计充值
        l = System.currentTimeMillis()
        payStatics_recovery(day)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   pay_statics_recovery, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        // 钻石出入统计
        l = System.currentTimeMillis()
        staticTotalReport_recovery(day)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticTotalReport_recovery, cost  ${System.currentTimeMillis() - l} ms"
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
        final  user = new HashSet(1000)
        final  count = new AtomicInteger()
        final  coin = new AtomicLong()
        def cny = new BigDecimal(0)

        def toMap() { [user: user.size(), count: count.get(), coin: coin.get(), cny: cny.doubleValue()] }
    }

    static class PayStat {
        final Set user = new HashSet(2000)
        final AtomicInteger count = new AtomicInteger()
        final AtomicLong coin = new AtomicLong()
        def BigDecimal cny = new BigDecimal(0)

        def toMap() { [user: user.size(), count: count.get(), coin: coin.get(), cny: cny.doubleValue()] }

        def add(def user_id, BigDecimal deltaCny, Long deltaCoin) {
            count.incrementAndGet()
            user.add(user_id)
            cny = cny.add(deltaCny)
            coin.addAndGet(deltaCoin)
        }

        def add(def user_id, BigDecimal deltaCny, Long deltaCoin, Integer deltaCount) {
            count.addAndGet(deltaCount)
            user.add(user_id)
            cny = cny.add(deltaCny)
            coin.addAndGet(deltaCoin)
        }
    }
}