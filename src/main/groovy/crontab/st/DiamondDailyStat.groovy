#!/usr/bin/env groovy
package crontab.st

import com.mongodb.BasicDBObject
import com.mongodb.DBCollection
import com.mongodb.DBObject
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
])
import com.mongodb.Mongo
import com.mongodb.MongoURI

/**
 * 关于钻石的统计
 */
class DiamondDailyStat {
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
    static DBCollection diamond_logs = mongo.getDB('xy_admin').getCollection('diamond_logs')
    static DBCollection diamond_cost_logs = mongo.getDB('xy_admin').getCollection('diamond_cost_logs')
    static DBCollection diamond_dailyReport_stat = mongo.getDB('xy_admin').getCollection('diamond_dailyReport_stat')
    static DAY_MILLON = 24 * 3600 * 1000L
    static long zeroMill = new Date().clearTime().getTime()
    static Long yesTday = zeroMill - DAY_MILLON

    private static Map getTimeBetween() {
        def gteMill = yesTday - day * DAY_MILLON
        return [$gte: gteMill, $lt: gteMill + DAY_MILLON]
    }

    /**
     * 统计钻石的入账，出账，总账
     */
    private static void statics_diamond() {
        //本日期初结余=昨日期末结余
        def begin_surplus = lastDaySurplus(yesTday)
        def inc_total = 0
        def desc_total = 0
        def inc_detail = new HashMap<String, Long>()
        def desc_detail = new HashMap<String, Long>()
        def query = $$('timestamp': getTimeBetween())
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
        def curr_date = new Date(yesTday - day * DAY_MILLON)
        def myId = curr_date.format("yyyyMMdd") + "_diamond_dailyReport_stat"

        def row = $$('_id': myId, 'inc_total': inc_total, 'desc_total': desc_total, 'total': total, 'timestamp': curr_date.getTime(),
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
        def last_day = diamond_dailyReport_stat.findOne($$(_id: "${ymd}_diamond_daily_stat".toString()))
        return (last_day?.get('end_surplus') ?: 0) as Long;
    }

    final static Integer day = 0;

    static void main(String[] args) {
        long l = System.currentTimeMillis()
        long begin = l

        statics_diamond()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DiamondDailyStat.class.getSimpleName()},statics_diamond cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        jobFinish(begin);
    }

    /**
     * 标记任务完成  用于运维监控
     * @return
     */
    private static jobFinish(Long begin) {
        def timerName = 'DiamondDailyStat'
        Long totalCost = System.currentTimeMillis() - begin
        saveTimerLogs(timerName, totalCost)
        println "${new Date().format('yyyy-MM-dd')}:${DiamondDailyStat.class.getSimpleName()}:finish  cost time:  ${System.currentTimeMillis() - begin} ms"
    }

    //落地定时执行的日志
    private static saveTimerLogs(String timerName, Long totalCost) {
        def timerLogsDB = mongo.getDB("xyrank").getCollection("timer_logs")
        def tmp = System.currentTimeMillis()
        def id = timerName + "_" + new Date().format("yyyyMMdd")
        def update = new BasicDBObject(timer_name: timerName, cost_total: totalCost, cat: 'day', unit: 'ms', timestamp: tmp)
        timerLogsDB.findAndModify(new BasicDBObject('_id', id), null, null, false, new BasicDBObject('$set', update), true, true)
    }

    protected static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    protected static BasicDBObject $$(Map map) {
        return new BasicDBObject(map)
    }

}