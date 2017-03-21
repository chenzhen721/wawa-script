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
class DiamondStat {
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
    static DBCollection diamond_stat = mongo.getDB('xy_admin').getCollection('diamond_stat')
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
        def inc_total = 0
        def desc_total = 0
        def total = 0
        def query = $$('timestamp': getTimeBetween())
        def cursors = diamond_logs.find(query).batchSize(5000)
        while (cursors.hasNext()) {
            def obj = cursors.next()
            def cost = obj['cost'] as Long
            if (cost > 0) {
                inc_total += 1
            } else {
                desc_total += 1
            }
            total += 1
        }
        def curr_date = new Date(yesTday - day * DAY_MILLON)
        def myId = "diamond_stat_" + curr_date.format("yyyyMMdd")

        def row = $$('_id': myId, 'inc_total': inc_total, 'desc_total': desc_total, 'total': total, 'timestamp': curr_date.getTime())
        diamond_stat.save($$(row))
    }

    final static Integer day = 0;

    static void main(String[] args) {
        long l = System.currentTimeMillis()
        long begin = l

        statics_diamond()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DiamondStat.class.getSimpleName()},statics_diamond cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        jobFinish(begin);
    }

    /**
     * 标记任务完成  用于运维监控
     * @return
     */
    private static jobFinish(Long begin) {
        def timerName = 'DiamondStat'
        Long totalCost = System.currentTimeMillis() - begin
        saveTimerLogs(timerName, totalCost)
        println "${new Date().format('yyyy-MM-dd')}:${DiamondStat.class.getSimpleName()}:finish  cost time:  ${System.currentTimeMillis() - begin} ms"
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