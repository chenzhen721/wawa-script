#!/usr/bin/env groovy
package crontab.st

import com.mongodb.BasicDBObject
import com.mongodb.DBCollection
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
])
import com.mongodb.Mongo
import com.mongodb.MongoURI

/**
 * 用于保存用户在运行时的数据快照
 */
class DailyReportSnapshot {
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
    static DBCollection finance_log = mongo.getDB('xy_admin').getCollection('finance_log') //充值日志
    static DBCollection users = mongo.getDB('xy').getCollection('users')

    //写log
    static DBCollection award_daily_logs = mongo.getDB('xy_admin').getCollection('stat_daily')

    static DAY_MILLON = 24 * 3600 * 1000L
    static long zeroMill = new Date().clearTime().getTime()
    static Long yesTday = zeroMill - DAY_MILLON
    static BEGIN = '$gte'
    private static Map getTimeBetween() {
        def gteMill = yesTday - day * DAY_MILLON
        return [$gte: gteMill, $lt: gteMill + DAY_MILLON]
    }

    static void user_snapshot() {
        def timeBetween = getTimeBetween()
        def YMD = new Date(timeBetween.get(BEGIN)).format("yyyyMMdd")
        def remain = userRemainByAggregate()
        def result = [type: 'allcost', timestamp: timeBetween.get(BEGIN), user_remain: remain]
        award_daily_logs.update($$(_id: "${YMD}_allcost".toString()), $$(result), true, false)
    }

    static userRemainByAggregate() {
        def coin = 0
        def cash = 0
        users.aggregate([
                new BasicDBObject('$match', $$($or: [['finance.diamond_count': [$gt: 0]], ['finance.cash_count': [$gt: 0]]])),
                new BasicDBObject('$project', [coin: '$finance.diamond_count', cash: '$finance.cash_count']),
                new BasicDBObject('$group', [_id: null, coin: [$sum: '$coin'], cash: [$sum: '$cash']])
        ]).results().each { BasicDBObject obj ->
            coin = obj.get('coin') as Long
            cash = obj.get('cash') as Long
        }
        [diamond: coin, cash: cash]
    }

    static Integer day = 0

    static void main(String[] args) {
        long l = System.currentTimeMillis()
        long begin = l
        user_snapshot()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReportSnapshot.class.getSimpleName()},user_snapshot cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        jobFinish(begin)
    }

    protected static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value)
    }

    protected static BasicDBObject $$(Map map) {
        return new BasicDBObject(map)
    }

    /**
     * 标记任务完成  用于运维监控
     * @return
     */
    private static jobFinish(Long begin) {
        def timerName = 'UserAwardDailyStat'
        Long totalCost = System.currentTimeMillis() - begin
        saveTimerLogs(timerName, totalCost)
        println "${new Date().format('yyyy-MM-dd')}:${DailyReportSnapshot.class.getSimpleName()}:finish  cost time:  ${System.currentTimeMillis() - begin} ms"
    }

    //落地定时执行的日志
    private static saveTimerLogs(String timerName, Long totalCost) {
        def timerLogsDB = mongo.getDB("xyrank").getCollection("timer_logs")
        def tmp = System.currentTimeMillis()
        def id = timerName + "_" + new Date().format("yyyyMMdd")
        def update = new BasicDBObject(timer_name: timerName, cost_total: totalCost, cat: 'day', unit: 'ms', timestamp: tmp)
        timerLogsDB.findAndModify(new BasicDBObject('_id', id), null, null, false, new BasicDBObject('$set', update), true, true)
    }
}