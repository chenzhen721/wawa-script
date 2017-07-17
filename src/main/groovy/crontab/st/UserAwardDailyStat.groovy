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
 * 用户奖励统计
 */
class UserAwardDailyStat {
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
    static DBCollection award_logs = mongo.getDB('xylog').getCollection('user_award_logs')

    //写log
    static DBCollection award_daily_logs = mongo.getDB('xy_admin').getCollection('stat_award_daily')

    static DAY_MILLON = 24 * 3600 * 1000L
    static long zeroMill = new Date().clearTime().getTime()
    static Long yesTday = zeroMill - DAY_MILLON
    static BEGIN = '$gte'
    private static Map getTimeBetween() {
        def gteMill = yesTday - day * DAY_MILLON
        return [$gte: gteMill, $lt: gteMill + DAY_MILLON]
    }

    /**
     * 翻牌统计：open_card
     * 寻宝（挖矿）统计: family_event
     * 道具使用统计：use_item
     */
    static void statics_award() {
        def timeBetween = getTimeBetween()
        def YMD = new Date(timeBetween.get(BEGIN)).format("yyyyMMdd")
        ['open_card', 'family_event', 'use_item'].collect().each { String type ->
            def _id = "${YMD}_${type}".toString()
            def query = new BasicDBObject(["type": type, "timestamp": getTimeBetween()])
            def count = award_logs.count(query) as Number
            award_logs.aggregate([
                    new BasicDBObject('$match', query),
                    new BasicDBObject('$project', [user_id: '$user_id', defense: '$award.defense', steal: '$award.steal', exp: '$award.exp', coin: '$award.coin', diamond: '$award.diamond', cash: '$award.cash', attack: '$award.attack']),
                    new BasicDBObject('$group', [_id: null, ids: [$addToSet: '$user_id'], defense: [$sum: '$defense'], steal: [$sum: '$steal'], exp: [$sum: '$exp'], coin: [$sum: '$coin'], diamond: [$sum: '$diamond'], cash: [$sum: '$cash'], attack: [$sum: '$attack']]) //top N 算法
            ]).results().each { row ->
                def ids = row.removeField("ids") as List
                row.put("user_count", ids.size())
                row.put("total_count", count)
                row.put("_id", _id)
                row.put("type", type)
                row.put("timestamp", timeBetween.get(BEGIN))
                award_daily_logs.update($$('_id', _id), row, true, false)
            }
        }
    }

    /**
     * 家族贡献领奖：family_award
     */
    static void statics_family_award() {
        def timeBetween = getTimeBetween()
        def YMD = new Date(timeBetween.get(BEGIN)).format("yyyyMMdd")
        def _id = YMD + "_family_award"
        def query = $$([type: 'family_award', "timestamp": getTimeBetween()])
        def count = award_logs.count(query) as Number
        award_logs.aggregate([
                new BasicDBObject('$match', query),
                new BasicDBObject('$project', [user_id: '$user_id', award: '$award']),
                new BasicDBObject('$group', [_id: null, ids: [$addToSet: '$user_id'], diamond: [$sum: '$award']]) //top N 算法
        ]).results().each { row ->
            def ids = row.removeField("ids") as List
            row.put("user_count", ids.size())
            row.put("total_count", count)
            row.put("_id", _id)
            row.put("type", "family_award")
            row.put("timestamp", timeBetween.get(BEGIN))
            award_daily_logs.update($$('_id', _id), row, true, false)
        }
    }

    final static Integer day = 0;

    static void main(String[] args) {
        long l = System.currentTimeMillis()
        long begin = l
        statics_award()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${UserAwardDailyStat.class.getSimpleName()},statics_award cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        l = System.currentTimeMillis()
        statics_family_award()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${UserAwardDailyStat.class.getSimpleName()},statics_family_award cost  ${System.currentTimeMillis() - l} ms"
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
        println "${new Date().format('yyyy-MM-dd')}:${UserAwardDailyStat.class.getSimpleName()}:finish  cost time:  ${System.currentTimeMillis() - begin} ms"
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