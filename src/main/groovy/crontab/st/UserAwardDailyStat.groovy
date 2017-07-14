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
    static DBCollection card_logs = mongo.getDB('xylog').getCollection('user_award_logs')
    static DBCollection family_event_logs = mongo.getDB('game_log').getCollection('family_event_logs')

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
     * 翻牌统计，type：open_card
     */
    static void statics_open_card() {
        def timeBetween = getTimeBetween()
        def YMD = new Date(timeBetween.get(BEGIN)).format("yyyyMMdd")
        def _id = YMD + "_open_card"
        def query = new BasicDBObject(["type": "open_card", "timestamp": getTimeBetween()])
        def count = card_logs.count(query) as Number
        card_logs.aggregate([
            new BasicDBObject('$match', query),
            new BasicDBObject('$project', [user_id: '$user_id', defense: '$award.defense', steal: '$award.steal', exp: '$award.exp', coin: '$award.coin', diamond: '$award.diamond', cash: '$award.cash', attack: '$award.attack']),
            new BasicDBObject('$group', [_id: null, ids: [$addToSet: '$user_id'], defense: [$sum: '$defense'], steal: [$sum: '$steal'], exp: [$sum: '$exp'], coin: [$sum: '$coin'], diamond: [$sum: '$diamond'], cash: [$sum: '$cash'], attack: [$sum: '$attack']]) //top N 算法
        ]).results().each { row ->
            def ids = row.removeField("ids") as List
            row.put("user_count", ids.size())
            row.put("total_count", count)
            row.put("_id", _id)
            row.put("type", "open_card")
            row.put("timestamp", timeBetween.get(BEGIN))
            award_daily_logs.update($$('_id', _id), row, true, false)
        }
    }


    /**
     * 寻宝统计，type: family_event
     */
    static void statics_family_event() {
        def timeBetween = getTimeBetween()
        def YMD = new Date(timeBetween.get(BEGIN)).format("yyyyMMdd")
        def _id = YMD + "_family_event"
        def query = new BasicDBObject(["status": 4, "users.0": [$exists: 1], "timestamp": getTimeBetween()])
        def count = family_event_logs.count(query) as Number //寻宝次数
        family_event_logs.aggregate([
            new BasicDBObject('$match', query),
            new BasicDBObject('$project', [rewards: '$rewards']),
            new BasicDBObject('$group', [_id: null, rewards: [$addToSet: '$rewards']])
        ]).results().each { row ->
            def record = [:]
            def uids = [] as Set
            row.get("rewards").each { BasicDBObject reward ->
                uids.add(reward.removeField("_id"))
                reward.each {String key, Integer value ->
                    Integer val = record.get(key) == null ? 0 : record.get(key) as Integer
                    record.put(key, val + value)
                }
            }
            record.put("user_count", uids.size())
            record.put("total_count", count)
            record.put("_id", _id)
            record.put("type", "family_event")
            record.put("timestamp", timeBetween.get(BEGIN))
            award_daily_logs.update($$('_id', _id), $$(record), true, false)
        }
    }


    final static Integer day = 0;

    static void main(String[] args) {
        long l = System.currentTimeMillis()
        long begin = l
        statics_open_card()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${UserAwardDailyStat.class.getSimpleName()},statics_open_card cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        l = System.currentTimeMillis()
        statics_family_event()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${UserAwardDailyStat.class.getSimpleName()},statics_family_event cost  ${System.currentTimeMillis() - l} ms"
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
        def timerName = 'DiamondDailyStat'
        Long totalCost = System.currentTimeMillis() - begin
//        saveTimerLogs(timerName, totalCost)
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