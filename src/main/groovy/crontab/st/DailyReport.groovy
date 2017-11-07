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
 * 每日用户进出统计
 */
class DailyReport {
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
    static DBCollection award_logs = mongo.getDB('xylog').getCollection('user_award_logs') //道具奖励日志
    static DBCollection finance_log = mongo.getDB('xy_admin').getCollection('finance_log') //充值日志
//    static DBCollection cash_cost_logs = mongo.getDB('xylog').getCollection('cash_cost_logs') //现金花费日志
    static DBCollection diamond_cost_logs = mongo.getDB('xylog').getCollection('diamond_cost_logs') //钻石消耗日志
//    static DBCollection cash_apply_logs = mongo.getDB('xy_admin').getCollection('cash_apply_logs') //提现操作日志
    static DBCollection ops_log = mongo.getDB('xy_admin').getCollection('ops') //后台操作日志
    static DBCollection users = mongo.getDB('xy').getCollection('users')
    static DBCollection active_logs = mongo.getDB('xylog').getCollection('active_logs')

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


    //status说明： 0为道具增加， 1为道具减少
    //==========================增加钻石、现金、道具等============================



    /**
     * 充值得钻石：user_pay
     * 后台加钻石：hand_diamond
     */
    static void statics_pay() {
        def timeBetween = getTimeBetween()
        def YMD = new Date(timeBetween.get(BEGIN)).format("yyyyMMdd")
        //def type = 'user_pay'
        ['user_pay', 'hand_diamond'].collect { String type ->
            def _id = "${YMD}_${type}".toString()
            def query
            if (type == 'user_pay') {
                query = $$([via: [$ne: 'Admin'], "timestamp": getTimeBetween()])
            } else {
                query = $$([via: 'Admin', "timestamp": getTimeBetween()])
            }
            finance_log.aggregate([
                    new BasicDBObject('$match', query),
                    new BasicDBObject('$project', [user_id: '$user_id', diamond: '$diamond', income: '$cny']),
                    new BasicDBObject('$group', [_id: null, count: [$sum: 1], ids: [$addToSet: '$user_id'], diamond: [$sum: '$diamond'], income: [$sum: '$income']])
            ]).results().each { row ->
                def ids = row.removeField("ids") as List
                row.put("user_count", ids.size())
                row.put("total_count", row['count'])
                row.put("_id", _id)
                row.put("type", type)
                row.put("timestamp", timeBetween.get(BEGIN))
                row.put("status", 0)
                award_daily_logs.update($$('_id', _id), row, true, false)
            }
        }
    }

    //=================================消耗=================================

    /**
     * 手动减钻：hand_cut_diamond
     */
    static void statics_hand_cut_diamond() {
        def timeBetween = getTimeBetween()
        def YMD = new Date(timeBetween.get(BEGIN)).format("yyyyMMdd")
        def query = $$(type: 'finance_cut_coin', timestamp: getTimeBetween())

        ops_log.aggregate([
                new BasicDBObject('$match', query),
                new BasicDBObject('$project', [user_id: '$data.user_id', diamond: '$data.coin']),
                new BasicDBObject('$group', [_id: null, count: [$sum: 1], ids: [$addToSet: '$user_id'], diamond: [$sum: '$diamond']])
        ]).results().each { row ->
            def type = "hand_cut_diamond"
            def _id = "${YMD}_${type}".toString()
            def ids = row.removeField("ids") as List
            row.put("user_count", ids.size())
            row.put("total_count", row['count'])
            row.put("_id", _id)
            row.put("type", type)
            row.put("timestamp", timeBetween.get(BEGIN))
            row.put("status", 1)
            award_daily_logs.update($$('_id', _id), row, true, false)
        }
    }

    //=================================消耗 end=================================

    static Integer day = 0

    static void main(String[] args) {
        long l = System.currentTimeMillis()
        long begin = l

        l = System.currentTimeMillis()
        statics_pay()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},statics_pay cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //===============减=================
        l = System.currentTimeMillis()
        statics_hand_cut_diamond()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},statics_hand_cut_diamond cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)
        //================减 end=============


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
        println "${new Date().format('yyyy-MM-dd')}:${DailyReport.class.getSimpleName()}:finish  cost time:  ${System.currentTimeMillis() - begin} ms"
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