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
    static DBCollection cash_cost_logs = mongo.getDB('xylog').getCollection('cash_cost_logs') //现金花费日志
    static DBCollection diamond_cost_logs = mongo.getDB('xylog').getCollection('diamond_cost_logs') //钻石消耗日志
    static DBCollection cash_logs = mongo.getDB('xylog').getCollection('cash_logs') //提现操作日志
    static DBCollection cash_apply_logs = mongo.getDB('xy_admin').getCollection('cash_apply_logs') //提现操作日志
    static DBCollection ops_log = mongo.getDB('xy_admin').getCollection('ops') //后台操作日志
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
        println YMD
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

    //==========================增加钻石、现金、道具等============================
    /**
     * 翻牌统计：open_card
     * 寻宝（挖矿）统计: family_event
     * 道具使用统计：use_item
     * 新人奖励统计（目前只有现金奖励）：new_user
     */
    static void statics_award() {
        def timeBetween = getTimeBetween()
        def YMD = new Date(timeBetween.get(BEGIN)).format("yyyyMMdd")

        def query = new BasicDBObject([type: [$ne: 'family_award'], timestamp: getTimeBetween()])
        award_logs.aggregate([
                new BasicDBObject('$match', query),
                new BasicDBObject('$project', [type: '$type', user_id: '$user_id', defense: '$award.defense', steal: '$award.steal', exp: '$award.exp', coin: '$award.coin', diamond: '$award.diamond', cash: '$award.cash', attack: '$award.attack']),
                new BasicDBObject('$group', [_id: '$type', count: [$sum: 1], ids: [$addToSet: '$user_id'], defense: [$sum: '$defense'], steal: [$sum: '$steal'], exp: [$sum: '$exp'], coin: [$sum: '$coin'], diamond: [$sum: '$diamond'], cash: [$sum: '$cash'], attack: [$sum: '$attack']]) //top N 算法
        ]).results().each { row ->
            def type = row['_id']
            def _id = "${YMD}_${type}".toString()
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

    /**
     * 家族贡献领奖：family_award
     */
    static void statics_family_award() {
        def timeBetween = getTimeBetween()
        def YMD = new Date(timeBetween.get(BEGIN)).format("yyyyMMdd")
        def _id = YMD + "_family_award"
        def query = $$([type: 'family_award', "timestamp": getTimeBetween()])
        award_logs.aggregate([
                new BasicDBObject('$match', query),
                new BasicDBObject('$project', [user_id: '$user_id', award: '$award']),
                new BasicDBObject('$group', [_id: null, count: [$sum: 1], ids: [$addToSet: '$user_id'], diamond: [$sum: '$award']]) //top N 算法
        ]).results().each { row ->
            def ids = row.removeField("ids") as List
            row.put("user_count", ids.size())
            row.put("total_count", row['count'])
            row.put("_id", _id)
            row.put("type", "family_award")
            row.put("timestamp", timeBetween.get(BEGIN))
            row.put("status", 0)
            award_daily_logs.update($$('_id', _id), row, true, false)
        }
    }

    /**
     * 现金兑换钻石统计
     * +钻石 cash_exchange_add_diamond
     * -现金 cash_exchange_decrease_cash
     *
     */
    static void statics_exchange_diamond() {
        def timeBetween = getTimeBetween()
        def YMD = new Date(timeBetween.get(BEGIN)).format("yyyyMMdd")
        def type = 'cash_exchange'
        def _id = "${YMD}_${type}".toString()
        def query = $$([type: type, "timestamp": getTimeBetween()])
        cash_cost_logs.aggregate([
                new BasicDBObject('$match', query),
                new BasicDBObject('$project', [user_id: '$session._id', cost: '$cost']),
                new BasicDBObject('$group', [_id: null, count: [$sum: 1], ids: [$addToSet: '$user_id'], diamond: [$sum: '$cost']])
        ]).results().each { row ->
            //钻石增加
            def ids = row.removeField("ids") as List
            row.put("user_count", ids.size())
            row.put("total_count", row['count'])
            row.put("_id", _id + "_add_diamond")
            row.put("type", type + "_add_diamond")
            row.put("timestamp", timeBetween.get(BEGIN))
            row.put("status", 0)
            award_daily_logs.update($$('_id', _id + "_add_diamond"), row, true, false)
            //现金减少
            row.put("_id", _id + "_decrease_cash")
            row.put("type", type + "_decrease_cash")
            row.put("cash", row.removeField('diamond'))
            row.put("status", 1)
            award_daily_logs.update($$('_id', _id + "_decrease_cash"), row, true, false)
        }
    }

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

    /**
     * 拒绝提现: apply_refuse
     */
    static void statics_refuse_cash() {
        def timeBetween = getTimeBetween()
        def YMD = new Date(timeBetween.get(BEGIN)).format('yyyyMMdd')
        def query = $$(status: 3, last_modify: timeBetween)
        cash_apply_logs.aggregate([
                new BasicDBObject('$match', query),
                new BasicDBObject('$project', [user_id: '$user_id', cash: '$amount']),
                new BasicDBObject('$group', [_id: null, count: [$sum: 1], ids: [$addToSet: '$user_id'], cash: [$sum: '$cash']])
        ]).results().each { row ->
            def type = 'apply_refuse'
            def _id = "${YMD}_${type}".toString()
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

    //=================================消耗=================================
    /**
     * 开家族: apply_family
     * 翻牌: open_card
     */
    static void statics_diamond_cost() {
        def timeBetween = getTimeBetween()
        def YMD = new Date(timeBetween.get(BEGIN)).format("yyyyMMdd")
        def query = new BasicDBObject(["timestamp": getTimeBetween()])
        def result = [:], costs = 0L, users = new HashSet()
        diamond_cost_logs.aggregate([
                new BasicDBObject('$match', query),
                new BasicDBObject('$project', [type: '$type', user_id: '$session._id', diamond: '$cost']),
                new BasicDBObject('$group', [_id: '$type', count: [$sum: 1], ids: [$addToSet: '$user_id'], diamond: [$sum: '$diamond']])
        ]).results().each { row ->
            def type = row['_id'] + "_decrease"
            def _id = "${YMD}_${type}".toString()
            def ids = row.removeField("ids") as List
            row.put("user_count", ids.size())
            row.put("total_count", row['count'])
            row.put("_id", _id)
            row.put("type", type)
            row.put("timestamp", timeBetween.get(BEGIN))
            row.put("status", 1)

            costs += row['diamond']?:0
            users.addAll(ids)

            award_daily_logs.update($$('_id', _id), row, true, false)
        }
        result.putAll([user_cost: [cost: costs, user: users.size()]])
        award_daily_logs.update($$(_id: "${YMD}_allcost".toString()), $$($set: result), true, false)
    }

    /**
     * 手动减钻：hand_cut_diamond
     */
    static void statics_hand_cut_diamond() {
        def timeBetween = getTimeBetween()
        def YMD = new Date(timeBetween.get(BEGIN)).format("yyyyMMdd")
        def query = $$(type: 'finance_cut_coin', timestamp: getTimeBetween())

        ops_log.aggregate([
                new BasicDBObject('$match', query),
                new BasicDBObject('$project', [user_id: '$data.user_id', diamond: '$coin']),
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

    /**
     * 申请提现: apply_cash cash减少
     */
    static void statics_cash_apply() {
        def timeBetween = getTimeBetween()
        def YMD = new Date(timeBetween.get(BEGIN)).format('yyyyMMdd')
        def query = new BasicDBObject(status: [$ne: 3], timestamp: timeBetween)
        cash_apply_logs.aggregate([
                new BasicDBObject('$match', query),
                new BasicDBObject('$project', [user_id: '$user_id', cash: '$amount', expend: '$income']),
                new BasicDBObject('$group', [_id: null, count: [$sum: 1], ids: [$addToSet: '$user_id'], cash: [$sum: '$cash']])
        ]).results().each { row ->
            def type = 'apply_cash'
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

    /**
     * 申请提现: apply_agree
     */
    static void statics_cash_agree() {
        def timeBetween = getTimeBetween()
        def YMD = new Date(timeBetween.get(BEGIN)).format('yyyyMMdd')
        def query = new BasicDBObject(status: 2, last_modify: timeBetween)
        cash_apply_logs.aggregate([
                new BasicDBObject('$match', query),
                new BasicDBObject('$project', [user_id: '$user_id', cash: '$amount', expend: '$income']),
                new BasicDBObject('$group', [_id: null, count: [$sum: 1], ids: [$addToSet: '$user_id'], cash: [$sum: '$cash'], expend: [$sum: '$expend']])
        ]).results().each { row ->
            def type = 'apply_agree'
            def _id = "${YMD}_${type}".toString()
            def ids = row.removeField("ids") as List
            row.put("user_count", ids.size())
            row.put("total_count", row['count'])
            row.put("_id", _id)
            row.put("type", type)
            row.put("timestamp", timeBetween.get(BEGIN))
            award_daily_logs.update($$('_id', _id), row, true, false)
        }
    }

    static Integer day = 0

    static void main(String[] args) {
        long l = System.currentTimeMillis()
        long begin = l
        user_snapshot()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},user_snapshot cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        l = System.currentTimeMillis()
        statics_award()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},statics_award cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        l = System.currentTimeMillis()
        statics_family_award()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},statics_family_award cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        l = System.currentTimeMillis()
        statics_exchange_diamond()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},statics_exchange_diamond cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        l = System.currentTimeMillis()
        statics_pay()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},statics_pay cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        l = System.currentTimeMillis()
        statics_cash_apply()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},statics_cash_apply cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        l = System.currentTimeMillis()
        statics_refuse_cash()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},statics_refuse_cash cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //===============减=================
        l = System.currentTimeMillis()
        statics_diamond_cost()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},statics_diamond_cost cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        l = System.currentTimeMillis()
        statics_hand_cut_diamond()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},statics_hand_cut_diamond cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        l = System.currentTimeMillis()
        statics_cash_apply()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},statics_cash_apply cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)
        //================减 end=============

        l = System.currentTimeMillis()
        statics_cash_agree()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},statics_cash_agree cost  ${System.currentTimeMillis() - l} ms"
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