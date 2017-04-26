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
 * 关于现金的统计
 */
class CashStat {
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
    static DBCollection red_packet_logs = mongo.getDB('game_log').getCollection('red_packet_logs')
    static DBCollection red_packet_cost_logs = mongo.getDB('game_log').getCollection('red_packet_cost_logs')
    static DBCollection red_packet_apply_logs = mongo.getDB('game_log').getCollection('red_packet_apply_logs')
    static DBCollection cash_dailyReport_stat = mongo.getDB('xy_admin').getCollection('cash_dailyReport_stat')
    static DAY_MILLON = 24 * 3600 * 1000L
    static long zeroMill = new Date().clearTime().getTime()
    static Long yesTday = zeroMill - DAY_MILLON

    private static Map getTimeBetween() {
        def gteMill = yesTday - day * DAY_MILLON
        return [$gte: gteMill, $lt: gteMill + DAY_MILLON]
    }

    /**
     * 现金报表
     */
    private static void statics_cash() {
        //本日期初结余=昨日期末结余
        def begin_surplus = lastDaySurplus(yesTday)
        def inc_total = 0
        def inc_detail = new HashMap<String, Long>()
        def apply_refuse = 0
        def apply_pass_amount = 0 // 提现审核通过税前金额
        def apply_pass_income = 0 // 提现审核通过税后金额
        def desc_total = 0

        def desc_detail = new HashMap<String, Long>()
        def query = $$('timestamp': getTimeBetween())
        def red_packet_cursors = red_packet_logs.find(query).batchSize(5000)
        def red_packet_cost_cursors = red_packet_cost_logs.find(query).batchSize(5000)

        // 提现未通过 这里查询的是审核时间 而不是申请时间
        query = $$('last_modify': getTimeBetween(),'status':1)
        def red_packet_apply_refuse_cursors = red_packet_apply_logs.find(query).batchSize(5000)


        // 用户增加的现金，排除提现审核失败退款的现金
        while (red_packet_cursors.hasNext()) {
            def obj = red_packet_cursors.next()
            def cash_count = obj['cash_count'] as Long
            def type = obj['type'] as String
            if (type != 'apply_refuse') {
                inc_total += cash_count
                def current_type_acquire_cash = inc_detail.containsKey(type) ? inc_detail[type] : 0L
                current_type_acquire_cash += cash_count
                inc_detail.put(type, current_type_acquire_cash)
            }
        }


        // 提现拒绝
        while (red_packet_apply_refuse_cursors.hasNext()) {
            def obj = red_packet_apply_refuse_cursors.next()
            def amount = obj['amount'] as Long
            apply_refuse += amount
            inc_total += amount
            desc_total += amount
        }

        // 提现未处理，统计的是申请时间，因为申请就会扣费
        query = $$('timestamp': getTimeBetween(),'status':3)
        def red_packet_apply_cursors = red_packet_apply_logs.find(query).batchSize(5000)
        while (red_packet_apply_cursors.hasNext()){
            def obj = red_packet_apply_cursors.next()
            def amount = obj['amount'] as Long
            def income = obj['income'] as Long
            // 税前所得
            apply_pass_amount += amount
            // 个人所得 仅方便财务查看,不计入减少项总计
            apply_pass_income += income
            // 减少项 加入 税前所得
            desc_total += amount
        }

        // 提现成功
        query = $$('last_modify': getTimeBetween(),'status':2)
        def red_packet_apply_pass_cursors = red_packet_apply_logs.find(query).batchSize(5000)
        println("red_packet_apply_pass_cursors is ${red_packet_apply_pass_cursors.size()}")
        while (red_packet_apply_pass_cursors.hasNext()){
            def obj = red_packet_apply_pass_cursors.next()
            def amount = obj['amount'] as Long
            def income = obj['income'] as Long
            // 税前所得
            apply_pass_amount += amount
            // 个人所得 仅方便财务查看
            apply_pass_income += income
            // 减少项 加入 税前所得
            desc_total += amount
        }

        // 提现失败
        inc_detail.put('apply_refuse',apply_refuse)
        // 这一列是为了财务显示方便，不计入总计
        desc_detail.put('apply_refuse',apply_refuse)
        // 提现成功税前
        desc_detail.put('apply_pass_amount',apply_pass_amount)
        // 提现成功税后
        desc_detail.put('apply_pass_income',apply_pass_income)

        // 现金 红包兑换
        println("red_packet_cost_cursors is ${red_packet_cost_cursors.size()}")
        while (red_packet_cost_cursors.hasNext()) {
            def obj = red_packet_cost_cursors.next()
            def cash_count = obj['cash_count'] as Long
            def type = obj['type'] as String
            desc_total += cash_count
            def current_type_minus_total = desc_detail.containsKey(type) ? desc_detail[type] : 0L
            current_type_minus_total += cash_count
            desc_detail.put(type, current_type_minus_total)
        }

        // 期末 = (红包 + 提现失败) - (提现税前 + 兑换 + 提现失败)
        def end_surplus = inc_total - desc_total
        def curr_date = new Date(yesTday - day * DAY_MILLON)
        def myId = curr_date.format("yyyyMMdd") + "_cash_dailyReport_stat"

        def row = $$('_id': myId, 'inc_total': inc_total, 'desc_total': desc_total, 'timestamp': curr_date.getTime(),
                'inc_detail': inc_detail, 'desc_detail': desc_detail, 'begin_surplus': begin_surplus, 'end_surplus': end_surplus + begin_surplus)
        cash_dailyReport_stat.update($$('_id', myId), $$(row), true, false)
    }

    /**
     * 日期末结余
     * @param begin
     * @return
     */
    static Long lastDaySurplus(Long begin) {
        long yesterDay = begin - DAY_MILLON
        String ymd = new Date(yesterDay).format("yyyyMMdd")
        def last_day = cash_dailyReport_stat.findOne($$(_id: ymd + "_cash_dailyReport_stat"))

        return (last_day?.get('end_surplus') ?: 0) as Long;
    }

    final static Integer day = 0;

    static void main(String[] args) {
        long l = System.currentTimeMillis()
        long begin = l

        statics_cash()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${CashStat.class.getSimpleName()},statics_cash cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        jobFinish(begin);
    }

    /**
     * 标记任务完成  用于运维监控
     * @return
     */
    private static jobFinish(Long begin) {
        def timerName = 'CashStat'
        Long totalCost = System.currentTimeMillis() - begin
        saveTimerLogs(timerName, totalCost)
        println "${new Date().format('yyyy-MM-dd')}:${CashStat.class.getSimpleName()}:finish  cost time:  ${System.currentTimeMillis() - begin} ms"
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