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
 * 每日充值消费报表（财务-真实柠檬币比例）
 */
class FinanceDaily {

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

    static historyMongo = new Mongo(new MongoURI(getProperties('mongo_history.uri', 'mongodb://192.168.31.246:27017/?w=1') as String))

    static DBCollection finance_daily_log = mongo.getDB('xy_admin').getCollection('finance_daily_log')
    static DBCollection stat_daily = mongo.getDB('xy_admin').getCollection('stat_daily')
    static DBCollection finance_log = mongo.getDB('xy_admin').getCollection('finance_log')
    static DBCollection award_daily_logs = mongo.getDB('xy_admin').getCollection('stat_daily')

    static String END_SURPLUS = 'end_surplus' //钻石结余
    static String CASH_END_SURPLUS = 'cash_end_surplus' //现金结余
    static DAY_MILLON = 24 * 3600 * 1000L
    static long zeroMill = new Date().clearTime().getTime()
    static Long yesTday = zeroMill - DAY_MILLON

    //比较每日统计和用户剩余柠檬快照差额 邮件报警
    static Long EMAIL_THRESHOLD = 3500
    static BEGIN = '$gte'
    private static Map getTimeBetween() {
        def gteMill = yesTday - day * DAY_MILLON
        return [$gte: gteMill, $lt: gteMill + DAY_MILLON]
    }

    //财务每日统计
    static Boolean financeDayStatic(Long begin) {
        def timebtn = getTimeBetween()
        begin = timebtn[BEGIN]
        Long end = begin + DAY_MILLON
        String ymd = new Date(begin).format("yyyyMMdd")
        def timebetween = [timestamp: [$gte: begin, $lt: end]]

        //统计充值相关
        def charge = charge_total(timebetween)
        //扣费前充值金额(用户端充值金额)
        def charge_cny = charge['charge_cny'] as Number
        //扣费后充值金额(公司预收账款)
        def cut_charge_cny = charge['cut_charge_cny'] as Number

        //新增钻石(所有的)
        def inc = increase(timebetween, 'diamond')
        //充值新增钻石
        Long charge_coin = (inc['user_pay']?:0) as Long
        //非充值新增钻石 (非充值手段新增的钻石，如玩游戏,手动加币)
        def inc_coin = (inc['total'] as Number) - charge_coin

        //总消费钻石
        def dec = decrease(timebetween, 'diamond')
        def dec_total = dec['total'] as Number
        //运营后台减钻
        def hand_cut_coin = (dec['hand_cut_diamond']?:0) as Number

        //总新增钻石= 新增钻石 - 运营后台减钻
        def inc_total = (inc['total'] as Number) - hand_cut_coin

        //本期初结余=上期末结余
        def begin_surplus = lastDaySurplus(begin, END_SURPLUS)
        //期末钻石余额= 上期末节余 + 增加的钻石 - 总消费钻石
        def end_surplus = begin_surplus + inc_total - dec_total

        //今日消费差额 = 总新增钻石 - 总消费钻石
        def today_balance = inc_total - dec_total

        //今日用户剩余钻石快照
        Long remain = userRemain(begin, 'diamond')

        //昨日用户剩余钻石快照
        Long yesterday_remain = userRemain(begin - DAY_MILLON, 'diamond')

        //用户剩余钻石快照差额 = 今日用户剩余钻石快照 - /昨日用户剩余钻石快照
        def remian_balance = remain - yesterday_remain

        //比较统计和快照差额
        Long balance = today_balance - remian_balance

        //新增现金
        def inc_cash = increase(timebetween, 'cash')
        def inc_total_cash = (inc_cash['total']?:0) as Long
        //消耗现金
        def dec_cash = decrease(timebetween, 'cash')
        def dec_total_cash = (dec_cash['total']?:0) as Long
        //当日提现支出（提现审核通过，提现金额，扣税金额，实发（目前取不到））
        def cash_pay = cash_pay()
        //本期初结余=上期末结余
        def cash_begin_surplus = lastDaySurplus(begin, CASH_END_SURPLUS)
        //期末现金余额= 上期末节余 + 增加的现金 - 总消费现金
        def cash_end_surplus = cash_begin_surplus + inc_total_cash - dec_total_cash

        //今日现金差额 = 总新增现金 - 总消费现金
        def cash_today_balance = inc_total_cash - dec_total_cash

        //今日用户剩余现金快照
        Long cash_remain = userRemain(begin, 'cash')

        //昨日用户剩余现金快照
        Long cash_yesterday_remain = userRemain(begin - DAY_MILLON, 'cash')

        //用户剩余现金快照差额 = 今日用户剩余现金快照 - /昨日用户剩余现金快照
        def cash_remian_balance = cash_remain - cash_yesterday_remain

        //比较统计和快照差额
        Long cash_balance = cash_today_balance - cash_remian_balance

        def obj = new BasicDBObject(
                inc: inc,
                dec: dec,
                inc_total: inc_total, //总新增钻石
                dec_total: dec_total, //总消费钻石
                hand_cut_coin: hand_cut_coin,//运营后台扣币
                charge_cny: charge_cny, //用户端充值金额
                cut_charge_cny: cut_charge_cny,//公司预收账款
                charge_coin: charge_coin, //充值新增钻石
                inc_coin: inc_coin, //非充值新增钻石
                begin_surplus: begin_surplus,
                end_surplus: end_surplus,
                charge: charge, //充值相关信息
                today_balance: today_balance, //今日消费差额
                remian_balance: remian_balance, //用户剩余钻石快照差额
                balance: balance, //比较统计和快照差额
                remain: remain,
                type: 'finance_daily',
                date: ymd,
                timestamp: begin,
                cash_inc: inc_cash, //现金增加明细
                cash_dec: dec_cash, //现金减少明细
                cash_begin_surplus: cash_begin_surplus,
                cash_end_surplus: cash_end_surplus,
                cash_today_balance: cash_today_balance,
                cash_remian_balance: cash_remian_balance,
                cash_balance: cash_balance,
                cash_remain: cash_remain,
                cash_pay: cash_pay //当日审核通过的金额，认为实发
        )
        finance_daily_log.update($$(_id: "${ymd}_finance".toString()), new BasicDBObject('$set', obj), true, false)

        println "${new Date().format('yyyy-MM-dd HH:mm:ss')} from ${new Date(begin).format("yyyy-MM-dd HH:mm:ss")} to ${new Date(end).format("yyyy-MM-dd HH:mm:ss")}".toString()
        Boolean flag = Math.abs(balance) > EMAIL_THRESHOLD
        if (flag) {
            println "balance : ${balance}".toString()
        }
        return flag
    }

    static BasicDBObject charge_total(Map timebetween) {
        Map data = new HashMap()
        charge(data, timebetween)
        def incData = $$(data)
        return incData
    }

    static BasicDBObject increase(Map timebetween, String field) {
        Number totalCoin = 0
        Map data = new HashMap()
        def query = $$(timebetween)
        query.put("status", 0)
        query.put(field, [$exists: true])
        def f = $$("type", 1)
        f.put(field, 1)
        award_daily_logs.find(query, f).toArray().each {BasicDBObject obj ->
            totalCoin += obj.get(field) as Long
            data.put(obj.get('type'), obj.get(field))
        }
        def incData = $$('total': totalCoin)
        incData.putAll(data)
        return incData
    }

    static BasicDBObject decrease(Map timebetween, String field) {
        Number totalCoin = 0
        Map data = new HashMap()
        def query = $$(timebetween)
        query.put("status", 1)
        query.put(field, [$exists: true])
        def f = $$("type", 1)
        f.put(field, 1)
        award_daily_logs.find(query, f).toArray().each {BasicDBObject obj ->
            totalCoin += obj.get(field) as Long
            data.put(obj.get('type'), obj.get(field))
        }
        def decData = $$('total': totalCoin)
        decData.putAll(data)
        return decData
    }

    static BasicDBObject cash_pay(Map timebetween) {
        def cash = 0
        def expand = 0
        def query = $$(timebetween)
        query.putAll(type: 'apply_agree')
        award_daily_logs.find(query, $$(type: 1, cash: 1, expand: 1)).toArray().each {BasicDBObject obj ->
            cash += obj.get('cash') as Long
            expand += obj.get('expand') as Long
        }
        return [total_cash: cash, total_expand: expand]
    }

    /**
     * 充值相关
     * @return
     */
    static charge(Map data, Map timebetween) {
        def YMD = timebetween['timestamp'][BEGIN]
        def result = stat_daily.findOne($$(_id: "${YMD}_finance".toString()))

        data.put('charge_cny', result?.get('total')?:0)
        data.put('cut_charge_cny', result?.get('total_cut')?:0)
        data.put('charge_coin', result?.get('charge_coin')?:0)
        data.put('total_coin', result?.get('total_coin')?:0)
        data.put('hand_coin', result?.get('hand_coin')?:0)
    }

    static Long lastDaySurplus(Long begin, String field) {
        long yesterDay = begin - DAY_MILLON
        String ymd = new Date(yesterDay).format("yyyyMMdd")
        def last_day = finance_daily_log.findOne($$(_id: "${ymd}_finance".toString()))
        return (last_day?.get(field) ?: 0) as Long;
    }

    static Long userRemain(Long begin, String field) {
        String ymd = new Date(begin).format("yyyyMMdd")
        def last_day = stat_daily.findOne($$(_id: "${ymd}_allcost".toString()))
        if (last_day != null && last_day['user_remain'] != null) {
            return last_day['user_remain'][field] as Long
        }
        return 0L
    }

    final static Integer day = 0

    static void main(String[] args) {
        long l = System.currentTimeMillis()
        //生成历史财务报表
        Boolean needEmailNotify = financeDayStatic(yesTday)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}:${FinanceDaily.class.getSimpleName()}:finish  cost time: ${System.currentTimeMillis() - l} ms : needEmailNotify:${needEmailNotify}"
    }

    static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value)
    }

    static BasicDBObject $$(Map map) {
        return new BasicDBObject(map)
    }
}