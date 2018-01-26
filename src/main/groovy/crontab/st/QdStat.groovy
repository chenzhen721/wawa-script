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
 * 渠道统计
 */
class QdStat {
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
    static chatLog = mongo.getDB('chat_log')
    static DAY_MILLON = 24 * 3600 * 1000L
    static long zeroMill = new Date().clearTime().getTime()
    static Long yesTday = zeroMill - DAY_MILLON

    static DBCollection users = mongo.getDB('xy').getCollection('users')
    static DBCollection finance_log = mongo.getDB('xy_admin').getCollection('finance_log')
    static DBCollection stat_daily = mongo.getDB('xy_admin').getCollection('stat_daily')
    static DBCollection day_login = mongo.getDB("xylog").getCollection("day_login")
    static DBCollection channels = mongo.getDB('xy_admin').getCollection('channels')
    static DBCollection stat_channels = mongo.getDB('xy_admin').getCollection('stat_channels')
    static DBCollection stat_regpay = mongo.getDB('xy_admin').getCollection('stat_regpay')
    static DBCollection catch_record = mongo.getDB('xy_catch').getCollection('catch_record')

    /*static create_chat_index(String currentMonth) {
        def index_name = '_user_id_timestamp_index_'
        DBCollection chat_collection = chatLog.getCollection(currentMonth)
        def isExists = Boolean.TRUE
        chat_collection.getIndexInfo().each {
            BasicDBObject obj ->
                def name = obj['name'] as String
                if(name == index_name){
                    isExists = Boolean.FALSE
                }
        }
        if(isExists){
            def user_id_timestamp_index = $$('user_id': 1, 'timestamp': -1)
            chat_collection.createIndex(user_id_timestamp_index,index_name)
            println  "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${QdStat1.class.getSimpleName()},create index success"
        }
    }*/

    static regStatics(int i){
        def begin = yesTday - i * DAY_MILLON
        if (begin < 1511107200000) return
        def end = begin + DAY_MILLON
        def YMD = new Date(begin).format('yyyyMMdd')
        //def regs = users.count(new BasicDBObject(timestamp: [$gte: begin, $lt: end]))
        def total_regs = []
        users.aggregate([
                $$('$match', [timestamp: [$gte: begin, $lt: end]]),
                $$('$project', [user_id: '$_id', qd: '$qd']),
                $$('$group', [_id: '$qd', user_id: [$addToSet: '$user_id']])
        ]).results().each {BasicDBObject obj->
            //qd信息,  对应的users
            def qd = obj['_id'] as String
            def regs = obj['user_id'] as Set
            total_regs.addAll(regs)
            def update = [type: 'qd', qd: qd, timestamp: begin, regs: regs, reg_count: regs.size()]
            [1, 3, 7, 30].each {
                update.put("pay_user_count${it}".toString(), 0)
                update.put("pay_total${it}".toString(), 0)
            }
            stat_regpay.update($$(_id: "${YMD}_${qd}_regpay".toString()), $$($set: update), true, false)
        }

        def update = [type: 'total', timestamp: begin, regs: total_regs, reg_count: total_regs.size()]
        [1, 3, 7, 30].each {
            update.put("pay_user_count${it}".toString(), 0)
            update.put("pay_total${it}".toString(), 0)
        }
        stat_regpay.update($$(_id: "${YMD}_regpay".toString()), $$($set: update), true, false)
    }



    /**
     * 充值钻石：pay_coin
     * 充值金额：pay_cny
     * 充值人数：pay_user
     * 充值ARPU：pay_cny/pay_user
     * 日活：logins
     * 活跃付费：pay_cny/logins
     * 抓中次数：bingo_count //
     * 抓取次数：doll_count //
     * 当日抓中率：bingo_count/doll_count //
     * 新增抓中率：reg_bingo_count/reg_user_count //
     * 新增人数：regs
     * 新增充值金额：reg_pay_cny
     * 新增充值人数：reg_pay_user
     * 新增付费率：reg_pay_user/regs
     * 新增ARPU：reg_pay_cny/reg_pay_user
     * 新增用户抓取人数：reg_user_count //
     * 新增抓取率：reg_user_count/regs
     * 充值用户次日留存：1_pay //todo
     * 1、3、7、30日留存
     * "stay": {
     *     "1_day": 0,
     *     "30_day": 0,
     *     "7_day": 0,
     *     "3_day": 0
     * }
     *
     * @param i
     * @return
     */
    static statics(int i) {
        Long begin = yesTday - i * DAY_MILLON
        Long end = begin + DAY_MILLON
        def YMD = new Date(begin).format('yyyyMMdd')

        //充值信息
        finance_log.aggregate([
                $$($match: [timestamp: [$gte: begin, $lt: end], via: [$ne: 'Admin']]),
                $$($group: [_id: '$qd', uids: [$addToSet: '$user_id'], cnys: [$addToSet: '$cny'], diamond: [$sum: '$diamond']])
        ]).results().each {BasicDBObject obj->
            def cid = obj['_id']
            def pay_coin = obj['diamond'] as Integer ?: 0
            def uids = obj['uids'] as Set ?: new HashSet()
            def cnys = obj['cnys'] as Set ?: new HashSet()
            def pay_cny = cnys.sum{ it as Double ?: 0 }

            def regpay = stat_regpay.findOne($$(_id: "${YMD}_${cid}_regpay".toString(), type: 'qd')) ?: new HashMap()
            //注册人数 新增人数 新增充值金额 新增充值人数
            def regs = regpay['regs'] as Set ?: []
            def reg_pay_cny = 0d
            def reg_pay_user = []

            for(int j = 0; j < uids.size(); j++) {
                if (regs.contains(uids[j])) {
                    reg_pay_user.add(uids[j])
                    reg_pay_cny = reg_pay_cny + (cnys[j] as Double ?: 0d)
                }
            }
            def update = $$([qd: cid, timestamp: begin, pay_coin: pay_coin, pay_cny: pay_cny, pay_user: uids.size(), regs: regs.size(), reg_pay_cny: reg_pay_cny, reg_pay_user: reg_pay_user.size()])
            stat_channels.update($$(_id: "${YMD}_${cid}".toString()), $$($set: update), true, false)
        }
        //登录信息
        day_login.aggregate([
                $$($match: [timestamp: [$gte: begin, $lt: end]]),
                $$($group: [_id: '$qd', uids: [$addToSet: '$user_id']])
        ]).results().each {BasicDBObject obj->
            def cid = obj['_id']
            def uids = obj['uids'] as Set ?: []
            def update = $$([logins: uids.size()])
            stat_channels.update($$(_id: "${YMD}_${cid}".toString()), $$($set: update), true, false)
        }
    }

    /**
     * 统计渠道抓取情况
     * @param i
     */
    static dollQdStatic(int i) {
        def begin = yesTday - i * DAY_MILLON
        def end = begin + DAY_MILLON
        def YMD = new Date(begin).format('yyyyMMdd')
        def time = [timestamp: [$gte: begin, $lt: end], is_delete: [$ne: true]]
        def query = new BasicDBObject(time)
        //查询当天抓取人，并且对人进行渠道分类
        def allids = catch_record.distinct('user_id', query)
        users.aggregate([
            $$($match: [_id: [$in: allids]]),
            $$($group: [_id: '$qd', uids: [$addToSet: '$_id']])
        ]).results().each {BasicDBObject obj ->
            //每个渠道用户
            def cid = obj['_id'] as String
            def uids = obj['uids'] as Set
            def qd_query = $$(time).append('user_id', [$in: uids])
            def regpay = stat_regpay.findOne($$(_id: "${YMD}_${cid}_regpay".toString(), type: 'qd')) ?: new HashMap()
            //渠道新增
            def regs = regpay['regs'] as Set ?: []
            //抓中次数 抓取次数 新增用户抓取人数
            def list = catch_record.find(qd_query).toArray()
            def doll_count = list?.size() ?: 0
            def bingo_count = 0, reg_user_count = 0
            for(DBObject record : list) {
                def status = record['status'] as Boolean ?: false
                if (status) {
                    bingo_count = bingo_count + 1
                }
                def user_id = record['user_id'] as Integer
                if (regs.contains(user_id)) {
                    reg_user_count = reg_user_count + 1
                }
            }
            def update = $$([doll_count: doll_count, bingo_count: bingo_count, reg_user_count: reg_user_count])
            stat_channels.update($$(_id: "${YMD}_${cid}".toString()), $$($set: update), true, false)
        }
    }

    //充值次日留存
    static payRetentionQdStatic(int i) {
        def begin = yesTday - i * DAY_MILLON
        def end = begin + DAY_MILLON
        def YMD = new Date(begin).format('yyyyMMdd')

        finance_log.aggregate([
            $$($match: [timestamp: [$gte: begin, $lt: end], via: [$ne: 'Admin']]),
            $$($group: [_id: '$qd', uids: [$addToSet: '$user_id']])
        ]).results().each {BasicDBObject obj ->
            def cid = obj['_id']
            def uids = obj['uids'] as Set ?: new HashSet()
            def retention = day_login.count($$(user_id: [$in: uids], timestamp: [$gte: end, $lt: end + DAY_MILLON]))
            stat_channels.update($$(_id: "${YMD}_${cid}".toString()), $$($set: ['1_pay': retention]), true, false)
        }
    }

    //父渠道信息汇总
    static parentQdstatic(int i) {
        def channel_db = mongo.getDB('xy_admin').getCollection('channels')
        def channels = channel_db.find($$(parent_qd: [$ne: null]), $$(parent_qd: 1)).toArray()
        Map<String, DBObject> parentMap = new HashMap<String, DBObject>()
        for (DBObject obj : channels) {
            String parent_id = obj.get("parent_qd") as String
            parentMap.put(parent_id, obj)
        }
        Long begin = yesTday - i * DAY_MILLON

        for (String key : parentMap.keySet()) {
            DBObject obj = parentMap.get(key)
            def parent_id = obj.get("parent_qd") as String
            def childqds = channel_db.find($$(parent_qd: parent_id), $$(_id: 1)).toArray()
            def qds = childqds.collect { ((Map) it).get('_id').toString()}
            DBObject query = $$('qd', [$in: qds]).append("timestamp", begin)
            def stat_child_channels = stat_channels.find(query).toArray()
            stat_channels.aggregate([
                $$($match: query),
                $$($group: [_id: null, uids: [$addToSet: '$user_id']])
            ]).results().each {BasicDBObject item ->

            }
        }
    }

    public static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    public static BasicDBObject $$(Map map) {
        return new BasicDBObject(map)
    }

    static Integer begin_day = 0;

    static void main(String[] args) {
        long l = System.currentTimeMillis()
        long begin = l

        //渠道新增统计
        l = System.currentTimeMillis()
        //regStatics(begin_day)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   regStatics, cost  ${System.currentTimeMillis() - l} ms"

        //渠道统计
        l = System.currentTimeMillis()
        statics(begin_day)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${QdStat.class.getSimpleName()},statics cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //抓取统计
        l = System.currentTimeMillis()
        dollQdStatic(begin_day)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${QdStat.class.getSimpleName()},dollQdStatic cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //付费次日留存统计
        l = System.currentTimeMillis()
        payRetentionQdStatic(begin_day + 1)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${QdStat.class.getSimpleName()},payRetentionQdStatic cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //父级渠道的统计
        l = System.currentTimeMillis()
        //parentQdstatic(begin_day)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${QdStat.class.getSimpleName()},parentQdstatic cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //落地定时执行的日志
        jobFinish(begin)
    }

    /**
     * 标记任务完成  用于运维监控
     * @return
     */
    private static jobFinish(Long begin) {
        def timerName = 'QdStat'
        Long totalCost = System.currentTimeMillis() - begin
        saveTimerLogs(timerName, totalCost)
        println "${new Date().format('yyyy-MM-dd')}:${QdStat.class.getSimpleName()}:finish  cost  ${System.currentTimeMillis() - begin} ms"
    }

    //落地定时执行的日志
    private static saveTimerLogs(String timerName, Long totalCost) {
        def timerLogsDB = mongo.getDB("xyrank").getCollection("timer_logs")
        def tmp = System.currentTimeMillis()
        def id = timerName + "_" + new Date().format("yyyyMMdd")
        def update = $$(timer_name: timerName, cost_total: totalCost, cat: 'day', unit: 'ms', timestamp: tmp)
        timerLogsDB.findAndModify($$('_id', id), null, null, false, $$('$set', update), true, true)
    }

}