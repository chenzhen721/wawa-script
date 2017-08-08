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
import org.apache.commons.lang.StringUtils

import java.text.SimpleDateFormat
import com.mongodb.DBObject

/**
 * 每月统计一份数据
 *
 * date: 13-10-16 下午2:46
 * @author: haigen.xiong@ttpod.com
 */
class StaticsEveryMonth {

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
    static historyMongo = new Mongo(new MongoURI(getProperties('mongo_history.uri', 'mongodb://192.168.31.231:20000,192.168.31.236:20000,192.168.31.231:20001/?w=1&slaveok=true') as String))


    static DBCollection coll = mongo.getDB('xy_admin').getCollection('stat_month')

    private static BATCH_SIZE = 50000

    static loginStatics() {
        Map<Integer, Integer> total_map = new HashMap<Integer, Integer>(2500000)
        Map<Integer, Integer> mobile_map = new HashMap<Integer, Integer>(800000)
        def dayLog = mongo.getDB("xylog").getCollection("day_login");

        Calendar cal = getCalendar()
        long firstDayOfCurrentMonth = cal.getTimeInMillis()  //当月第一天

        cal.add(Calendar.MONTH, -1);
        long firstDayOfLastMonth = cal.getTimeInMillis()  //上月第一天
        String ym = new Date(firstDayOfLastMonth).format("yyyyMM")
        def query = new BasicDBObject(timestamp: [$gte: firstDayOfLastMonth, $lt: firstDayOfCurrentMonth])
        def field = new BasicDBObject(user_id: 1)
        // def total = dayLog.distinct('user_id', query).toArray().size()
        def total = 0
        def totalList = dayLog.find(query, field).toArray()
        for (DBObject obj : totalList) {
            Integer uid = obj.get("user_id") as Integer
            total_map.put(uid, uid)
        }
        total = total_map.size()
        // def mobile_num = dayLog.distinct('user_id', query.append('uid', [$ne: null])).toArray().size()   // distinct too big 16M
        def mobile_num = 0
        def mobileList = dayLog.find(query.append('uid', [$ne: null]), field).toArray()
        for (DBObject obj : mobileList) {
            Integer uid = obj.get("user_id") as Integer
            mobile_map.put(uid, uid)
        }
        mobile_num = mobile_map.size()

        def obj = new BasicDBObject(
                _id: "${ym}_login".toString(),
                total: total,
                moblie: mobile_num,
                type: 'login',
                timestamp: new SimpleDateFormat("yyyyMM").parse(ym).getTime()
        )
        coll.save(obj)
    }

//    static regPayMonthStatic(int i){
//        Calendar cal = getCalendar()
//        cal.add(Calendar.MONTH, -i)
//        long firstDayOfCurrentMonth = cal.getTimeInMillis()  //当月第一天
//        cal.add(Calendar.MONTH, -1)
//        long firstDayOfLastMonth = cal.getTimeInMillis()  //上月第一天
//        String ym = new Date(firstDayOfLastMonth).format("yyyyMM")
//    }

    //用户登录流失率统计,从两个月前的数据统计起（14年从7月开始），财务管理-运营数据月报
    static loginMonthStatic(int i) {
        def day_login = historyMongo.getDB("xylog_history").getCollection("day_login_history")
        Calendar cal = getCalendar()
        cal.add(Calendar.MONTH, -i)
        long firstDayOfCurrentMonth = cal.getTimeInMillis()  //当月第一天
        cal.add(Calendar.MONTH, -1)
        long firstDayOfLastMonth = cal.getTimeInMillis()  //上月第一天
        String ym = new Date(firstDayOfLastMonth).format("yyyyMM")
        cal.add(Calendar.MONTH, -1)
        long firstDayOfBefore = cal.getTimeInMillis() //上上个月第一天
        def before_total_map = new HashSet<Integer>(250000)
        def before_pc_map = new HashSet<Integer>(100000)
        def before_mobile_map = new HashSet<Integer>(150000)
        def last_total_map = new HashSet<Integer>(250000)
        def last_pc_map = new HashSet<Integer>(100000)
        def last_mobile_map = new HashSet<Integer>(150000)
        def query = new BasicDBObject(timestamp: [$gte: firstDayOfBefore, $lt: firstDayOfLastMonth])
        long count = day_login.count(query)
        Integer num = count / BATCH_SIZE
        if (count % BATCH_SIZE > 0) {
            num++
        }
        //查询上上个月前的登录用户数据
        num.times {
            day_login.find(query).skip(it * BATCH_SIZE).limit(BATCH_SIZE)
                    .toArray().each { BasicDBObject obj ->
                def user_id = obj.get("user_id") as Integer
                def uid = obj.get("uid") as String
                before_total_map.add(user_id)
                if (StringUtils.isBlank(uid)) {
                    before_pc_map.add(user_id)
                } else {
                    before_mobile_map.add(user_id)
                }
            }
        }
        //查询上个月的登录用户数据
        query = new BasicDBObject(timestamp: [$gte: firstDayOfLastMonth, $lt: firstDayOfCurrentMonth])
        count = day_login.count(query)
        num = count / BATCH_SIZE as Integer
        if (count % BATCH_SIZE > 0) {
            num++
        }
        num.times {
            day_login.find(query).skip(it * BATCH_SIZE).limit(BATCH_SIZE)
                    .toArray().each { BasicDBObject obj ->
                def user_id = obj.get("user_id") as Integer
                def uid = obj.get("uid") as String
                last_total_map.add(user_id)
                if (StringUtils.isBlank(uid)) {
                    last_pc_map.add(user_id)
                } else {
                    last_mobile_map.add(user_id)
                }
            }
        }
        def beforeNum = before_total_map.size() as Integer
        def beforePcNum = before_pc_map.size() as Integer
        def beforeMobileNum = before_mobile_map.size() as Integer
        def drainNum = 0, drainPcNum = 0, drainMobileNum = 0, drain = 0, pcDrain = 0, mobileDrain = 0
        if (beforeNum > 0) {
            for (Integer beforeId : before_total_map) {
                if (!last_total_map.contains(beforeId)) {
                    drainNum++
                }
            }
            drain = (drainNum / beforeNum).doubleValue()
        }
        if (beforePcNum > 0) {
            for (Integer beforeId : before_pc_map) {
                if (!last_pc_map.contains(beforeId)) {
                    drainPcNum++
                }
            }
            pcDrain = (drainPcNum / beforePcNum).doubleValue()
        }
        if (beforeMobileNum > 0) {
            for (Integer beforeId : before_mobile_map) {
                if (!last_mobile_map.contains(beforeId)) {
                    drainMobileNum++
                }
            }
            mobileDrain = (drainMobileNum / beforeMobileNum).doubleValue()
        }

        def obj = new BasicDBObject(
                total: [count: last_total_map.size(), drain: drainNum, rate: drain],
                pc: [count: last_pc_map.size(), drain: drainPcNum, rate: pcDrain],
                moblie: [count: last_mobile_map.size(), drain: drainMobileNum, rate: mobileDrain],
                type: 'login',
                timestamp: new SimpleDateFormat("yyyyMM").parse(ym).getTime()
        )
        coll.update(new BasicDBObject(_id: "${ym}_login".toString()), new BasicDBObject('$set', obj), true, false)
    }

    //用户充值流失率统计,从两个月前的数据统计起（14年从7月开始），财务管理-运营数据月报
    static payMonthStatic(int i) {
        def finance_log_DB = mongo.getDB('xy_admin').getCollection('finance_log')
        def channel = mongo.getDB('xy_admin').getCollection('channels')
        def users = mongo.getDB('xy').getCollection('users')
        def pc = channel.find(new BasicDBObject(client: [$in:['1','5','6']])).toArray()*._id
        //println pc
        Calendar cal = getCalendar()
        cal.add(Calendar.MONTH, -i)
        long firstDayOfCurrentMonth = cal.getTimeInMillis()  //当月第一天
        cal.add(Calendar.MONTH, -1);
        long firstDayOfLastMonth = cal.getTimeInMillis()  //上月第一天
        String ym = new Date(firstDayOfLastMonth).format("yyyyMM")
        cal.add(Calendar.MONTH, -1)
        long firstDayOfBefore = cal.getTimeInMillis() //上上个月第一天
        def before_total_map = new HashSet<Integer>(250000)
        def before_pc_map = new HashSet<Integer>(100000)
        def before_mobile_map = new HashSet<Integer>(150000)
        def last_total_map = new HashSet<Integer>(250000)
        def last_pc_map = new HashSet<Integer>(100000)
        def last_mobile_map = new HashSet<Integer>(150000)
        def query = new BasicDBObject([timestamp: [$gte: firstDayOfBefore, $lt: firstDayOfLastMonth], via: [$nin: ['Admin', null]]])
        long count = finance_log_DB.count(query)
        Integer num = count / BATCH_SIZE
        if (count % BATCH_SIZE > 0) {
            num++
        }
        //查询上上个月的充值用户数据
        num.times {
            finance_log_DB.find(query).skip(it * BATCH_SIZE).limit(BATCH_SIZE)
                    .toArray().each { BasicDBObject obj ->
                def user_id = obj.get("user_id") as Integer
                def to_id = obj.get("to_id") as Integer
                def uid = to_id ?: user_id
                def qd = obj.get("qd") as String
                if (to_id != null && !to_id.equals(user_id)) {
                    def tid = users.findOne(new BasicDBObject(_id: to_id), new BasicDBObject(qd: 1))?.get('qd')
                    qd = tid as String ?: 'MM'
                }
                before_total_map.add(uid)
                if (StringUtils.isBlank(qd) || pc.contains(qd)) {
                    before_pc_map.add(uid)
                } else {
                    before_mobile_map.add(uid)
                }
            }
        }
        query = new BasicDBObject([timestamp: [$gte: firstDayOfLastMonth, $lt: firstDayOfCurrentMonth], via: [$ne: 'Admin']])
        count = finance_log_DB.count(query)
        num = count / BATCH_SIZE
        if (count % BATCH_SIZE > 0) {
            num++
        }
        //查询上个月的充值数据包括充值金额、人数，新增充值金额、人数
        def payMap = new HashMap(20000), allCny = 0, pcCny = 0, mobileCny = 0
        num.times {
            finance_log_DB.find(query).skip(it * BATCH_SIZE).limit(BATCH_SIZE)
                    .toArray().each { BasicDBObject obj ->
                def user_id = obj.get("user_id") as Integer
                def to_id = obj.get("to_id") as Integer
                def uid = to_id ?: user_id
                def cny = (obj.get('cny') ?: 0.0d) as Double
                def qd = obj.get("qd") as String
                if (to_id != null && !to_id.equals(user_id)) {
                    def tid = users.findOne(new BasicDBObject(_id: to_id), new BasicDBObject(qd: 1))?.get('qd')
                    qd = tid as String ?: 'MM'
                }
                last_total_map.add(uid)
                allCny = new BigDecimal(allCny + cny).doubleValue()
                if (StringUtils.isBlank(qd) || pc.contains(qd)) {
                    last_pc_map.add(uid)
                    pcCny = new BigDecimal(pcCny + cny).doubleValue()
                } else {
                    last_mobile_map.add(uid)
                    mobileCny = new BigDecimal(mobileCny + cny).doubleValue()
                }
                def uid_cny = payMap.get(uid) ?: 0 as Double
                payMap.put(uid, uid_cny + cny)
            }
        }
        //查询充值人数中新注册充值信息
        def new_pc_count = 0 as Integer, new_pc_cny = 0 as Double
        def new_mobile_count = 0 as Integer, new_mobile_cny = 0 as Double
        if (payMap.size() > 0) {
            def q = new BasicDBObject(_id: [$in: last_total_map], timestamp: [$gte: firstDayOfLastMonth, $lt: firstDayOfCurrentMonth])
            users.find(q, new BasicDBObject(_id: 1)).toArray().each { BasicDBObject obj ->
                def _id = obj.get('_id') as Integer
                if (last_pc_map.contains(_id)) {
                    new_pc_count++
                    new_pc_cny = new_pc_cny + (payMap.get(_id) as Double)
                } else {
                    new_mobile_count++
                    new_mobile_cny = new_mobile_cny + (payMap.get(_id) as Double)
                }
            }
        }
        def beforeNum = before_total_map.size() as Integer
        def beforePcNum = before_pc_map.size() as Integer
        def beforeMobileNum = before_mobile_map.size() as Integer
        def drainNum = 0, drainPcNum = 0, drainMobileNum = 0, drain = 0, pcDrain = 0, mobileDrain = 0
        if (beforeNum > 0) {
            for (Integer beforeId : before_total_map) {
                if (!last_total_map.contains(beforeId)) {
                    drainNum++
                }
            }
            drain = (drainNum / beforeNum).doubleValue()
        }
        if (beforePcNum > 0) {
            for (Integer beforeId : before_pc_map) {
                if (!last_pc_map.contains(beforeId)) {
                    drainPcNum++
                }
            }
            pcDrain = (drainPcNum / beforePcNum).doubleValue()
        }
        if (beforeMobileNum > 0) {
            for (Integer beforeId : before_mobile_map) {
                if (!last_mobile_map.contains(beforeId)) {
                    drainMobileNum++
                }
            }
            mobileDrain = (drainMobileNum / beforeMobileNum).doubleValue()
        }

        def obj = new BasicDBObject(
                total: [cny: allCny, count: last_total_map.size(), drain: drainNum, rate: drain],
                pc: [cny: pcCny, count: last_pc_map.size(), new_cny: new_pc_cny, new_count: new_pc_count, drain: drainPcNum, rate: pcDrain],
                moblie: [cny: mobileCny, count: last_mobile_map.size(), new_cny: new_mobile_cny, new_count: new_mobile_count, drain: drainMobileNum, rate: mobileDrain],
                type: 'allpay',
                timestamp: new SimpleDateFormat("yyyyMM").parse(ym).getTime()
        )
        coll.update(new BasicDBObject(_id: "${ym}_pay".toString()), new BasicDBObject('$set', obj), true, false)
    }

    //商务渠道每月基础数据统计，对应联运管理-渠道投入回报统计
    static qdMonthStatic(int i) {
        Calendar cal = getCalendar()
        cal.add(Calendar.MONTH, -i)
        long firstDayOfCurrentMonth = cal.getTimeInMillis()  //当月第一天
        cal.add(Calendar.MONTH, -1)
        long firstDayOfLastMonth = cal.getTimeInMillis()  //上月第一天
        def stat_channel = mongo.getDB('xy_admin').getCollection('stat_channels')
        def finance_log = mongo.getDB('xy_admin').getCollection('finance_log')
        def users = mongo.getDB('xy').getCollection('users')
        def monthStr = cal.getTime().format('yyyyMM_')
        def channelDB = mongo.getDB('xy_admin').getCollection('channels')
        def channels = [:]
        channelDB.find(new BasicDBObject()).toArray().each {BasicDBObject obj ->
            channels.put(obj.get('_id') as String, obj)
        }
        //上月激活、扣量激活与注册人数
        stat_channel.aggregate(
                $$('$match', [timestamp: [$gte: firstDayOfLastMonth, $lt: firstDayOfCurrentMonth]]),
                $$('$project', [qd: '$qd', active: '$active', reg: '$reg', cpa1: '$cpa1']),
                $$('$group', [_id: '$qd', active: [$sum: '$active'], reg: [$sum: '$reg'], cpa1: [$sum: '$cpa1']])
        ).results().each { BasicDBObject obj ->
            def qd = obj.remove('_id') as String
            if (StringUtils.isNotBlank(qd)) {
                def channel = channels.get(qd)
                def parent_qd = channel?.getAt('parent_qd') as String
                def update = obj
                update.append('type', 'channel')
                update.append('qd', qd)
                update.append('client', ((channel?.getAt('client'))?:'2'))
                if(StringUtils.isNotBlank(parent_qd)) update.append('parent_qd', parent_qd)
                update.append('timestamp', firstDayOfLastMonth)
                coll.update($$('_id', "${monthStr}${qd}".toString()), update, true, false)
            }
        }
        def start = firstDayOfLastMonth
        def end = firstDayOfCurrentMonth
        //更新（N月注册在下j月的充值信息）
        for (int j = 0; j < 7; j++) {
            def paykey = (j == 0 ? 'pay_retention' : "pay_retention${j}".toString())
            def month = new Date(start).format('yyyyMM_')
            stat_channel.distinct('qd', new BasicDBObject(timestamp: [$gte: start, $lt: end])).each { String qd ->
                if (StringUtils.isNotBlank(qd)) {
                    def uids = users.find(new BasicDBObject(qd: qd, timestamp: [$gte: start, $lt: end]), new BasicDBObject(_id: 1)).toArray()*._id
                    if (uids.size() > 0) {
                        def uset = new HashSet(uids.size())
                        def cny = new BigDecimal(0)
                        finance_log.find(new BasicDBObject([$or      : [[user_id: [$in: uids]], [to_id: [$in: uids]]],
                                                            via      : [$ne: 'Admin'],
                                                            timestamp: [$gte: firstDayOfLastMonth, $lt: firstDayOfCurrentMonth]
                        ])).toArray().each { BasicDBObject obj ->
                            uset.add(obj['to_id'] ?: obj['user_id'])
                            if (obj['cny'] != null) {
                                cny = cny.add(new BigDecimal(((Double) obj['cny']).doubleValue()))
                            }
                        }
                        if (uset.size() > 0 || cny > 0) {
                            def setVal = $$("${paykey}.cny".toString(), cny.doubleValue())
                            setVal.append("${paykey}.pay".toString(), uset.size())
                            coll.update($$('_id', "${month}${qd}".toString()), new BasicDBObject($set: setVal))
                        }
                    }
                }
            }
            end = start
            cal.add(Calendar.MONTH, -1)
            start = cal.getTimeInMillis()
        }
    }

    //商务渠道(父渠道)每月基础数据统计
    static parentQdMonthStatic(int i) {
        Calendar cal = getCalendar()
        cal.add(Calendar.MONTH, -i)
        long firstDayOfCurrentMonth = cal.getTimeInMillis()  //当月第一天
        cal.add(Calendar.MONTH, -1)
        long firstDayOfLastMonth = cal.getTimeInMillis()  //上月第一天
        def channel = mongo.getDB('xy_admin').getCollection('channels')
        //查询所有有子渠道的父渠道信息
        def list = channel.distinct('parent_qd',new BasicDBObject(parent_qd:[$ne:null]))

        list.each { String parent ->
            //查询子渠道
            def child = channel.find(new BasicDBObject(parent_qd: "${parent}".toString())).toArray()*._id
            //更新（N月注册在上月的充值信息）
            def start = firstDayOfLastMonth
            def end = firstDayOfCurrentMonth
            cal = getCalendar()
            cal.add(Calendar.MONTH, -i - 1)
            for (int j = 0; j < 7; j++) {
                def paykey = (j == 0 ? 'pay_retention' : "pay_retention${j}".toString())
                def month = new Date(start).format('yyyyMM_')
                coll.aggregate(
                        $$('$match', [type: 'channel', qd: [$in: child], timestamp: [$gte: start, $lt: end]]),
                        $$('$project', [qd: '$qd', cny: "\$${paykey}.cny".toString(), pay: "\$${paykey}.pay".toString()]),
                        $$('$group', [_id: '', cny: [$sum: '$cny'], pay: [$sum: '$pay']])
                ).results().each { BasicDBObject obj ->
                    def cny = obj.get('cny') ?: 0
                    def pay = obj.get('pay') ?: 0
                    def setVal = $$("${paykey}.cny".toString(), cny)
                    setVal.append("${paykey}.pay".toString(), pay)
                    coll.update($$('_id', "${month}${parent}".toString()), new BasicDBObject($set: setVal))
                }
                end = start
                cal.add(Calendar.MONTH, -1)
                start = cal.getTimeInMillis()
            }
        }
    }

    private static Calendar getCalendar() {
        Calendar cal = Calendar.getInstance()//获取当前日期

        cal.set(Calendar.DAY_OF_MONTH, 1)//设置为1号,当前日期既为本月第一天
        cal.set(Calendar.HOUR_OF_DAY, 0)
        cal.set(Calendar.MINUTE, 0)
        cal.set(Calendar.SECOND, 0)
        cal.set(Calendar.MILLISECOND, 0)

        return cal
    }


    public static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    static void main(String[] args) {
        try{
            long l = System.currentTimeMillis()
            loginMonthStatic(0)
            println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   StaticsEveryMonth:loginMonthStatic, cost  ${System.currentTimeMillis() - l} ms"
            Thread.sleep(1000L)
            payMonthStatic(0)
            println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   StaticsEveryMonth:payMonthStatic, cost  ${System.currentTimeMillis() - l} ms"
            Thread.sleep(1000L)
            qdMonthStatic(0)
            println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   StaticsEveryMonth:qdMonthStatic, cost  ${System.currentTimeMillis() - l} ms"
            Thread.sleep(1000L)
            parentQdMonthStatic(0)
            println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   StaticsEveryMonth:parentQdMonthStatic, cost  ${System.currentTimeMillis() - l} ms"
        }catch (Exception e){
            println "Exception : " + e
        }


    }

}

