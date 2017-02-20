#!/usr/bin/env groovy
package crontab.st

import com.mongodb.BasicDBObject
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
])
import com.mongodb.Mongo
import com.mongodb.DBObject
import com.mongodb.MongoURI
import groovy.json.JsonSlurper
import org.apache.commons.lang.StringUtils

/**
 * 每天统计一份数据
 *
 * date: 13-2-28 下午2:46
 * @author: yangyang.cong@ttpod.com
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

    static mongo = new Mongo(new MongoURI(getProperties('mongo.uri', 'mongodb://192.168.31.246:27017/?w=1') as String))

    static DAY_MILLON = 24 * 3600 * 1000L
    static long zeroMill = new Date().clearTime().getTime()
    static Long yesTday = zeroMill - DAY_MILLON

    static statics(int i) {
        def coll = mongo.getDB('xy_admin').getCollection('stat_channels')
        def users = mongo.getDB('xy').getCollection('users')
        def finance_log = mongo.getDB('xy_admin').getCollection('finance_log')
        def trade_logs = mongo.getDB('xylog').getCollection('trade_logs')
        def room_cost = mongo.getDB('xylog').getCollection('room_cost')
        def stat_daily = mongo.getDB('xy_admin').getCollection('stat_daily')
        def day_login = mongo.getDB("xylog").getCollection("day_login")
        def weixin_event_logs = mongo.getDB("xy_union").getCollection("weixin_event_logs")
        def ria_event_logs = mongo.getDB("xylog").getCollection("ria_event_logs")

        Long begin = yesTday - i * DAY_MILLON
        def timeBetween = [$gte: begin, $lt: begin + DAY_MILLON]
        // 查询30天新充值用户
        def new_payed_user = new ArrayList(2000)
        stat_daily.find(new BasicDBObject(type: "allpay", timestamp: [$gte: begin - 30 * DAY_MILLON, $lt: begin + DAY_MILLON]), new BasicDBObject(first_pay: 1))
                .toArray().each { BasicDBObject obj ->
            def uids = (obj.get('first_pay') as List) ?: []
            new_payed_user.addAll(uids)
        }

        mongo.getDB('xy_admin').getCollection('channels').find(
                new BasicDBObject("_id", [$ne: null]), new BasicDBObject("reg_discount", 1).append("child_qd", 1).append("sence_id",1)
        ).toArray().each { BasicDBObject channnel ->
            def cId = channnel.removeField("_id")
            //String sence_id = channnel.removeField("sence_id") as String
            def user_query = new BasicDBObject(qd: cId, timestamp: timeBetween)
            def YMD = new Date(begin).format("yyyyMMdd")
            def st = new BasicDBObject(_id: "${YMD}_${cId}" as String, qd: cId, timestamp: begin)
            def regUsers = users.find(user_query, new BasicDBObject('status', 1))*.get('_id')
            def regNum = regUsers.size()
            st.append("regs", regUsers).append("reg", regNum)
            //设置注册扣量cpa2
            def discountMap = channnel.removeField("reg_discount") as Map
            if (discountMap != null && discountMap.size() > 0) {
                def cpa = null
                def keyList = discountMap.keySet().toArray().sort { String a, String b ->
                    Long.parseLong(b) <=> Long.parseLong(a)
                }
                for (it in keyList) {
                    if (begin >= (it as Long)) {
                        def discount = discountMap.get(it) as Integer
                        cpa = new BigDecimal((double) (regNum * discount / 100)).toInteger()
                        break
                    }
                }
                if (cpa != null) st.append("cpa2", cpa)
            }

            //优化后
            def iter = finance_log.aggregate(
                    new BasicDBObject('$match', [via: [$ne: 'Admin'], qd: cId, timestamp: timeBetween]),
                    new BasicDBObject('$project', [cny: '$cny', user_id: '$user_id']),
                    new BasicDBObject('$group', [_id: null, cny: [$sum: '$cny'], count: [$sum: 1], pays: [$addToSet: '$user_id']])
            ).results().iterator()

            if (iter.hasNext()) {
                def obj = iter.next()
                obj.removeField('_id')
                st.putAll(obj)
                st['pay'] = st['pays'].size()
                if (!channnel.isEmpty()) {
                    st.putAll(channnel)
                }
            }

            //查询该渠道下30天的新充值用户
            def payed_user = new HashSet(), reg_user = new HashSet()
            coll.find(new BasicDBObject(qd: cId, timestamp: [$gte: begin - 30 * DAY_MILLON, $lt: begin + DAY_MILLON]),
                    new BasicDBObject(pays: 1, regs: 1)).toArray().each { BasicDBObject obj ->
                def pays = (obj.get('pays') as List) ?: []
                def regs = (obj.get('regs') as List) ?: []
                payed_user.addAll(pays)
                payed_user.addAll(st['pays'] ?: [])
                reg_user.addAll(regs)
                reg_user.addAll(regUsers)
            }
            //每个用户30天内的充值金额
            def total = 0, reg_total = 0, pay_user = new HashSet(), pay_reg = new HashSet()
            finance_log.aggregate(
                    new BasicDBObject('$match', [user_id: [$in: payed_user], timestamp: [$gte: begin - 30 * DAY_MILLON, $lt: begin + DAY_MILLON]]),
                    new BasicDBObject('$project', [_id: '$user_id', cny: '$cny']),
                    new BasicDBObject('$group', [_id: '$_id', cny: [$sum: '$cny']])
            ).results().each { BasicDBObject obj ->
                def uid = obj.get('_id') as Integer
                def cny = obj.get('cny') as Double
                total += cny
                pay_user.add(uid)
                if (reg_user.contains(uid)) {
                    reg_total += cny
                    pay_reg.add(uid)
                }
            }
            st.put('reg_pay_cny', reg_total)
            st.put('reg_user30', reg_user.size())
            st.put('reg_pay_user', pay_reg.size())
            st.put('first_pay_cny', total)
            st.put('first_pay_user', pay_user.size())


            coll.update(new BasicDBObject(_id: st.remove('_id') as String), new BasicDBObject($set: st), true, false)
            //注册次日留存
            Long before = begin - DAY_MILLON
            def before_day = new Date(before)
            def before_ymd = before_day.format('yyyyMMdd')
            //查询前一天的注册用户
            def beforeObj = coll.findOne(new BasicDBObject(_id: "${before_ymd}_${cId}".toString()), new BasicDBObject(regs: 1))
            def regs = (beforeObj?.get('regs') as List) ?: []
            def reg_retention = day_login.count(new BasicDBObject(user_id: [$in: regs], timestamp: [$gte: begin, $lt: begin + DAY_MILLON]))
            coll.update(new BasicDBObject(_id: "${before_ymd}_${cId}".toString()), new BasicDBObject($set: [reg_retention: reg_retention]))

            //查询当月注册用户截止到当前日期的充值信息
            def cal = Calendar.getInstance()
            cal.setTimeInMillis(begin)
            cal.set(Calendar.DAY_OF_MONTH, 1)//设置为1号,当前日期既为本月第一天
            cal.set(Calendar.HOUR_OF_DAY, 0)
            cal.set(Calendar.MINUTE, 0)
            cal.set(Calendar.SECOND, 0)
            cal.set(Calendar.MILLISECOND, 0)
            def month_begin = cal.getTimeInMillis()
            def month_reg = users.find(new BasicDBObject(qd: cId, timestamp: [$gte: month_begin, $lt: begin + DAY_MILLON])).toArray()*._id
            def cny = 0, uset = new HashSet()
            finance_log.aggregate(
                    new BasicDBObject('$match', [
                            $or      : [[user_id: [$in: month_reg]], [to_id: [$in: month_reg]]],
                            via      : [$ne: 'Admin'],
                            timestamp: [$gte: month_begin, $lt: begin + DAY_MILLON]]),
                    new BasicDBObject('$project', [cny: '$cny', user_id: '$user_id', to_id: '$to_id']),
                    new BasicDBObject('$group', [_id: [fid: '$user_id', tid: '$to_id'], cny: [$sum: '$cny'], uids: [$addToSet: '$to_id']])
            ).results().each { BasicDBObject obj ->
                cny += obj.get('cny') as Double
                def id = obj.remove('_id') as Map
                def fid = id.get('fid') as Integer
                def tid = id.get('tid') as Integer
                if (tid != null) {
                    uset.add(tid)
                } else {
                    if (fid != null) {
                        uset.add(fid)
                    }
                }
            }
            coll.update(new BasicDBObject(_id: "${YMD}_${cId}".toString()),
                    new BasicDBObject($set: [month_cny: cny, month_pay: uset.size()]))
        }

    }


    //父渠道信息汇总
    static parentQdstatic(int i) {
        def channel_db = mongo.getDB('xy_admin').getCollection('channels')
        def stat_channels = mongo.getDB('xy_admin').getCollection('stat_channels')
        def channels = channel_db.find(new BasicDBObject(parent_qd: [$ne: null]), new BasicDBObject(parent_qd: 1)).toArray()
        Map<String, DBObject> parentMap = new HashMap<String, DBObject>()
        for (DBObject obj : channels) {
            String parent_id = obj.get("parent_qd") as String
            parentMap.put(parent_id, obj)
        }

        for (String key : parentMap.keySet()) {
            DBObject obj = parentMap.get(key)
            Long begin = yesTday - i * DAY_MILLON
            def parent_id = obj.get("parent_qd") as String
            def childqds = channel_db.find(new BasicDBObject(parent_qd: parent_id), new BasicDBObject(_id: 1)).toArray()
            DBObject query = new BasicDBObject('qd', [$in: childqds.collect {
                ((Map) it).get('_id').toString()
            }]).append("timestamp", begin)
            def stat_child_channels = stat_channels.find(query).toArray()
            Integer payNum = 0
            Integer regNum = 0
            Integer cny = 0
            Integer month_pay = 0
            Double month_cny = 0
            Integer count = 0
            Integer daylogin = 0
            Integer day7login = 0
            Integer day30login = 0
            Integer stay1 = 0
            Integer stay3 = 0
            Integer stay7 = 0
            Integer stay30 = 0
            Integer cpa2 = 0
            int size = stat_child_channels.size()
            //println "stat_child_channels.size-------------->:$size"
            for (DBObject myObj : stat_child_channels) {
                payNum += (myObj.get("pay") ?: 0) as Integer
                regNum += (myObj.get("reg") ?: 0) as Integer
                cny += (myObj.get("cny") ?: 0) as Integer
                month_pay += (myObj.get("month_pay") ?: 0) as Integer
                month_cny += (myObj.get("month_cny") ?: 0) as Double
                count += (myObj.get("count") ?: 0) as Integer
                daylogin += (myObj.get("daylogin") ?: 0) as Integer
                day7login += (myObj.get("day7login") ?: 0) as Integer
                day30login += (myObj.get("day30login") ?: 0) as Integer
                def myStay = myObj.get("stay") as Map
                if (myStay != null) {
                    stay1 += (myStay.get("1_day") ?: 0) as Integer
                    stay3 += (myStay.get("3_day") ?: 0) as Integer
                    stay7 += (myStay.get("7_day") ?: 0) as Integer
                    stay30 += (myStay.get("30_day") ?: 0) as Integer
                }
                Integer currentCpa2 = (myObj.get("cpa2") != null) ? myObj.get("cpa2") as Integer : 0
                cpa2 += currentCpa2

            }
            def YMD = new Date(begin).format("yyyyMMdd")
            def st = new BasicDBObject(_id: "${YMD}_${parent_id}" as String, qd: parent_id, timestamp: begin)
            def setObject = new BasicDBObject(
                    pay: payNum,
                    reg: regNum,
                    cny: cny,
                    month_pay: month_pay,
                    month_cny: month_cny,
                    count: count,
                    daylogin: daylogin,
                    day7login: day7login,
                    day30login: day30login,
                    'stay.1_day': stay1,
                    'stay.3_day': stay3,
                    'stay.7_day': stay7,
                    'stay.30_day': stay30,
                    cpa2: cpa2,
                    qd: parent_id,
                    timestamp: begin
            )
//            def setObject = new BasicDBObject(qd: parent_id, timestamp: begin)
            stat_channels.findAndModify(st, null, null, false,
                    new BasicDBObject($set: setObject), true, true)
        }

    }

    /**
     * ASO优化统计 (App Store Optimization)
     * @param i
     * @return
     */
    static ASOstatics(int i) {
        def stat_aso = mongo.getDB('xy_admin').getCollection('stat_aso')
        def ad_logs = mongo.getDB('xylog').getCollection('ad_logs')
        Long begin = yesTday - i * DAY_MILLON
        def timeBetween = [$gte: begin, $lt: begin + DAY_MILLON]
        def iter = ad_logs.aggregate(
                new BasicDBObject('$match', [timestamp: timeBetween, status:1]),
                new BasicDBObject('$project', [from: '$from']),
                new BasicDBObject('$group', [_id: '$from', count: [$sum: 1]])
        ).results().iterator()
        def _id = new Date(begin).format("yyyyMMdd")
        def aso = new BasicDBObject(_id:_id, timestamp:begin);
        def data = new ArrayList();
        if (iter.hasNext()) {
            def obj = iter.next()
            data.add([(obj.get('_id') as String) : obj.get('count') as Integer] as Map)
        }
        aso.append('data',data)
        stat_aso.save(aso)
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

    static Integer begin_day = 0;

    static void main(String[] args) {
        long l = System.currentTimeMillis()
        long begin = l

        //渠道统计
        l = System.currentTimeMillis()
        statics(begin_day)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${QdStat.class.getSimpleName()},statics cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //父级渠道的统计
        l = System.currentTimeMillis()
        parentQdstatic(begin_day)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${QdStat.class.getSimpleName()},parentQdstatic cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //ASO优化统计 (App Store Optimization) mygreen
        l = System.currentTimeMillis()
        ASOstatics(begin_day)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${QdStat.class.getSimpleName()},ASOstatics cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //落地定时执行的日志
        jobFinish(begin)

    }

    /**
     * 标记任务完成  用于运维监控
     * @return
     */
    private static jobFinish(Long begin){
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
        def update = new BasicDBObject(timer_name: timerName, cost_total: totalCost, cat: 'day', unit: 'ms', timestamp: tmp)
        timerLogsDB.findAndModify(new BasicDBObject('_id', id), null, null, false, new BasicDBObject('$set', update), true, true)
    }

}