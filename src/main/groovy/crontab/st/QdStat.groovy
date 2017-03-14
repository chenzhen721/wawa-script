#!/usr/bin/env groovy
package crontab.st

import com.mongodb.BasicDBObject
import com.mongodb.DBObject
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
])
import com.mongodb.Mongo
import com.mongodb.MongoURI

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

    static mongo = new Mongo(new MongoURI(getProperties('mongo.uri', 'mongodb://192.168.31.231:20000,192.168.31.236:20000,192.168.31.231:20001/?w=1&slaveok=true') as String))
    static chatLog = mongo.getDB('chat_log')
    static DAY_MILLON = 24 * 3600 * 1000L
    static long zeroMill = new Date().clearTime().getTime()
    static Long yesTday = zeroMill - DAY_MILLON

    static statics(int i) {
        def coll = mongo.getDB('xy_admin').getCollection('stat_channels')
        def users = mongo.getDB('xy').getCollection('users')
        def finance_log = mongo.getDB('xy_admin').getCollection('finance_log')
        def stat_daily = mongo.getDB('xy_admin').getCollection('stat_daily')
        def day_login = mongo.getDB("xylog").getCollection("day_login")

        Long begin = yesTday - i * DAY_MILLON
        def timeBetween = [$gte: begin, $lt: begin + DAY_MILLON]
        // 查询30天新充值用户
        def new_payed_user = new ArrayList(2000)
        stat_daily.find($$(type: "allpay", timestamp: [$gte: begin - 30 * DAY_MILLON, $lt: begin + DAY_MILLON]), $$(first_pay: 1))
                .toArray().each { BasicDBObject obj ->
            def uids = (obj.get('first_pay') as List) ?: []
            new_payed_user.addAll(uids)
        }

        def chat_query = $$('timestamp': timeBetween)
        def chat_field = $$('user_id': 1)
        def currentMonth = new Date(begin).format('yyyy/M')
        def chatList = chatLog.getCollection(currentMonth).find(chat_query, chat_field).toArray()

        mongo.getDB('xy_admin').getCollection('channels').find(
                $$("_id", [$ne: null]), $$("reg_discount", 1).append("child_qd", 1).append("sence_id", 1)
        ).toArray().each { BasicDBObject channnel ->
            def cId = channnel.removeField("_id")
            //String sence_id = channnel.removeField("sence_id") as String
            def user_query = $$(qd: cId, timestamp: timeBetween)
            def YMD = new Date(begin).format("yyyyMMdd")
            def st = $$(_id: "${YMD}_${cId}" as String, qd: cId, timestamp: begin)
            def regUsers = users.find(user_query, $$('status', 1))*.get('_id')

            def regNum = regUsers.size()
            st.append("regs", regUsers).append("reg", regNum)

            // 统计该渠道下的发言率
            def current_speechs = 0
            def first_speechs = 0
            chatList.each {
                BasicDBObject obj ->
                    def userId = obj['user_id'] as Integer
                    def user = users.findOne($$('_id': userId, 'qd': cId), $$('_id': 1))
                    if (user != null) {
                        current_speechs += 1
                        regUsers.find {
                            if (it == userId)
                                first_speechs += 1
                        }
                    }
            }
            st.append('speechs', current_speechs)
            // 新增的发言率
            st.append('first_speechs', first_speechs)

            // 统计该渠道下的新增的消费率
            def betDB = mongo.getDB('game_log').getCollection('user_bet')
            def gameBetList = betDB.distinct('user_id', $$('timestamp': timeBetween, 'user_id': ['$in': regUsers])) as String[]
            def room_cost_db = mongo.getDB('xylog').getCollection('room_cost')
            def roomCostList = room_cost_db.distinct('session._id', $$('timestamp': timeBetween, 'session._id': ['$in': regUsers as String[]]))

            def first_cost_list = new HashSet()
            first_cost_list.addAll(gameBetList)
            first_cost_list.addAll(roomCostList)

            st.append('first_cost', first_cost_list.size())

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
                    $$('$match', [via: [$ne: 'Admin'], qd: cId, timestamp: timeBetween]),
                    $$('$project', [cny: '$cny', user_id: '$user_id']),
                    $$('$group', [_id: null, cny: [$sum: '$cny'], count: [$sum: 1], pays: [$addToSet: '$user_id']])
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
            coll.find($$(qd: cId, timestamp: [$gte: begin - 30 * DAY_MILLON, $lt: begin + DAY_MILLON]),
                    $$(pays: 1, regs: 1)).toArray().each { BasicDBObject obj ->
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
                    $$('$match', [user_id: [$in: payed_user], timestamp: [$gte: begin - 30 * DAY_MILLON, $lt: begin + DAY_MILLON]]),
                    $$('$project', [_id: '$user_id', cny: '$cny']),
                    $$('$group', [_id: '$_id', cny: [$sum: '$cny']])
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

            coll.update($$(_id: st.remove('_id') as String), $$($set: st), true, false)
            //注册次日留存
            Long before = begin - DAY_MILLON
            def before_day = new Date(before)
            def before_ymd = before_day.format('yyyyMMdd')
            //查询前一天的注册用户
            def beforeObj = coll.findOne($$(_id: "${before_ymd}_${cId}".toString()), $$(regs: 1))
            def regs = (beforeObj?.get('regs') as List) ?: []
            def reg_retention = day_login.count($$(user_id: [$in: regs], timestamp: [$gte: begin, $lt: begin + DAY_MILLON]))
            coll.update($$(_id: "${before_ymd}_${cId}".toString()), $$($set: [reg_retention: reg_retention]))

            //查询当月注册用户截止到当前日期的充值信息
            def cal = Calendar.getInstance()
            cal.setTimeInMillis(begin)
            cal.set(Calendar.DAY_OF_MONTH, 1)//设置为1号,当前日期既为本月第一天
            cal.set(Calendar.HOUR_OF_DAY, 0)
            cal.set(Calendar.MINUTE, 0)
            cal.set(Calendar.SECOND, 0)
            cal.set(Calendar.MILLISECOND, 0)
            def month_begin = cal.getTimeInMillis()
            def month_reg = users.find($$(qd: cId, timestamp: [$gte: month_begin, $lt: begin + DAY_MILLON])).toArray()*._id
            def cny = 0, uset = new HashSet()
            finance_log.aggregate(
                    $$('$match', [
                            $or      : [[user_id: [$in: month_reg]], [to_id: [$in: month_reg]]],
                            via      : [$ne: 'Admin'],
                            timestamp: [$gte: month_begin, $lt: begin + DAY_MILLON]]),
                    $$('$project', [cny: '$cny', user_id: '$user_id', to_id: '$to_id']),
                    $$('$group', [_id: [fid: '$user_id', tid: '$to_id'], cny: [$sum: '$cny'], uids: [$addToSet: '$to_id']])
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
            coll.update($$(_id: "${YMD}_${cId}".toString()),
                    $$($set: [month_cny: cny, month_pay: uset.size()]))
        }

    }

    //父渠道信息汇总
    static parentQdstatic(int i) {
        def channel_db = mongo.getDB('xy_admin').getCollection('channels')
        def stat_channels = mongo.getDB('xy_admin').getCollection('stat_channels')
        def channels = channel_db.find($$(parent_qd: [$ne: null]), $$(parent_qd: 1)).toArray()
        Map<String, DBObject> parentMap = new HashMap<String, DBObject>()
        for (DBObject obj : channels) {
            String parent_id = obj.get("parent_qd") as String
            parentMap.put(parent_id, obj)
        }

        for (String key : parentMap.keySet()) {
            DBObject obj = parentMap.get(key)
            Long begin = yesTday - i * DAY_MILLON
            def parent_id = obj.get("parent_qd") as String
            def childqds = channel_db.find($$(parent_qd: parent_id), $$(_id: 1)).toArray()
            DBObject query = $$('qd', [$in: childqds.collect {
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
            def st = $$(_id: "${YMD}_${parent_id}" as String, qd: parent_id, timestamp: begin)
            def setObject = $$(
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
//            def setObject = $$(qd: parent_id, timestamp: begin)
            stat_channels.findAndModify(st, null, null, false,
                    $$($set: setObject), true, true)
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
                $$('$match', [timestamp: timeBetween, status: 1]),
                $$('$project', [from: '$from']),
                $$('$group', [_id: '$from', count: [$sum: 1]])
        ).results().iterator()
        def _id = new Date(begin).format("yyyyMMdd")
        def aso = $$(_id: _id, timestamp: begin);
        def data = new ArrayList();
        if (iter.hasNext()) {
            def obj = iter.next()
            data.add([(obj.get('_id') as String): obj.get('count') as Integer] as Map)
        }
        aso.append('data', data)
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