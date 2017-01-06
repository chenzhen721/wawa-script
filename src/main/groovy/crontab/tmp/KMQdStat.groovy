#!/usr/bin/env groovy
package crontab.tmp

import com.mongodb.BasicDBObject
import com.mongodb.DB
import com.mongodb.DBCollection
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
])
import com.mongodb.Mongo
import com.mongodb.DBObject
import com.mongodb.MongoURI
import com.mongodb.QueryBuilder
import groovy.json.JsonSlurper
import org.apache.commons.lang.StringUtils

import java.text.SimpleDateFormat
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.regex.Pattern

/**
 * 渠道统计数据恢复
 */
class KMQdStat {
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
    static historyMongo = new Mongo(new MongoURI(getProperties('mongo_history.uri', 'mongodb://192.168.31.246:27017/?w=1') as String))

    static DAY_MILLON = 24 * 3600 * 1000L

    static long zeroMill = new Date().clearTime().getTime()
    static Long yesTday = zeroMill - DAY_MILLON
//    static String YMD = new Date(yesTday).format("yyyyMMdd")

    static DB historyDB = historyMongo.getDB('xylog_history')
    static DBCollection finance_log_DB = mongo.getDB('xy_admin').getCollection('finance_log')
    static DBCollection day_login_history = historyDB.getCollection('day_login_history')
    static room_cost = mongo.getDB('xylog').getCollection('room_cost')
    static lives = mongo.getDB('xylog').getCollection('room_edit')
    static stat_lives = mongo.getDB('xy_admin').getCollection('stat_lives')
    static DBCollection stat_daily = mongo.getDB('xy_admin').getCollection('stat_daily')

    //TODO 快播渠道用户不准确特殊处理，以后移除
    static updateAllKmUser() {
        def users = mongo.getDB('xy').getCollection('users')
        QueryBuilder query = QueryBuilder.start()
        Pattern pattern = Pattern.compile("^km-*", Pattern.CASE_INSENSITIVE)
        query.and("tuid").regex(pattern)
        query.and("qd").is(null)
        Long begin = zeroMill
        query.and(new BasicDBObject('timestamp', [$lte: begin]))
        def km_users = users.find(query.get()).limit(30000).toArray()*.get('_id')
        def i = 1;
        while (km_users != null && km_users.size() > 0 && ++i < 50) {
            users.update(new BasicDBObject(_id: [$in: km_users]),
                    new BasicDBObject('$set', new BasicDBObject("qd": "f101")), false, true);
            km_users = null;
            km_users = users.find(query.get()).limit(30000).toArray()*.get('_id');
        }
    }

    static statics(int i) {
        def coll = mongo.getDB('xy_admin').getCollection('stat_channels')

        def users = mongo.getDB('xy').getCollection('users')
        def finance_log = mongo.getDB('xy_admin').getCollection('finance_log')

        Long begin = zeroMill - i * DAY_MILLON
        def timeBetween = [$gte: begin, $lt: begin + DAY_MILLON]
        println new Date(begin).format("yyyyMMdd")
        //TODO 剔除快播 f101
        mongo.getDB('xy_admin').getCollection('channels').find(
                new BasicDBObject("_id", [$nin: ["f101"]]), new BasicDBObject("active_discount", 1).append("reg_discount", 1).append("child_qd", 1)
        ).toArray().each { BasicDBObject channnel ->
            def cId = channnel.removeField("_id")
            def user_query = new BasicDBObject(qd: cId, timestamp: timeBetween)
            def YMD = new Date(begin).format("yyyyMMdd")
            def st = new BasicDBObject(_id: "${YMD}_${cId}" as String, qd: cId, timestamp: begin)
            def regUsers = users.find(user_query, new BasicDBObject('status', 1))*.get('_id')
            st.append("regs", regUsers).append("reg", regUsers.size())

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
            def _id = st['_id']
            st.removeField('_id')
            coll.update(new BasicDBObject('_id', _id), new BasicDBObject('$set', st), true, false)

        }

    }

    static staticsKM(int i) {
        def coll = mongo.getDB('xy_admin').getCollection('stat_channels')
        def finance_log = mongo.getDB('xy_admin').getCollection('finance_log')
        def users = mongo.getDB('xy').getCollection('users')

        Long begin = zeroMill - i * DAY_MILLON
        def timeBetween = [$gte: begin, $lt: begin + DAY_MILLON]
        def YMD = new Date(begin).format("yyyyMMdd")
        def cId = "f101"
        def st = new BasicDBObject(_id: "${YMD}_${cId}" as String, qd: cId, timestamp: begin)
        def user_query = new BasicDBObject(qd: cId, timestamp: timeBetween)
        //def regUserCount =  users.count(user_query)
        def regUsers = users.find(user_query, new BasicDBObject('status', 1))*.get('_id')
        st.append("regs", regUsers).append("reg", regUsers.size())
        //st.append("reg",regUserCount)
        def iter = finance_log.aggregate(
                new BasicDBObject('$match', ['via': 'km-', timestamp: timeBetween]),
                new BasicDBObject('$project', [cny: '$cny', user_id: '$user_id']),
                new BasicDBObject('$group', [_id: null, cny: [$sum: '$cny'], count: [$sum: 1], pays: [$addToSet: '$user_id']])
        ).results().iterator()
        if (iter.hasNext()) {
            def obj = iter.next()
            obj.removeField('_id')
            st.putAll(obj)
            st['pay'] = st['pays'].size()
        }
        //coll.save(st)
        def _id = st['_id']
        st.removeField('_id')
        coll.update(new BasicDBObject('_id', _id), new BasicDBObject('$set', st), true, false)
    }

    static parentQdstatic(int i) {
        def channel_db = mongo.getDB('xy_admin').getCollection('channels')
        def stat_channels = mongo.getDB('xy_admin').getCollection('stat_channels')
        def channels = channel_db.find(new BasicDBObject(parent_qd: [$exists: true]), new BasicDBObject(parent_qd: 1)).toArray()
        Map<String, DBObject> parentMap = new HashMap<String, DBObject>()
        for (DBObject obj : channels) {
            String parent_id = obj.get("parent_qd") as String
            parentMap.put(parent_id, obj)
        }

        for (String key : parentMap.keySet()) {
            DBObject obj = parentMap.get(key)
            Long begin = zeroMill - i * DAY_MILLON
            def parent_id = obj.get("parent_qd") as String
            println "parent_id-------------->:$parent_id"
            def childqds = channel_db.find(new BasicDBObject(parent_qd: parent_id), new BasicDBObject(_id: 1)).toArray()
            DBObject query = new BasicDBObject('qd', [$in: childqds.collect {
                ((Map) it).get('_id').toString()
            }]).append("timestamp", begin)
            def stat_child_channels = stat_channels.find(query, new BasicDBObject(qd: 1, pay: 1, reg: 1, cny: 1, count: 1, timestamp: 1)).toArray()
            Integer payNum = 0
            Integer regNum = 0
            Integer cny = 0
            Integer count = 0
            int size = stat_child_channels.size()
            //println "stat_child_channels.size-------------->:$size"
            for (DBObject myObj : stat_child_channels) {
                Integer currentPayNum = (myObj.get("pay") != null) ? myObj.get("pay") as Integer : 0

                payNum = payNum + currentPayNum
                Integer currentRegNum = (myObj.get("reg") != null) ? myObj.get("reg") as Integer : 0
                regNum = regNum + currentRegNum
                Integer currentCny = (myObj.get("cny") != null) ? myObj.get("cny") as Integer : 0
                cny = cny + currentCny
                Integer currentCount = (myObj.get("count") != null) ? myObj.get("count") as Integer : 0
                count = count + currentCount
            }
            def YMD = new Date(begin).format("yyyyMMdd")
            def st = new BasicDBObject(_id: "${YMD}_${parent_id}" as String, qd: parent_id, timestamp: begin)
            def incObject = new BasicDBObject(pay: payNum, reg: regNum, cny: cny, count: count)
            def setObject = new BasicDBObject(qd: parent_id, timestamp: begin)

            stat_channels.findAndModify(st, null, null, false,
                    new BasicDBObject($inc: incObject, $set: setObject), true, true)
            println "parent_id-------------->:is end"
        }

    }

//    static void fetchUmengData(int i) {
//        long l = System.currentTimeMillis()
//        def date = new Date() - i
//        //http://api.umeng.com/channels?appkey=53ab9ff256240b97cf0164a5&auth_token=wLL2nMK8Lcn0NhmJxxlU&date=2014-07-02
//        def data = new JsonSlurper().parseText(
//                new URL("http://api.umeng.com/channels?appkey=53ab9ff256240b97cf0164a5&auth_token=wLL2nMK8Lcn0NhmJxxlU&per_page=200&date=${date.format('yyyy-MM-dd')}").getText()
//        )
//        if (data.size() > 0) {
//            def day = date.format("yyyyMMdd_")
//            def coll = mongo.getDB('xy_admin').getCollection('stat_channels')
//
//            data.each { Map row ->
//                coll.update(new BasicDBObject('_id', "${day}${row['channel']}".toString()), new BasicDBObject('$set', [active: row['install'] as Integer]))
//            }
//        }
//        //println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   currentDay, cost  ${System.currentTimeMillis() -l} ms"
//    }

    static void liveStatic(int i) {
        def gteMill = yesTday - i * DAY_MILLON
        def day_logins = mongo.getDB("xylog_history").getCollection("day_login_history")
        def stat_channels = mongo.getDB("xy_admin").getCollection('stat_channels')
        println 'yesterday:' + gteMill
        StringBuffer str = new StringBuffer()
        ['qq', 'qq_2', 'juwan', 'wifi_1', 'qixingliandong', 'sogou_store'].each { String qd ->
            def channel = stat_channels.findOne(new BasicDBObject('_id', new Date(gteMill).format("yyyyMMdd_") + qd))
            if (channel != null) {
                def id = new Date(gteMill).format("yyyyMMdd_") + qd
                str.append(id).append(",").append(qd).append(",").append(new Date(gteMill).format("yyyyMMdd"))
                [1, 3, 7, 30].each { Integer d ->
                    def allUids = channel?.get('regs') as Collection
                    def count = 0
                    if (allUids && allUids.size() > 0) {
                        long begin = channel['timestamp'] as long
                        Long gt = begin + d * DAY_MILLON
                        count = day_logins.count(new BasicDBObject(user_id: [$in: allUids], timestamp:
                                [$gte: gt, $lt: gt + DAY_MILLON]))

                    }
                    str.append(",").append(count)
                }
                str.append(System.lineSeparator())
            }
        }
        File file = new File("liveStatic.csv");
        if (!file.exists()) {
            file.createNewFile();
        }
        file.withWriterAppend { Writer writer ->
            writer.write(str.toString())
            writer.flush()
            writer.close()
        }
    }

    static void finance_log_generate() {
        def list = mongo.getDB('xy_admin').getCollection('finance_log').find(new BasicDBObject(via: 'unionpay', timestamp: [$gte: 1412092800000, $lte: 1414771200000]))
                .toArray()
        StringBuffer str = new StringBuffer()
        list.each { BasicDBObject obj ->
            str.append(obj['_id'])
            str.append(",").append(obj['rid'])
            str.append(",").append(obj['cny'])
            str.append(",").append(new Date(obj['timestamp'] as Long).format("yyyy-MM-dd HH:mm:ss"))
            str.append(System.lineSeparator())
        }
        File file = new File("unionpay.csv");
        if (!file.exists()) {
            file.createNewFile();
        }
        file.withWriterAppend { Writer writer ->
            writer.write(str.toString())
            writer.flush()
            writer.close()
        }
    }

    static void tmpStatic() {
/*        def r = 1..24
        r.each {
            staticsKM(it);
        }*/
        def r = 1..60
        r.each {
            statics(it)
            parentQdstatic(it)
        }
        r.each {
            fetchUmengData(it)
        }
    }

    //*******************************************************************************


    public static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    public static BasicDBObject $$(Map map) {
        return new BasicDBObject(map)
    }

    //充值统计
    static payStatics(int i) {
        def gteMill = yesTday - i * DAY_MILLON
        def YMD = new Date(gteMill).format("yyyyMMdd")
        def time = [$gte: gteMill, $lt: gteMill + DAY_MILLON]

        Map<String, Number> old_ids = new HashMap<String, Number>()
        finance_log_DB.aggregate(
                new BasicDBObject('$match', new BasicDBObject('via', [$ne: 'Admin'])),
                new BasicDBObject('$project', [_id: '$user_id', timestamp: '$timestamp']),
                new BasicDBObject('$group', [_id: '$_id', timestamp: [$min: '$timestamp']])
        ).results().each {
            def obj = new BasicDBObject(it as Map)
            old_ids.put(obj.get('_id') as String, obj.get('timestamp') as Number)
        }
        def typeMap = new HashMap<String, PayStat>()
        PayStat total = new PayStat()
        [pc    : ['ali_pc', 'YB', 'vpay', 'paypal', 'vpay_SMS', 'tenpay'],//PayType
         mobile: ['ali_m', 'unionpay', 'YB_wap', 'tenpay_m', 'Ipay', 'itunes', 'ali_wap', 'unipay', 'vnet_SMS'],
        ].each { String k, List<String> v ->
            PayStat all = new PayStat()
            PayStat delta = new PayStat()
            def cursor = finance_log_DB.find($$([timestamp: time, via: [$in: v.toArray()]]),
                    new BasicDBObject(user_id: 1, cny: 1, coin: 1, timestamp: 1)).batchSize(50000)
            while (cursor.hasNext()) {
                def obj = cursor.next()
                def user_id = obj['user_id'] as String
                def cny = new BigDecimal(((Number) obj.get('cny')).doubleValue())
                def coin = obj.get('coin') as Long
                all.add(user_id, cny, coin)
                total.add(user_id, cny, coin)
                //该用户之前无充值记录或首冲记录为当天则算为当天新增用户
                if (old_ids.containsKey(user_id)) {
                    def userTimestamp = old_ids.get(user_id) as Long
                    Long day = gteMill
                    Long userday = new Date(userTimestamp).clearTime().getTime()
                    if (day.equals(userday)) {
                        delta.add(user_id, cny, coin)
                    }
                }
            }
            typeMap.put(k + 'all', all)
            typeMap.put(k + 'delta', delta)
        }
        stat_daily.update(new BasicDBObject(_id: YMD + '_allpay'),
                new BasicDBObject(type: 'allpay',
                        user_pay: total.toMap(),
                        user_pay_pc: typeMap.get('pcall').toMap(),
                        user_pay_pc_delta: typeMap.get('pcdelta').toMap(),
                        user_pay_mobile: typeMap.get('mobileall').toMap(),
                        user_pay_mobile_delta: typeMap.get('mobiledelta').toMap(),
                        timestamp: gteMill
                ), true, false)
    }

    static class PayStat {
        final Set user = new HashSet(2000)
        final AtomicInteger count = new AtomicInteger()
        final AtomicLong coin = new AtomicLong()
        def BigDecimal cny = new BigDecimal(0)

        def toMap() { [user: user.size(), count: count.get(), coin: coin.get(), cny: cny.doubleValue()] }

        def add(def user_id, BigDecimal deltaCny, Long deltaCoin) {
            count.incrementAndGet()
            user.add(user_id)
            cny = cny.add(deltaCny)
            coin.addAndGet(deltaCoin)
        }
    }

    //PC、手机全部渠道的活跃统计
    static activeStatics(int i) {
        def gteMill = yesTday - i * DAY_MILLON
        def ltMill = gteMill + DAY_MILLON
        def YMD = new Date(gteMill).format('yyyyMMdd')
        def day7Mill = gteMill - 6 * DAY_MILLON
        def day30Mill = gteMill - 29 * DAY_MILLON
        def pcMap = new HashMap<String, Integer>(3)
        def mobileMap = new HashMap<String, Integer>(3)
        def iosMap = new HashMap<String, Integer>(3)
        def h5Map = new HashMap<String, Integer>(3)
        def riaMap = new HashMap<String, Integer>(3)
        def totalMap = new HashMap<String, Integer>(3)
        def totalDaySet = new HashSet<Integer>(200000)
        Set<Integer> totalDay7Set = new HashSet<Integer>(2000000)
        Set<Integer> totalDay30Set = new HashSet<Integer>(2000000)
        ['1', '2', '4', '5', '6'].each { String client ->
            def qb = new BasicDBObject()
            qb.put('client', client)
            def qds = mongo.getDB('xy_admin').getCollection('channels').find(qb,
                    $$('_id', 1)).toArray().collect { DBObject it -> it.get('_id') }
            def day_login = mongo.getDB("xylog").getCollection("day_login")
            if (qds.size() > 0) {
                //def logins = day_login.find(new BasicDBObject(qd: [$in: qds], timestamp: [$gte: day30Mill, $lt: ltMill]), new BasicDBObject([user_id: 1, timestamp: 1])).toArray()

                def logins = day_login_history.find(new BasicDBObject(qd: [$in: qds], timestamp: [$gte: day30Mill, $lt: ltMill]),
                        new BasicDBObject([user_id: 1, timestamp: 1])).toArray()

                Set<Integer> daySet = new HashSet<Integer>(200000)
                Set<Integer> day7Set = new HashSet<Integer>(2000000)
                Set<Integer> day30Set = new HashSet<Integer>(2000000)
                for (DBObject login : logins) {
                    Integer uid = login.get("user_id") as Integer
                    Long timestamp = login.get('timestamp') as Long
                    day30Set.add(uid)
                    totalDay30Set.add(uid)
                    if (timestamp >= day7Mill) {
                        day7Set.add(uid)
                        totalDay7Set.add(uid)
                    }
                    if (timestamp >= gteMill) {
                        daySet.add(uid)
                        totalDaySet.add(uid)
                    }
                }
                if ('1'.equals(client)) {
                    pcMap.putAll(['daylogin': daySet.size(), 'day7login': day7Set.size(), 'day30login': day30Set.size()])
                }else if('4'.equals(client)){
                    iosMap.putAll(['daylogin': daySet.size(), 'day7login': day7Set.size(), 'day30login': day30Set.size()])
                }else if('5'.equals(client)){
                    h5Map.putAll(['daylogin': daySet.size(), 'day7login': day7Set.size(), 'day30login': day30Set.size()])
                }else if('6'.equals(client)){
                    riaMap.putAll(['daylogin': daySet.size(), 'day7login': day7Set.size(), 'day30login': day30Set.size()])
                }
                else {
                    if (mobileMap.size() <= 0) {
                        mobileMap.putAll(['daylogin': daySet.size(), 'day7login': day7Set.size(), 'day30login': day30Set.size()])
                    } else {
                        mobileMap.put('daylogin', mobileMap.get('daylogin') + daySet.size())
                        mobileMap.put('day7login', mobileMap.get('day7login') + daySet.size())
                        mobileMap.put('day30login', mobileMap.get('day30login') + daySet.size())
                    }
                }
            }
        }
        totalMap.putAll(['daylogin': totalDaySet.size(), 'day7login': totalDay7Set.size(), 'day30login': totalDay30Set.size()])
        def info  = new BasicDBObject(type: 'alllogin', login_total: totalMap, pc_login: pcMap, mobile_login: mobileMap,
                ios_login:iosMap,h5_login:h5Map, ria_login:riaMap,timestamp: gteMill);
        println "${YMD + '_alllogin'} : ${info}"
        //coll.update(new BasicDBObject(_id: YMD + '_alllogin'),info, true, false)
        stat_daily.update(new BasicDBObject(_id: YMD + '_alllogin'),info, true, false)
    }

    /**
     * 1,7,30活跃统计
     * @param i
     */
    static void activeQdStatics(int i) {
        def gteMill = yesTday - i * DAY_MILLON
        def ltMill = gteMill + DAY_MILLON
        def YMD = new Date(gteMill).format('yyyyMMdd')
        def day7Mill = gteMill - 6 * DAY_MILLON
        def day30Mill = gteMill - 29 * DAY_MILLON
        def coll = mongo.getDB('xy_admin').getCollection('stat_channels')
        mongo.getDB('xy_admin').getCollection('channels').find(new BasicDBObject(),
                new BasicDBObject('_id', 1)).toArray().each { BasicDBObject obj ->
            def day_login = mongo.getDB("xylog_history").getCollection("day_login_history")
            def logins = day_login.find(new BasicDBObject(qd: obj['_id'], timestamp: [$gte: day30Mill, $lt: ltMill]), new BasicDBObject([user_id: 1, timestamp: 1])).toArray()
            Set<Integer> daySet = new HashSet<Integer>(200000)
            Set<Integer> day7Set = new HashSet<Integer>(2000000)
            Set<Integer> day30Set = new HashSet<Integer>(2000000)
            for (DBObject login : logins) {
                Integer uid = login.get("user_id") as Integer
                Long timestamp = login.get('timestamp') as Long
                day30Set.add(uid)
                if (timestamp >= day7Mill) {
                    day7Set.add(uid)
                }
                if (timestamp >= gteMill) {
                    daySet.add(uid)
                }
            }
            coll.update(new BasicDBObject('_id', "${YMD}_${obj['_id']}".toString()), new BasicDBObject('$set',
                    [daylogin: daySet.size(), day7login: day7Set.size(), day30login: day30Set.size()]
            ), true, false)
        }
    }

    /**
     * 1,3,7,30留存统计
     */
    static stayStatics(int i) {
        def gteMill = yesTday - i * DAY_MILLON
        def YMD = new Date(gteMill).format("yyyyMMdd")
        def channels = mongo.getDB("xy_admin").getCollection("channels")
        def day_logins = mongo.getDB("xylog_history").getCollection("day_login_history")
        def stat_channels = mongo.getDB("xy_admin").getCollection('stat_channels')

        channels.find(new BasicDBObject(), new BasicDBObject("_id": 1)).toArray().each { BasicDBObject qdObj ->
            String qd = qdObj.get("_id")
            def channel = stat_channels.findOne(new BasicDBObject('_id', new Date(gteMill).format("yyyyMMdd_") + qd))
            if (channel != null) {
                def map = new HashMap<Integer, Long>(4)
                [1, 3, 7, 30].each { Integer d ->
                    def allUids = channel?.get('regs') as Collection
                    if (allUids && allUids.size() > 0) {
                        Long gt = gteMill + d * DAY_MILLON
                        def count = day_logins.count(new BasicDBObject(user_id: [$in: allUids], timestamp:
                                [$gte: gt, $lt: gt + DAY_MILLON]))
                        map.put("${d}_day".toString(), count)
                    }
                }
                if (map.size() > 0) {
                    stat_channels.update(new BasicDBObject('_id', "${YMD}_${qd}".toString()),
                            new BasicDBObject('$set', new BasicDBObject("stay", map)))
                }
            }
        }
    }

    //友盟数据
    static void fetchUmengData(int i) {
        long l = System.currentTimeMillis()
        def gteMill = yesTday - i * DAY_MILLON
        def date = new Date(gteMill)
        //http://api.umeng.com/channels?appkey=53ab9ff256240b97cf0164a5&auth_token=wLL2nMK8Lcn0NhmJxxlU&date=2014-07-02
        def data = new JsonSlurper().parseText(
                new URL("http://api.umeng.com/channels?appkey=53ab9ff256240b97cf0164a5&auth_token=wLL2nMK8Lcn0NhmJxxlU&per_page=200&date=${date.format('yyyy-MM-dd')}").getText()
        )
        if (data.size() > 0) {
            def day = date.format("yyyyMMdd_")
            // def mongo  = new Mongo(new com.mongodb. MongoURI('mongodb://10.0.5.32:10000,10.0.5.33:10000,10.0.5.34:10000/?w=1&slaveok=true'))
            def coll = mongo.getDB('xy_admin').getCollection('stat_channels')

            data.each { Map row ->
                //查询umeng自定义事件三日发言
                def update = new BasicDBObject([active     : row['install'] as Integer,//新增用户
                                                active_user: row['active_user'] as Integer,//日活
                                                duration   : row['duration'] as String//平均使用时长
                ])
                if (row['id'] != null) {
//                    //查询自定义消息数，group_id值代表用户三日发言，channels代表渠道
//                    def event = new JsonSlurper().parseText(
//                            new URL("http://api.umeng.com/events/daily_data?appkey=53ab9ff256240b97cf0164a5" +
//                                    "&auth_token=wLL2nMK8Lcn0NhmJxxlU&period_type=daily&group_id=543cd5e5fd98c507a002ccce" +
//                                    "&start_date=${date.format('yyyy-MM-dd')}&end_date=${date.format('yyyy-MM-dd')}" +
//                                    "&channels=${row['id']}").getText()
//                    )
//                    def count = event?.getAt("data")?.getAt("all") as List
//                    if (count != null && count.size() > 0) {
//                        update.put("speechs",count[0])
//                    }
                }
                coll.update(new BasicDBObject('_id', "${day}${row['channel']}".toString()), new BasicDBObject('$set', update))
            }
        }
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   currentDay, cost  ${System.currentTimeMillis() - l} ms"
    }

    static void fetchSpeech() {
        def data = new JsonSlurper().parseText(
                new URL("http://api.umeng.com/channels?appkey=53ab9ff256240b97cf0164a5&auth_token=wLL2nMK8Lcn0NhmJxxlU&per_page=400&date=2014-11-14").getText()
        )
        if (data.size() > 0) {
            def coll = mongo.getDB('xy_admin').getCollection('stat_channels')
            def reqs = 0
            data.each { Map row ->
                //查询umeng自定义事件三日发言
                26.times {
                    def gteMill = yesTday - it * DAY_MILLON
                    def gte = new Date(gteMill)
                    if (row['id'] != null) {
                        if (reqs++ >= 280) {
                            reqs = 0
                            Thread.sleep(16 * 60 * 1000L)
                        }
                        //查询自定义消息数，group_id值代表用户三日发言，channels代表渠道
                        def content = new URL("http://api.umeng.com/events/parameter_list?appkey=53ab9ff256240b97cf0164a5" +
                                "&auth_token=wLL2nMK8Lcn0NhmJxxlU&period_type=daily&event_id=543ce217e8af9ceaa72f3847" +
                                "&start_date=${gte.format('yyyy-MM-dd')}&end_date=${gte.format('yyyy-MM-dd')}" +
                                "&channels=${row['id']}").getText("UTF-8")
                        def count = 0
                        if (StringUtils.isNotBlank(content) && content.length() >= 2) {
                            content = content.substring(1, content.length() - 1)
                            if (StringUtils.isNotBlank(content)) {
                                def event = content.split("\\},\\{")
                                if (event != null && event.size() >= 1) {
                                    for (int j = 0; j < event.size(); j++) {
                                        String str = event[j]
                                        if (str.contains("新注册用户数发言率")) {
                                            if (!str.startsWith("{")) str = "{" + str
                                            if (!str.endsWith("}")) str += "}"
                                            def obj = new JsonSlurper().parseText(str) as Map
                                            count = obj.get("num") as Integer
                                        }
                                    }
                                }
                            }
                        }
                        def update = new BasicDBObject()
                        update.put("speechs", count)
                        println "${gte.format('yyyyMMdd_')}${row['channel']}结果：" + count
                        coll.update(new BasicDBObject('_id', "${gte.format('yyyyMMdd_')}${row['channel']}".toString()), new BasicDBObject('$set', update))
                    }
                }
            }
        }
    }

    //*******************************************************************************

    //用户登录流失率统计,从两个月前的数据统计起（14年从7月开始）
    static loginMonthStatic(int i) {
        def coll = mongo.getDB('xy_admin').getCollection('stat_month')
        def day_login = mongo.getDB("xylog_history").getCollection("day_login_history")
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

    private static BATCH_SIZE = 50000
    //用户充值流失率统计,从两个月前的数据统计起（14年从7月开始）
    static payMonthStatic(int i) {
        def coll = mongo.getDB('xy_admin').getCollection('stat_month')
        def pc = ['ali_pc', 'YB', 'vpay', 'paypal', 'vpay_SMS', 'tenpay'] as List
        def mobile = ['ali_m', 'unionpay', 'YB_wap', 'tenpay_m', 'Ipay', 'itunes', 'ali_wap', 'unipay', 'vnet_SMS',"kx-","ql-"] as List
        def finance_log_DB = mongo.getDB('xy_admin').getCollection('finance_log')
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
        def query = new BasicDBObject([timestamp: [$gte: firstDayOfBefore, $lt: firstDayOfLastMonth], via: [$ne: 'Admin']])
        long count = finance_log_DB.count(query)
        Integer num = count / BATCH_SIZE
        if (count % BATCH_SIZE > 0) {
            num++
        }
        //查询上上个月前的充值用户数据
        num.times {
            finance_log_DB.find(query).skip(it * BATCH_SIZE).limit(BATCH_SIZE)
                    .toArray().each { BasicDBObject obj ->
                def user_id = obj.get("user_id") as Integer
                def via = obj.get("via") as String
                before_total_map.add(user_id)
                if (pc.contains(via)) {
                    before_pc_map.add(user_id)
                }
                if (mobile.contains(via)) {
                    before_mobile_map.add(user_id)
                }
            }
        }
        query = new BasicDBObject([timestamp: [$gte: firstDayOfLastMonth, $lt: firstDayOfCurrentMonth], via: [$ne: 'Admin']])
        count = finance_log_DB.count(query)
        num = count / BATCH_SIZE
        if (count % BATCH_SIZE > 0) {
            num++
        }
        //查询上个月的充值用户数据
        num.times {
            finance_log_DB.find(query).skip(it * BATCH_SIZE).limit(BATCH_SIZE)
                    .toArray().each { BasicDBObject obj ->
                def user_id = obj.get("user_id") as Integer
                def via = obj.get("via") as String
                last_total_map.add(user_id)
                if (pc.contains(via)) {
                    last_pc_map.add(user_id)
                }
                if (mobile.contains(via)) {
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
                type: 'allpay',
                timestamp: new SimpleDateFormat("yyyyMM").parse(ym).getTime()
        )
        coll.update(new BasicDBObject(_id: "${ym}_pay".toString()), new BasicDBObject('$set', obj), true, false)
    }

    private static Calendar getCalendar() {
        Calendar cal = Calendar.getInstance()//获取当前日期

        cal.set(Calendar.DAY_OF_MONTH, 1)//设置为1号,当前日期既为本月第一天
        cal.set(Calendar.HOUR_OF_DAY, 0)
        cal.set(Calendar.MINUTE, 0)
        cal.set(Calendar.SECOND, 0)

        return cal
    }


    static void main(String[] args) {
        long l = System.currentTimeMillis()
        //TODO 快播渠道用户不准确特殊处理，以后移除
        //updateAllKmUser()
        //println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${KMQdStat.class.getSimpleName()},updateAllKmUser cost  ${System.currentTimeMillis() -l} ms"

//        tmpStatic();
        /*150.times {
            liveStatic(it as Integer)
        }*/

        /*finance_log_generate();*/

//        150.times {
//            stayStatics(it as Integer)
//            fetchUmengData(it as Integer)
//        }
//        Thread.sleep(16 * 60 * 1000L)
/*        5.times {
            loginMonthStatic(it)
            payMonthStatic(it)
        }*/
        30.times {
            activeStatics(it)
        }
//        fetchSpeech()

        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${KMQdStat.class.getSimpleName()},total cost  ${System.currentTimeMillis() - l} ms"
    }

}