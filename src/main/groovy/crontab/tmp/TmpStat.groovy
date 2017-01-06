#!/usr/bin/env groovy
package crontab.tmp

import com.mongodb.BasicDBObject
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
class TmpStat {
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

    public static final String ls = System.lineSeparator();

    //********************根据充值人注册时的联运渠道统计start*******************************
    static payStaticsByUserChannel(int i) {
        def users = mongo.getDB('xy').getCollection('users')
        def finance_log = mongo.getDB('xy_admin').getCollection('finance_log')
        def channel = mongo.getDB('xy_admin').getCollection('channels')
        DBCollection coll = mongo.getDB('xy_admin').getCollection('stat_daily')

        Long begin = yesTday - i * DAY_MILLON
        def timeBetween = [$gte: begin, $lt: begin + DAY_MILLON]
        def logMap = new HashMap()
        //查询当天充值的所有用户及充值金额、获得的柠檬数、充值次数
        finance_log.aggregate(
                new BasicDBObject('$match', [via: [$ne: 'Admin'], timestamp: timeBetween]),
                new BasicDBObject('$project', [user_id: '$user_id', coin: '$coin', cny: '$cny', count: '$count']),
                new BasicDBObject('$group', [_id: '$user_id', coin: [$sum: '$coin'], cny: [$sum: '$cny'], count: [$sum: 1]])
        ).results().each { BasicDBObject logObj ->
            def id = logObj.get('_id') as Integer
            logMap.put(id, logObj)
        }
        if (logMap.size() > 0) {
            def qdMap = new HashMap(), clientMap = new HashMap()
            def YMD = new Date(begin).format("yyyyMMdd")
            //查询用户对应的注册渠道
            users.aggregate(
                    new BasicDBObject('$match', [_id: [$in: logMap.keySet()]]),
                    new BasicDBObject('$project', [qd: '$qd', userId: '$_id']),
                    new BasicDBObject('$group', [_id: '$qd', users: [$addToSet: '$userId']])
            ).results().each { BasicDBObject userObj ->
                qdMap.put(userObj.get('_id') as String, userObj.get('users'))
            }
            channel.aggregate(
                    new BasicDBObject('$match', [_id: [$in: qdMap.keySet()]]),
                    new BasicDBObject('$project', [client: '$client', qdId: '$_id']),
                    new BasicDBObject('$group', [_id: '$client', qdIds: [$addToSet: '$qdId']])
            ).results().each { BasicDBObject clientObj ->
                def client = clientObj.get('_id') as String
                client = "2".equals(client) ? client : "1"
                def payStat = clientMap.get(client) as PayStat
                if (payStat == null) {
                    payStat = new PayStat()
                    clientMap.put(client, payStat)
                }
                def qdIds = clientObj.get('qdIds') as List
                qdIds.each { String qdId ->
                    def uids = qdMap.remove(qdId) as List
                    uids.each { Integer userId ->
                        def finance = logMap.remove(userId) as BasicDBObject
                        def cny = finance.get('cny') ?: 0
                        def coin = finance.get('coin') ?: 0
                        def count = finance.get('count') ?: 0
                        payStat.add(userId, cny as Double, coin as Long, count as Integer)
                    }
                }
            }
            logMap.each { Integer k, BasicDBObject finance ->
                def payStat = clientMap.get("1") as PayStat
                if (payStat == null) {
                    payStat = new PayStat()
                    clientMap.put("1", payStat)
                }
                def cny = finance.get('cny') ?: 0
                def coin = finance.get('coin') ?: 0
                def count = finance.get('count') ?: 0
                payStat.add(k, cny as Double, coin as Long, count as Integer)
            }
            def user_pc = clientMap.get('1') as PayStat
            def user_mobile = clientMap.get('2') as PayStat
            def setVal = [type: 'allpay', timestamp: begin] as Map
            if (user_pc != null) {
                setVal.put('user_pc', user_pc.toMap())
            }
            if (user_mobile != null) {
                setVal.put('user_mobile', user_mobile.toMap())
            }
            coll.update(new BasicDBObject(_id: YMD + '_allpay'), new BasicDBObject('$set': setVal), true, false)
        }
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

        def add(def user_id, BigDecimal deltaCny, Long deltaCoin, Integer deltaCount) {
            count.addAndGet(deltaCount)
            user.add(user_id)
            cny = cny.add(deltaCny)
            coin.addAndGet(deltaCoin)
        }
    }
    //************************根据充值人注册时的联运渠道统计end***************************
    //************************联运接口统计start***************************
    static staticsQd(int i) {
        def coll = mongo.getDB('xy_admin').getCollection('stat_channels')
        def users = mongo.getDB('xy').getCollection('users')
        def finance_log = mongo.getDB('xy_admin').getCollection('finance_log')
        def trade_logs = mongo.getDB('xylog').getCollection('trade_logs')

        Long begin = yesTday - i * DAY_MILLON
        def timeBetween = [$gte: begin, $lt: begin + DAY_MILLON]

        //TODO 剔除快播 f101
        mongo.getDB('xy_admin').getCollection('channels').find(
                new BasicDBObject("_id", [$nin: ["f101"]]), new BasicDBObject("reg_discount", 1).append("child_qd", 1)
        ).toArray().each { BasicDBObject channnel ->
            def cId = channnel.removeField("_id")
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
                    Long.parseLong(b) - Long.parseLong(a)
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
            //统计当天扣费信息
            def query = new BasicDBObject()
            query.append("qd", cId).append("via", "letu").append("resp.amount", ['$exists': true])
            query.append('time', timeBetween)
            List list = trade_logs.aggregate(
                    new BasicDBObject('$match', query),
                    new BasicDBObject('$project', [via: '$via', qd: '$qd', amount: '$resp.amount', uid: '$uid']),
                    new BasicDBObject('$group', [_id: '$qd', amounts: [$push: '$amount'], uids: [$addToSet: '$uid']])
            ).results().toList()
            list.each { BasicDBObject obj ->
                def uids = obj.remove("uids") as List
                def amounts = obj.remove("amounts") as List
                def amount = 0 as Double
                if (amounts != null) {
                    amounts.each { String am ->
                        amount += new BigDecimal(am).doubleValue()
                    }
                }
                st.put('deduct', [users: (uids == null ? 0 : uids.size()), amount: amount])
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
            coll.update(new BasicDBObject(_id: st.remove('_id') as String), new BasicDBObject($set: st), true, false)

        }

    }

    static parentQdstatic(int i) {
        def channel_db = mongo.getDB('xy_admin').getCollection('channels')
        def stat_channels = mongo.getDB('xy_admin').getCollection('stat_channels')
        def channels = channel_db.find(new BasicDBObject(parent_qd: [$ne: null]), new BasicDBObject(parent_qd: 1)).toArray()
        Map<String, DBObject> parentMap = new HashMap<String, DBObject>()
        for (DBObject obj : channels) {
            String parent_id = obj.get("parent_qd") as String
            parentMap.put(parent_id, obj)
        }

        Long begin = yesTday - i * DAY_MILLON
        for (String key : parentMap.keySet()) {
            DBObject obj = parentMap.get(key)
            def parent_id = obj.get("parent_qd") as String
            def childqds = channel_db.find(new BasicDBObject(parent_qd: parent_id), new BasicDBObject(_id: 1)).toArray()
            DBObject query = new BasicDBObject('qd', [$in: childqds.collect {
                ((Map) it).get('_id').toString()
            }]).append("timestamp", begin)
            def stat_child_channels = stat_channels.find(query).toArray()
            Integer payNum = 0
            Integer regNum = 0
            Integer cny = 0
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
                Integer currentPayNum = (myObj.get("pay") != null) ? myObj.get("pay") as Integer : 0
                payNum += currentPayNum
                Integer currentRegNum = (myObj.get("reg") != null) ? myObj.get("reg") as Integer : 0
                regNum += currentRegNum
                Integer currentCny = (myObj.get("cny") != null) ? myObj.get("cny") as Integer : 0
                cny += currentCny
                Integer currentCount = (myObj.get("count") != null) ? myObj.get("count") as Integer : 0
                count += currentCount
                Integer currentDaylogin = (myObj.get("daylogin") != null) ? myObj.get("daylogin") as Integer : 0
                daylogin += currentDaylogin
                Integer currentDay7login = (myObj.get("day7login") != null) ? myObj.get("day7login") as Integer : 0
                day7login += currentDay7login
                Integer currentDay30login = (myObj.get("day30login") != null) ? myObj.get("day30login") as Integer : 0
                day30login += currentDay30login
                def myStay = myObj.get("stay") as Map
                if (myStay != null) {
                    Integer currentStay1 = (myStay.get("1_day") != null) ? myObj.get("1_day") as Integer : 0
                    stay1 += currentStay1
                    Integer currentStay3 = (myStay.get("3_day") != null) ? myStay.get("3_day") as Integer : 0
                    stay3 += currentStay3
                    Integer currentStay7 = (myStay.get("7_day") != null) ? myStay.get("7_day") as Integer : 0
                    stay7 += currentStay7
                    Integer currentStay30 = (myStay.get("30_day") != null) ? myStay.get("30_day") as Integer : 0
                    stay30 += currentStay30
                }
                Integer currentCpa2 = (myObj.get("cpa2") != null) ? myObj.get("cpa2") as Integer : 0
                cpa2 += currentCpa2

            }
            def YMD = new Date(begin).format("yyyyMMdd")
            def st = new BasicDBObject(_id: "${YMD}_${parent_id}" as String, qd: parent_id, timestamp: begin)
            def incObject = new BasicDBObject(
                    pay: payNum,
                    reg: regNum,
                    cny: cny,
                    count: count,
                    daylogin: daylogin,
                    day7login: day7login,
                    day30login: day30login,
                    'stay.1_day': stay1,
                    'stay.3_day': stay3,
                    'stay.7_day': stay7,
                    'stay.30_day': stay30,
                    cpa2: cpa2
            )
            def setObject = new BasicDBObject(qd: parent_id, timestamp: begin)

            stat_channels.findAndModify(st, null, null, false,
                    new BasicDBObject($inc: incObject, $set: setObject), true, true)
        }

    }

    /**
     * 1,7,30活跃统计
     * @param i
     */
    static void activeStatics(int i) {
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
            ))
        }
    }

    /**
     * 扣费统计
     */
    static staticLetuData(int i) {
        def gteMill = yesTday - i * DAY_MILLON
        def prefix = new Date(gteMill).format("yyyyMMdd_")
        def trade_logs = mongo.getDB("xylog").getCollection("trade_logs")
        def stat_channels = mongo.getDB("xy_admin").getCollection('stat_channels')
        def query = new BasicDBObject(via: "letu", "resp.amount": [$exists: true], time: [$gte: gteMill, $lt: gteMill + DAY_MILLON])
        def list = trade_logs.aggregate(
                new BasicDBObject('$match', query),
                new BasicDBObject('$project', [via: '$via', qd: '$qd', amount: '$resp.amount', uid: '$uid']),
                new BasicDBObject('$group', [_id: '$qd', amounts: [$push: '$amount'], uids: [$addToSet: '$uid']])
        ).results().toList()
        list.each { BasicDBObject obj ->
            def qd = obj.get("_id") as String
            def uids = obj.remove("uids") as List
            obj.put("users", uids == null ? 0 : uids.size())
            def amounts = obj.remove("amounts") as List
            def amount = 0 as Double
            if (amounts != null) {
                amounts.each { String am ->
                    amount += new BigDecimal(am).doubleValue()
                }
            }
            obj.put("amount", amount)
            stat_channels.update(new BasicDBObject('_id', "${prefix}${qd}".toString()),
                    new BasicDBObject('$set', new BasicDBObject("deduct", [amount: amount, users: (uids == null ? 0 : uids.size())])))
        }
    }

    static void fetchClickiData(int i) {
        def coll = mongo.getDB('xy_admin').getCollection('stat_channels')
        long l = System.currentTimeMillis()
        def gteMill = yesTday - i * DAY_MILLON
        def date = new Date(gteMill)
        def dateStr = date.format('yyyy-MM-dd')
        def limit = 10//最大支持单次10条查询
        def offset = 0//分页参数，起始为0
        def hasMore = Boolean.TRUE
        while (hasMore) {
            def data = new JsonSlurper().parseText(
                    new URL("http://www.clicki.cn/api/page/url?begindate=${dateStr}&enddate=${dateStr}&offset=${offset}&limit=${limit}&token=01771f89a7ecd4d6074b53ba5d9c450c").getText()
            )
            if (data != null) {
                def succ = data["success"] as Boolean
                if (succ == null || !succ) {
                    break
                }
                def items = data["items"]
                items.each { Map row ->
                    def page = row["page_url_name"] as String
                    def visitors = row["visitors"] as Integer
                    visitors = visitors ?: 0
                    if (StringUtils.isNotBlank(page) && page.startsWith('http://www.2339.com') && page.contains('?id=')) {
                        //解析页面对应的渠道号
                        def channels = page.split(/\?id=/)[1]
                        def qd = channels, parent_qd
                        if (channels.contains("_")) {
                            parent_qd = channels.split("_")[0]
                            qd = channels.split("_")[1]
                        }
                        def day = date.format("yyyyMMdd_")
                        def update = new BasicDBObject(visitors: visitors)
                        coll.update(new BasicDBObject('_id', "${day}${qd}".toString()), new BasicDBObject('$set', update))
                    }
                }
                def total = data['total'] as Integer
                total = total ?: 0
                if (total <= (limit + offset)) {
                    hasMore = Boolean.FALSE
                } else {
                    offset += limit
                }
            }
        }
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   currentDay, cost  ${System.currentTimeMillis() - l} ms"
    }

    /**
     * 友盟接口请求有限制（15分钟300条）
     * @param i
     */
    static void fetchUmengData(int i) {
        long l = System.currentTimeMillis()
        def gteMill = yesTday - i * DAY_MILLON
        def date = new Date(gteMill)//

        def list = []
        def day = date.format("yyyyMMdd_")
        def coll = mongo.getDB('xy_admin').getCollection('stat_channels')
        def reqs = 0 as Integer
        ['53ab9ff256240b97cf0164a5', '544f71eafd98c5a62b002aa3'].each { String appkey ->
            def page = 1, per_page = 400
            def hasMore = true
            while (hasMore) {
                if (reqs++ >= 290) {
                    reqs = 0
                    Thread.sleep(16 * 60 * 1000L)
                }
                def data = null
                data = new JsonSlurper().parseText(
                        new URL("http://api.umeng.com/channels?appkey=${appkey}&auth_token=wLL2nMK8Lcn0NhmJxxlU&per_page=${per_page}&page=${page++}&date=${date.format('yyyy-MM-dd')}").getText()
                )
                if (data.size() > 0) {
                    // def mongo  = new Mongo(new com.mongodb. MongoURI('mongodb://10.0.5.32:10000,10.0.5.33:10000,10.0.5.34:10000/?w=1&slaveok=true'))
                    data.each { Map row ->
                        //查询umeng自定义事件三日发言
                        def update = new BasicDBObject([active     : row['install'] as Integer,//新增用户
                                                        active_user: row['active_user'] as Integer,//日活
                                                        duration   : row['duration'] as String//平均使用时长
                        ])
                        def channel = mongo.getDB('xy_admin').getCollection('channels').findOne(
                                new BasicDBObject("_id", row['channel'] as String), new BasicDBObject("active_discount", 1)
                        )
                        def active = row['install'] as Integer
                        if (channel != null && active != null) {
                            //设置激活扣量cpa1
                            def discountMap = channel.removeField("active_discount") as Map
                            if (discountMap != null && discountMap.size() > 0) {
                                def cpa = null
                                def keyList = discountMap.keySet().toArray().sort { String a, String b ->
                                    Long.parseLong(b) - Long.parseLong(a)
                                }
                                for (it in keyList) {
                                    if (gteMill >= (it as Long)) {
                                        def discount = discountMap.get(it) as Integer
                                        cpa = new BigDecimal((double) (active * discount / 100)).toInteger()
                                        break
                                    }
                                }
                                if (cpa != null) update.append("cpa1", cpa)
                            }
                        }
                        row.put('update', update)
                        list.add(row)
                    }
                }
                if (data == null || data.size() < per_page) {
                    hasMore = false
                }
            }
        }
        list.each { Map row ->
            def update = row['update'] as BasicDBObject
            try {
                if (row['id'] != null) {
                    if (reqs++ >= 290) {
                        reqs = 0
                        Thread.sleep(16 * 60 * 1000L)
                    }
                    def content = new URL("http://api.umeng.com/events/parameter_list?appkey=53ab9ff256240b97cf0164a5" +
                            "&auth_token=wLL2nMK8Lcn0NhmJxxlU&period_type=daily&event_id=543ce217e8af9ceaa72f3847" +
                            "&start_date=${date.format('yyyy-MM-dd')}&end_date=${date.format('yyyy-MM-dd')}" +
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
                    update.put("speechs", count)
                }
            } catch (Exception e) {

            }
            coll.update(new BasicDBObject('_id', "${day}${row['channel']}".toString()), new BasicDBObject('$set', update))
        }
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   currentDay, cost  ${System.currentTimeMillis() - l} ms"
    }

    /**
     * 更新父渠道激活、扣量激活、发言数等信息
     */
    static parentTongjiQdstatic(int i) {
        def channel_db = mongo.getDB('xy_admin').getCollection('channels')
        def stat_channels = mongo.getDB('xy_admin').getCollection('stat_channels')
        def channels = channel_db.find(new BasicDBObject(parent_qd: [$ne: null]), new BasicDBObject(parent_qd: 1)).toArray()
        Map<String, DBObject> parentMap = new HashMap<String, DBObject>()
        for (DBObject obj : channels) {
            String parent_id = obj.get("parent_qd") as String
            parentMap.put(parent_id, obj)
        }

        Long begin = yesTday - i * DAY_MILLON
        for (String key : parentMap.keySet()) {
            DBObject obj = parentMap.get(key)
            def parent_id = obj.get("parent_qd") as String
            def childqds = channel_db.find(new BasicDBObject(parent_qd: parent_id), new BasicDBObject(_id: 1)).toArray()
            DBObject query = new BasicDBObject('qd', [$in: childqds.collect {
                ((Map) it).get('_id').toString()
            }]).append("timestamp", begin)
            def stat_child_channels = stat_channels.find(query).toArray()
            Integer cpa1 = 0
            Integer visitors = 0
            Integer active = 0
            Integer active_user = 0
            Integer speechs = 0
            Boolean hasVisitor = Boolean.FALSE
            int size = stat_child_channels.size()
            //println "stat_child_channels.size-------------->:$size"
            for (DBObject myObj : stat_child_channels) {
                Integer currentCpa1 = (myObj.get("cpa1") != null) ? myObj.get("cpa1") as Integer : 0
                cpa1 += currentCpa1
                Integer currentActive = (myObj.get("active") != null) ? myObj.get("active") as Integer : 0
                active += currentActive
                Integer currentActive_user = (myObj.get("active_user") != null) ? myObj.get("active_user") as Integer : 0
                active_user += currentActive_user
                Integer currentSpeechs = (myObj.get("speechs") != null) ? myObj.get("speechs") as Integer : 0
                speechs += currentSpeechs
                if (myObj.containsField("visitors")) {
                    hasVisitor = Boolean.TRUE
                    visitors += (myObj.get("visitors") != null) ? myObj.get("visitors") as Integer : 0
                }
            }
            def YMD = new Date(begin).format("yyyyMMdd")
            def st = new BasicDBObject(_id: "${YMD}_${parent_id}" as String, qd: parent_id, timestamp: begin)
            def incObject = new BasicDBObject(cpa1: cpa1, active: active, active_user: active_user, speechs: speechs)
            def setObject = new BasicDBObject(qd: parent_id, timestamp: begin)
            if (hasVisitor) incObject.append('visitors', visitors)

            stat_channels.findAndModify(st, null, null, false,
                    new BasicDBObject($inc: incObject, $set: setObject), true, true)
        }

    }

    //***************************************************

    static void main(String[] args) {
        long l = System.currentTimeMillis()

//        185.times {
//            payStaticsByUserChannel(it + 20)
//        }
        13.times {
//            staticsQd(it + 1)
//            parentQdstatic(it + 1)
//            activeStatics(it + 1)
            staticLetuData(it)
//            fetchClickiData(it + 1)
        }
//        2.times {
//            fetchUmengData(it + 1)
//            parentTongjiQdstatic(it + 1)
//            if (it == 0) Thread.sleep(20 * 60 * 1000L)
//        }

        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${TmpStat.class.getSimpleName()},total cost  ${System.currentTimeMillis() - l} ms"
    }

}