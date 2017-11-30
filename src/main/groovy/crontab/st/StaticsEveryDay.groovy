#!/usr/bin/env groovy
package crontab.st

import com.mongodb.BasicDBObject
import com.mongodb.DBCollection
import com.mongodb.DBObject
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
        @Grab('org.apache.httpcomponents:httpclient:4.2.5')
])

import com.mongodb.Mongo
import com.mongodb.MongoURI
import org.apache.commons.lang.StringUtils

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/**
 * 每天统计一份数据
 */
class StaticsEveryDay {

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


    static DAY_MILLON = 24 * 3600 * 1000L

    static long zeroMill = new Date().clearTime().getTime()
    static Long yesTday = zeroMill - DAY_MILLON
    static String YMD = new Date(yesTday).format("yyyyMMdd")

    //手续费比例
    static Map<String, Double> PAY_RATES = ['itunes': 0.7]
    static String QD_DEFAULT = 'laihou_default'

    static DBCollection coll = mongo.getDB('xy_admin').getCollection('stat_daily')
    static DBCollection finance_log_DB = mongo.getDB('xy_admin').getCollection('finance_log')
    static DBCollection users = mongo.getDB('xy').getCollection('users')
    static DBCollection channel_pay_DB = mongo.getDB('xy_admin').getCollection('channel_pay')
    static DBCollection channels = mongo.getDB('xy_admin').getCollection('channels')
    static DBCollection diamond_cost_log = mongo.getDB('xylog').getCollection('diamond_cost_logs')
    /**
     * 1：WEB  2：Android   4：iOS   5：H5
     * @param i
     * @return
     */
    static loginStatics(int i) {
        // yesTday 昨天凌晨 - 1天 = 前天凌晨
        def begin = yesTday - i * DAY_MILLON
        def end = begin + DAY_MILLON
        def YMD = new Date(begin).format('yyyyMMdd')
        def log = mongo.getDB("xylog").getCollection("day_login");
        def query = new BasicDBObject(timestamp: [$gte: begin, $lt: end])
        def channel = mongo.getDB('xy_admin').getCollection('channels')
        def pcqds = channel.find(new BasicDBObject(client: [$in: ['1', '5']])).toArray()*._id
        //统计流失率
        def lossbegin = begin - 15 * DAY_MILLON
        def lossend = lossbegin + DAY_MILLON
        def pc_uids = new ArrayList(10000), mobile_uids = new ArrayList(100000)
        log.find(new BasicDBObject(timestamp: [$gte: lossbegin, $lt: lossend])).toArray().each { BasicDBObject obj ->
            if (pcqds.contains(obj.get('qd') as String)) {
                pc_uids.add(obj.get('user_id') as Integer)
            } else {
                mobile_uids.add(obj.get('user_id') as Integer)
            }
        }

        def pc_unloss = log.distinct('user_id', new BasicDBObject(user_id: [$in: pc_uids], timestamp: [$gte: lossend, $lt: end])).size()
        def mobile_unloss = log.distinct('user_id', new BasicDBObject(user_id: [$in: mobile_uids], timestamp: [$gte: lossend, $lt: end])).size()

        //统计回访用户
        def rtnbegin = begin - 6 * DAY_MILLON
        def rtndaybegin = begin - 22 * DAY_MILLON
        def rtndayend = rtndaybegin + DAY_MILLON
        def uids = log.distinct('user_id', new BasicDBObject(timestamp: [$gte: rtndaybegin, $lt: rtndayend]))
        def unlossuids = log.distinct('user_id', new BasicDBObject(user_id: [$in: uids], timestamp: [$gte: rtndayend, $lt: rtnbegin]))
        def rtnCount = log.distinct('user_id', new BasicDBObject(user_id: [$in: uids, $nin: unlossuids], timestamp: [$gte: rtnbegin, $lt: end])).size()

        def pc = log.count(query.append('qd', [$in: pcqds]))
        def mobile = log.count(query.append('qd', [$nin: pcqds]))
        def obj = new BasicDBObject(
                _id: "${YMD}_login".toString(),
                total: pc + mobile,
                pc: pc,
                moblie: mobile,
                type: 'login',
                timestamp: begin
        )
        coll.save(obj)
        def loss_id = "${new Date(lossbegin).format('yyyyMMdd')}_login".toString()
        def pc_loss = pc_uids.size() - pc_unloss, mobile_loss = mobile_uids.size() - mobile_unloss
        def loss = new BasicDBObject($set: [
                loss       : pc_loss + mobile_loss,
                pc_loss    : pc_loss,
                mobile_loss: mobile_loss
        ])
        coll.update(new BasicDBObject(_id: loss_id), loss)
        def rtn_id = "${new Date(rtndaybegin).format('yyyyMMdd')}_login".toString()
        def rtn = new BasicDBObject($set: [
                return: rtnCount
        ])
        coll.update(new BasicDBObject(_id: rtn_id), rtn)
    }


    static class PayType {
        final user = new HashSet(1000)
        final count = new AtomicInteger()
        final coin = new AtomicLong()
        def cny = new BigDecimal(0)

        def toMap() { [user: user.size(), count: count.get(), coin: coin.get(), cny: cny.doubleValue()] }
    }

    /**
     * 充值统计
     * @return
     */
    static financeStatics(int i) {
        def begin = yesTday - i * DAY_MILLON
        def end = begin + DAY_MILLON
        def YMD = new Date(begin).format('yyyyMMdd')
        def list = finance_log_DB.find(new BasicDBObject(timestamp: [$gte: begin, $lt: end])).toArray()

        def total = new BigDecimal(0)
        def total_cut = new BigDecimal(0)
        def totalCoin = new AtomicLong()
        def charge_coin = new AtomicLong()
        def hand_coin = new AtomicLong()

        def pays = MapWithDefault.<String, PayType> newInstance(new HashMap()) { new PayType() }
        Double android_recharge = 0d
        Double ios_recharge = 0d
        Double other_recharge = 0d
        def android_recharge_set = new HashSet()
        def ios_recharge_set = new HashSet()
        def other_recharge_set = new HashSet()
        list.each { obj ->
            def cny = obj.containsField('cny') ? obj['cny'] as Double : 0.0d
            def payType = pays[obj.via]
            payType.count.incrementAndGet()
            payType.user.add(obj.user_id)
            def userId = obj['user_id'] as Integer
            def user = users.findOne($$('_id': userId), $$('qd': 1))
            if (user == null) {
                return
            }
            def qd = user.containsField('qd') ? user['qd'] : QD_DEFAULT
            // client = 2 android 4 ios
            def channel = channels.findOne($$('_id': qd), $$('client': 1))
            def client = channel?.containsField('client') ? channel['client'] as Integer : 2
            def via = obj.containsField('via') ? obj['via'] : ''
            if (via != 'Admin') {
                // 统计android和ios的充值人数，去重
                if (client == 2) {
                    android_recharge_set.add(userId)
                } else if (client == 4) {
                    ios_recharge_set.add(userId)
                } else {
                    other_recharge_set.add(userId)
                }
            }

            if (cny != null) {
                cny = new BigDecimal(cny)
                total = total.add(cny)
                total_cut = total_cut.add(cny.multiply(PAY_RATES.get(via) ?: 1))
                payType.cny = payType.cny.add(cny)
                // 统计android和ios的充值金额
                if (client == 2) {
                    android_recharge += cny
                } else if (client == 4) {
                    ios_recharge += cny
                } else {
                    other_recharge += cny
                }
            }
            def coin = obj.get('diamond') as Long
            if (coin) {
                totalCoin.addAndGet(coin)
                payType.coin.addAndGet(coin)
                if (!via.equals('Admin')) {
                    charge_coin.addAndGet(coin)
                } else {
                    hand_coin.addAndGet(coin)
                }
            }
        }
        def obj = new BasicDBObject(
                _id: "${YMD}_finance".toString(),
                total: total.doubleValue(), //总充值额度
                total_cut: total_cut.doubleValue(), //预收款
                total_coin: totalCoin, //总钻石
                charge_coin: charge_coin, //充值加钻
                hand_coin: hand_coin, //手动加钻
                type: 'finance',
                android_recharge: android_recharge,
                ios_recharge: ios_recharge,
                other_recharge: other_recharge,
                ios_recharge_count: ios_recharge_set.size(),
                android_recharge_count: android_recharge_set.size(),
                other_recharge_count: other_recharge_set.size(),
                timestamp: begin
        )
        pays.each { String key, PayType type -> obj.put(StringUtils.isBlank(key) ? '' : key.toLowerCase(), type.toMap()) }

        coll.save(obj)

    }

    //充值统计(充值方式划分)
    static payStatics(int i) {
        def gteMill = yesTday - i * DAY_MILLON
        def YMD = new Date(gteMill).format("yyyyMMdd")
        def time = [$gte: gteMill, $lt: gteMill + DAY_MILLON]

        //当天新增用户
        def regs = users.find(new BasicDBObject(timestamp: [$gte: gteMill, $lt: gteMill + DAY_MILLON]))*._id

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
        //总数
        PayStat total = new PayStat()
        PayStat reg_total = new PayStat() //充值新增
        def pc = channel_pay_DB.find(new BasicDBObject([client: [$in: ["1", "5"]], _id: [$ne: 'Admin']])).toArray()*._id
        def mobile = channel_pay_DB.find(new BasicDBObject([client: ['$nin': ["1", "5"]], _id: [$ne: 'Admin']])).toArray()*._id
        [pc    : pc,//PayType
         mobile: mobile,
        ].each { String k, List<String> v ->
            PayStat all = new PayStat()
            PayStat delta = new PayStat()
            def cursor = finance_log_DB.find($$([timestamp: time, via: [$in: v.toArray()]]),
                    new BasicDBObject(user_id: 1, cny: 1, diamond: 1, timestamp: 1)).batchSize(50000)
            while (cursor.hasNext()) {
                def obj = cursor.next()
                def user_id = obj['user_id'] as String
                def cny = new BigDecimal(((Number) obj.get('cny')).doubleValue())
                def coin = obj.get('diamond') as Long
                all.add(user_id, cny, coin)
                total.add(user_id, cny, coin)
                if (regs.contains(user_id)) { //新充值用户
                    reg_total.add(user_id, cny, coin)
                }
                //该用户之前无充值记录或首冲记录为当天则算为当天新增用户
                if (old_ids.containsKey(user_id)) {
                    def userTimestamp = old_ids.get(user_id) as Long
                    Long day = gteMill
                    Long userday = new Date(userTimestamp).clearTime().getTime()//首冲日期
                    if (day.equals(userday)) {
                        delta.add(user_id, cny, coin)
                    }
                }
            }
            typeMap.put(k + 'all', all)
            typeMap.put(k + 'delta', delta)
        }

        coll.update(new BasicDBObject(_id: YMD + '_allpay'),
                new BasicDBObject(type: 'allpay',
                        user_pay: total.toMap(),
                        user_reg_pay: reg_total.toMap(), //新增充值
                        user_pay_pc: typeMap.get('pcall').toMap(),
                        user_pay_pc_delta: typeMap.get('pcdelta').toMap(),
                        user_pay_mobile: typeMap.get('mobileall').toMap(),
                        user_pay_mobile_delta: typeMap.get('mobiledelta').toMap(),
                        timestamp: gteMill
                ), true, false)
    }

    //次日留存
    static payStaticsRetation(int i) {
        //记录在前一日的report中
        def stat_report = mongo.getDB('xy_admin').getCollection('stat_report')
        def gteMill = yesTday - (i + 1) * DAY_MILLON
        def YMD = new Date(gteMill).format("yyyyMMdd")
        def time = [$gte: gteMill, $lt: gteMill + DAY_MILLON]
        //充值次日留存
        def ids = finance_log_DB.find($$(timestamp: time)).toArray()*.user_id
        def log = mongo.getDB("xylog").getCollection("day_login")
        def retention = log.count($$(timestamp: [$gte: gteMill + DAY_MILLON, $lt: gteMill + 2 * DAY_MILLON], user_id: [$in: ids]))
        stat_report.update(new BasicDBObject(_id: "${YMD}_allreport".toString()), $$($set: ['1_pay': retention]), true, false)
    }

    /**
     * 根据充值人注册时的联运渠道统计
     */
    static payStaticsByUserChannel(int i) {
        def users = mongo.getDB('xy').getCollection('users')
        def finance_log = mongo.getDB('xy_admin').getCollection('finance_log')
        def channel = mongo.getDB('xy_admin').getCollection('channels')

        Long begin = yesTday - i * DAY_MILLON
        def timeBetween = [$gte: begin, $lt: begin + DAY_MILLON]
        def logMap = new HashMap(), payuids = new HashSet(), qdList = []
        //查询当天充值的所有用户及充值金额、获得的钻石数、充值次数
        finance_log.aggregate(
                new BasicDBObject('$match', [via: [$ne: 'Admin'], timestamp: timeBetween]),
                new BasicDBObject('$project', [user_id: '$user_id', to_id: '$to_id', diamond: '$diamond', cny: '$cny', count: '$count', qd: '$qd']),
                new BasicDBObject('$group', [_id: [fid: '$user_id', tid: '$to_id'], qd: [$first: '$qd'], coin: [$sum: '$diamond'], cny: [$sum: '$cny'], count: [$sum: 1]])
        ).results().each { BasicDBObject logObj ->
            def id = logObj.remove('_id') as Map
            def fid = id.get('fid') as Integer
            def tid = id.get('tid') as Integer
            def finance = logMap.get(fid) as BasicDBObject
            logObj.append('_id', fid)
            if (finance == null) {
                finance = logObj.copy()
                logMap.put(fid, finance)
            } else {
                def cny = finance.get('cny') ?: 0 as Double
                def coin = finance.get('coin') ?: 0 as Integer
                def count = finance.get('count') ?: 0 as Integer
                finance.put('cny', cny + (logObj.get('cny') ?: 0 as Double))
                finance.put('coin', coin + (logObj.get('coin') ?: 0 as Integer))
                finance.put('count', count + (logObj.get('count') ?: 0 as Integer))
            }
            //充值用户对应的注册渠道信息
            if (finance.getAt('qd')) qdList.add(finance.getAt('qd') as String)
            //充值用户信息
            if (fid != null) payuids.add(fid)
            if (tid != null && tid != fid) payuids.add(tid)
        }
        if (logMap.size() > 0) {
            def clientMap = new HashMap(), channelMap = new HashMap()
            def YMD = new Date(begin).format("yyyyMMdd")
            //查询渠道对应的平台
            channel.find(new BasicDBObject(_id: [$in: qdList])).toArray().each { BasicDBObject obj ->
                channelMap.put(obj.get('_id'), obj.get('client'))
            }
            logMap.each { Integer uid, BasicDBObject finance ->
                def qd = finance.get('qd') as String
                //未知渠道数据统一计算在官方渠道
                def client = channelMap.get(qd) ?: '5'
                def payStat = clientMap.get(client as String) as PayStat
                if (payStat == null) {
                    payStat = new PayStat()
                    clientMap.put(client, payStat)
                }
                def cny = finance.get('cny') ?: 0
                def coin = finance.get('coin') ?: 0
                def count = finance.get('count') ?: 0
                payStat.add(uid, cny as Double, coin as Long, count as Integer)
            }
            def user_pc = clientMap.get('1') as PayStat
            def user_mobile = clientMap.get('2') as PayStat
            def user_ios = clientMap.get('4') as PayStat
            def user_h5 = clientMap.get('5') as PayStat
            def user_ria = clientMap.get('6') as PayStat
            def setVal = [type: 'allpay', timestamp: begin] as Map
            if (user_pc != null) {
                setVal.put('user_pc', user_pc.toMap())
            }
            if (user_mobile != null) {
                setVal.put('user_mobile', user_mobile.toMap())
            }
            if (user_ios != null) {
                setVal.put('user_ios', user_ios.toMap())
            }
            if (user_h5 != null) {
                setVal.put('user_h5', user_h5.toMap())
            }
            if (user_ria != null) {
                setVal.put('user_ria', user_ria.toMap())
            }
            //查询新增充值用户
            def newuser = new HashSet(1000)
            def oldlogs = new HashSet(1000)
            finance_log.find(new BasicDBObject('via': [$ne: 'Admin'],
                    $or: [[user_id: [$in: payuids]], [to_id: [$in: payuids]]],
                    timestamp: [$lt: begin])).toArray().each { BasicDBObject obj ->
                def fid = obj.get('user_id') as Integer
                def tid = obj.get('to_id') as Integer
                if (fid != null) oldlogs.add(fid)
                if (tid != null && tid != fid) oldlogs.add(tid)
            }
            for (Integer userId : payuids) {
                if (!oldlogs.contains(userId)) {
                    newuser.add(userId)
                }
            }
            setVal.put('first_pay', newuser)
            setVal.put('first_pay_cout', newuser.size())
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

    //PC、手机全部渠道的活跃统计
    static activeStatics(int i) {
        def gteMill = yesTday - i * DAY_MILLON
        def ltMill = gteMill + DAY_MILLON
        def YMD = new Date(gteMill).format('yyyyMMdd')
        def day7Mill = gteMill - 6 * DAY_MILLON
        def day30Mill = gteMill - 29 * DAY_MILLON
        def clients = [null, emptyMap(3), emptyMap(3), null, emptyMap(3), emptyMap(3), emptyMap(3)]
        def totalMap = new HashMap<String, Integer>(3)
        //[次日， 7日， 30日]
        def totalDays = [emptySet(200000), emptySet(200000), emptySet(200000)]
        /*def totalDaySet = new HashSet<Integer>(200000)
        Set<Integer> totalDay3Set = new HashSet<Integer>(2000000)
        Set<Integer> totalDay7Set = new HashSet<Integer>(2000000)
        Set<Integer> totalDay30Set = new HashSet<Integer>(2000000)*/
        ['1', '2', '4', '5', '6'].each { String client ->
            def qb = new BasicDBObject()
            qb.put('client', client)
            def qds = mongo.getDB('xy_admin').getCollection('channels').find(qb,
                    $$('_id', 1)).toArray().collect { DBObject it -> it.get('_id') }
            def day_login = mongo.getDB("xylog").getCollection("day_login")
            if (qds.size() > 0) {
                def logins = day_login.find(new BasicDBObject(qd: [$in: qds], timestamp: [$gte: day30Mill, $lt: ltMill]), new BasicDBObject([user_id: 1, timestamp: 1])).toArray()
                /*Set<Integer> daySet = new HashSet<Integer>(200000)
                Set<Integer> day3Set = new HashSet<Integer>(200000)
                Set<Integer> day7Set = new HashSet<Integer>(2000000)
                Set<Integer> day30Set = new HashSet<Integer>(2000000)*/
                def daySets = [emptySet(200000), emptySet(200000), emptySet(200000)]
                for (DBObject login : logins) {
                    Integer uid = login.get("user_id") as Integer
                    Long timestamp = login.get('timestamp') as Long
                    (daySets.get(2) as Set).add(uid)
                    (totalDays.get(2) as Set).add(uid)
                    if (timestamp >= day7Mill) {
                        (daySets.get(1) as Set).add(uid)
                        (totalDays.get(1) as Set).add(uid)
                    }
                    if (timestamp >= gteMill) {
                        (daySets.get(0) as Set).add(uid)
                        (totalDays.get(0) as Set).add(uid)
                    }
                }

                def map = clients.get(Integer.parseInt(client))
                if (map == null) {
                    map = clients.get(5)
                    map.put('daylogin', map.get('daylogin') + (daySets.get(0) as Set).size())
                    map.put('day7login', map.get('day7login') + (daySets.get(1) as Set).size())
                    map.put('day30login', map.get('day30login') + (daySets.get(2) as Set).size())
                } else {
                    map.putAll(['daylogin': (daySets.get(0) as Set).size(), 'day7login': (daySets.get(1) as Set).size(), 'day30login': (daySets.get(2) as Set).size()])
                }
            }
        }
        totalMap.putAll(['daylogin': (totalDays.get(0) as Set).size(), 'day7login': (totalDays.get(1) as Set).size(), 'day30login': (totalDays.get(2) as Set).size()])
        def info = new BasicDBObject(type: 'alllogin', login_total: totalMap, pc_login: clients.get(1), mobile_login: clients.get(2),
                ios_login: clients.get(4), h5_login: clients.get(5), ria_login: clients.get(6), timestamp: gteMill);
        coll.update(new BasicDBObject(_id: YMD + '_alllogin'), $$($set: info), true, false)
    }

    static Set emptySet(int cap) {
        return new HashSet(cap)
    }

    static Map emptyMap(int cap) {
        return new HashMap(cap)
    }

    /**
     * 运营数据-运营数据总表
     * @param i
     * @return
     */
    static staticTotalReport(int i) {
        long l = System.currentTimeMillis()
        def gteMill = yesTday - i * DAY_MILLON
        def date = new Date(gteMill)//
        def prefix = date.format('yyyyMMdd_')
        //运营统计报表
        def stat_report = mongo.getDB('xy_admin').getCollection('stat_report')
        // 查询充值渠道、人数等信息
        def pay = coll.findOne(new BasicDBObject(_id: "${prefix}allpay".toString()))
        // 查询充值额度信息
        def pay_coin = coll.findOne(new BasicDBObject(_id: "${prefix}finance".toString()))
        // 查询注册人数
        def regs = users.count(new BasicDBObject(timestamp: [$gte: gteMill, $lt: gteMill + DAY_MILLON]))
        // 查询消费人数
        def cost = coll.findOne(new BasicDBObject(_id: "${prefix}allcost".toString()))
        // 查询日活
        def login = coll.findOne(new BasicDBObject(_id: "${prefix}login".toString()))
        // 钻石消耗
        def costs = diamond_cost_log.find($$(type: 'catch_live', timestamp: [$gte: gteMill, $lt: gteMill + DAY_MILLON]), $$(cost: 1)).toArray()*.cost
        def diamond_count = 0
        costs.each { diamond_count = diamond_count + (it as Integer)}
        def map = new HashMap()
        map.put('type', 'allreport')
        map.put('timestamp', gteMill)
        map.put('pay_coin', (pay_coin?.get('total_coin') ?: 0) as Integer) //充值钻石
        if (pay != null) {
            def user_pay = pay.get('user_pay') as BasicDBObject
            map.put('pay_cny', (user_pay.get('cny') ?: 0) as Double) //1
            map.put('pay_user', (user_pay.get('user') ?: 0) as Integer) //2
            def user_reg_pay = pay.get('user_reg_pay') as BasicDBObject
            map.put('reg_pay_cny', (user_reg_pay.get('cny') ?: 0) as Double) //10
            map.put('reg_pay_user', (user_reg_pay.get('user') ?: 0) as Integer) //11
        }
        if (login != null) {
            map.put('logins', login.get('total') as Integer ?: 0) //日活 4
        }
        map.put('diamond_cost', diamond_count) //钻石消耗总数 8
        map.put('regs', regs) //9 新增总人数
        if (cost != null) {
            def user_cost = cost.get('user_cost') as BasicDBObject
            if(user_cost != null){
                def coin = user_cost.get('cost') as Double
                def cost_cny = new BigDecimal(coin / 100).toDouble()
                map.put('cost_cny', cost_cny)
                map.put('cost_user', (user_cost.get('user') ?: 0) as Integer)
            }
        }
        stat_report.update(new BasicDBObject(_id: "${prefix}allreport".toString()), $$($set: map), true, false)
    }

    static exchange_expire(int i) {
        //要加个时间控制
        def gteMill = yesTday - i * DAY_MILLON
        def url = "http://test-api.17laihou.com/job/catch_success_expire?stime=${new Date(gteMill).format('yyyy-MM-dd HH:mm:ss')}"
        println request(url)
    }


    static Integer DAY = 0;

    static void main(String[] args) { //待优化，可以到历史表查询记录
        long l = System.currentTimeMillis()
        long begin = l

        //01.充值的日报表
        /*l = System.currentTimeMillis()
        financeStatics(DAY)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   financeStatics, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //02.登录的日报表
        l = System.currentTimeMillis()
        loginStatics(DAY)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   loginStatics, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //03.用户充值日报表（充值方式划分）
        l = System.currentTimeMillis()
        payStatics(DAY)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   payStatics, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //04.用户登录活跃度统计
        l = System.currentTimeMillis()
        activeStatics(DAY)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   activeStatics, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //05.用户充值日报表（用户注册渠道划分）
        l = System.currentTimeMillis()
        payStaticsByUserChannel(DAY)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   payStaticsByUserChannel, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //06.运营数据总表
        l = System.currentTimeMillis()
        staticTotalReport(DAY)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticTotalReport, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //07.次日留存
        l = System.currentTimeMillis()
        payStaticsRetation(DAY)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   payStaticsRetation, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)*/

        //08.过期兑换
        l = System.currentTimeMillis()
        exchange_expire(DAY)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   payStaticsRetation, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //落地定时执行的日志
        jobFinish(begin)
    }

    static String request(String url) {
        HttpURLConnection conn = null
        def jsonText = ""
        try {
            conn = (HttpURLConnection) new URL(url).openConnection()
            conn.setRequestMethod("GET")
            conn.setDoOutput(true)
            conn.setConnectTimeout(3000)
            conn.setReadTimeout(3000)
            conn.connect()
            jsonText = conn.getInputStream().getText("UTF-8")

        } catch (Exception e) {
            println "request Exception : " + e
        } finally {
            if (conn != null) {
                conn.disconnect()
            }
        }
        return jsonText
    }

    /**
     * 标记任务完成  用于运维监控
     * @return
     */
    private static jobFinish(Long begin) {
        def timerName = 'StaticsEveryDay'
        Long totalCost = System.currentTimeMillis() - begin
        saveTimerLogs(timerName, totalCost)
        println "${new Date().format('yyyy-MM-dd')}:${StaticsEveryDay.class.getSimpleName()}:finish  cost  ${System.currentTimeMillis() - begin} ms"
    }

    //落地定时执行的日志
    private static saveTimerLogs(String timerName, Long totalCost) {
        def timerLogsDB = mongo.getDB("xyrank").getCollection("timer_logs")
        def tmp = System.currentTimeMillis()
        def id = timerName + "_" + new Date().format("yyyyMMdd")
        def update = new BasicDBObject(timer_name: timerName, cost_total: totalCost, cat: 'day', unit: 'ms', timestamp: tmp)
        timerLogsDB.findAndModify(new BasicDBObject('_id', id), null, null, false, new BasicDBObject('$set', update), true, true)
    }

    static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    static BasicDBObject $$(Map map) {
        return new BasicDBObject(map)
    }

}