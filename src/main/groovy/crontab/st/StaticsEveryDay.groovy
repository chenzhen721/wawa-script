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

    static final String api_domain = getProperties("api.domain", "http://localhost:8080/")

    static DAY_MILLON = 24 * 3600 * 1000L

    static long zeroMill = new Date().clearTime().getTime()
    static Long yesTday = zeroMill - DAY_MILLON
    static String YMD = new Date(yesTday).format("yyyyMMdd")

    static DBCollection coll = mongo.getDB('xy_admin').getCollection('stat_daily')
    static DBCollection room_cost_DB = mongo.getDB("xylog").getCollection("room_cost")
    static DBCollection medal_award_logs = mongo.getDB("xylog").getCollection("medal_award_logs")
    static DBCollection finance_log_DB = mongo.getDB('xy_admin').getCollection('finance_log')
    static DBCollection users = mongo.getDB('xy').getCollection('users')
    static DBCollection channel_pay_DB = mongo.getDB('xy_admin').getCollection('channel_pay')
    static DBCollection active_award_logs = mongo.getDB('xyactive').getCollection('active_award_logs')
    static DBCollection games_DB = mongo.getDB('xy_admin').getCollection('games')
    static DBCollection user_bet_DB = mongo.getDB('game_log').getCollection('user_bet')
    static DBCollection user_lottery_DB = mongo.getDB('game_log').getCollection('user_lottery')
    static DBCollection game_round_DB = mongo.getDB('game_log').getCollection('game_round')
    static String DEFAULT_QD = 'aiwan_default'

    static giftStatics() {

        def res = room_cost_DB.aggregate(
                new BasicDBObject('$match', [type: "send_gift", timestamp: [$gte: yesTday, $lte: zeroMill]]),
                new BasicDBObject('$project', [_id: '$session.data._id', name: '$session.data.name', count: '$session.data.count', cost: '$cost']),
                new BasicDBObject('$group', [_id: '$_id', name: [$first: '$name'], count: [$sum: '$count'], cost: [$sum: '$cost']])
        )
        Iterator records = res.results().iterator();
        while (records.hasNext()) {
            def obj = records.next()
            obj.gift_id = obj._id
            obj.type = "gift_detail"
            obj._id = "${YMD}_gift_detail_${obj._id}".toString()
            obj.timestamp = yesTday
            coll.save(obj)
        }
    }


    static loginStatics(int i) {
        // yesTday 昨天凌晨 - 1天 = 前天凌晨
        def begin = yesTday - i * DAY_MILLON
        def end = begin + DAY_MILLON
        def YMD = new Date(begin).format('yyyyMMdd')
        def log = mongo.getDB("xylog").getCollection("day_login");
        def query = new BasicDBObject(timestamp: [$gte: begin, $lt: end])
        def channel = mongo.getDB('xy_admin').getCollection('channels')
        def pcqds = channel.find(new BasicDBObject(client: '1')).toArray()*._id
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

    static financeStatics() {
        def list = mongo.getDB('xy_admin').getCollection('finance_log').find(new BasicDBObject(timestamp: [$gte: yesTday, $lt: zeroMill]))
                .toArray()

//        def cats = MapWithDefault.newInstance(new HashMap<String, BigDecimal>()) {
//            return new BigDecimal(0)
//        }
        def total = new BigDecimal(0)
        def totalCoin = new AtomicLong()

        def pays = MapWithDefault.<String, PayType> newInstance(new HashMap()) { new PayType() }

        list.each { obj ->
            def cny = obj.get('cny') as Double
//            def cat = getCat(obj)
            def payType = pays[obj.via]
            payType.count.incrementAndGet()
            payType.user.add(obj.user_id)
            if (cny != null) {
                cny = new BigDecimal(cny)
                total = total.add(cny)
//                cats[cat] = cats[cat].add(cny)
                payType.cny = payType.cny.add(cny)
            }
            def coin = obj.get('coin') as Long
            if (coin) {
                totalCoin.addAndGet(coin)
                payType.coin.addAndGet(coin)
            }
        }

        def obj = new BasicDBObject(
                _id: "${YMD}_finance".toString(),
                total: total.doubleValue(),
                total_coin: totalCoin,
                type: 'finance',
                timestamp: yesTday
        )
        pays.each { String key, PayType type -> obj.put(StringUtils.isBlank(key) ? '' : key.toLowerCase(), type.toMap()) }
//        cats.each { k, v ->
//            obj.put(k, v.doubleValue())
//        }
        coll.save(obj)

    }
    static Set dianXin = ['SZX-NET', 'UNICOM-NET', 'TELECOM-NET '] as HashSet

    static String getCat(Map obj) {
        String via = obj.get('via')
        String shop
        println("vai is ${via}")
        if ('Admin'.equals(via)) {
            return "1"
        } else if ('ali_pc'.equals(via)) {
            return '5'
        } else if ('Ipay'.equals(via) || 'ali_m'.equals(via)) {
            return '7'
        } else if ((shop = obj.get('shop')) != null) {
            if (shop.endsWith('-B2C')) {//银行 2
                return "2"
            } else if (dianXin.contains(shop)) { // 电信卡3
                return "3"
            } else if (shop.endsWith('-NET')) {//4.游戏点卡
                return "4"
            }
        }

        return '7'
    }

    public static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    public static BasicDBObject $$(Map map) {
        return new BasicDBObject(map)
    }

    static weekstarStatic(int i) {
        def gteMill = yesTday - i * DAY_MILLON
        def YMD = new Date(gteMill).format("yyyyMMdd")
        def timestamp = [$gte: gteMill, $lt: gteMill + DAY_MILLON]
        //获取周星礼物
        room_cost_DB.aggregate(
                new BasicDBObject('$match', [type: 'send_gift', 'session.data.category_id': 210, timestamp: timestamp]),
                new BasicDBObject('$project', [_id: '$session.data._id', name: '$session.data.name', count: '$session.data.count', cost: '$cost']),
                new BasicDBObject('$group', [_id: '$_id', name: [$first: '$name'], count: [$sum: '$count'], cost: [$sum: '$cost']])
        ).results().each { BasicDBObject obj ->
            def _id = obj.remove('_id') as Integer
            obj.put('gift_id', _id)
            obj.put('type', 'week_stars')
            obj.put('timestamp', gteMill)
            obj.put('_id', "${YMD}_${_id}_week_stars".toString())
            coll.save(obj)
        }
    }

    static userRemainByAggregate() {
        def coin = 0
        def bean = 0
        users.aggregate(
                new BasicDBObject('$match', new BasicDBObject('finance.coin_count': [$gt: 0])),
                new BasicDBObject('$project', [coin: '$finance.coin_count']),
                new BasicDBObject('$group', [_id: null, coin: [$sum: '$coin']])
        ).results().each { BasicDBObject obj ->
            coin = obj.get('coin') as Long
        }
        users.aggregate(
                new BasicDBObject('$match', new BasicDBObject('finance.bean_count': [$gt: 0])),
                new BasicDBObject('$project', [bean: '$finance.bean_count']),
                new BasicDBObject('$group', [_id: null, bean: [$sum: '$bean']])
        ).results().each { BasicDBObject obj ->
            bean = obj.get('bean') as Long
        }
        //println "userRemainByAggregate totalcoin:---->:${coin}"
        //println "userRemainByAggregate totalbean:---->:${bean}"
        [coin: coin, bean: bean]
    }

    static costStatics() {
        def cost = dayCost() as Map
        def remain = userRemainByAggregate()
        cost.putAll([
                _id        : YMD + '_allcost',
                type       : 'allcost',
                user_remain: remain,
                timestamp  : yesTday
        ])
        coll.save(new BasicDBObject(cost))
    }

    /**
     * 日虚拟币消耗
     * @return
     */
    static dayCost() {
        def q = new BasicDBObject('type': 'send_gift', 'timestamp': [$gte: yesTday, $lt: zeroMill])
        def result = [:], costs = 0L, users = new HashSet()
        room_cost_DB.aggregate(
                new BasicDBObject('$match', q),
                new BasicDBObject('$project', [type: '$type', cost: '$cost', user: '$session._id']),
                new BasicDBObject('$group', [_id: '$type', cost: [$sum: '$cost'], user: [$addToSet: '$user']])
        ).results().each { BasicDBObject obj ->
            def type = obj.get('_id')
            def cost = obj.get('cost') as Integer
            cost = cost ?: 0
            costs += cost
            def user = obj.get('user') as Set
            users.addAll(user)
            result.put(type, [cost: cost, user: user.size()])
        }

        // 加入游戏下注的逻辑
        def gameList = games_DB.find()
        gameList.each {
            def id = it._id as Integer
            def game_cost = 0L
            def query = $$('timestamp': [$gte: yesTday, $lt: zeroMill], 'game_id': id)
            def field = $$('cost': 1, 'user_id': 1)
            def list = user_bet_DB.find(query, field).toArray()
            def game_user = new HashSet()
            list.each {
                BasicDBObject obj ->
                    game_cost += obj['cost'] as Long
                    users.add(obj['user_id'] as Integer)
                    game_user.add(obj['user_id'] as Integer)
            }
            costs += game_cost
            result.put(id.toString(), [cost: game_cost, user: game_user.size()])
        }

        // 对游戏和送礼加起来统计

        result.put('user_cost', [cost: costs, user: users.size()])

        return result
    }

    //充值统计(充值方式划分)
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
        def pc = channel_pay_DB.find(new BasicDBObject([client: "1", _id: [$ne: 'Admin']])).toArray()*._id
        def mobile = channel_pay_DB.find(new BasicDBObject([client: "2", _id: [$ne: 'Admin']])).toArray()*._id
        [pc    : pc,//PayType
         mobile: mobile,
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
        coll.update(new BasicDBObject(_id: YMD + '_allpay'),
                new BasicDBObject(type: 'allpay',
                        user_pay: total.toMap(),
                        user_pay_pc: typeMap.get('pcall').toMap(),
                        user_pay_pc_delta: typeMap.get('pcdelta').toMap(),
                        user_pay_mobile: typeMap.get('mobileall').toMap(),
                        user_pay_mobile_delta: typeMap.get('mobiledelta').toMap(),
                        timestamp: gteMill
                ), true, false)
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
        //查询当天充值的所有用户及充值金额、获得的柠檬数、充值次数
        finance_log.aggregate(
                new BasicDBObject('$match', [via: [$ne: 'Admin'], timestamp: timeBetween]),
                new BasicDBObject('$project', [user_id: '$user_id', to_id: '$to_id', coin: '$coin', cny: '$cny', count: '$count', qd: '$qd']),
                new BasicDBObject('$group', [_id: [fid: '$user_id', tid: '$to_id'], qd: [$first: '$qd'], coin: [$sum: '$coin'], cny: [$sum: '$cny'], count: [$sum: 1]])
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
                def client = channelMap.get(qd) ?: '1'
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
            setVal.put('first_pay_cout', newuser)
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
                def logins = day_login.find(new BasicDBObject(qd: [$in: qds], timestamp: [$gte: day30Mill, $lt: ltMill]), new BasicDBObject([user_id: 1, timestamp: 1])).toArray()
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
                } else if ('4'.equals(client)) {
                    iosMap.putAll(['daylogin': daySet.size(), 'day7login': day7Set.size(), 'day30login': day30Set.size()])
                } else if ('5'.equals(client)) {
                    h5Map.putAll(['daylogin': daySet.size(), 'day7login': day7Set.size(), 'day30login': day30Set.size()])
                } else if ('6'.equals(client)) {
                    riaMap.putAll(['daylogin': daySet.size(), 'day7login': day7Set.size(), 'day30login': day30Set.size()])
                } else {
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
        def info = new BasicDBObject(type: 'alllogin', login_total: totalMap, pc_login: pcMap, mobile_login: mobileMap,
                ios_login: iosMap, h5_login: h5Map, ria_login: riaMap, timestamp: gteMill);
        coll.update(new BasicDBObject(_id: YMD + '_alllogin'), info, true, false)
    }

    /**
     * 统计每日签到人数和奖励信息（按渠道和版本划分）
     * todo 爱玩只有签到日志 没有抽奖目前
     * @param i
     */
//    static staticSign(int i) {
//        long l = System.currentTimeMillis()
//        def gteMill = yesTday - i * DAY_MILLON
//        def date = new Date(gteMill)//
//        def coll = mongo.getDB('xy_admin').getCollection('stat_sign')
//        def day_login = mongo.getDB('xylog').getCollection('day_login')
//        def lotterylog = mongo.getDB('xylog').getCollection('lottery_logs')
//        def users = mongo.getDB('xy').getCollection('users')
//        //查询签到抽奖情况
//        def award_map = new HashMap()
//        lotterylog.find(new BasicDBObject(active_name: 'sign_chest', timestamp: [$gte: gteMill, $lt: gteMill + DAY_MILLON]))
//                .toArray().each { BasicDBObject obj ->
//            award_map.put(obj.get('user_id') as Integer, obj.get('award_name'))
//        }
//        def map = new HashMap<String, SignType>()
//        day_login.find(new BasicDBObject($or: [[sign: true, sign_time: [$gte: gteMill, $lt: gteMill + DAY_MILLON]], [award: true, award_time: [$gte: gteMill, $lt: gteMill + DAY_MILLON]]]))
//                .toArray().each { BasicDBObject obj ->
//            def userId = obj.get('user_id') as Integer
//            def user = users.findOne(new BasicDBObject(_id: userId), new BasicDBObject(app_ver: 1, qd: 1))
//            def version = (user?.get('app_ver') ?: '0.0.0') as String
//            def qd = (user?.get('qd') ?: 'MM') as String//default official
//            def awardName = award_map.get(userId) as String
//            def _id = "${qd}_${version}".toString()
//            SignType signType = map.get(_id)
//            if (signType == null) {
//                signType = new SignType(qd, version)
//                map.put(_id, signType)
//            }
//            signType.add(userId, awardName)
//        }
//        map.each { String k, SignType signType ->
//            coll.update(new BasicDBObject(_id: "${date.format('yyyyMMdd')}_${k}".toString()),
//                    new BasicDBObject(signType.toMap()).append('timestamp', gteMill), true, false)
//        }
//    }

    /**
     * 爱玩签到
     * @param i
     * @return
     */
    static staticSign(int i) {
        def gteMill = yesTday - i * DAY_MILLON
        def date = new Date(gteMill).format('yyyyMMdd')
//        def coll = mongo.getDB('xy_admin').getCollection('stat_sign')
        def sign_log = mongo.getDB('xylog').getCollection('sign_logs')
        def map = new HashMap()
        def total_count = 0
        def total_coin = 0L
        sign_log.aggregate(
                $$('$match': $$('timestamp': [$gte: gteMill, $lt: gteMill + DAY_MILLON])),
                $$('$project': $$('coin': '$coin', qd: '$qd', 'user_id': '$user_id')),
                $$('$group': $$('_id': '$qd', 'coin': $$('$sum': '$coin'), 'users': $$('$addToSet': '$user_id')))
        ).results().each {
            BasicDBObject obj ->
                def tmp = new HashMap()
                def qd = obj['_id'] as String
                // 如果渠道是null就算在aiwan_default上
                if(StringUtils.isBlank(qd)){
                    qd = DEFAULT_QD
                }
                def users = obj['users'] as HashSet
                def user_count = users.size()
                def coin = obj['coin'] as Long
                total_coin += coin
                total_count += user_count
                tmp.put('qd', qd)
                tmp.put('user_count', user_count)
                tmp.put('coin', coin)
                // 将null的渠道归并到aiwan_default
                if(map.containsKey(qd.toString())){
                    def v = map.get(qd.toString()) as Map
                    def current_coin = v['coin'] as Long
                    def current_count = v['user_count'] as Integer
                    coin += current_coin
                    user_count += current_count
                    tmp.put('user_count', user_count)
                    tmp.put('coin', coin)
                }
                map.put(qd.toString(),tmp)
        }
        def id = date + '_check_in'
        def row = ['_id':id,'timestamp':gteMill,'qd':map,'total_count':total_count,'total_coin':total_coin,'type':'check_in']
        coll.save($$(row))
    }

    static class SignType {
        final String qd
        final String version
        def userList = new HashSet()
        def awardMap = new HashMap<String, Integer>()

        def SignType(String qd, String version) {
            this.qd = qd
            this.version = version
        }

        def add(Integer userId, String awardName) {
            userList.add(userId)
            if (StringUtils.isNotBlank(awardName)) {
                Integer num = awardMap.get(awardName) ?: 0
                num++
                awardMap.put(awardName, num)
            }
        }

        def toMap() {
            [qd: qd, version: version, user_count: userList.size(), award: awardMap]
        }
    }

    /**
     * 统计运营数据总表
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
        // 查询充值信息
        def pay = coll.findOne(new BasicDBObject(_id: "${prefix}allpay".toString()))
        // 查询充值柠檬
        def pay_coin = coll.findOne(new BasicDBObject(_id: "${prefix}finance".toString()))
        // 查询注册人数
        def regs = users.count(new BasicDBObject(timestamp: [$gte: gteMill, $lt: gteMill + DAY_MILLON]))
        // 查询消费信息
        def cost = coll.findOne(new BasicDBObject(_id: "${prefix}allcost".toString()))
        def map = new HashMap()
        map.put('type', 'allreport')
        map.put('timestamp', gteMill)
        map.put('pay_coin', (pay_coin?.get('total_coin') ?: 0) as Integer)
        if (pay != null) {
            def user_pay = pay.get('user_pay') as BasicDBObject
            map.put('pay_cny', (user_pay.get('cny') ?: 0) as Double)
            map.put('pay_user', (user_pay.get('user') ?: 0) as Integer)
        }
        map.put('regs', regs)
        if (cost != null) {
            def user_cost = cost.get('user_cost') as BasicDBObject
            def coin = user_cost.get('cost') as Double
            def cost_cny = new BigDecimal(coin / 100).toDouble()
            map.put('cost_cny', cost_cny)
            map.put('cost_user', (user_cost.get('user') ?: 0) as Integer)
        }
        stat_report.update(new BasicDBObject(_id: "${prefix}allreport".toString()), new BasicDBObject(map), true, false)
    }

    /**
     * 特殊用户消费统计，该用户送礼物的主播及主播获得的维C
     * 消费类型有：send_gift,grab_sofa,song,buy_guard,send_treasure,level_up,nest_send_gift等
     * @param i
     */
    static staticMvp(int i) {
        def begin = yesTday - i * DAY_MILLON
        def timebetween = [$gte: begin, $lt: begin + DAY_MILLON]
        def mvp = mongo.getDB('xy_admin').getCollection('mvps')
        def stat_mvp = mongo.getDB('xy_admin').getCollection('stat_mvps')
        def YMD = new Date(begin).format('yyyyMMdd_')
        def ids = []
        mvp.find(new BasicDBObject(type: 1), new BasicDBObject(timestamp: 0))
                .toArray().each { BasicDBObject obj ->
            ids.add(obj.get('user_id') as String)
        }
        if (ids.size() == 0) return
        room_cost_DB.aggregate(
                new BasicDBObject('$match', ['session._id'            : [$in: ids],
                                             'session.data.xy_star_id': [$ne: null],
                                             timestamp                : timebetween]),
                new BasicDBObject('$project', [id    : '$session._id',
                                               star  : '$session.data.xy_star_id',
                                               cost  : '$star_cost',
                                               earned: '$session.data.earned']),
                new BasicDBObject('$group', [_id   : [id: '$id', star: '$star'],
                                             cost  : ['$sum': '$cost'],
                                             earned: ['$sum': '$earned']])
        ).results().each { BasicDBObject obj ->
            def id = obj.get('_id')['id'] as Integer
            def star = obj.get('_id')['star'] as Integer
            def cost = obj.get('cost') as Integer
            def earned = obj.get('earned') as Integer
            def update = new BasicDBObject()
            update.putAll(user_id: id, star_id: star, cost: cost, star_earned: earned, type: 'mvp_cost', timestamp: begin)
            stat_mvp.update(new BasicDBObject(_id: "${YMD}${id}_${star}".toString()), update, true, false)
        }

        //每日消费大于1000的用户
        finance_log_DB.aggregate(
                new BasicDBObject('$match', [via: [$ne: 'Admin'], timestamp: timebetween]),
                new BasicDBObject('$project', [_id: '$user_id', cny: '$cny', coin: '$coin']),
                new BasicDBObject('$group', [_id: '$_id', cny: [$sum: '$cny'], coin: [$sum: '$coin']]),
                new BasicDBObject('$match', [cny: [$gte: 1000]]),
                new BasicDBObject('$sort', [cny: -1])
        ).results().each { BasicDBObject obj ->
            def id = obj.removeField('_id') as Integer
            def update = new BasicDBObject()
            def cny = obj.get('cny')
            def coin = obj.get('coin')
            update.putAll(user_id: id, cny: cny, coin: coin, type: 'mvp_pay_most', timestamp: begin)
            stat_mvp.update(new BasicDBObject(_id: "${YMD}${id}_vip".toString()), update, true, false)
        }
    }

    static missionStatic(int i) {
        def begin = yesTday - i * DAY_MILLON
        def YMD = new Date(begin).format('yyyyMMdd_')
        def timebetween = [$gte: begin, $lt: begin + DAY_MILLON]
        def mission = mongo.getDB('xylog').getCollection('mission_logs')
        def row = new BasicDBObject()
        mission.aggregate(
                new BasicDBObject('$match', [timestamp: timebetween]),
                new BasicDBObject('$project', [mission_id: '$mission_id', coin: '$coin']),
                new BasicDBObject('$group', [_id: '$mission_id', count: [$sum: 1], coin: [$sum: '$coin']])
        ).results().each { BasicDBObject obj ->
            row.put(obj.removeField('_id'), obj)
        }
        row._id = "${YMD}_mission".toString()
        row.timestamp = begin
        row.type = "mission"

        coll.save(row)
    }

    /**
     * 统计每个游戏每天有多少人玩过
     * 统计每个有效游戏的局数
     * 统计每个游戏每天下注金额
     * 统计玩家输赢的总金额
     */
    static gameStatic() {
        def timebetween = [$gte: yesTday, $lt: zeroMill]
        def YMD = new Date(yesTday).format('yyyyMMdd_')
        def gameList = games_DB.find()
        def row = new BasicDBObject()
        def playerTotal = new HashMap()
        def roundTotal = new HashMap()
        def betsTotal = new HashMap()
        def lotteryTotal = new HashMap()
        gameList.each {
            it ->
                def gameId = it._id as Integer
                // 统计游戏参与人数
                def query = $$('game_id': gameId, 'timestamp': timebetween)
                def player_total = user_bet_DB.distinct('user_id', query)
                def count = player_total.size()
                playerTotal.put(gameId.toString(), count)

                // 统计每个游戏下注情况
                def list = user_bet_DB.find(query)
                def bets_coin_total = 0
                while (list.hasNext()) {
                    def obj = list.next()
                    def cost = obj['cost'] as Integer
                    bets_coin_total += cost
                }
                betsTotal.put(gameId.toString(), bets_coin_total)

                // 统计玩家赢得情况
                def userLottery = user_lottery_DB.find(query)
                def win = 0
                while (userLottery.hasNext()) {
                    def obj = userLottery.next()
                    def coin = obj['coin'] as Integer
                    if (coin >= 0) {
                        win += coin
                    }
                }
                lotteryTotal.put(gameId.toString(), win)

                // 统计有效局数
                def rounds = game_round_DB.find(query)
                def total_player = 0
                while (rounds.hasNext()) {
                    def obj = rounds.next()
                    if (obj.containsField('total_user_count')) {
                        total_player += obj['total_user_count'] as Integer
                    }
                }
                roundTotal.put(gameId.toString(), total_player)
        }
        row.player_total = playerTotal
        row.round_total = roundTotal
        row.bet_total = betsTotal
        row.lottery_total = lotteryTotal
        row._id = "${YMD}_game".toString()
        row.timestamp = yesTday
        row.type = "game"
        coll.save(row)
    }

    static Integer DAY = 0;

    static void main(String[] args) { //待优化，可以到历史表查询记录
        long l = System.currentTimeMillis()
//        //01.送礼日报表
        long begin = l

        // 游戏统计
        gameStatic()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   gameStatic, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        giftStatics()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   giftStatics, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //04.充值的日报表
        l = System.currentTimeMillis()
        financeStatics()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   financeStatics, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //05.登录的日报表
        l = System.currentTimeMillis()
        loginStatics(DAY)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   loginStatics, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //10.房间消费日报表
        l = System.currentTimeMillis()
        costStatics()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   costStatics, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //14.用户充值日报表（充值方式划分）
        l = System.currentTimeMillis()
        payStatics(DAY)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   payStatics, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //15.用户登录活跃度统计
        l = System.currentTimeMillis()
        activeStatics(DAY)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   activeStatics, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //17.用户充值日报表（用户注册渠道划分）
        l = System.currentTimeMillis()
        payStaticsByUserChannel(DAY)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   payStaticsByUserChannel, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //18.签到统计
        l = System.currentTimeMillis()
        staticSign(DAY)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticSign, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //19.运营数据总表
        l = System.currentTimeMillis()
        staticTotalReport(DAY)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticTotalReport, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //21.特殊用户统计,运营监控用户
        l = System.currentTimeMillis()
        staticMvp(DAY)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticMvp, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //新手任务统计
        l = System.currentTimeMillis()
        missionStatic(DAY)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   missionStatic, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //落地定时执行的日志
        jobFinish(begin)
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

}