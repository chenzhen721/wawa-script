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

import java.text.SimpleDateFormat
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong


/**
 * 每天统计一份数据
 *
 * date: 13-2-28 下午2:46
 * @author: yangyang.cong@ttpod.com
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

    static mongo = new Mongo(new MongoURI(getProperties('mongo.uri', 'mongodb://192.168.31.246:27017/?w=1') as String))

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


    static luckStatics(int day) {
        def gteMill = yesTday - day * DAY_MILLON
        def YMD = new Date(gteMill).format("yyyyMMdd")
        def timestamp = [$gte: gteMill, $lt: gteMill + DAY_MILLON]
        def res = mongo.getDB("xylog").getCollection("room_luck").aggregate(
                $$('$match', [timestamp: timestamp]),
                $$('$project', [got: '$got']),
                $$('$group', [_id: null, data: [$sum: '$got']]),
        ).results().iterator()
        Long _out = res.hasNext() ? res.next()['data'] : 0L

        res = room_cost_DB.aggregate(
                $$('$match', [type: "send_gift", 'session.data.category_id': 1, timestamp: timestamp]),
                $$('$project', [cost: '$cost']),
                $$('$group', [_id: null, data: [$sum: '$cost']]),
        ).results().iterator()
        Long _in = (res.hasNext() ? res.next()['data'] : 0L) * 0.4

        //Long _in = res.sum {it.session.data.count * it.session.data.coin_price * 0.4} as Long // 忽略了mei zhong jiang de

        def obj = new BasicDBObject(
                _id: "${YMD}_luck".toString(),
                in: _in,
                out: _out,
                type: 'luck',
                timestamp: gteMill
        )
        println obj
        coll.save(obj)
    }


    static loginStatics(int i) {
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

    /* static userRemain (){
         def q = new BasicDBObject()//'finance.coin_count':[$gt:0])
         def list = mongo.getDB("xy").getCollection("users").
                 find(q,new BasicDBObject('finance',1))
                 .toArray()
         if (list.isEmpty()){
             return [coin:0,bean:0]
         }
         [coin:list.sum {it.finance.coin_count} as Long,
                 bean:list.sum {it.finance.bean_count?:0} as Long]
     }*/


    static class PayType {
        final user = new HashSet(1000)
        final count = new AtomicInteger()
        final coin = new AtomicLong()
        def cny = new BigDecimal(0)

        def toMap() { [user: user.size(), count: count.get(), coin: coin.get(), cny: cny.doubleValue()] }
    }

    static financeStatics() {
        def list = mongo.getDB('xy_admin').getCollection('finance_log').find(new BasicDBObject(timestamp: [$gte: yesTday, $lte: zeroMill]))
                .toArray()

        def cats = MapWithDefault.newInstance(new HashMap<String, BigDecimal>()) {
            return new BigDecimal(0)
        }
        def total = new BigDecimal(0)
        def totalCoin = new AtomicLong()

        def pays = MapWithDefault.<String, PayType> newInstance(new HashMap()) { new PayType() }

        list.each { obj ->
            def cny = obj.get('cny') as Double
            def cat = getCat(obj)
            def payType = pays[obj.via]
            payType.count.incrementAndGet()
            payType.user.add(obj.user_id)
            if (cny != null) {
                cny = new BigDecimal(cny)
                total = total.add(cny)
                cats[cat] = cats[cat].add(cny)
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
        cats.each { k, v ->
            obj.put(k, v.doubleValue())
        }
        coll.save(obj)

    }
    static Set dianXin = ['SZX-NET', 'UNICOM-NET', 'TELECOM-NET '] as HashSet

    static String getCat(Map obj) {
        String via = obj.get('via')
        String shop
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

    static vipStatics() {
        def res = room_cost_DB.aggregate(
                new BasicDBObject('$match', [type: "buy_vip", timestamp: [$gte: yesTday, $lte: zeroMill]]),
                new BasicDBObject('$project', [_id: '$session.l', day: '$session.day', cost: '$cost']),
                new BasicDBObject('$group', [_id: '$_id', count: [$sum: 1], cost: [$sum: '$cost']])
        )
        Iterator iter = res.results().iterator();
        def row = new BasicDBObject()
        while (iter.hasNext()) {
            def obj = iter.next()
            def id = obj.removeField("_id")
            if (null == id) {
                row.put("vip1", obj)
            } else {
                row.put("vip2", obj)
            }
        }
        row.type = "buy_vip"
        row._id = "${YMD}_buy_vip".toString()
        row.timestamp = yesTday
        coll.save(row)
    }

    static carStatics() {
        def res = room_cost_DB.aggregate(
                new BasicDBObject('$match', [type: "buy_car", timestamp: [$gte: yesTday, $lte: zeroMill]]),
                new BasicDBObject('$project', [_id: '$live', cost: '$cost']),
                new BasicDBObject('$group', [_id: '$_id', count: [$sum: 1], cost: [$sum: '$cost']])
        )
        Iterator iter = res.results().iterator();
        while (iter.hasNext()) {
            def row = iter.next()
            row.car_id = new Integer(row.get("_id").toString())
            row.type = "buy_car"
            row._id = "${YMD}_buy_car_${row.car_id}".toString()
            row.timestamp = yesTday
            coll.save(row)
        }
    }

    static sofaStatics() {
        def res = room_cost_DB.aggregate(
                new BasicDBObject('$match', [type: "grab_sofa", timestamp: [$gte: yesTday, $lte: zeroMill]]),
                new BasicDBObject('$project', [user_id: '$session._id', cost: '$cost']),
                new BasicDBObject('$group', [_id: null, count: [$sum: 1], cost: [$sum: '$cost'], users: [$addToSet: '$user_id']])
        )
        Iterator iter = res.results().iterator();
        if (iter.hasNext()) {
            def row = iter.next()
            row.type = "grab_sofa"
            row.users = ((Collection) row.get("users")).size()
            row._id = "${YMD}_grab_sofa".toString()
            row.timestamp = yesTday
            coll.save(row)
        }
    }

    /**
     * 求爱签
     */
    @Deprecated
    static labelStatics() {
        def res = room_cost_DB.aggregate(
                new BasicDBObject('$match', [type: "label", timestamp: [$gte: yesTday, $lte: zeroMill]]),
                new BasicDBObject('$project', [user_id: '$session._id', cost: '$cost']),
                new BasicDBObject('$group', [_id: null, count: [$sum: 1], cost: [$sum: '$cost'], users: [$addToSet: '$user_id']])
        )
        Iterator iter = res.results().iterator();
        if (iter.hasNext()) {
            def row = iter.next()
            row.type = "grab_label"
            row.users = ((Collection) row.get("users")).size()
            row._id = "${YMD}_grab_label".toString()
            row.timestamp = yesTday
            coll.save(row)
        }
    }

    /**
     * 守护统计
     */
    static guardStatics() {
        def res = room_cost_DB.aggregate(
                new BasicDBObject('$match', [type: "buy_guard", timestamp: [$gte: yesTday, $lte: zeroMill]]),
                new BasicDBObject('$project', [user_id: '$session._id', cost: '$cost']),
                new BasicDBObject('$group', [_id: null, count: [$sum: 1], cost: [$sum: '$cost'], users: [$addToSet: '$user_id']])
        )
        Iterator iter = res.results().iterator();
        if (iter.hasNext()) {
            def row = iter.next()
            row.type = "buy_guard"
            row.users = ((Collection) row.get("users")).size()
            row._id = "${YMD}_buy_guard".toString()
            row.timestamp = yesTday
            coll.save(row)
        }
    }

    static eggStatics(int day) {

        def gteMill = yesTday - day * DAY_MILLON
        def YMD = new Date(gteMill).format("yyyyMMdd")
        def timestamp = [$gte: gteMill, $lt: gteMill + DAY_MILLON]

        def row = new BasicDBObject()
        def rowCount = 0
        def rowPrice = 0
        def rowUsers = new HashSet(5000)

        [100, 250, 500].each {
            def iter = room_cost_DB.aggregate(
                    new BasicDBObject('$match', [type: "open_egg", cost: it, timestamp: timestamp]),
                    new BasicDBObject('$project', [got: '$session.got', price: '$session.got_price', user_id: '$session._id', cost: '$cost']),
                    new BasicDBObject('$group', [_id: '$got', price: [$sum: '$price'], count: [$sum: 1], users: [$addToSet: '$user_id']])
            ).results().iterator()
            def cuizi = new BasicDBObject()
            def cuiziCount = 0
            Long cuiziPrice = 0
            def cuiziUsers = new HashSet(2000)
            while (iter.hasNext()) {
                def got = iter.next()
                cuiziCount += (Number) got.get("count")
                cuiziPrice += (Number) got.get("price")
                def users = (Collection) got.get("users")
                cuiziUsers.addAll(users)
                got.put("users", users.size())
                cuizi.put(got.removeField("_id"), got)
            }
            cuizi.put("count", cuiziCount.intValue())
            cuizi.put("users", cuiziUsers.size())
            cuizi.put("price", cuiziPrice)
            rowCount += cuiziCount
            rowPrice += cuiziPrice
            rowUsers.addAll(cuiziUsers)
            row.put(it.toString(), cuizi)
        }
        //座驾价值
        def carPrice = 0
        room_cost_DB.aggregate(
                new BasicDBObject('$match', [type: "open_egg", "session.car_id": [$ne: null], timestamp: timestamp]),
                new BasicDBObject('$project', [price: '$session.got_price']),
                new BasicDBObject('$group', [_id: null, price: [$sum: '$price']])
        ).results().each { BasicDBObject obj ->
            carPrice += (Number) obj.get('price')
        }
        row.put("count", rowCount.intValue())
        row.put("users", rowUsers.size())
        row.put("total_price", rowPrice)
        row.put("nocar_price", rowPrice - carPrice)
        row._id = "${YMD}_open_egg".toString()
        row.timestamp = gteMill
        row.type = "open_egg"

        coll.save(row)
    }

    //必中砸蛋统计
    static eggBingoStatics(int day) {

        def gteMill = yesTday - day * DAY_MILLON
        def YMD = new Date(gteMill).format("yyyyMMdd")
        def timestamp = [$gte: gteMill, $lt: gteMill + DAY_MILLON]

        def row = new BasicDBObject()
        def rowCount = 0
        def rowPrice = 0
        def rowUsers = new HashSet(5000)
        def carPrice = 0;
        def open_count = 400; //400次必中
        [100*open_count, 250*open_count, 500*open_count].each {
            def iter = room_cost_DB.aggregate(
                    new BasicDBObject('$match', [type: "open_bingo_egg", cost: it, timestamp: timestamp]),
                    new BasicDBObject('$project', [got: '$session.got', price: '$session.got_price', car_price: '$session.got_car_price', user_id: '$session._id', cost: '$cost']),
                    new BasicDBObject('$group', [_id: '$got', price: [$sum: '$price'], car_price : [$sum: '$car_price'], count: [$sum: 1], users: [$addToSet: '$user_id']])
            ).results().iterator()
            def cuizi = new BasicDBObject()
            def cuiziCount = 0
            Long cuiziPrice = 0
            def cuiziUsers = new HashSet(2000)
            while (iter.hasNext()) {
                def got = iter.next()
                cuiziCount += (Number) got.get("count")
                cuiziPrice += (Number) got.get("price")
                carPrice += (Number) got.get("car_price")
                def users = (Collection) got.get("users")
                cuiziUsers.addAll(users)
                got.put("users", users.size())
                //cuizi.put(got.removeField("_id"), got)
            }
            cuizi.put("count", cuiziCount.intValue())
            cuizi.put("users", cuiziUsers.size())
            cuizi.put("price", cuiziPrice)
            rowCount += cuiziCount
            rowPrice += cuiziPrice
            rowUsers.addAll(cuiziUsers)
            row.put(it.toString(), cuizi)
        }
        row.put("count", rowCount.intValue())
        row.put("users", rowUsers.size())
        row.put("total_price", rowPrice)
        row.put("nocar_price", rowPrice - carPrice)
        row._id = "${YMD}_open_bingo_egg".toString()
        row.timestamp = gteMill
        row.type = "open_bingo_egg"
        coll.save(row)
    }

    //铃铛统计
    static bellStatics(int day) {

        def gteMill = yesTday - day * DAY_MILLON
        def YMD = new Date(gteMill).format("yyyyMMdd")
        def timestamp = [$gte: gteMill, $lt: gteMill + DAY_MILLON]
        def type = "send_bell"
        def row = new BasicDBObject()
        def rowCount = 0
        def rowCost = 0
        def rowEarned = 0
        def rowPrice = 0
        def rowUsers = new HashSet(5000)

        [586, 587, 588].each {
            def iter = room_cost_DB.aggregate(
                    new BasicDBObject('$match', [type: type, 'session.data._id': it, timestamp: timestamp]),
                    new BasicDBObject('$project', [gift_id: '$session.data._id', count: '$session.data.count', price: '$session.data.total_price', earned: '$session.data.earned', user_id: '$session._id', cost: '$cost']),
                    new BasicDBObject('$group', [_id: '$gift_id', count: [$sum: '$count'], cost: [$sum: '$cost'], price: [$sum: '$price'], earned: [$sum: '$earned'], users: [$addToSet: '$user_id']])
            ).results().iterator()
            def bell = new BasicDBObject()
            def bellCount = 0
            Long bellCost = 0
            Long bellEarned = 0
            Long bellPrice = 0
            def bellUsers = new HashSet(2000)
            while (iter.hasNext()) {
                def got = iter.next()
                bellCount += (Number) got.get("count")
                bellCost += (Number) got.get("cost")
                bellEarned += (Number) got.get("earned")
                bellPrice += (Number) got.get("price")
                def users = (Collection) got.get("users")
                bellUsers.addAll(users)
                //got.put("users", users.size())
                //bell.put(got.removeField("_id").toString(), got)
            }
            bell.put("count", bellCount.intValue())
            bell.put("users", bellUsers.size())
            bell.put("cost", bellCost)
            bell.put("earned", bellEarned)
            bell.put("price", bellPrice)
            rowCount += bellCount
            rowCost += bellCost
            rowEarned += bellEarned
            rowPrice += bellPrice
            rowUsers.addAll(bellUsers)
            row.put(it.toString(), bell)
        }
        row.put("total_count", rowCount.intValue())
        row.put("total_users", rowUsers.size())
        row.put("total_cost", rowCost)
        row.put("total_earned", rowEarned)
        row.put("total_price", rowPrice)
        row._id = "${YMD}_${type}".toString()
        row.timestamp = gteMill
        row.type = type

        coll.save(row)

        ['586': '幸运铜铃', '587': '天赐银铃', '588': '鸿运金铃'].each { String k, String name ->
            def count = ((row.get(k)?.getAt('count')) ?: 0) as Integer
            def cost = ((row.get(k)?.getAt('cost')) ?: 0) as Integer
            //同时放入礼物统计中
            def obj = new BasicDBObject()
            obj.gift_id = k as Integer
            obj.name = name
            obj.count = count
            obj.cost = cost
            obj.type = "gift_detail"
            obj._id = "${YMD}_gift_detail_${k}".toString()
            obj.timestamp = yesTday
            coll.save(obj)
        }
    }

    static cardStatic(int i) {
        def gteMill = yesTday - i * DAY_MILLON
        def YMD = new Date(gteMill).format("yyyyMMdd")
        def timestamp = [$gte: gteMill, $lt: gteMill + DAY_MILLON]
        def row = new BasicDBObject()
        room_cost_DB.aggregate(
                new BasicDBObject('$match', [timestamp: timestamp, type: 'open_card']),
                new BasicDBObject('$project', [_id: '$session._id', cost: '$cost', got: '$session.got_coin']),
                new BasicDBObject('$group', [_id: null, cost: [$sum: '$cost'], got: [$sum: '$got'], count: [$sum: 1], users: [$addToSet: '$_id']])
        ).results().each { BasicDBObject obj ->
            def users = obj.remove('users') as Set
            row.putAll(obj)
            row.append('users', users?.size() ?: 0)
        }
        row._id = "${YMD}_open_card".toString()
        row.timestamp = gteMill
        row.type = "open_card"
        coll.save(row)
    }

    static footballStatic(int i) {
        def gteMill = yesTday - i * DAY_MILLON
        def YMD = new Date(gteMill).format("yyyyMMdd")
        def timestamp = [$gte: gteMill, $lt: gteMill + DAY_MILLON]

        def row = new BasicDBObject()
        def rowCount = 0
        def rowPrice = 0
        def rowUsers = new HashSet(5000)

        [500, 1000, 2000].each {
            def iter = room_cost_DB.aggregate(
                    new BasicDBObject('$match', [type: "football_shoot", cost: it, timestamp: timestamp]),
                    new BasicDBObject('$project', [got: '$session.got', price: '$session.got_price', user_id: '$session._id', cost: '$cost']),
                    new BasicDBObject('$group', [_id: '$got', price: [$sum: '$price'], count: [$sum: 1], users: [$addToSet: '$user_id']])
            ).results().iterator()
            def zuqiu = new BasicDBObject()
            def zuqiuCount = 0
            Long zuqiuPrice = 0
            def zuqiuUsers = new HashSet(2000)
            while (iter.hasNext()) {
                def got = iter.next()
                zuqiuCount += (Number) got.get("count")
                zuqiuPrice += (Number) got.get("price")
                def users = (Collection) got.get("users")
                zuqiuUsers.addAll(users)
                got.put("users", users.size())
                zuqiu.put(got.removeField("_id"), got)
            }
            zuqiu.put("count", zuqiuCount.intValue())
            zuqiu.put("users", zuqiuUsers.size())
            zuqiu.put("price", zuqiuPrice)
            rowCount += zuqiuCount
            rowPrice += zuqiuPrice
            rowUsers.addAll(zuqiuUsers)
            row.put(it.toString(), zuqiu)
        }
        def carPrice = 0
        room_cost_DB.aggregate(
                new BasicDBObject('$match', [type: "football_shoot", "session.car_id": [$ne: null], timestamp: timestamp]),
                new BasicDBObject('$project', [price: '$session.got_price']),
                new BasicDBObject('$group', [_id: null, price: [$sum: '$price']])
        ).results().each { BasicDBObject obj ->
            carPrice += (Number) obj.get('price')
        }
        row.put("count", rowCount.intValue())
        row.put("users", rowUsers.size())
        row.put("total_price", rowPrice)
        row.put("nocar_price", rowPrice - carPrice)
        row._id = "${YMD}_football_shoot".toString()
        row.timestamp = gteMill
        row.type = "football_shoot"

        coll.save(row)
    }

    static carRaceStatic(int i) {
        def gteMill = yesTday - i * DAY_MILLON
        def YMD = new Date(gteMill).format("yyyyMMdd")
        def timestamp = [$gte: gteMill, $lt: gteMill + DAY_MILLON]

        def row = new BasicDBObject()
        def rowCount = 0
        def rowPrice = 0
        def rowUsers = new HashSet(5000)

        [100, 200, 500, 2000].each {
            def iter = room_cost_DB.aggregate(
                    new BasicDBObject('$match', [type: "car_race", cost: it, timestamp: timestamp]),
                    new BasicDBObject('$project', [got: '$session.got', price: '$session.got_price', user_id: '$session._id', cost: '$cost']),
                    new BasicDBObject('$group', [_id: '$got', price: [$sum: '$price'], count: [$sum: 1], users: [$addToSet: '$user_id']])
            ).results().iterator()
            def zuqiu = new BasicDBObject()
            def zuqiuCount = 0
            Long zuqiuPrice = 0
            def zuqiuUsers = new HashSet(2000)
            while (iter.hasNext()) {
                def got = iter.next()
                zuqiuCount += (Number) got.get("count")
                zuqiuPrice += (Number) got.get("price")
                def users = (Collection) got.get("users")
                zuqiuUsers.addAll(users)
                got.put("users", users.size())
                zuqiu.put(got.removeField("_id"), got)
            }
            zuqiu.put("count", zuqiuCount.intValue())
            zuqiu.put("users", zuqiuUsers.size())
            zuqiu.put("price", zuqiuPrice)
            rowCount += zuqiuCount
            rowPrice += zuqiuPrice
            rowUsers.addAll(zuqiuUsers)
            row.put(it.toString(), zuqiu)
        }
        row.put("count", rowCount.intValue())
        row.put("users", rowUsers.size())
        row.put("total_price", rowPrice)
        row._id = "${YMD}_car_race".toString()
        row.timestamp = gteMill
        row.type = "car_race"
        coll.save(row)
    }

    static prettyNumStatic(int i) {
        def gteMill = yesTday - i * DAY_MILLON
        def YMD = new Date(gteMill).format("yyyyMMdd")
        def timestamp = [$gte: gteMill, $lt: gteMill + DAY_MILLON]
        def row = new BasicDBObject()
        row.putAll([cost: 0, count: 0])
        room_cost_DB.aggregate(
                new BasicDBObject('$match', [type: "buy_prettynum", timestamp: timestamp]),
                new BasicDBObject('$project', [user_id: '$session._id', cost: '$cost']),
                new BasicDBObject('$group', [_id: null, cost: [$sum: '$cost'], count: [$sum: 1], users: [$addToSet: '$user_id']])
        ).results().each { BasicDBObject obj ->
            def users = obj.remove('users') as Set
            row.putAll(obj)
            row.put('users', users?.size() ?: 0)
        }
        row.put('_id', "${YMD}_buy_prettynum".toString())
        row.put('timestamp', gteMill)
        row.put('type', 'buy_prettynum')

        coll.save(row)
    }

    static fundingStatic(int i) {
        def gteMill = yesTday - i * DAY_MILLON
        def YMD = new Date(gteMill).format("yyyyMMdd")
        def timestamp = [$gte: gteMill, $lt: gteMill + DAY_MILLON]
        mongo.getDB('xyactive').getCollection('fundings_logs').aggregate(
                new BasicDBObject('$match', [award_time: timestamp]),
                new BasicDBObject('$project', [cid: '$cid', total: '$total', title: '$title', users: '$users']),
                new BasicDBObject('$group', [_id: '$cid', price: [$first: '$total'], title: [$first: '$title'], count: [$sum: 1], users: [$addToSet: '$users']])
        ).results().each { BasicDBObject obj ->
            def users = obj.remove('users') as Set
            def userList = [] as Set
            users?.each { List list ->
                userList.addAll(list)
            }
            obj.put('users', userList.size())
            obj.put('type', 'fundings_logs')
            obj.put('timestamp', gteMill)
            obj.put('gid', obj.get('_id') as Integer)
            obj.put('_id', "${YMD}_${obj.get('_id')}_fundings_logs".toString())
            coll.save(obj)
        }

    }

    static fortuneStatic(int i) {
        def gteMill = yesTday - i * DAY_MILLON
        def YMD = new Date(gteMill).format("yyyyMMdd")
        def timestamp = [$gte: gteMill, $lt: gteMill + DAY_MILLON]
        def obj = new BasicDBObject([count: 0, cost: 0, users: 0])
        room_cost_DB.aggregate(
                new BasicDBObject('$match', [type: 'send_fortune', timestamp: timestamp]),
                new BasicDBObject('$project', [count: '$session.fortune.count', cost: '$cost', users: '$session._id']),
                new BasicDBObject('$group', [_id: null, count: [$sum: '$count'], cost: [$sum: '$cost'], users: [$addToSet: '$users']])
        ).results().each { BasicDBObject o ->
            def users = o.remove('users') as Set
            obj.put('users', users.size())
            obj.put('count', ((o.get('count')) ?: 0) as Integer)
            obj.put('cost', ((o.get('cost')) ?: 0) as Integer)
        }
        obj.put('type', 'send_fortune')
        obj.put('timestamp', gteMill)
        obj.put('_id', "${YMD}_send_fortune".toString())
        coll.save(obj)

        //同时放入礼物统计中
        def count = obj.get('count')
        def cost = obj.get('cost')
        def row = new BasicDBObject()
        row.gift_id = 283
        row.name = '财神'
        row.count = count
        row.cost = cost
        row.type = "gift_detail"
        row._id = "${YMD}_gift_detail_${obj._id}".toString()
        row.timestamp = yesTday
        coll.save(row)
    }

    static treasureStatic(int i) {
        def gteMill = yesTday - i * DAY_MILLON
        def YMD = new Date(gteMill).format("yyyyMMdd")
        def timestamp = [$gte: gteMill, $lt: gteMill + DAY_MILLON]
        def obj = new BasicDBObject([count: 0, cost: 0, users: 0])
        room_cost_DB.aggregate(
                new BasicDBObject('$match', [type: 'send_treasure', timestamp: timestamp]),
                new BasicDBObject('$project', [count: '$session.treasure.count', cost: '$cost', users: '$session._id']),
                new BasicDBObject('$group', [_id: null, count: [$sum: '$count'], cost: [$sum: '$cost'], users: [$addToSet: '$users']])
        ).results().each { BasicDBObject o ->
            def users = o.remove('users') as Set
            obj.put('users', users.size())
            obj.put('count', ((o.get('count')) ?: 0) as Integer)
            obj.put('cost', ((o.get('cost')) ?: 0) as Integer)
        }
        obj.put('type', 'send_treasure')
        obj.put('timestamp', gteMill)
        obj.put('_id', "${YMD}_send_treasure".toString())
        coll.save(obj)

        //同时放入礼物统计中
        def count = obj.get('count')
        def cost = obj.get('cost')
        def row = new BasicDBObject()
        row.gift_id = 1000
        row.name = '远古宝藏'
        row.category_name = '趣味'
        row.count = count
        row.cost = cost
        row.type = "gift_detail"
        row._id = "${YMD}_gift_detail_${obj._id}".toString()
        row.timestamp = yesTday
        coll.save(row)
    }

    static applyfamilyStatic(int i) {
        def gteMill = yesTday - i * DAY_MILLON
        def YMD = new Date(gteMill).format("yyyyMMdd")
        def timestamp = [$gte: gteMill, $lt: gteMill + DAY_MILLON]
        room_cost_DB.aggregate(
                new BasicDBObject('$match', [type: 'apply_family', timestamp: timestamp]),
                new BasicDBObject('$project', [cost: '$cost']),
                new BasicDBObject('$group', [_id: null, count: [$sum: 1], cost: [$sum: '$cost']])
        ).results().each { BasicDBObject obj ->
            obj.put('type', 'apply_family')
            obj.put('timestamp', gteMill)
            obj.put('_id', "${YMD}_apply_family".toString())
            coll.save(obj)
        }
    }

    static broadcastStatic(int i) {
        def gteMill = yesTday - i * DAY_MILLON
        def YMD = new Date(gteMill).format("yyyyMMdd")
        def timestamp = [$gte: gteMill, $lt: gteMill + DAY_MILLON]
        room_cost_DB.aggregate(
                new BasicDBObject('$match', [type: 'broadcast', timestamp: timestamp]),
                new BasicDBObject('$project', [cost: '$cost', users: '$session._id']),
                new BasicDBObject('$group', [_id: null, count: [$sum: 1], cost: [$sum: '$cost'], users: [$addToSet: '$users']])
        ).results().each { BasicDBObject obj ->
            def users = obj.remove('users') as Set
            obj.put('users', users.size())
            obj.put('type', 'broadcast')
            obj.put('timestamp', gteMill)
            obj.put('_id', "${YMD}_broadcast".toString())
            coll.save(obj)
        }
    }


    static rewardStatic(int i) {
        def gteMill = yesTday - i * DAY_MILLON
        def YMD = new Date(gteMill).format("yyyyMMdd")
        def timestamp = [$gte: gteMill, $lt: gteMill + DAY_MILLON]
        room_cost_DB.aggregate(
                new BasicDBObject('$match', [type: 'reward_post', timestamp: timestamp]),
                new BasicDBObject('$project', [cost: '$cost', users: '$session._id']),
                new BasicDBObject('$group', [_id: null, count: [$sum: 1], cost: [$sum: '$cost'], users: [$addToSet: '$users']])
        ).results().each { BasicDBObject obj ->
            def users = obj.remove('users') as Set
            obj.put('users', users.size())
            obj.put('type', 'reward_post')
            obj.put('timestamp', gteMill)
            obj.put('_id', "${YMD}_reward_post".toString())
            coll.save(obj)
        }
    }

    static levelupStatic(int i) {
        def gteMill = yesTday - i * DAY_MILLON
        def YMD = new Date(gteMill).format("yyyyMMdd")
        def timestamp = [$gte: gteMill, $lt: gteMill + DAY_MILLON]
        room_cost_DB.aggregate(
                new BasicDBObject('$match', [type: 'level_up', timestamp: timestamp]),
                new BasicDBObject('$project', [cost: '$cost', stars: '$session.data.xy_star_id', users: '$session._id']),
                new BasicDBObject('$group', [_id: null, count: [$sum: 1], cost: [$sum: '$cost'], stars: [$addToSet: '$stars'], users: [$addToSet: '$users']])
        ).results().each { BasicDBObject obj ->
            def users = obj.remove('users') as Set
            obj.put('users', users.size())
            def stars = obj.remove('stars') as Set
            obj.put('stars', stars.size())
            obj.put('type', 'level_up')
            obj.put('timestamp', gteMill)
            obj.put('_id', "${YMD}_level_up".toString())
            coll.save(obj)
        }
    }

    static songStatic(int i) {
        def gteMill = yesTday - i * DAY_MILLON
        def YMD = new Date(gteMill).format("yyyyMMdd")
        def timestamp = [$gte: gteMill, $lt: gteMill + DAY_MILLON]
        room_cost_DB.aggregate(
                new BasicDBObject('$match', [type: 'song', timestamp: timestamp]),
                new BasicDBObject('$project', [cost: '$cost', users: '$session._id']),
                new BasicDBObject('$group', [_id: null, count: [$sum: 1], cost: [$sum: '$cost'], users: [$addToSet: '$users']])
        ).results().each { BasicDBObject obj ->
            def users = obj.remove('users') as Set
            obj.put('users', users.size())
            obj.put('type', 'song')
            obj.put('timestamp', gteMill)
            obj.put('_id', "${YMD}_song".toString())
            coll.save(obj)
        }
    }

    //H5用户购买观看权限
    static buyWatchStatic(int i) {
        def gteMill = yesTday - i * DAY_MILLON
        def YMD = new Date(gteMill).format("yyyyMMdd")
        def timestamp = [$gte: gteMill, $lt: gteMill + DAY_MILLON]
        room_cost_DB.aggregate(
                new BasicDBObject('$match', [type: 'buy_watch', timestamp: timestamp]),
                new BasicDBObject('$project', [cost: '$cost', users: '$session._id']),
                new BasicDBObject('$group', [_id: null, count: [$sum: 1], cost: [$sum: '$cost'], users: [$addToSet: '$users']])
        ).results().each { BasicDBObject obj ->
            def users = obj.remove('users') as Set
            obj.put('users', users.size())
            obj.put('type', 'buy_watch')
            obj.put('timestamp', gteMill)
            obj.put('_id', "${YMD}_buy_watch".toString())
            coll.save(obj)
        }
    }

    static nestStatic(int i) {
        def gteMill = yesTday - i * DAY_MILLON
        def YMD = new Date(gteMill).format("yyyyMMdd")
        def timestamp = [$gte: gteMill, $lt: gteMill + DAY_MILLON]
        room_cost_DB.aggregate(
                new BasicDBObject('$match', [type: 'nest_send_gift', timestamp: timestamp]),
                new BasicDBObject('$project', [cost: '$cost', users: '$session._id']),
                new BasicDBObject('$group', [_id: null, count: [$sum: 1], cost: [$sum: '$cost'], users: [$addToSet: '$users']])
        ).results().each { BasicDBObject obj ->
            def users = obj.remove('users') as Set
            obj.put('users', users.size())
            obj.put('type', 'nest_send_gift')
            obj.put('timestamp', gteMill)
            obj.put('_id', "${YMD}_nest_send_gift".toString())
            coll.save(obj)
        }
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

    @Deprecated
    static userRemain() {
        def q = new BasicDBObject('finance.coin_count': [$gt: 0])
        def clist = mongo.getDB("xy").getCollection("users").
                find(q, new BasicDBObject('finance.coin_count', 1))
                .toArray()
        def coin = clist.sum { it.finance.coin_count } as Long
        //println "totalcoin:---->:${coin}"

        def bq = new BasicDBObject('finance.bean_count': [$gt: 0])
        def blist = mongo.getDB("xy").getCollection("users").
                find(bq, new BasicDBObject('finance.bean_count', 1))
                .toArray()
        def bean = blist.sum { it.finance.bean_count ?: 0 } as Long
        //println "totalbean:---->:${bean}"

        [coin: coin, bean: bean]
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
                new BasicDBObject('$project', [ bean: '$finance.bean_count']),
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

    static dayCost() {
        def q = new BasicDBObject(timestamp: [$gte: yesTday, $lt: zeroMill])
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
        coll.update(new BasicDBObject(_id: YMD + '_alllogin'),info, true, false)
    }

    @Deprecated
    static KMStatics() {
        //消费
        def stat_cost = mongo.getDB('xy_union').getCollection('stat_cost')
        [1, 7, 15, 30].each {
            def timebetween = [$gte: zeroMill - (it * DAY_MILLON), $lt: zeroMill]
            def res = room_cost_DB.aggregate(
                    new BasicDBObject('$match', [qd: "f101", timestamp: timebetween, 'session.data.xy_star_id': [$ne: null]]),
                    new BasicDBObject('$project', [star_id: '$session.data.xy_star_id', earned: '$session.data.earned']),
                    new BasicDBObject('$group', [_id: '$star_id', earned: [$sum: '$earned']])
            )
            Iterator records = res.results().iterator();
            while (records.hasNext()) {
                def obj = records.next()
                def count = room_cost_DB.distinct("session._id",
                        new BasicDBObject('timestamp', timebetween)
                                .append("qd", "f101").append('session.data.xy_star_id', obj['_id'] as Integer))
                        .toArray().size() ?: 0
                def data = new BasicDBObject('$set', new BasicDBObject('qd', "f101").append("${it}_count", count)
                        .append("${it}_earned", obj['earned']).append('timestamp', zeroMill))

                if (it == 1) {
                    data.append('$inc', new BasicDBObject('total_earned', obj['earned']))
                }

                def id = obj._id + "_" + new Date(zeroMill).format("yyyyMMdd") + "_f101"
                stat_cost.update(new BasicDBObject('_id', id), data, true, false)
            }
        }

        //留存率
        def day_logins = mongo.getDB("xylog").getCollection("day_login")
        def stat_login = mongo.getDB('xy_union').getCollection('stat_login')

        [1, 7].each { Integer d ->
            def begin_date = yesTday - (d * DAY_MILLON)
            def regs = mongo.getDB("xy_admin").getCollection('stat_channels')
                    .findOne(new BasicDBObject('_id', new Date(begin_date).format("yyyyMMdd") + "_f101"))
            def allUids = regs?.get('regs') as Collection
            def count = 0
            if (allUids && allUids.size() > 0) {
                long begin = regs['timestamp'] as long
                Long gt = begin + d * DAY_MILLON
                count = day_logins.count(new BasicDBObject(user_id: [$in: allUids], timestamp:
                        [$gte: gt, $lt: gt + DAY_MILLON]))

            }
            def id = new Date(zeroMill).format("yyyyMMdd") + "_f101"
            def data = new BasicDBObject('$set', new BasicDBObject("${d}_days", count).append('timestamp', zeroMill))
            stat_login.update(new BasicDBObject('_id', id), data, true, false)

        }
    }

    static build360StaticsXml() {
        try {
            def fold = new File("/empty/www.2339.com/seo360/")
            if (!fold.exists()) {
                fold.mkdir()
            }

            def lstText = new URL("http://api.memeyule.com/seo/star_lst").getText("utf-8")
            def lst_fileName = "360image_2339_${new Date().format('yyyyMMdd')}.lst"
            new File(fold, lst_fileName).setText(lstText, "utf-8")

            def xmlText = new URL("http://api.memeyule.com/seo/star_xml?size=1000").getText("utf-8")
            //360image_show_20130214_01.xml
            def fileName = "360image_2339_${new Date().format('yyyyMMdd')}_01.xml"
            new File(fold, fileName).setText(xmlText, "utf-8")
        } catch (Exception e) {
            println e
        }
    }


    static baiduStaticsXml() {
        try {
            def xmlText = new URL("http://api.memeyule.com/baidu/static_star_xml?size=1000").getText("utf-8")
            def fold = new File("/empty/www.2339.com/baidu/")
            if (!fold.exists()) {
                fold.mkdir()
            }
            new File(fold, "baidu.xml").setText(xmlText, "utf-8")
        } catch (Exception e) {
            println e
        }

        // new File(fold,new Date().format('yyyy-MM-dd')+'.xml').setText(xmlText,"utf-8")
    }

    private static final Integer MEDAL_ID = 127
    private static final Integer MAX_COINS = 500000
    private static final Long SYS_MEDAL_MILLS = 5 * 365 * 24 * 3600 * 1000L

    static payStaticsAward(Integer day) {
        Long begin = yesTday - day * DAY_MILLON
        def timestamp = [$gte: begin, $lt: begin + DAY_MILLON]
        finance_log_DB.aggregate(
                new BasicDBObject('$match', ['via': [$ne: 'Admin'], timestamp: timestamp]),
                new BasicDBObject('$project', [_id: '$to_id', coin: '$coin']),
                new BasicDBObject('$group', [_id: '$_id', coin_sum: [$sum: '$coin']]),
                new BasicDBObject('$match', new BasicDBObject('coin_sum', [$gte: MAX_COINS]))
        ).results().each {
            def obj = new BasicDBObject(it as Map)
            def userId = obj['_id'] as Integer
            //判断用户是否有此徽章
            if (medal_award_logs.count(new BasicDBObject([mid: MEDAL_ID, uid: userId])) == 0) {
                Long now = System.currentTimeMillis()
                String entryKey = "medals." + MEDAL_ID
                //奖励徽章
                if (users.update(new BasicDBObject('_id', userId).append(entryKey, [$not: [$gte: now]]),
                        new BasicDBObject('$set', new BasicDBObject(entryKey, now + SYS_MEDAL_MILLS))).getN() == 1) {
                    //徽章日志
                    medal_award_logs.insert($$([_id: userId + "_" + now, mid: MEDAL_ID, uid: userId, timestamp: now, via:'award']
                    ))
                }
            }

        }
    }

    private static final List<Integer> COIN_LEVES = [0, 300000, 500000, 700000, 1000000, 3000000, 6000000]
    //充值领礼包
    static payStaticsBagAward(Integer day) {
        Long begin = yesTday - day * DAY_MILLON
        def timestamp = [$gte: begin, $lt: begin + DAY_MILLON]
        finance_log_DB.aggregate(
                new BasicDBObject('$match', ['via': [$ne: 'Admin'], timestamp: timestamp]),
                new BasicDBObject('$project', [_id: '$to_id', coin: '$coin']),
                new BasicDBObject('$group', [_id: '$_id', coin_sum: [$sum: '$coin']]),
                new BasicDBObject('$match', new BasicDBObject('coin_sum', [$gte: COIN_LEVES[1]]))
        ).results().each {
            def obj = new BasicDBObject(it as Map)
            def userId = obj['_id'] as Integer
            def coin_sum = obj['coin_sum'] as Integer
            Integer level = getBagLevel(coin_sum)
            //println "userId : ${userId}, coin_sum : ${coin_sum}, leve : ${level}"
            users.update(new BasicDBObject('_id', userId).append('charge_award_bag', new BasicDBObject($not: [$gt: level])),
                    new BasicDBObject('$set', [charge_award_bag: level]), false, false);
        }
    }

    private static Integer getBagLevel(Integer coin_sum) {
        Integer i = 1;
        for (; i < COIN_LEVES.size(); i++) {
            if (coin_sum < COIN_LEVES.get(i))
                break;
        }
        return i - 1;
    }

    //2015.01.08-2015.01.19 充值返利活动后端 ==================================================Begin
    private static final Long _begin = new SimpleDateFormat("yyyyMMdd").parse("20150109").getTime()
    private static final Long _end = new SimpleDateFormat("yyyyMMdd").parse("20150120").getTime()
    private static final Integer COINS_RATE = 10000
    private static final Integer PRIZE_COINS = 2 * COINS_RATE
    /**
     * 充值返利活动后端
     * 2015.01.08-2015.01.19
     */
    static chargeReturn() {
        Long now = System.currentTimeMillis();
        if (now < _begin || now > _end) return;
        //充值用户
        Iterable<DBObject> charge2 = finance_log_DB.aggregate(
                new BasicDBObject('$match', ['via': [$ne: 'Admin'], to_id: [$ne: null], timestamp: [$gte: yesTday, $lte: zeroMill]]),
                new BasicDBObject('$project', [_id: '$to_id', coin: '$coin']),
                new BasicDBObject('$group', [_id: '$_id', coin_sum: [$sum: '$coin']]),
                new BasicDBObject('$match', new BasicDBObject('coin_sum', [$gte: PRIZE_COINS]))
        ).results()
        awardUsers(charge2)
    }

    //符合条件的用户
    private static awardUsers(Iterable<DBObject> iterable) {
        iterable.each {
            def obj = new BasicDBObject(it as Map)
            def userId = obj['_id'] as Integer
            def coin_sum = obj['coin_sum'] as Long
            awardCoins(userId, coin_sum)
        }
    }

    //奖励用户
    private static awardCoins(Integer userId, Long coins) {
        long returnCoins = returnCoins(coins)
        if (returnCoins > 0) {
            def log_id = "${userId}_${coins}_${returnCoins}_${new Date(yesTday).format("yyyyMMdd")}".toString()
            if (active_award_logs.count(new BasicDBObject(_id: log_id, 'type': "charge20150108")) == 0) {
                if (active_award_logs.save(new BasicDBObject([_id   : log_id, coins: coins, return_coins: returnCoins, uid: userId,
                                                              'type': "charge20150108", timestamp: System.currentTimeMillis()])).getN() == 1) {
                    if (users.update(new BasicDBObject('_id', userId),
                            new BasicDBObject($inc: new BasicDBObject("finance.coin_count", returnCoins)), false, false).getN() == 1) {

                    }
                }
            }
        }

    }

    /**
     *   获得返币
     *   ≥20000 	3%
     ≥50000 	5%
     ≥100000 	6%
     ≥300000 	8%
     ≥500000 	10%
     ≥1000000 	15%
     */
    private static long returnCoins(Long coin) {
        double rate = 0;
        if (coin < 2 * COINS_RATE)
            rate = 0;
        else if (coin >= 2 * COINS_RATE && coin < 5 * COINS_RATE)
            rate = 0.03;
        else if (coin >= 5 * COINS_RATE && coin < 10 * COINS_RATE)
            rate = 0.05;
        else if (coin >= 10 * COINS_RATE && coin < 30 * COINS_RATE)
            rate = 0.06;
        else if (coin >= 30 * COINS_RATE && coin < 50 * COINS_RATE)
            rate = 0.08;
        else if (coin >= 50 * COINS_RATE && coin < 100 * COINS_RATE)
            rate = 0.1;
        else if (coin >= 100 * COINS_RATE)
            rate = 0.15;
        return (long) (coin * rate);
    }

    //充值返利活动后端 ==================================================End

    /**
     * 统计每日签到人数和奖励信息（按渠道和版本划分）
     * @param i
     */
    static staticSign(int i) {
        long l = System.currentTimeMillis()
        def gteMill = yesTday - i * DAY_MILLON
        def date = new Date(gteMill)//
        def coll = mongo.getDB('xy_admin').getCollection('stat_sign')
        def day_login = mongo.getDB('xylog').getCollection('day_login')
        def lotterylog = mongo.getDB('xylog').getCollection('lottery_logs')
        def users = mongo.getDB('xy').getCollection('users')
        //查询签到抽奖情况
        def award_map = new HashMap()
        lotterylog.find(new BasicDBObject(active_name: 'sign_chest', timestamp: [$gte: gteMill, $lt: gteMill + DAY_MILLON]))
                .toArray().each { BasicDBObject obj ->
            award_map.put(obj.get('user_id') as Integer, obj.get('award_name'))
        }
        def map = new HashMap<String, SignType>()
        day_login.find(new BasicDBObject($or: [[sign: true, sign_time: [$gte: gteMill, $lt: gteMill + DAY_MILLON]], [award: true, award_time: [$gte: gteMill, $lt: gteMill + DAY_MILLON]]]))
                .toArray().each { BasicDBObject obj ->
            def userId = obj.get('user_id') as Integer
            def user = users.findOne(new BasicDBObject(_id: userId), new BasicDBObject(app_ver: 1, qd: 1))
            def version = (user?.get('app_ver') ?: '0.0.0') as String
            def qd = (user?.get('qd') ?: 'MM') as String//default official
            def awardName = award_map.get(userId) as String
            def _id = "${qd}_${version}".toString()
            SignType signType = map.get(_id)
            if (signType == null) {
                signType = new SignType(qd, version)
                map.put(_id, signType)
            }
            signType.add(userId, awardName)
        }
        map.each { String k, SignType signType ->
            coll.update(new BasicDBObject(_id: "${date.format('yyyyMMdd')}_${k}".toString()),
                    new BasicDBObject(signType.toMap()).append('timestamp', gteMill), true, false)
        }
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
     * 火拼统计
     */
    static staticPk(int i) {
        def gteMill = yesTday - i * DAY_MILLON
        def date = new Date(gteMill)//
        def prefix = date.format('yyyyMMdd_')
        def pk_logs = mongo.getDB('xylog').getCollection('pk_logs')
        def count = 0, succ = 0, person = new HashSet()
        pk_logs.find(new BasicDBObject(expire: [$gte: gteMill, $lt: gteMill + DAY_MILLON]))
                .toArray().each { BasicDBObject obj ->
            count++
            def status = obj.get('status') as Integer
            def winId = obj.get('win') as Integer
            def pkids = obj.get('pk_ids') as Set
            if (status == 5 && winId != null) {
                person.addAll(pkids)
                succ++
            }
        }
        def map = [type: 'allpk', count: count, success: succ, pks: person.size(), timestamp: gteMill]
        coll.update(new BasicDBObject(_id: "${prefix}pk".toString()), new BasicDBObject(map), true, false)
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

    static dianleStatic(int i) {
        def begin = yesTday - i * DAY_MILLON
        def YMD = new Date(begin).format('yyyyMMdd_')
        def timebetween = [$gte: begin, $lt: begin + DAY_MILLON]
        def trade_log = mongo.getDB('xylog').getCollection('trade_logs')
        trade_log.aggregate(
                //new BasicDBObject('$match', [via: 'dianle', time: timebetween]),
                new BasicDBObject('$match', [via: 'baidu_jf', time: timebetween]),
                new BasicDBObject('$project', [coin: '$resp.coin']),
                new BasicDBObject('$group', [_id: null, count: [$sum: 1], coin: [$sum: '$coin']])
        ).results().each { BasicDBObject obj ->
            obj._id = "${YMD}_baidu_jf".toString()
            obj.timestamp = begin
            obj.type = "baidu_jf"

            coll.save(obj)
        }
    }

    private static final Integer TIME_OUT = 5 * 1000;

    static mvipExpireCheck(){
        HttpURLConnection conn = null;
        def jsonText = "";
        try{
            def api_url = new URL(api_domain+"activitygift/medal_check")
            conn = (HttpURLConnection)api_url.openConnection()
            conn.setRequestMethod("GET")
            conn.setDoOutput(true)
            conn.setConnectTimeout(TIME_OUT);
            conn.setReadTimeout(TIME_OUT);
            conn.connect()
            jsonText = conn.getInputStream().getText("UTF-8")
        }catch (Exception e){
            println "staticWeek Exception : " + e;
        }finally{
            if (conn != null) {
                conn.disconnect();
                conn = null;
            }
        }
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')} result : ${jsonText}"
    }


    static Integer DAY = 0;

    static void main(String[] args) { //待优化，可以到历史表查询记录
        long l = System.currentTimeMillis()
        //01.送礼日报表
        long begin = l

        giftStatics()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   giftStatics, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //02.幸运礼物 中奖日报表
        l = System.currentTimeMillis()
        luckStatics(DAY)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   luckStatics, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //03.砸蛋日报表
        l = System.currentTimeMillis()
        eggStatics(DAY)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   eggStatics, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //必中砸蛋日报表
        l = System.currentTimeMillis()
        eggBingoStatics(DAY)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   eggBingoStatics, cost  ${System.currentTimeMillis() - l} ms"
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

        //06.买座驾的日报表
        l = System.currentTimeMillis()
        carStatics()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   carStatics, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //07.买vip的日报表
        l = System.currentTimeMillis()
        vipStatics()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   luckStatics, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //08.抢沙发的日报表
        l = System.currentTimeMillis()
        sofaStatics()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   sofaStatics, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //09.翻牌的日报表
        l = System.currentTimeMillis()
        cardStatic(DAY)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   cardStatic, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //09.踢球的日报表
        l = System.currentTimeMillis()
        footballStatic(DAY)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   footballStatic, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //09.赛车游戏的日报表
        l = System.currentTimeMillis()
        carRaceStatic(DAY)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   carRaceStatic, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //09.靓号的日报表
        l = System.currentTimeMillis()
        prettyNumStatic(DAY)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   prettyNumStatic, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //09.众筹的日报表
        l = System.currentTimeMillis()
        fundingStatic(DAY)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   fundingStatic, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //10.房间消费日报表
        l = System.currentTimeMillis()
        costStatics()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   costStatics, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //守护消费日报表
        l = System.currentTimeMillis()
        guardStatics()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   guardStatics, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //财神消费日报表
        l = System.currentTimeMillis()
        fortuneStatic(DAY)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   fortuneStatic, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //宝藏消费日报表
        l = System.currentTimeMillis()
        treasureStatic(DAY)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   treasureStatic, cost  ${System.currentTimeMillis() - l} ms"
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

        //16.用户每日充值5000RMB奖励徽章
        l = System.currentTimeMillis()
        payStaticsAward(DAY)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   payStaticsAward, cost  ${System.currentTimeMillis() - l} ms"
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

        //20.火拼统计
        l = System.currentTimeMillis()
        staticPk(DAY)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticPk, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //21.特殊用户统计,运营监控用户
        l = System.currentTimeMillis()
        staticMvp(DAY)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticMvp, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //铃铛统计
        l = System.currentTimeMillis()
        bellStatics(DAY)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   bellStatics, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //周星统计
        l = System.currentTimeMillis()
        weekstarStatic(DAY)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   weekstarStatic, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //家族申请统计
        l = System.currentTimeMillis()
        applyfamilyStatic(DAY)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   applyfamilyStatic, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //广播统计
        l = System.currentTimeMillis()
        broadcastStatic(DAY)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   broadcastStatic, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //接单统计(唐伯虎点秋香)
        /*
        l = System.currentTimeMillis()
        rewardStatic(DAY)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   rewardStatic, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)
        */

        //升级统计
        l = System.currentTimeMillis()
        levelupStatic(DAY)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   levelupStatic, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //点歌统计
        l = System.currentTimeMillis()
        songStatic(DAY)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   songStatic, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)


        //购买观看权限统计
        l = System.currentTimeMillis()
        buyWatchStatic(DAY)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   buyWatchStatic, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)
        //小窝送礼统计
        /*
        l = System.currentTimeMillis()
        nestStatic(DAY)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   nestStatic, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)
        */

        //新手任务统计
        l = System.currentTimeMillis()
        missionStatic(DAY)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   missionStatic, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //外站下载统计（点乐, 百度积分墙）
        l = System.currentTimeMillis()
        dianleStatic(DAY)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   dianleStatic, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //TODO  2016/11 暂停 每日充值一定额度赠送礼包
        /*l = System.currentTimeMillis()
        payStaticsBagAward(DAY)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   payStaticsBagAward, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)*/

        //M特权用户失效检测
        l = System.currentTimeMillis()
        mvipExpireCheck()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   mvipExpireCheck, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //落地定时执行的日志
        jobFinish(begin)
    }

    /**
     * 标记任务完成  用于运维监控
     * @return
     */
    private static jobFinish(Long begin){
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

