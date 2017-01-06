#!/usr/bin/env groovy
package tmp

@Grapes([
@Grab('org.mongodb:mongo-java-driver:2.14.2'),
@Grab('commons-lang:commons-lang:2.6'),
@Grab('redis.clients:jedis:2.1.0'),
])
import com.mongodb.*
import redis.clients.jedis.Jedis

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/**
 * 恢复历史消费记录统计
 */
class CostStat {

    static Properties props = null;
    static String profilepath="/empty/crontab/db.properties";

    static getProperties(String key, Object defaultValue){
        try {
            if(props == null){
                props = new Properties();
                props.load(new FileInputStream(profilepath));
            }
        } catch (Exception e) {
            println e;
        }
        return props.get(key, defaultValue)
    }

    static final String jedis_host = getProperties("main_jedis_host", "192.168.31.249")
    static final String chat_jedis_host = getProperties("chat_jedis_host", "192.168.31.249")
    static final String live_jedis_host = getProperties("live_jedis_host", "192.168.31.249")

    static final Integer main_jedis_port = getProperties("main_jedis_port",6379) as Integer
    static final Integer chat_jedis_port = getProperties("chat_jedis_port",6379) as Integer
    static final Integer live_jedis_port = getProperties("live_jedis_port",6379) as Integer

    static mainRedis = new Jedis(jedis_host,main_jedis_port)

    static mongo  = new Mongo(new MongoURI(getProperties('mongo.uri','mongodb://192.168.31.249:27017/?w=1') as String))
    static historyMongo = new Mongo(new MongoURI(getProperties('mongo_history.uri', 'mongodb://192.168.31.246:27017/?w=1') as String))


    def final Long DAY_MILL = 24*3600*1000L
    static DAY_MILLON = 24 * 3600 * 1000L
    static apply = mongo.getDB('xy_admin').getCollection('applys')
    static finance_log = mongo.getDB('xy_admin').getCollection('finance_log')
    static users = mongo.getDB('xy').getCollection('users')
    static rooms = mongo.getDB('xy').getCollection('rooms')
    static day_login = mongo.getDB('xylog').getCollection('day_login')
    static lottery_logs = mongo.getDB('xylog').getCollection('lottery_logs')
    static room_cost =  mongo.getDB("xylog").getCollection("room_cost")
    static room_cost_2015 =  historyMongo.getDB('xylog_history').getCollection("room_cost_2015")
    static room_cost_2014 =  historyMongo.getDB('xylog_history').getCollection("room_cost_2014")
    static lottery_logs_history  =  historyMongo.getDB('xylog_history').getCollection("lottery_logs_history")
    static stat_daily = mongo.getDB('xy_admin').getCollection('stat_daily')
    static DBCollection stat_daily_bak  = mongo.getDB('xy_admin').getCollection('stat_daily_bak')
    static DBCollection finance_dailyEarned = mongo.getDB('xy_admin').getCollection('finance_dailyEarned')
    static financeTmpDB = mongo.getDB('xy_admin').getCollection('finance_dailyReport')
    static long zeroMill = new Date().clearTime().getTime()
    static Long yesTday = zeroMill - DAY_MILLON

    //恢复15年消费统计
    static recoverCostStatistic(){
        Integer index = 186;
        Date date = Date.parse("yyyy-MM-dd HH:mm" ,"2015-01-01 00:00")
        while(index-- >= 0){
            date = date - 1;
            def l = System.currentTimeMillis()
            def dayCost =  dayCost(date)
            def _id = date.format("yyyyMMdd") + '_allcost'
            println _id + " cost  ${System.currentTimeMillis() -l} ms  "
            println dayCost
            stat_daily.update(new BasicDBObject('_id', _id), new BasicDBObject('$set', dayCost))
        }

    }

    static dayCost(Date date) {
        long begin = date.clearTime().getTime()
        def q = new BasicDBObject(timestamp: [$gte: begin, $lt: begin + DAY_MILLON], type:[$ne:[$in:['send_gift','song','broadcast']]] )
        def result = [:], costs = 0L, users = new HashSet()
        room_cost_2014.aggregate(
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

    /**
     * 恢复日报表中的统计数据
     */
    static recoverDailyReport(){
        int day = 0
        while(day <= 130){
            println ymd(day)
            //staticAppFreeGift(day)
            staticsRedPacketCoin(day)
            //staticGameAddCoin(day)
            //staticGameFishAddCoin(day)
            //staticGameFishSubtractCoin(day)
            //staticGameSubtractCoin(day)
            //staticDianle(day)
            //staticsActivityAwardCoins(day)
            day++;
        }
    }

     static Map getTimeBetween(int day){
        def gteMill = yesTday - day * DAY_MILLON
        return  [$gte: gteMill, $lt: gteMill + DAY_MILLON]
    }

    static String getID(int day){
        return "finance_" + ymd(day)
    }

    static String ymd(int day){
        return new Date(yesTday - day * DAY_MILLON).format("yyyyMMdd")
    }
    private final static Map<String, Long> APP_FREE_GIFT = ['100':10,'103':100, '102':10]
    //手机直播免费礼物奖励
    static staticAppFreeGift(int day)
    {
        def q = new BasicDBObject(timestamp:getTimeBetween(day),active_name:'app_meme_luck_gift')
        def lottery_log = mongo.getDB('xylog').getCollection('lottery_logs')
        def coinList = lottery_log.find(q,new BasicDBObject(award_name:1)).toArray()
        def award_coin = coinList.sum {
            return APP_FREE_GIFT[it.award_name as String]
        } as Long
        if(null == award_coin)
            award_coin = 0L

        println "staticAppFreeGift award_coin:---->:${award_coin}"
        financeTmpDB.update(new BasicDBObject(_id:getID(day)),new BasicDBObject('$set',new BasicDBObject(app_free_gift_coin:award_coin)))
    }

    static final List<String> ACTIVE_NAMES = ['chest20160812']
    //各种活动抽奖获取柠檬
    static staticsActivityAwardCoins(int day)
    {
        def query = new BasicDBObject(timestamp:getTimeBetween(day),active_name:[$in:ACTIVE_NAMES])
        Long award_coin = 0;
        def lottery_log = mongo.getDB('xylog').getCollection('lottery_logs')
        lottery_log.aggregate(
                new BasicDBObject('$match', query),
                new BasicDBObject('$project', [award_coin: '$award_coin']),
                new BasicDBObject('$group', [_id: null, award_coin: [$sum: '$award_coin']])
        ).results().each { BasicDBObject obj ->
            award_coin = obj.get('award_coin') as Long
        }
        println "ActivityAwardCoins:---->:${award_coin}"
        //financeTmpDB.update(new BasicDBObject(_id:getID(day)),new BasicDBObject('$set',new BasicDBObject(activity_award_coin:award_coin)))
    }

    //红包送币
    static staticsRedPacketCoin(int day)
    {
        def timeBetween = getTimeBetween(day)
        if(financeTmpDB.count(new BasicDBObject(_id:getID(day),redPacket_coin:[$gt:0])) == 0){
            def q = new BasicDBObject(timestamp:timeBetween,active_name:'red_packet')
            def lottery_log = mongo.getDB('xylog').getCollection('lottery_logs')
            def coinList = lottery_log.find(q,new BasicDBObject(award_coin:1)).toArray()
            if(coinList == null || coinList.size() == 0){
                coinList = lottery_logs_history.find(q,new BasicDBObject(award_coin:1)).toArray()
            }
            def redPacket_coin = coinList.sum {it.award_coin?:0} as Long
            if(null == redPacket_coin)
                redPacket_coin = 0L

            println "RedPacket_coin:---->:${redPacket_coin}"
            financeTmpDB.update(new BasicDBObject(_id:getID(day)),new BasicDBObject('$set',new BasicDBObject(redPacket_coin:redPacket_coin)))
        }

    }

    //德州扑克
    static staticGameAddCoin(int day) {
        def trade_log = mongo.getDB('xylog').getCollection('trade_logs')
        def query = new BasicDBObject(time: getTimeBetween(day), via: 'texasholdem', "resp.result": "0", 'resp.coin': [$gt: 0])
        def texasholdem_coinList = trade_log.find(query, new BasicDBObject(resp: 1)).toArray()
        def texasholdem_game_coin = texasholdem_coinList.sum { it.resp?.coin ?: 0 } as Long
        if (null == texasholdem_game_coin) {
            texasholdem_game_coin = 0L
        }
        println "texasholdem_game_coin:---->:${texasholdem_game_coin}".toString()
        financeTmpDB.update(new BasicDBObject(_id: getID(day)), new BasicDBObject('$set', new BasicDBObject(texasholdem_game_coin: texasholdem_game_coin)))
    }

    //德州扑克 兑换游戏币减币
    static staticGameSubtractCoin(int day) {
        def timeBetween = getTimeBetween(day)
        def trade_log = mongo.getDB('xylog').getCollection('trade_logs')
        def query = new BasicDBObject(time: timeBetween, via: 'texasholdem', 'resp.result': '0', 'resp.coin': [$lt: 0])
        def texasholdem_coinList = trade_log.find(query, new BasicDBObject(resp: 1)).toArray()
        def texasholdem_game_coin = texasholdem_coinList.sum { Math.abs(it.resp?.coin ?: 0) } as Long
        if (null == texasholdem_game_coin) {
            texasholdem_game_coin = 0L
        }
        println "texasholdem_subtract_coin:---->:${texasholdem_game_coin}".toString()
        financeTmpDB.update(new BasicDBObject(_id: getID(day)), new BasicDBObject('$set', new BasicDBObject(texasholdem_subtract_coin: texasholdem_game_coin)))
    }

    static staticDianle(int day) {
        def q = new BasicDBObject(time: getTimeBetween(day), via: 'dianle')
        //def q = new BasicDBObject(time: getTimeBetween(day), via: 'dianle')
        def trade_log = mongo.getDB('xylog').getCollection('trade_logs')
        def coinList = trade_log.find(q, new BasicDBObject(resp: 1)).toArray()
        def share_coin = coinList.sum { it.resp?.coin ?: 0 } as Long
        if (null == share_coin) {
            share_coin = 0L
        }
        println "dianle_share_coin:---->:${share_coin}".toString()
        financeTmpDB.update(new BasicDBObject(_id: getID(day)), new BasicDBObject('$set', new BasicDBObject(dianle_share_coin: share_coin)))
    }

    //捕鱼
    static staticGameFishAddCoin(int day) {
        def trade_log = mongo.getDB('xylog').getCollection('trade_logs')
        def query = new BasicDBObject(time: getTimeBetween(day), via: 'fishing', "resp.result": "0", 'resp.coin': [$gt: 0])
        def texasholdem_coinList = trade_log.find(query, new BasicDBObject(resp: 1)).toArray()
        def texasholdem_game_coin = texasholdem_coinList.sum { it.resp?.coin ?: 0 } as Long
        if (null == texasholdem_game_coin) {
            texasholdem_game_coin = 0L
        }
        println "fishing_game_coin:---->:${texasholdem_game_coin}".toString()
        financeTmpDB.update(new BasicDBObject(_id: getID(day)), new BasicDBObject('$set', new BasicDBObject(fishing_game_coin: texasholdem_game_coin)))
    }

    //捕鱼 兑换游戏币减币
    static staticGameFishSubtractCoin(int day) {
        def timeBetween = getTimeBetween(day)
        def trade_log = mongo.getDB('xylog').getCollection('trade_logs')
        def query = new BasicDBObject(time: timeBetween, via: 'fishing', 'resp.result': '0', 'resp.coin': [$lt: 0])
        def texasholdem_coinList = trade_log.find(query, new BasicDBObject(resp: 1)).toArray()
        def texasholdem_game_coin = texasholdem_coinList.sum { Math.abs(it.resp?.coin ?: 0) } as Long
        if (null == texasholdem_game_coin) {
            texasholdem_game_coin = 0L
        }
        println "fishing_subtract_coin:---->:${texasholdem_game_coin}".toString()
        financeTmpDB.update(new BasicDBObject(_id: getID(day)), new BasicDBObject('$set', new BasicDBObject(fishing_subtract_coin: texasholdem_game_coin)))
    }

    static recoverfootballStatic(int day) {
        Date date = Date.parse("yyyy-MM-dd HH:mm" ,"2015-01-01 00:00")
        def gteMill = date.clearTime().getTime() - day * DAY_MILLON
        def YMD = new Date(gteMill).format("yyyyMMdd")
        def timestamp = [$gte: gteMill, $lt: gteMill + DAY_MILLON]

        def row = new BasicDBObject()
        def rowCount = 0
        def rowPrice = 0
        def rowUsers = new HashSet(5000)

        [500, 1000, 2000].each {
            def iter = room_cost_2014.aggregate(
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
        room_cost_2014.aggregate(
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

        println "football_shoot : " + row
        stat_daily.save(row)
    }

    static recoverEggStatics(int day) {
        Date date = Date.parse("yyyy-MM-dd HH:mm", "2015-01-01 00:00")
        def gteMill = date.clearTime().getTime() - day * DAY_MILLON
        def YMD = new Date(gteMill).format("yyyyMMdd")
        def timestamp = [$gte: gteMill, $lt: gteMill + DAY_MILLON]

        def row = new BasicDBObject()
        def rowCount = 0
        def rowPrice = 0
        def rowUsers = new HashSet(5000)

        [100, 250, 500].each {
            def iter = room_cost_2014.aggregate(
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
        room_cost_2014.aggregate(
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
        println "open_egg : " + row
        stat_daily.save(row)
    }

    /**
     * 恢复砸蛋非座驾价值统计
     */
    static recoverNocarPrice(int day){
        Date date = Date.parse("yyyy-MM-dd HH:mm", "2015-01-01 00:00")
        def gteMill = date.clearTime().getTime() - day * DAY_MILLON
        def YMD = new Date(gteMill).format("yyyyMMdd")
        def _id = "${YMD}_open_egg".toString()
        def timestamp = [$gte: gteMill, $lt: gteMill + DAY_MILLON]
        stat_daily.find(new BasicDBObject(_id:_id)).toArray().each {DBObject obj ->

            def carTotalPrice = 0l
            ['100', '250', '500'].each {String key ->
                def m = obj[key] as Map
                if(m != null){
                    m.keySet().each {String prize ->
                        if(prize.contains("Day")){
                            def pMap = m[prize] as Map
                            def carPrice = pMap.price
                            carTotalPrice += carPrice
                            println "${prize} : ${carPrice}"
                        }
                    }
                }
            }
            Long total_price = obj.total_price as Long
            Long nocar_price = total_price - carTotalPrice
            obj.put("nocar_price", nocar_price)
            println obj
            stat_daily.save(obj)
        }
    }

    /**
     * 恢复踢球非座驾价值统计
     */
    static recoverNocarPriceFootBall(int day){
        Date date = Date.parse("yyyy-MM-dd HH:mm", "2015-01-01 00:00")
        def gteMill = date.clearTime().getTime() - day * DAY_MILLON
        def YMD = new Date(gteMill).format("yyyyMMdd")
        def _id = "${YMD}_football_shoot".toString()
        def timestamp = [$gte: gteMill, $lt: gteMill + DAY_MILLON]
        stat_daily.find(new BasicDBObject(_id:_id)).toArray().each {DBObject obj ->
            def carTotalPrice = 0l
            ['500', '1000', '2000'].each {String key ->
                def m = obj[key] as Map
                if(m != null){
                    m.keySet().each {String prize ->
                        if(prize.contains("Day")){
                            def pMap = m[prize] as Map
                            def carPrice = pMap.price
                            carTotalPrice += carPrice
                            println "${prize} : ${carPrice}"
                        }
                    }
                }
            }
            Long total_price = obj.total_price as Long
            obj.put("nocar_price", total_price - carTotalPrice)
            println obj
            stat_daily.save(obj)
        }
    }

    static recoverCardCoin(Long begin){
        //Date date = Date.parse("yyyy-MM-dd HH:mm", "2014-10-07 00:00")
        //def gteMill = date.clearTime().getTime() - day * DAY_MILLON
        def YMD = new Date(begin).format("yyyyMMdd")

        def _id = "${YMD}_open_card".toString()
        def card_coin = (stat_daily.findOne(new BasicDBObject(_id:_id), new BasicDBObject('got', 1))?.get('got') ?: 0 ) as Long
        //def card_coin = (finance_dailyEarned.findOne(new BasicDBObject(_id:new Date(begin).format("yyyy-MM-dd")))?.get('card_coin') ?: 0 ) as Long
        String fid = "finance_" + YMD
        println "${fid} : ${card_coin}"
        //financeTmpDB.update(new BasicDBObject(_id:fid), new BasicDBObject('$set', new BasicDBObject(card_coin: card_coin)))
    }

    static class PayType {
        final user = new HashSet(1000)
        final count = new AtomicInteger()
        final coin = new AtomicLong()
        def cny = new BigDecimal(0)

        def toMap() { [user: user.size(), count: count.get(), coin: coin.get(), cny: cny.doubleValue()] }
    }

    /**
     * 修复每日充值统计
     * @param begin
     * @return
     */
    static recoverFinanceStatics(Long begin) {
        def YMD = new Date(begin).format("yyyyMMdd")
        def list = mongo.getDB('xy_admin').getCollection('finance_log').find(new BasicDBObject(timestamp: [$gte: begin, $lte: begin+DAY_MILLON]))
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

        String _id = "${YMD}_finance".toString()
        def obj = new BasicDBObject(
                _id: _id,
                total: total.doubleValue(),
                total_coin: totalCoin,
                type: 'finance',
                timestamp: begin
        )

        pays.each { String key, PayType type -> obj.put(org.apache.commons.lang.StringUtils.isBlank(key) ? '' : key.toLowerCase(), type.toMap()) }
        cats.each { k, v ->
            obj.put(k, v.doubleValue())
        }
        println "${YMD}: ${obj}"
        def old_daily = stat_daily.findOne(_id)
        if(old_daily != null){
            stat_daily_bak.save(old_daily)
            stat_daily.save(obj)
        }

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

    static recoverShenZhouFu(){
        DBCursor logs = mongo.getDB('xy_admin').getCollection('finance_log').find(new BasicDBObject(via:'shenzhoufu')).batchSize(10000)
        while (logs.hasNext()){
            def finance = logs.next();
            Double cny = finance.get('cny') as Double
            Long coin = finance.get('coin') as Long
            if((cny*100) != coin){
                println finance
                finance.put('via', 'shenzhoufu_game')
                mongo.getDB('xy_admin').getCollection('finance_log').save(finance)
            }
        }
    }

    static void main(String[] args){
        def l = System.currentTimeMillis()
        //recoverCostStatistic();
        recoverDailyReport();
        /*int i = 0
        while(i++ <= 50){
            //recoverfootballStatic(i)
            //recoverEggStatics(i)
            //recoverNocarPrice(i)
            //recoverNocarPriceFootBall(i)
        }*/
        //recoverCardCoin(Date.parse("yyyy-MM-dd HH:mm:ss" ,"2016-05-19 00:00:00").getTime())
        //recoverFinanceStatics(Date.parse("yyyy-MM-dd HH:mm:ss" ,"2014-11-24 00:00:00").getTime())
/*
        Long begin = Date.parse("yyyy-MM-dd HH:mm:ss" ,"2014-12-01 00:00:00").getTime()
        while(begin < new Date().clearTime().getTime()){
            recoverFinanceStatics(begin)
            begin += DAY_MILLON
        }*/

        //recoverShenZhouFu();
        println "CostStat cost  ${System.currentTimeMillis() -l} ms".toString()
    }
}
