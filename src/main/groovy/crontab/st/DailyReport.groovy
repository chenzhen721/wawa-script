#!/usr/bin/env groovy
package crontab.tmp

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
 *
 *
 * date: 14-5-12 下午2:46
 * @author: haigen.xiong@ttpod.com
 */
class DailyReport {
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

    static mongo = new Mongo(new MongoURI(getProperties('mongo.uri', 'mongodb://192.168.31.231:10000,192.168.31.236:10000,192.168.31.231:10001/?w=1&slaveok=true') as String))
    static DAY_MILLON = 24 * 3600 * 1000L
    static long zeroMill = new Date().clearTime().getTime()
    static Long yesTday = zeroMill - DAY_MILLON
    static String myId = "finance_" + new Date(yesTday).format("yyyyMMdd")
    static financeTmpDB = mongo.getDB('xy_admin').getCollection('finance_dailyReport')
    static bet_log = mongo.getDB('game_log').getCollection('user_bet')

    static {
        def curr_date = new Date(yesTday - day * DAY_MILLON)//
        myId = "finance_" + curr_date.format("yyyyMMdd")
        def obj = new BasicDBObject(_id: myId, timestamp: curr_date.getTime())
        financeTmpDB.save(obj)
    }


    private static Map getTimeBetween() {
        def gteMill = yesTday - day * DAY_MILLON
        return [$gte: gteMill, $lt: gteMill + DAY_MILLON]
    }

    static userRemainByAggregate() {

        println "userRemainByAggregate coin_count begin : ${new Date().format('yyyy-MM-dd HH:mm:ss')}"
        def coin = remainByAggregate(new BasicDBObject('finance.coin_count': [$gt: 0]), 'finance.coin_count')
        println "userRemainByAggregate coin_count end : ${new Date().format('yyyy-MM-dd HH:mm:ss')} totalcoin:---->:${coin}"

        def starQ = new BasicDBObject('finance.bean_count': [$gt: 0], priv: 2)
        def star_bean = remainByAggregate(starQ, 'finance.bean_count')
        println "userRemainByAggregate star_bean---->:${star_bean}"

        def starObtainQ = new BasicDBObject('finance.bean_count_total': [$gt: 0], priv: 2)
        def star_obtain_bean = remainByAggregate(starObtainQ, 'finance.bean_count_total')
        println "userRemainByAggregate star_obtain_bean---->:${star_obtain_bean}"

        def usrQ = new BasicDBObject('finance.bean_count': [$gt: 0], priv: [$nin: [2]])
        def usr_bean = remainByAggregate(usrQ, 'finance.bean_count')
        println "userRemainByAggregate usr_bean---->:${usr_bean}"

        def usrObtainQ = new BasicDBObject('finance.bean_count_total': [$gt: 0], priv: [$nin: [2]])
        def usr_obtain_bean = remainByAggregate(usrObtainQ, 'finance.bean_count_total')
        println "userRemainByAggregate usr_obtain_bean---->:${usr_obtain_bean}"

        def obj = new BasicDBObject(
                total_coin: coin,//用户剩余柠檬
                star_total_bean: star_bean,//主播剩余维C
                star_obtain_bean: star_obtain_bean,//主播获得总维C
                usr_total_bean: usr_bean,//用户剩余维C
                usr_obtain_bean: usr_obtain_bean,//用户获得总维C
                sj: new Date(System.currentTimeMillis()).format("yyyy-MM-dd HH:mm:ss"))
        financeTmpDB.update(new BasicDBObject(_id: myId), new BasicDBObject('$set', obj))
    }

    static Long remainByAggregate(BasicDBObject query, String field) {
        Long remain = 0
        def users = mongo.getDB("xy").getCollection("users")
        users.aggregate(
                new BasicDBObject('$match', query),
                new BasicDBObject('$project', [remain: '$' + field]),
                new BasicDBObject('$group', [_id: null, remain: [$sum: '$remain']])
        ).results().each { BasicDBObject obj ->
            remain = obj.get('remain') as Long
        }
        return remain;
    }

    //===============================减阳光 begin=============================//
    //01.日消耗
    static staticsRoomSpendCoin() {
        def timeBetween = getTimeBetween()
        println "staticsRoomSpendCoin getTimeBetween:" + timeBetween
        def rq = new BasicDBObject(timestamp: timeBetween, type: [$nin: ['label', "song"]])
        def coinList = mongo.getDB('xylog').getCollection("room_cost").find(rq, new BasicDBObject(cost: 1)).toArray()
        def coin_spend_day = coinList.sum { it.cost ?: 0 } as Long
        // 日消耗要加游戏统计的
        def query = $$('timestamp': timeBetween)
        def field = $$('cost': 1)
        def list = bet_log.find(query, field).toArray()
        def game_spend_coin = list.sum { it.cost ?: 0 } as Long
        println("game_spend_coin is ${game_spend_coin}")
        coin_spend_day += game_spend_coin
        println "coin_spend_day:---->:${coin_spend_day}"
        financeTmpDB.update(new BasicDBObject(_id: myId), new BasicDBObject('$set', new BasicDBObject(coin_spend_room: coin_spend_day)))
    }

    //02.游戏减币
    static staticsBetCoin() {
        def field = $$('cost': 1)
//        def list = bet_log.find(query, field).toArray()
        def game_spend_coin = 0
        def map = new HashMap()
        println "game_spend_coin:---->:${game_spend_coin}"
        def gameList = mongo.getDB('xy_admin').getCollection('games')
        gameList.find().each {
            BasicDBObject obj->
                def id = obj['_id'] as Integer
                def query = $$('timestamp': timeBetween,'game_id':id)
                def list = bet_log.find(query, field).toArray()
                def sum = list.sum { it.cost ?: 0 } as Long
                game_spend_coin += sum
                map.put(id.toString(),sum)
        }
        financeTmpDB.update($$(_id: myId), $$('$set', $$(game_spend_coin: game_spend_coin,'game_dec':map)))
    }

    //03.在途阳光
    static staticsTransitCoin() {
        def timeBetween = getTimeBetween()
        def rq = new BasicDBObject(status: 3, timestamp: timeBetween)
        def coinList = mongo.getDB('xy').getCollection("songs").find(rq, new BasicDBObject(cost: 1)).toArray()
        def song_coin = coinList.sum { it.cost ?: 0 } as Long
        if (null == song_coin)
            song_coin = 0L
        println "transit_coin:song_coin---->:${song_coin}"

        def rqlabe = new BasicDBObject(status: [$in: [0, 3]], timestamp: timeBetween)
        def labelList = mongo.getDB('xylog').getCollection("labels").find(rqlabe, new BasicDBObject(cost: 1)).toArray()
        def label_coin = labelList.sum { it.cost ?: 0 } as Long
        if (null == label_coin)
            label_coin = 0L
        println "transit_coin:label_coin---->:${label_coin}"

        def transit_coin = song_coin + label_coin
        println "transit_coin:---->:${transit_coin}"
        financeTmpDB.update(new BasicDBObject(_id: myId), new BasicDBObject('$set', new BasicDBObject(transit_coin: transit_coin)))
    }

    //06.手工减币
    static staticsCutCoin() {
        def timeBetween = getTimeBetween()
        def q = new BasicDBObject(type: 'finance_cut_coin', timestamp: timeBetween)
        def ops_log = mongo.getDB('xy_admin').getCollection('ops')
        def coinList = ops_log.
                find(q, new BasicDBObject(data: 1))
                .toArray()
        Long hand_cut_coin = coinList.sum { it.data.coin ?: 0 } as Long
        if (null == hand_cut_coin) {
            hand_cut_coin = 0L
        }
        println "hand_cut_coin:---->:${hand_cut_coin}"
        financeTmpDB.update(new BasicDBObject(_id: myId), new BasicDBObject('$set', new BasicDBObject(hand_cut_coin: hand_cut_coin)))
    }

    //07.举报扣币
    static staticsAccuseSubtractCoin() {
        def timeBetween = getTimeBetween()
        def q = new BasicDBObject(timestamp: timeBetween, type: "cost")
        def lottery_log = mongo.getDB('xylog').getCollection('accuse_logs')
        def coinList = lottery_log.
                find(q, new BasicDBObject(coin: 1))
                .toArray()
        def accuse_subtract_coin = coinList.sum { it.coin ?: 0 } as Long
        if (null == accuse_subtract_coin)
            accuse_subtract_coin = 0L

        println "accuse_subtract_coin:---->:${accuse_subtract_coin}"
        financeTmpDB.update(new BasicDBObject(_id: myId), new BasicDBObject('$set', new BasicDBObject(accuse_subtract_coin: accuse_subtract_coin)))
    }

    //===============================减阳光 end=============================//

    //===============================add阳光 begin=============================//

    //01.充值-获得币(包括返币)
    static staticsChargeTotal() {
        def timeBetween = getTimeBetween()
        def q = new BasicDBObject(via: [$ne: 'Admin'], timestamp: timeBetween)
        def finance_log = mongo.getDB('xy_admin').getCollection('finance_log')
        def coinList = finance_log.
                find(q, new BasicDBObject(coin: 1, cny: 1, returnCoin: 1))
                .toArray()
        def charge_coin = coinList.sum { (it.coin ?: 0) + (it.returnCoin ?: 0) } as Long
        if (null == charge_coin)
            charge_coin = 0L
        println "charge_coin:---->:${charge_coin}"
        double charge_cny = 0.0d
        if (coinList.size() > 0)
            charge_cny = coinList.sum { it.cny ?: 0.0d } as Double
        println "charge_cny:---->:${charge_cny}"
        financeTmpDB.update(new BasicDBObject(_id: myId), new BasicDBObject('$set', new BasicDBObject(charge_coin: charge_coin, charge_cny: charge_cny)))
    }

    //02.VC换币--获得币
    static staticsExchange() {
        def timeBetween = getTimeBetween()
        def q = new BasicDBObject(timestamp: timeBetween)
        def exchange_log = mongo.getDB('xylog').getCollection('exchange_log')
        def coinList = exchange_log.
                find(q, new BasicDBObject(exchange: 1))
                .toArray()
        def exchange_coin = coinList.sum { it.exchange ?: 0 } as Long
        if (null == exchange_coin)
            exchange_coin = 0L
        println "exchange_coin:---->:${exchange_coin}"
        financeTmpDB.update(new BasicDBObject(_id: myId), new BasicDBObject('$set', new BasicDBObject(exchange_coin: exchange_coin)))
    }

    //03.手工加币--获得币
    static staticsAdminTotal() {
        def timeBetween = getTimeBetween()
        def q = new BasicDBObject(via: 'Admin', timestamp: timeBetween)
        def finance_log = mongo.getDB('xy_admin').getCollection('finance_log')
        def coinList = finance_log.
                find(q, new BasicDBObject(coin: 1, cny: 1))
                .toArray()
        def hand_coin = coinList.sum { it.coin ?: 0 } as Long
        if (null == hand_coin)
            hand_coin = 0L
        println "hand_coin:---->:${hand_coin}"
        financeTmpDB.update(new BasicDBObject(_id: myId), new BasicDBObject('$set', new BasicDBObject(hand_coin: hand_coin)))
    }

    //07.签到加币
    static staticsLoginCoin() {
        def timeBetween = getTimeBetween()
        def sign_log = mongo.getDB('xylog').getCollection('sign_logs')
        def query = $$('timestamp': timeBetween)
        def list = sign_log.find(query).toArray()
        def login_coin = list.sum { it.coin ?: 0 } as Long
        println "login_coin:---->:${login_coin}"
        financeTmpDB.update($$(_id: myId), $$('$set', $$(login_coin: login_coin)))
    }

    //08.任务加币(首充，首次关注，首次修改昵称等)
    static staticsMissionCoin() {
        def timeBetween = getTimeBetween()
        def q = new BasicDBObject(timestamp: timeBetween)
        def mission_logs = mongo.getDB('xylog').getCollection('mission_logs')
        def coinList = mission_logs.
                find(q, new BasicDBObject(coin: 1))
                .toArray()

        def mission_coin = coinList.sum { it.coin ?: 0 } as Long
        if (null == mission_coin)
            mission_coin = 0L
        println "mission_coin:---->:${mission_coin}"

        financeTmpDB.update(new BasicDBObject(_id: myId), new BasicDBObject('$set', new BasicDBObject(mission_coin: mission_coin)))
    }

    // 游戏加币
    static staticsGameCoin() {
        def timeBetween = getTimeBetween()
        def field = $$('coin': 1)
        def lottery_log = mongo.getDB('game_log').getCollection('user_lottery')
        def game_coin = 0
        def map = new HashMap()
        // 加入游戏节点
        def gameList = mongo.getDB('xy_admin').getCollection('games').find()
        gameList.each {
            BasicDBObject obj ->
                def id = obj['_id'] as Integer
                def query = $$('timestamp': timeBetween, 'coin': ['$gt': 0],'game_id':id)
                def list = lottery_log.find(query,field).toArray()
                def sum = list.sum { it.coin ?: 0 } as Long
                game_coin += sum
                map.put(id.toString(),sum)
        }
        financeTmpDB.update($$(_id: myId), $$('$set', $$(game_coin: game_coin,'game_inc':map)))
    }

    //16.活动中奖--获得币  活动类型："10month","CardYouxi","Christmas","Fortune","SF","chargeRank","dianliang","laodong", "new_year","qq_wx_wb_share","send_hongbao" ,"worldcup"
    static staticActives() {
        Long award_coin = 0
        def query = new BasicDBObject(timestamp: getTimeBetween(), award_coin: [$gt: 0])
        def lottery_log = mongo.getDB('xylog').getCollection('lottery_logs')
        lottery_log.aggregate(
                new BasicDBObject('$match', query),
                new BasicDBObject('$project', [award_coin: '$award_coin']),
                new BasicDBObject('$group', [_id: null, award_coin: [$sum: '$award_coin']])
        ).results().each { BasicDBObject obj ->
            award_coin = obj.get('award_coin') as Long
        }
        println "award_coin:---->:${award_coin}"
        financeTmpDB.update(new BasicDBObject(_id: myId), new BasicDBObject('$set', new BasicDBObject(award_coin: award_coin)))
    }

    private final static Map<String, Long> APP_FREE_GIFT = ['100': 10, '103': 100, '102': 10]
    //手机直播免费礼物奖励
    static staticAppFreeGift() {
        def q = new BasicDBObject(timestamp: getTimeBetween(), active_name: 'app_meme_luck_gift')
        def lottery_log = mongo.getDB('xylog').getCollection('lottery_logs')
        Long award_coin = 0;
        APP_FREE_GIFT.each { String giftId, Integer coin ->
            award_coin += lottery_log.count(q.append('award_name', giftId)) * APP_FREE_GIFT[giftId]
        }

        println "staticAppFreeGift award_coin:---->:${award_coin}"
        financeTmpDB.update(new BasicDBObject(_id: myId), new BasicDBObject('$set', new BasicDBObject(app_free_gift_coin: award_coin)))
    }

    static final List<String> ACTIVE_NAMES = ['chest20160812']
    //各种活动抽奖获取柠檬
    static staticsActivityAwardCoins() {
        def timeBetween = getTimeBetween()
        def query = new BasicDBObject(timestamp: timeBetween, active_name: [$in: ACTIVE_NAMES])
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
        financeTmpDB.update(new BasicDBObject(_id: myId), new BasicDBObject('$set', new BasicDBObject(activity_award_coin: award_coin)))
    }

    //===============================add阳光end=============================//

    //===============================VC begin=============================//
    //TODO 01.包包中的礼物折算成VC的价值  //性能问题暂停
    static staticsBagBean() {
        def bagQ = new BasicDBObject(bag: [$exists: true])
        def users = mongo.getDB('xy').getCollection('users')
        def beanList = users.
                find(bagQ, new BasicDBObject(bag: 1))
                .toArray()

        Long bag_bean = 0L
        def giftDB = mongo.getDB('xy_admin').getCollection('gifts')
        def categoryDB = mongo.getDB('xy_admin').getCollection('gift_categories')
        for (DBObject obj : beanList) {
            def bag = (Map) obj?.get("bag")
            for (Object key : bag.keySet()) {
                Integer gift_id = Integer.parseInt((String) key)
                Integer gift_num = bag?.get(key) as Integer
                if (gift_num > 0) {
                    def gift = giftDB.findOne(new BasicDBObject('_id', gift_id), new BasicDBObject('category_id': 1, "coin_price": 1))
                    def category_id = gift?.get("category_id")
                    def price = gift?.get("coin_price") as Integer
                    if (category_id) {
                        def giftCategory = categoryDB.findOne(new BasicDBObject('_id', category_id as Integer), new BasicDBObject('ratio', 1))
                        def ratio = giftCategory?.get("ratio") as Double
                        if (ratio) {
                            Double beanValue = price * gift_num * ratio
                            bag_bean = bag_bean + beanValue.toLong()
                        }
                    }
                }
            }
        }
        println "bag_bean:---->:${bag_bean}"
        financeTmpDB.update(new BasicDBObject(_id: myId), new BasicDBObject('$set', new BasicDBObject(bag_bean: bag_bean)))
    }

    //===============================VC add  begin=============================//
    //02.奇迹送VC给主播
    static staticsSpecialBean() {
        def q = new BasicDBObject('star1.bonus_time': getTimeBetween())
        def special_gifts = mongo.getDB('xylog').getCollection('special_gifts')
        def beanList = special_gifts.
                find(q, new BasicDBObject('star1': 1))
                .toArray()
        def special_bean = beanList.sum { it.star1.earned ?: 0 } as Long
        if (null == special_bean)
            special_bean = 0L
        println "special_bean:---->:${special_bean}"
        financeTmpDB.update(new BasicDBObject(_id: myId), new BasicDBObject('$set', new BasicDBObject(special_bean: special_bean)))
    }

    //03.房间普通用户送礼给用户或主播获VC
    static staticsRoomBean() {
        def rq = new BasicDBObject(timestamp: getTimeBetween())
/*

        def beanList =  mongo.getDB('xylog').getCollection("room_cost").find(rq,new BasicDBObject('session':1)).toArray()

        def room_bean = beanList.sum {((Map)(((Map)it.session)?.get("data")))?.get("earned") ?:0} as Long
*/

        Long room_bean = 0
        def room_cost = mongo.getDB('xylog').getCollection("room_cost")
        room_cost.aggregate(
                new BasicDBObject('$match', rq),
                new BasicDBObject('$project', ['earned': '$session.data.earned']),
                new BasicDBObject('$group', [_id: null, earned: [$sum: '$earned']])
        ).results().each { BasicDBObject obj ->
            room_bean = obj.get('earned') as Long
        }

        println "room_bean:---->:${room_bean}"

        financeTmpDB.update(new BasicDBObject(_id: myId), new BasicDBObject('$set', new BasicDBObject(room_bean: room_bean)))
    }

    //TODO 不符合财务要求 需要废弃 04.系统送出礼物到用户包包，折算成VC的价值
    static staticAddBagBean() {
        def q = new BasicDBObject(timestamp: getTimeBetween())
        def obtain_log = mongo.getDB('xylog').getCollection('obtain_gifts')
        def beanList = obtain_log.find(q, new BasicDBObject("gifts_bean": 1)).toArray()
        def obtain_gifts_bean = beanList.sum { it.gifts_bean ?: 0 } as Long
        if (null == obtain_gifts_bean)
            obtain_gifts_bean = 0L

        println "obtain_gifts_bean:---->:${obtain_gifts_bean}"
        financeTmpDB.update(new BasicDBObject(_id: myId), new BasicDBObject('$set', new BasicDBObject(obtain_gifts_bean: obtain_gifts_bean)))
    }
    //===============================VC add  end=============================//

    //===============================VC 减少  begin=============================//
    //01.主播提现
    static staticsCashBean() {
        def q = new BasicDBObject(timestamp: getTimeBetween(), exchange: [$gt: 0], status: [$in: [0, 1]])
        def withdrawl_log = mongo.getDB('xy_admin').getCollection('withdrawl_log')
        def beanList = withdrawl_log.
                find(q, new BasicDBObject(exchange: 1))
                .toArray()

        def cash_bean = beanList.sum { it.exchange ?: 0 } as Long
        if (null == cash_bean)
            cash_bean = 0L
        println "cash_bean:---->:${cash_bean}"

        financeTmpDB.update(new BasicDBObject(_id: myId), new BasicDBObject('$set', new BasicDBObject(cash_bean: (cash_bean * 100L))))
    }
    //02.VC兑换成行阳光

    //03.用户从包包中送出礼物给主播 包包的豆减少
    static staticsBagSendBean() {

        def rq = new BasicDBObject(timestamp: getTimeBetween(), type: "send_gift", cost: 0)
        /*
           def beanList =  mongo.getDB('xylog').getCollection("room_cost").find(rq,new BasicDBObject('session':1)).toArray()

           def bag_send_bean = beanList.sum {it.session.data.earned ?:0} as Long
   */
        Long bag_send_bean = 0
        def room_cost = mongo.getDB('xylog').getCollection("room_cost")
        room_cost.aggregate(
                new BasicDBObject('$match', rq),
                new BasicDBObject('$project', ['earned': '$session.data.earned']),
                new BasicDBObject('$group', [_id: null, earned: [$sum: '$earned']])
        ).results().each { BasicDBObject obj ->
            bag_send_bean = obj.get('earned') as Long
        }

        println "bag_send_bean:---->:${bag_send_bean}"

        financeTmpDB.update(new BasicDBObject(_id: myId), new BasicDBObject('$set', new BasicDBObject(bag_send_bean: bag_send_bean)))
    }
    //===============================VC 减少  end=============================//

    //===============================VCend=============================//


    final static Integer day = 0;

    static void main(String[] args) {
        long l = System.currentTimeMillis()
        long begin = l

        /*  userRemain()
          println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},userRemain cost  ${System.currentTimeMillis() -l} ms"
          Thread.sleep(1000L)
  */
        l = System.currentTimeMillis()
        userRemainByAggregate()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},userRemainByAggregate cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)
        //===============================减阳光 begin=============================//
        //01.日消耗
        l = System.currentTimeMillis()
        staticsRoomSpendCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticsRoomSpendCoin cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //02. 游戏下注消耗
        l = System.currentTimeMillis()
        staticsBetCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticsBetCoin cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //03.在途阳光
        l = System.currentTimeMillis()
        staticsTransitCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticsTransitCoin cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //06.手工减币
        l = System.currentTimeMillis()
        staticsCutCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticsCutCoin cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //07.举报扣币
        l = System.currentTimeMillis()
        staticsAccuseSubtractCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticsAccuseSubtractCoin cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //===============================减阳光 end================================//

        //===============================add阳光  begin=============================//
        //01.充值-获得币
        l = System.currentTimeMillis()
        staticsChargeTotal()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticsChargeTotal cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //02.VC换币--获得币
        l = System.currentTimeMillis()
        staticsExchange()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticsExchange cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //03.手工加币--获得币
        l = System.currentTimeMillis()
        staticsAdminTotal()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticsAdminTotal cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //07.签到加币
        l = System.currentTimeMillis()
        staticsLoginCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticsLoginCoin cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //08.任务加币
        l = System.currentTimeMillis()
        staticsMissionCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticsMissionCoin cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //09.游戏加币
        l = System.currentTimeMillis()
        staticsGameCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticsGameCoin cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //16.活动中奖--获得币
        l = System.currentTimeMillis()
        staticActives()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticActives cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //手机直播礼物
        l = System.currentTimeMillis()
        staticAppFreeGift()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticAppFreeGift cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //么么活动抽奖获取柠檬
        l = System.currentTimeMillis()
        staticsActivityAwardCoins()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticsActivityAwardCoins cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)
        //===============================add阳光 end=============================//

        //===============================VC begin=============================//
        //01.包包中的礼物
        /*
        l = System.currentTimeMillis()
        staticsBagBean()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticsBagBean cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)
        */

        //===============================VC add begin=============================//
        //02.奇迹-获VC
        l = System.currentTimeMillis()
        staticsSpecialBean()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticsSpecialBean cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //03.房间消费获VC
        l = System.currentTimeMillis()
        staticsRoomBean()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticsRoomBean cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //04.系统送出礼物到用户包包，折算成VC的价值
        l = System.currentTimeMillis()
        staticAddBagBean()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticAddBagBean cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //===============================VC add end=============================//

        //===============================VC 减少 begin=============================//
        //01.主播提现
        l = System.currentTimeMillis()
        staticsCashBean()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticsCashBean cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //02.VC兑换阳光

        //03.用户从包包中送出礼物给主播 包包的豆减少
        l = System.currentTimeMillis()
        staticsBagSendBean()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticsBagSendBean cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)
        //===============================VC 减少 end=============================//

        //===============================VC end=============================//

        jobFinish(begin);
    }

    /**
     * 标记任务完成  用于运维监控
     * @return
     */
    private static jobFinish(Long begin) {
        def timerName = 'DailyReport'
        Long totalCost = System.currentTimeMillis() - begin
        saveTimerLogs(timerName, totalCost)
        println "${new Date().format('yyyy-MM-dd')}:${DailyReport.class.getSimpleName()}:finish  cost time:  ${System.currentTimeMillis() - begin} ms"
    }

    //落地定时执行的日志
    private static saveTimerLogs(String timerName, Long totalCost) {
        def timerLogsDB = mongo.getDB("xyrank").getCollection("timer_logs")
        def tmp = System.currentTimeMillis()
        def id = timerName + "_" + new Date().format("yyyyMMdd")
        def update = new BasicDBObject(timer_name: timerName, cost_total: totalCost, cat: 'day', unit: 'ms', timestamp: tmp)
        timerLogsDB.findAndModify(new BasicDBObject('_id', id), null, null, false, new BasicDBObject('$set', update), true, true)
    }

    public static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    public static BasicDBObject $$(Map map) {
        return new BasicDBObject(map)
    }

}