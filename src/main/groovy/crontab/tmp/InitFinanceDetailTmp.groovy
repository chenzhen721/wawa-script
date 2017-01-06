#!/usr/bin/env groovy
package crontab.tmp

import com.mongodb.BasicDBObject
@Grapes([
@Grab('org.mongodb:mongo-java-driver:2.14.2'),
@Grab('commons-lang:commons-lang:2.6'),
@Grab('redis.clients:jedis:2.1.0'),
]) import com.mongodb.Mongo
import java.text.SimpleDateFormat
import com.mongodb.DBObject

/**
 *
 *
 * date: 13-2-28 下午2:46
 * @author: yangyang.cong@ttpod.com
 */
class InitFinanceDetailTmp {

    //static mongo = new Mongo("127.0.0.1", 10000)
    static mongo  = new Mongo(new com.mongodb. MongoURI('mongodb://10.0.5.32:10000,10.0.5.33:10000,10.0.5.34:10000/?w=1&slaveok=true'))
    static DAY_MILLON = 24 * 3600 * 1000L
    static long zeroMill = new Date().clearTime().getTime()
     //充值渠道汇总
   static Long begin = new SimpleDateFormat("yyyyMMdd").parse("20140501").getTime()
   static Long end =  new SimpleDateFormat("yyyyMMdd").parse("20140601").getTime()
   static String myId =  "finance_201405"
   static financeTmpDB = mongo.getDB('xy_admin').getCollection('stat_finance_tmp')


    static init()
    {
        def obj =  new BasicDBObject(_id:myId)
        financeTmpDB.save(obj)
    }

    //01.日消耗
    static staticsRoomSpendCoin()
    {
        def timeBetween = [$gte: begin, $lt: end]
        def rq = new BasicDBObject(timestamp: timeBetween,type:[$nin: ['label',"song"]])
        def coinList =  mongo.getDB('xylog').getCollection("room_cost").find(rq,new BasicDBObject(cost:1)).toArray()
        def coin_spend_day = coinList.sum {it.cost?:0} as Long
        println "coin_spend_day:---->:${coin_spend_day}"
        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(coin_spend_room:coin_spend_day)))
    }
    //02.私信消耗
    static staticsMailSpendCoin()
    {
        def timeBetween = [$gte: begin, $lt: end]
        def rq = new BasicDBObject(timestamp: timeBetween)
        def coinList =  mongo.getDB('xy').getCollection("mails").find(rq,new BasicDBObject(_id:1)).toArray()
        int size = 0
        if(coinList)
            size = coinList.size()
        def coin_spend_mail = size * 5L
        println "coin_spend_mail:---->:${coin_spend_mail}"
        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(coin_spend_mail:coin_spend_mail)))
    }

    //03.在途星币
    static staticsTransitCoin()
    {
        def timeBetween = [$gte: begin, $lt: end]
        def rq = new BasicDBObject(status:3,timestamp: timeBetween)
        def coinList =  mongo.getDB('xy').getCollection("songs").find(rq,new BasicDBObject(cost:1)).toArray()
        def song_coin = coinList.sum {it.cost?:0} as Long
        if(null == song_coin)
            song_coin = 0L
        println "transit_coin:song_coin---->:${song_coin}"

        def rqlabe = new BasicDBObject(status:[$in:[0,3]],timestamp: timeBetween)
        def labelList =  mongo.getDB('xylog').getCollection("labels").find(rqlabe,new BasicDBObject(cost:1)).toArray()
        def label_coin = labelList.sum {it.cost?:0} as Long
        if(null == label_coin)
            label_coin = 0L
        println "transit_coin:label_coin---->:${label_coin}"

        def transit_coin = song_coin + label_coin
        println "transit_coin:---->:${transit_coin}"
        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(transit_coin:transit_coin)))
    }

    //04.点歌扣星币
    static staticsSongSpendCoin()
    {
        def timeBetween = [$gte: begin, $lt: end]
        def rq = new BasicDBObject(status:[$in:[1,2,4]],timestamp: timeBetween)
        def coinList =  mongo.getDB('xy').getCollection("songs").find(rq,new BasicDBObject(cost:1)).toArray()
        def coin_spend_song = coinList.sum {it.cost?:0} as Long
        if(null == coin_spend_song)
            coin_spend_song = 0L
        println "coin_spend_song:---->:${coin_spend_song}"
        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(coin_spend_song:coin_spend_song)))
    }

    //05.求爱签扣星币
    static staticsLabelSpendCoin()
    {
        def timeBetween = [$gte: begin, $lt: end]
        def rqlabe = new BasicDBObject(status:[$in:[1,2,4]],timestamp: timeBetween)
        def labelList =  mongo.getDB('xylog').getCollection("labels").find(rqlabe,new BasicDBObject(cost:1)).toArray()
        def coin_spend_label = labelList.sum {it.cost?:0} as Long
        if(null == coin_spend_label)
            coin_spend_label = 0L
        println "coin_spend_label---->:${coin_spend_label}"
        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(coin_spend_label:coin_spend_label)))
    }

    //06.手工减币
    static staticsCutCoin()
    {
        def timeBetween = [$gte: begin, $lt: end]
        def q = new BasicDBObject(type: 'finance_cut_coin',timestamp:timeBetween)
        def ops_log = mongo.getDB('xy_admin').getCollection('ops')
        def coinList = ops_log.
                find(q,new BasicDBObject(data:1))
                .toArray()
        Long hand_cut_coin = 0L
        for(DBObject cnyObj : coinList)
        {
            Long coin = 0.0d
            def data = cnyObj.get("data")
            if(null != data)
                coin = ((Map)data).get("coin") as Long
            hand_cut_coin = hand_cut_coin +  coin
        }
        println "hand_cut_coin:---->:${hand_cut_coin}"
        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(hand_cut_coin:hand_cut_coin)))
    }

    //07.举报扣币
    static staticsAccuseSubtractCoin()
    {
        def timeBetween = [$gte: begin, $lt: end]
        def q = new BasicDBObject(timestamp:timeBetween,type: "cost")
        def lottery_log = mongo.getDB('xylog').getCollection('accuse_logs')
        def coinList = lottery_log.
                find(q,new BasicDBObject(coin:1))
                .toArray()
        def accuse_subtract_coin = coinList.sum {it.coin?:0} as Long
        if(null ==accuse_subtract_coin)
            accuse_subtract_coin = 0L

        println "accuse_subtract_coin:---->:${accuse_subtract_coin}"
        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(accuse_subtract_coin:accuse_subtract_coin)))
    }

    //08.活动扣币
    static staticsActiveSubtractCoin()
    {
        def timeBetween = [$gte: begin, $lt: end]
        def q = new BasicDBObject(timestamp:timeBetween,type: "worldcup")
        def lottery_log = mongo.getDB('xyactive').getCollection('cost_logs')
        def costList = lottery_log.
                find(q,new BasicDBObject(cost:1))
                .toArray()
        def active_subtract_coin = costList.sum {it.cost?:0} as Long
        if(null ==active_subtract_coin)
            active_subtract_coin = 0L

        println "active_subtract_coin:---->:${active_subtract_coin}"
        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(active_subtract_coin:active_subtract_coin)))
    }

    //===============================减星币 end=============================//


    //===============================add星币 begin=============================//

    //01.充值-获得币
    static staticsChargeTotal()
    {
        def timeBetween = [$gte: begin, $lt: end]
        def q = new BasicDBObject(via:[$ne:'Admin'],timestamp:timeBetween)
        def finance_log = mongo.getDB('xy_admin').getCollection('finance_log')
        def coinList = finance_log.
                find(q,new BasicDBObject(coin:1,cny:1))
                .toArray()
        def charge_coin = coinList.sum {it.coin?:0} as Long
        if(null == charge_coin)
            charge_coin = 0L

        println "charge_coin:---->:${charge_coin}"
        double charge_cny = 0.0d
        for(DBObject cnyObj : coinList)
        {
            double cny = 0.0d
            if(null != cnyObj.get("cny"))
                cny = new Double(cnyObj.get("cny"))
            charge_cny = charge_cny +  cny
        }
        println "charge_cny:---->:${charge_cny}"
        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(charge_coin:charge_coin,charge_cny:charge_cny)))
    }

    //02.星豆换币--获得币
    static staticsExchange()
    {
        def timeBetween = [$gte: begin, $lt: end]
        def q = new BasicDBObject(timestamp:timeBetween)
        def exchange_log = mongo.getDB('xylog').getCollection('exchange_log')
        def coinList = exchange_log.
                find(q,new BasicDBObject(exchange:1))
                .toArray()
        def exchange_coin = coinList.sum {it.exchange?:0} as Long
        if(null == exchange_coin)
            exchange_coin = 0L
        println "exchange_coin:---->:${exchange_coin}"
        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(exchange_coin:exchange_coin)))
    }

    //03.手工加币--获得币
    static staticsAdminTotal()
    {
        def timeBetween = [$gte: begin, $lt: end]
        def q = new BasicDBObject(via:'Admin',timestamp:timeBetween)
        def finance_log = mongo.getDB('xy_admin').getCollection('finance_log')
        def coinList = finance_log.
                find(q,new BasicDBObject(coin:1,cny:1))
                .toArray()
        def hand_coin = coinList.sum {it.coin?:0} as Long
        if(null == hand_coin)
            hand_coin = 0L
        println "hand_coin:---->:${hand_coin}"
        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(hand_coin:hand_coin)))
    }

    //04.送幸运礼物--获得币
    static staticsLuck()
    {
        def timeBetween = [$gte: begin, $lt: end]
        def q = new BasicDBObject(timestamp:timeBetween)
        def room_luck = mongo.getDB('xylog').getCollection('room_luck')
        def coinList = room_luck.
                find(q,new BasicDBObject(got:1))
                .toArray()
        def luck_coin = coinList.sum {it.got?:0} as Long
        if(null == luck_coin)
            luck_coin = 0L
        println "luck_coin:---->:${luck_coin}"
        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(luck_coin:luck_coin)))
    }

    //05.砸金蛋加币
    static staticEggCoin()
    {
        def timeBetween = [$gte: begin, $lt: end]

        def qWan = new BasicDBObject(type:'open_egg','session.got':'COIN10000',timestamp:timeBetween)
        def room_cost = mongo.getDB('xylog').getCollection('room_cost')
        def coinWanList = room_cost.
                find(qWan,new BasicDBObject(_id:1))
                .toArray()
        def egg_coin =  10000L * (coinWanList.size())
        println "egg_coin:---->:${egg_coin}"
        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(egg_coin:egg_coin)))

    }
    //06.K歌晋级
    static staticsKSongCoin()
    {
        def timeBetween = [$gte: begin, $lt: end]
        def q = new BasicDBObject(timestamp:timeBetween)
        def gift_logs = mongo.getDB('xy_sing').getCollection('gift_logs')
        def coinList = gift_logs.
                find(q,new BasicDBObject(award_total:1))
                .toArray()

        def k_coin = coinList.sum {it.award_total?:0} as Long
        if(null == k_coin)
            k_coin = 0L
        println "k_coin:---->:${k_coin}"
        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(k_coin:k_coin)))
    }

    //07.签到加币
    static staticsLoginCoin()
    {
        def timeBetween = [$gte: begin, $lt: end]
        def awards_login = mongo.getDB('xylog').getCollection('awards_login')
        def qAwardLogin1 = new BasicDBObject(type:1,timestamp:timeBetween)
        def awardLoginList1 = awards_login.
                find(qAwardLogin1,new BasicDBObject(_id:1))
                .toArray()

        def loginCoin1 =  8L * (awardLoginList1.size())
        println "loginCoin1:---->:${loginCoin1}"

        def qAwardLogin2 = new BasicDBObject(type:2,timestamp:timeBetween)
        def awardLoginList2 = awards_login.
                find(qAwardLogin2,new BasicDBObject(_id:1))
                .toArray()
        def loginCoin2 =  20L * (awardLoginList2.size())
        println "loginCoin2:---->:${loginCoin2}"

        def qAwardLogin3 = new BasicDBObject(type:3,timestamp:timeBetween)
        def awardLoginList3 = awards_login.
                find(qAwardLogin3,new BasicDBObject(_id:1))
                .toArray()
        def loginCoin3 =  50L * (awardLoginList3.size())
        println "loginCoin3:---->:${loginCoin3}"
        Thread.sleep(1000L)

        def qAwardLogin4 = new BasicDBObject(type:4,timestamp:timeBetween)
        def awardLoginList4 = awards_login.
                find(qAwardLogin4,new BasicDBObject(_id:1))
                .toArray()
        def loginCoin4 =  100L * (awardLoginList4.size())
        println "loginCoin4:---->:${loginCoin4}"
        Thread.sleep(1000L)


        def qSign = new BasicDBObject(award:true,timestamp:timeBetween)
        def day_login = mongo.getDB('xylog').getCollection('day_login')
        def signList = day_login.
                find(qSign,new BasicDBObject(_id:1))
                .toArray()

        def signCoin =  4L * (signList.size())
        println "signCoin:---->:${signCoin}"
        Thread.sleep(1000L)

        Long login_coin =  signCoin + loginCoin1 + loginCoin2 + loginCoin3 + loginCoin4
        println "login_coin:---->:${login_coin}"
        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(login_coin:login_coin)))
    }

    //08.任务加币(首充，首次关注，首次修改昵称等)
    static staticsMissionCoin()
    {
        def timeBetween = [$gte: begin, $lt: end]
        def q = new BasicDBObject(timestamp:timeBetween)
        def mission_logs = mongo.getDB('xylog').getCollection('mission_logs')
        def coinList = mission_logs.
                find(q,new BasicDBObject(coin:1))
                .toArray()

        def mission_coin = coinList.sum {it.coin?:0} as Long
        if(null == mission_coin)
            mission_coin = 0L
        println "mission_coin:---->:${mission_coin}"

        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(mission_coin:mission_coin)))
    }

    //09.合作商家合作加币
    static staticsUnionCoin()
    {
        def timeBetween = [$gte: begin, $lt: end]
        def q = new BasicDBObject(timestamp:timeBetween)
        def orders_weixin = mongo.getDB('xy_union').getCollection('orders_weixin')
        def coinList = orders_weixin.
                find(q,new BasicDBObject(coin:1))
                .toArray()

        def union_coin = coinList.sum {it.coin?:0} as Long
        if(null==union_coin)
            union_coin = 0L
        println "union_coin:---->:${union_coin}"
        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(union_coin:union_coin)))
    }

    //10.财神加币
    static staticsFortuneCoin()
    {
        def timeBetween = [$gte: begin, $lt: end]
        def q = new BasicDBObject(timestamp:timeBetween,active_name:'Fortune')
        def lottery_log = mongo.getDB('xylog').getCollection('lottery_logs')
        def coinList = lottery_log.
                find(q,new BasicDBObject(award_coin:1))
                .toArray()
        def fortune_coin = 16000L * (coinList.size())
        println "fortune_coin:---->:${fortune_coin}"
        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(fortune_coin:fortune_coin)))
    }

    //11.宝藏送币
    static staticsTreasureCoin()
    {
        def timeBetween = [$gte: begin, $lt: end]
        def q = new BasicDBObject(timestamp:timeBetween,active_name:'treasure')
        def lottery_log = mongo.getDB('xylog').getCollection('lottery_logs')
        def coinList = lottery_log.
                find(q,new BasicDBObject(obtain_coin:1))
                .toArray()
        def treasure_coin = coinList.sum {it.obtain_coin?:0} as Long
        if(null ==treasure_coin)
            treasure_coin = 0L

        println "treasure_coin:---->:${treasure_coin}"
        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(treasure_coin:treasure_coin)))
    }

    //12.举报奖励币
    static staticsAccuseAddCoin()
    {
        def timeBetween = [$gte: begin, $lt: end]
        def q = new BasicDBObject(timestamp:timeBetween,type: "award")
        def lottery_log = mongo.getDB('xylog').getCollection('accuse_logs')
        def coinList = lottery_log.
                find(q,new BasicDBObject(coin:1))
                .toArray()
        def accuse_add_coin = coinList.sum {it.coin?:0} as Long
        if(null ==accuse_add_coin)
            accuse_add_coin = 0L

        println "accuse_add_coin:---->:${accuse_add_coin}"
        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(accuse_add_coin:accuse_add_coin)))
    }

    //13.点歌退星币
    static staticsSongRefundCoin()
    {
        def timeBetween = [$gte: begin, $lt: end]
        def rq = new BasicDBObject(status:[$in:[2,4]],last_update: timeBetween)
        def coinList =  mongo.getDB('xy').getCollection("songs").find(rq,new BasicDBObject(cost:1)).toArray()
        def coin_refund_song = coinList.sum {it.cost?:0} as Long
        if(null == coin_refund_song)
            coin_refund_song = 0L
        println "coin_refund_song:---->:${coin_refund_song}"
        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(coin_refund_song:coin_refund_song)))
    }

    //14.求爱签退星币
    static staticsLabelRefundCoin()
    {
        def timeBetween = [$gte: begin, $lt: end]
        def rqlabe = new BasicDBObject(status:[$in:[2,4]],last_update: timeBetween)
        def labelList =  mongo.getDB('xylog').getCollection("labels").find(rqlabe,new BasicDBObject(cost:1)).toArray()
        def coin_refund_label = labelList.sum {it.cost?:0} as Long
        if(null == coin_refund_label)
            coin_refund_label = 0L
        println "coin_refund_label---->:${coin_refund_label}"
        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(coin_refund_label:coin_refund_label)))
    }

    //15.踢足球奖星币
    static staticFootballCoin()
    {
        def timeBetween = [$gte: begin, $lt: end]

        def qWan = new BasicDBObject(type:'football_shoot','session.got':'COIN100000',timestamp:timeBetween)
        def room_cost = mongo.getDB('xylog').getCollection('room_cost')
        def coinWanList = room_cost.
                find(qWan,new BasicDBObject(_id:1))
                .toArray()
        def football_coin =  100000L * (coinWanList.size())
        println "football_coin:---->:${football_coin}"
        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(football_coin:football_coin)))

    }

    //16.活动中奖--获得币  活动类型："10month","CardYouxi","Christmas","Fortune","SF","chargeRank","dianliang","laodong", "new_year","qq_wx_wb_share","send_hongbao" ,"worldcup"
    static staticActives()
    {
        def timeBetween = [$gte: begin, $lt: end]
        def q = new BasicDBObject(timestamp:timeBetween,active_name:[$in: ['laodong','worldcup']])
        def lottery_log = mongo.getDB('xylog').getCollection('lottery_logs')
        def coinList = lottery_log.
                find(q,new BasicDBObject(award_coin:1))
                .toArray()

        def award_coin = coinList.sum {it.award_coin?:0} as Long
        println "award_coin:---->:${award_coin}"
        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(award_coin:award_coin)))
    }

    //代理充值加币
    static staticsBrokerCoin()
    {
        def timeBetween = [$gte: begin, $lt: end]
        def q = new BasicDBObject(timestamp:timeBetween)
        def broker_ops = mongo.getDB('xylog').getCollection('broker_ops')
        def coinList = broker_ops.
                find(q,new BasicDBObject(coin:1))
                .toArray()

        def broker_coin = coinList.sum {it.coin?:0} as Long
        if(null == broker_coin)
            broker_coin = 0L
        println "broker_coin:---->:${broker_coin}"
        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(broker_coin:broker_coin)))
    }

    //===============================add星币end=============================//

    //===============================星豆begin=============================//
    //00.系统送星豆给主播
    static staticsSystemBean()
    {
        def timeBetween = [$gte: begin, $lt: end]
        def q = new BasicDBObject(timestamp:timeBetween)
        def bean_ops = mongo.getDB('xy_admin').getCollection('bean_ops')
        def beanList = bean_ops.
                find(q,new BasicDBObject(bean:1))
                .toArray()

        def system_bean = beanList.sum {it.bean?:0} as Long
        if(null == system_bean)
            system_bean = 0L
        println "system_bean:---->:${system_bean}"

        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(system_bean:system_bean)))
    }

    //01.奇迹送星豆给主播
    static staticsSpecialBean()
    {
        def timeBetween = [$gte: begin, $lt: end]
        def q = new BasicDBObject('star1.bonus_time':timeBetween)
        def special_gifts = mongo.getDB('xylog').getCollection('special_gifts')
        def beanList = special_gifts.
                find(q,new BasicDBObject('star1.earned':1))
                .toArray()

        Long special_bean = 0L
        for(DBObject beanObj : beanList)
        {
            Long bean = 0.0d
            def star1 = beanObj.get("star1")
            if(null != star1)
                bean = ((Map)star1).get("earned") as Long
            special_bean = special_bean +  bean
        }
        println "special_bean:---->:${special_bean}"
        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(special_bean:special_bean)))
    }

    //02.房间普通用户送礼给用户或主播获星豆
    static staticsRoomBean()
    {
        def timeBetween = [$gte: begin, $lt: end]
        def rq = new BasicDBObject(timestamp: timeBetween)
        def beanList =  mongo.getDB('xylog').getCollection("room_cost").find(rq,new BasicDBObject('session.data.earned':1)).toArray()
        def room_bean = 0L
        for(DBObject beanObj : beanList)
        {
            Long bean = 0L
            def session = beanObj.get("session")
            if(session)
            {
                def  data = (Map)(((Map)session).get("data"))
                bean =  data?.get("earned") as Long
            }
            if(bean)
                room_bean = room_bean +  bean
        }
        println "room_bean:---->:${room_bean}"
        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(room_bean:room_bean)))
    }

    //03.系统送出礼物到用户包包，折算成星豆的价值
    static staticAddBagBean()
    {
        def timeBetween = [$gte: begin, $lt: end]
        def q = new BasicDBObject(timestamp:timeBetween)
        def obtain_log = mongo.getDB('xylog').getCollection('obtain_gifts')
        def beanList = obtain_log.find(q,new BasicDBObject("gifts_bean":1)).toArray()
        def obtain_gifts_bean = beanList.sum {it.gifts_bean?:0} as Long
        if(null == obtain_gifts_bean)
            obtain_gifts_bean = 0L

        println "obtain_gifts_bean:---->:${obtain_gifts_bean}"
        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(obtain_gifts_bean:obtain_gifts_bean)))
    }

    //04.主播提现
    static staticsCashBean()
    {
        def timeBetween = [$gte: begin, $lt: end]
        def q = new BasicDBObject(timestamp:timeBetween,exchange:[$gt: 0],status:[$in:[0,1]])
        def withdrawl_log = mongo.getDB('xy_admin').getCollection('withdrawl_log')
        def beanList = withdrawl_log.
                find(q,new BasicDBObject(exchange:1))
                .toArray()

        def cash_bean = beanList.sum {it.exchange?:0} as Long
        if(null == cash_bean)
            cash_bean = 0L
        println "cash_bean:---->:${cash_bean}"

        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(cash_bean:(cash_bean*100L))))
    }

    //05.包包中的礼物折算成星豆的价值  //性能问题
    static  staticsBagBean()
    {
        def bagQ = new BasicDBObject(bag:[$exists: true])
        def users = mongo.getDB('xy').getCollection('users')
        def beanList = users.
                find(bagQ,new BasicDBObject(bag:1))
                .toArray()

        Long bag_bean = 0L
        def giftDB = mongo.getDB('xy_admin').getCollection('gifts')
        def categoryDB = mongo.getDB('xy_admin').getCollection('gift_categories')
        for(DBObject obj:beanList)
        {
            def bag = (Map)obj?.get("bag")
            for(Object key :bag.keySet())
            {
                Integer gift_id = Integer.parseInt((String)key)
                Integer gift_num = bag?.get(key) as Integer
                if(gift_num>0)
                {
                    def gift = giftDB.findOne(new BasicDBObject('_id',gift_id),new BasicDBObject('category_id':1,"coin_price":1))
                    def category_id =  gift?.get("category_id")
                    def price = gift?.get("coin_price") as Integer
                    if(category_id)
                    {
                        def giftCategory =  categoryDB.findOne(new BasicDBObject('_id',category_id as Integer),new BasicDBObject('ratio',1))
                        def ratio =  giftCategory?.get("ratio") as Double
                        if(ratio)
                        {
                            Double beanValue = price * gift_num * ratio
                            bag_bean = bag_bean +  beanValue.toLong()
                        }
                    }
                }
            }
        }
        println "bag_bean:---->:${bag_bean}"
        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(bag_bean:bag_bean)))
    }

    //===============================星豆end=============================//

    static staticsQd()
    {
        def  coll = mongo.getDB('xy_admin').getCollection('stat_channels_tmp')

        def finance_log = mongo.getDB('xy_admin').getCollection('finance_log')

        def timeBetween = [$gte: begin, $lt: end]

        mongo.getDB('xy_admin').getCollection('channels').find(new BasicDBObject(),new BasicDBObject("name",1)
        ).toArray().each {BasicDBObject channnel ->
            def cId = channnel.removeField("_id")
            def st = new BasicDBObject(_id:"20140510_${cId}" as String,qd:cId,timestamp:System.currentTimeMillis())
            def iter = finance_log.aggregate(
                    new BasicDBObject('$match', [via:[$ne:'Admin'],qd:cId,timestamp:timeBetween]),
                    new BasicDBObject('$project', [cny:'$cny',coin:'$coin',user_id:'$user_id']),
                    new BasicDBObject('$group', [_id: null,cny: [$sum: '$cny'],coin:[$sum: '$coin']])
            ).results().iterator()
            if(iter.hasNext()){
                def obj = iter.next()
                obj.removeField('_id')
                st.putAll(obj)
                if(!channnel.isEmpty()){
                    st.putAll(channnel)
                }
            }
            coll.save(st)
        }

    }

    static void main(String[] args)
    {
        long l = System.currentTimeMillis()
        long begin = l

        init()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitFinanceDetailTmp.class.getSimpleName()},init cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)
        //===============================减星币 begin=============================//
        //01.日消耗
        l = System.currentTimeMillis()
        staticsRoomSpendCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitFinanceDetailTmp.class.getSimpleName()},staticsRoomSpendCoin cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //02.私信消耗
        l = System.currentTimeMillis()
        staticsMailSpendCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitFinanceDetailTmp.class.getSimpleName()},staticsMailSpendCoin cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //03.在途星币
        l = System.currentTimeMillis()
        staticsTransitCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitFinanceDetailTmp.class.getSimpleName()},staticsTransitCoin cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //04.点歌扣星币
        l = System.currentTimeMillis()
        staticsSongSpendCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitFinanceDetailTmp.class.getSimpleName()},staticsSongSpendCoin cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //05.求爱签扣星币
        l = System.currentTimeMillis()
        staticsLabelSpendCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitFinanceDetailTmp.class.getSimpleName()},staticsLabelSpendCoin cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //06.手工减币
        l = System.currentTimeMillis()
        staticsCutCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitFinanceDetailTmp.class.getSimpleName()},staticsCutCoin cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //07.举报扣币
        l = System.currentTimeMillis()
        staticsAccuseSubtractCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitFinanceDetailTmp.class.getSimpleName()},staticsAccuseSubtractCoin cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //08.活动扣币
        l = System.currentTimeMillis()
        staticsActiveSubtractCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitFinanceDetailTmp.class.getSimpleName()},staticsActiveSubtractCoin cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //===============================减星币 end================================//

        //===============================add星币  begin=============================//
        //01.充值-获得币
        l = System.currentTimeMillis()
        staticsChargeTotal()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitFinanceDetailTmp.class.getSimpleName()},staticsChargeTotal cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //02.星豆换币--获得币
        l = System.currentTimeMillis()
        staticsExchange()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitFinanceDetailTmp.class.getSimpleName()},staticsExchange cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //03.手工加币--获得币
        l = System.currentTimeMillis()
        staticsAdminTotal()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitFinanceDetailTmp.class.getSimpleName()},staticsAdminTotal cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //04.送幸运礼物--获得币
        l = System.currentTimeMillis()
        staticsLuck()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitFinanceDetailTmp.class.getSimpleName()},staticsLunck cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //05.砸金蛋加币
        l = System.currentTimeMillis()
        staticEggCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitFinanceDetailTmp.class.getSimpleName()},staticsTotal cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //06.K歌晋级
        l = System.currentTimeMillis()
        staticsKSongCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitFinanceDetailTmp.class.getSimpleName()},staticsKSongCoin cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //07.签到加币
        l = System.currentTimeMillis()
        staticsLoginCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitFinanceDetailTmp.class.getSimpleName()},staticsLoginCoin cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //08.任务加币
        l = System.currentTimeMillis()
        staticsMissionCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitFinanceDetailTmp.class.getSimpleName()},staticsMissionCoin cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //09.合作加币
        l = System.currentTimeMillis()
        staticsUnionCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitFinanceDetailTmp.class.getSimpleName()},staticsUnionCoin cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //10.财神加币
        l = System.currentTimeMillis()
        staticsFortuneCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitFinanceDetailTmp.class.getSimpleName()},staticsFortuneCoin cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //11.宝藏送币
        l = System.currentTimeMillis()
        staticsTreasureCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitFinanceDetailTmp.class.getSimpleName()},staticsTreasureCoin cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //12.举报奖币
        l = System.currentTimeMillis()
        staticsAccuseAddCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitFinanceDetailTmp.class.getSimpleName()},staticsAccuseAddCoin cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //13.点歌退星币
        l = System.currentTimeMillis()
        staticsSongRefundCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitFinanceDetailTmp.class.getSimpleName()},staticsSongRefundCoin cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)


        //14.求爱签退星币
        l = System.currentTimeMillis()
        staticsLabelRefundCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitFinanceDetailTmp.class.getSimpleName()},staticsLabelRefundCoin cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)


        //15.踢足球奖星币
        l = System.currentTimeMillis()
        staticFootballCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitFinanceDetailTmp.class.getSimpleName()},staticFootballCoin cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //16.活动中奖--获得币
        l = System.currentTimeMillis()
        staticActives()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitFinanceDetailTmp.class.getSimpleName()},staticActives cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)
        //===============================add星币 end=============================//

        //===============================星豆 begin=============================//
        //01.奇迹-获星豆
        l = System.currentTimeMillis()
        staticsSpecialBean()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitFinanceDetailTmp.class.getSimpleName()},staticsSpecialBean cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //02.房间消费获豆
        l = System.currentTimeMillis()
        staticsRoomBean()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitFinanceDetailTmp.class.getSimpleName()},staticsRoomBean cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //03.系统送出礼物到用户包包，折算成星豆的价值
        l = System.currentTimeMillis()
        staticAddBagBean()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitFinanceDetailTmp.class.getSimpleName()},staticAddBagBean cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //04.主播提现
        l = System.currentTimeMillis()
        staticsCashBean()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitFinanceDetailTmp.class.getSimpleName()},staticsCashBean cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //05.包包中的礼物
        l = System.currentTimeMillis()
        staticsBagBean()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitFinanceDetailTmp.class.getSimpleName()},staticsBagBean cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)
        //===============================星豆 end=============================//

        l = System.currentTimeMillis()
        staticsQd()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitFinanceDetailTmp.class.getSimpleName()},staticsQd cost  ${System.currentTimeMillis() -l} ms"

        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitFinanceDetailTmp.class.getSimpleName()},------------>:finish cost  ${System.currentTimeMillis() -begin} ms"
    }


}