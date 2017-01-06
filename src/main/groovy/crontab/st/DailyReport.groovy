#!/usr/bin/env groovy
package crontab.tmp

import com.mongodb.BasicDBObject
import com.mongodb.DBObject
@Grapes([
@Grab('org.mongodb:mongo-java-driver:2.14.2'),
@Grab('commons-lang:commons-lang:2.6'),
@Grab('redis.clients:jedis:2.1.0'),
]) import com.mongodb.Mongo
import com.mongodb.MongoURI

/**
 *
 *
 * date: 14-5-12 下午2:46
 * @author: haigen.xiong@ttpod.com
 */
class DailyReport {
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

    static mongo  = new Mongo(new MongoURI(getProperties('mongo.uri','mongodb://192.168.31.249:27017/?w=1') as String))
    static DAY_MILLON = 24 * 3600 * 1000L
    static long zeroMill = new Date().clearTime().getTime()
    static Long yesTday = zeroMill - DAY_MILLON
    static String myId =  "finance_" + new Date(yesTday).format("yyyyMMdd")
    static financeTmpDB = mongo.getDB('xy_admin').getCollection('finance_dailyReport')

    static {
        def curr_date = new Date(yesTday - day * DAY_MILLON)//
        myId =  "finance_" + curr_date.format("yyyyMMdd")
        def obj =  new BasicDBObject(_id : myId, timestamp : curr_date.getTime())
        financeTmpDB.save(obj)
    }


    private static Map getTimeBetween(){
        def gteMill = yesTday - day * DAY_MILLON
        return  [$gte: gteMill, $lt: gteMill + DAY_MILLON]
    }

    @Deprecated
    static userRemain()
    {
        def coinQ = new BasicDBObject('finance.coin_count':[$gt:0])
        def coinList = mongo.getDB("xy").getCollection("users").
                find(coinQ,new BasicDBObject('finance.coin_count':1))
                .toArray()
        def coin = coinList.sum {it.finance.coin_count?:0} as Long
        println "totalcoin:---->:${coin}"

        def starQ = new BasicDBObject('finance.bean_count':[$gt:0],priv:2)
        def starList = mongo.getDB("xy").getCollection("users").
                find(starQ,new BasicDBObject('finance.bean_count',1))
                .toArray()
        def star_bean = starList.sum {it.finance.bean_count?:0} as Long
        println "star_bean---->:${star_bean}"

        def starObtainQ = new BasicDBObject('finance.bean_count_total':[$gt:0],priv:2)
        def starObtainList = mongo.getDB("xy").getCollection("users").
                find(starObtainQ,new BasicDBObject('finance.bean_count_total',1))
                .toArray()
        def star_obtain_bean = starObtainList.sum {it.finance.bean_count_total?:0} as Long
        println "star_obtain_bean---->:${star_obtain_bean}"

        def usrQ = new BasicDBObject('finance.bean_count':[$gt:0],priv:[$nin:[2]])
        def userList = mongo.getDB("xy").getCollection("users").
                find(usrQ,new BasicDBObject('finance.bean_count',1))
                .toArray()
        def usr_bean = userList.sum {it.finance.bean_count?:0} as Long
        println "usr_bean---->:${usr_bean}"

        def usrObtainQ = new BasicDBObject('finance.bean_count_total':[$gt:0],priv:[$nin:[2]])
        def usrObtainList = mongo.getDB("xy").getCollection("users").
                find(usrObtainQ,new BasicDBObject('finance.bean_count_total',1))
                .toArray()
        def usr_obtain_bean = usrObtainList.sum {it.finance.bean_count_total?:0} as Long
        println "usr_obtain_bean---->:${usr_obtain_bean}"

        def obj =  new BasicDBObject(
                _id:myId,
                total_coin:coin,//用户剩余柠檬
                star_total_bean:star_bean,//主播剩余维C
                star_obtain_bean:star_obtain_bean,//主播获得总维C
                usr_total_bean:usr_bean,//用户剩余维C
                usr_obtain_bean:usr_obtain_bean,//用户获得总维C
                timestamp:yesTday,
                sj:new Date(System.currentTimeMillis()).format("yyyy-MM-dd HH:mm:ss"))
        financeTmpDB.save(obj)
    }

    static userRemainByAggregate()
    {

        println "userRemainByAggregate coin_count begin : ${new Date().format('yyyy-MM-dd HH:mm:ss')}"
        def coin = remainByAggregate(new BasicDBObject('finance.coin_count':[$gt:0]), 'finance.coin_count')
        println "userRemainByAggregate coin_count end : ${new Date().format('yyyy-MM-dd HH:mm:ss')} totalcoin:---->:${coin}"

        def starQ = new BasicDBObject('finance.bean_count':[$gt:0],priv:2)
        def star_bean = remainByAggregate(starQ, 'finance.bean_count')
        println "userRemainByAggregate star_bean---->:${star_bean}"

        def starObtainQ = new BasicDBObject('finance.bean_count_total':[$gt:0],priv:2)
        def star_obtain_bean = remainByAggregate(starObtainQ, 'finance.bean_count_total')
        println "userRemainByAggregate star_obtain_bean---->:${star_obtain_bean}"

        def usrQ = new BasicDBObject('finance.bean_count':[$gt:0],priv:[$nin:[2]])
        def usr_bean = remainByAggregate(usrQ, 'finance.bean_count')
        println "userRemainByAggregate usr_bean---->:${usr_bean}"

        def usrObtainQ = new BasicDBObject('finance.bean_count_total':[$gt:0],priv:[$nin:[2]])
        def usr_obtain_bean = remainByAggregate(usrObtainQ, 'finance.bean_count_total')
        println "userRemainByAggregate usr_obtain_bean---->:${usr_obtain_bean}"

        def obj =  new BasicDBObject(
                total_coin:coin,//用户剩余柠檬
                star_total_bean:star_bean,//主播剩余维C
                star_obtain_bean:star_obtain_bean,//主播获得总维C
                usr_total_bean:usr_bean,//用户剩余维C
                usr_obtain_bean:usr_obtain_bean,//用户获得总维C
                sj:new Date(System.currentTimeMillis()).format("yyyy-MM-dd HH:mm:ss"))
        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',obj))
    }

    static Long remainByAggregate(BasicDBObject query, String field) {
        Long remain = 0
        def users = mongo.getDB("xy").getCollection("users")
        users.aggregate(
                new BasicDBObject('$match', query),
                new BasicDBObject('$project', [remain: '$'+field]),
                new BasicDBObject('$group', [_id: null, remain: [$sum: '$remain']])
        ).results().each { BasicDBObject obj ->
            remain = obj.get('remain') as Long
        }
        return remain;
    }

    //===============================减星币 begin=============================//
    //01.日消耗
    static staticsRoomSpendCoin()
    {
        def timeBetween = getTimeBetween()
        println "staticsRoomSpendCoin getTimeBetween:" + timeBetween
        def rq = new BasicDBObject(timestamp: timeBetween,type:[$nin: ['label',"song"]])
        def coinList =  mongo.getDB('xylog').getCollection("room_cost").find(rq,new BasicDBObject(cost:1)).toArray()
        def coin_spend_day = coinList.sum {it.cost?:0} as Long
        println "coin_spend_day:---->:${coin_spend_day}"
        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(coin_spend_room:coin_spend_day)))
    }

     //02.私信消耗
    static staticsMailSpendCoin()
    {
        def timeBetween = getTimeBetween()
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
        def timeBetween = getTimeBetween()
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
        def timeBetween = getTimeBetween()
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
        def timeBetween = getTimeBetween()
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
        def timeBetween = getTimeBetween()
        def q = new BasicDBObject(type: 'finance_cut_coin',timestamp:timeBetween)
        def ops_log = mongo.getDB('xy_admin').getCollection('ops')
        def coinList = ops_log.
                find(q,new BasicDBObject(data:1))
                .toArray()
        Long hand_cut_coin = coinList.sum{it.data.coin?:0} as Long
        if(null == hand_cut_coin){
            hand_cut_coin = 0L
        }
        println "hand_cut_coin:---->:${hand_cut_coin}"
        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(hand_cut_coin:hand_cut_coin)))
    }

    //07.举报扣币
    static staticsAccuseSubtractCoin()
    {
        def timeBetween = getTimeBetween()
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
        def timeBetween = getTimeBetween()
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

    //09.水果乐园 德州扑克 兑换游戏币减币
    static staticGameSubtractCoin() {
        gameSubtractCoin('kunbo', 'kunbo_subtract_coin')
        gameSubtractCoin('texasholdem', 'texasholdem_subtract_coin')
        gameSubtractCoin('fishing', 'fishing_subtract_coin')
        gameSubtractCoin('niuniu', 'niuniu_subtract_coin')
    }

    static Long gameSubtractCoin(String via, String field) {
        def timeBetween = getTimeBetween()
        def trade_log = mongo.getDB('xylog').getCollection('trade_logs')
        def q = new BasicDBObject(time: timeBetween, via: via, 'resp.result': '0', 'resp.coin': [$lt: 0])
        def coinList = trade_log.find(q, new BasicDBObject(resp: 1)).toArray()
        def game_coin = coinList.sum { Math.abs(it.resp?.coin ?: 0) } as Long
        if (null == game_coin) {
            game_coin = 0L
        }
        println "${field}  SubtractCoin :---->:${game_coin}".toString()
        financeTmpDB.update(new BasicDBObject(_id: myId), new BasicDBObject('$set', new BasicDBObject(field, game_coin)))
        return game_coin;
    }


    //===============================减星币 end=============================//

    //===============================add星币 begin=============================//

    //01.充值-获得币(包括返币)
    static staticsChargeTotal()
    {
        def timeBetween = getTimeBetween()
        def q = new BasicDBObject(via:[$ne:'Admin'],timestamp:timeBetween)
        def finance_log = mongo.getDB('xy_admin').getCollection('finance_log')
        def coinList = finance_log.
                find(q,new BasicDBObject(coin:1,cny:1,returnCoin:1))
                .toArray()
        def charge_coin = coinList.sum {(it.coin?:0)+(it.returnCoin?:0)} as Long
        if(null == charge_coin)
            charge_coin = 0L
        println "charge_coin:---->:${charge_coin}"
        double charge_cny = 0.0d
        if(coinList.size()>0)
            charge_cny = coinList.sum {it.cny?:0.0d} as Double
        println "charge_cny:---->:${charge_cny}"
        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(charge_coin:charge_coin,charge_cny:charge_cny)))
    }

    //02.星豆换币--获得币
    static staticsExchange()
    {
        def timeBetween = getTimeBetween()
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
        def timeBetween = getTimeBetween()
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
        def timeBetween = getTimeBetween()
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
        def timeBetween = getTimeBetween()

        def qWan = new BasicDBObject(type:'open_egg','session.got':'COIN10000',timestamp:timeBetween)
        def room_cost = mongo.getDB('xylog').getCollection('room_cost')
        def coinWanList = room_cost.
                find(qWan,new BasicDBObject(_id:1))
                .toArray()
        def egg_coin =  10000L * (coinWanList.size())
        println "egg_coin:---->:${egg_coin}"
        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(egg_coin:egg_coin)))

    }

    //05.砸金蛋(400次必中)加币
    static staticEggBingoCoin()
    {
        def timeBetween = getTimeBetween()
        def egg_bingo_coin = 0l

        def room_cost = mongo.getDB('xylog').getCollection('room_cost')
        def query = new BasicDBObject(type:'open_bingo_egg',timestamp:timeBetween)
        room_cost.aggregate(
                new BasicDBObject('$match', query),
                new BasicDBObject('$project', [got: '$session.got_coin']),
                new BasicDBObject('$group', [_id: null, got: [$sum: '$got']])
        ).results().each { BasicDBObject obj ->
            egg_bingo_coin = obj['got'] as Long
        }
        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(egg_bingo_coin:egg_bingo_coin)))

    }

    //翻牌
    static cardStatic() {
        long card_coin = 0l
        def timeBetween = getTimeBetween()
        def room_cost = mongo.getDB('xylog').getCollection('room_cost')
        def query = new BasicDBObject(type:'open_card',timestamp:timeBetween)
        room_cost.aggregate(
                new BasicDBObject('$match', query),
                new BasicDBObject('$project', [got: '$session.got_coin']),
                new BasicDBObject('$group', [_id: null, got: [$sum: '$got']])
        ).results().each { BasicDBObject obj ->
            card_coin = obj['got'] as Long
        }
        println "card_coin:---->:${card_coin}"
        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set', new BasicDBObject(card_coin : card_coin)))
    }

    //06.K歌晋级
    static staticsKSongCoin(){
        def timeBetween = getTimeBetween()
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
        def timeBetween = getTimeBetween()
/*      TODO 老版本签到 已暂停
        def awards_login = mongo.getDB('xylog').getCollection('awards_login')
        def qAwardLogin1 = new BasicDBObject(type:1,timestamp:timeBetween)
        def awardLoginList1 = awards_login.
                find(qAwardLogin1,new BasicDBObject(_id:1))
                .toArray()
        def loginCoin1 =  8L * (awardLoginList1.size())
        Thread.sleep(1000L)

        def qAwardLogin2 = new BasicDBObject(type:2,timestamp:timeBetween)
        def awardLoginList2 = awards_login.
                find(qAwardLogin2,new BasicDBObject(_id:1))
                .toArray()
        def loginCoin2 =  20L * (awardLoginList2.size())
        Thread.sleep(1000L)

        def qAwardLogin3 = new BasicDBObject(type:3,timestamp:timeBetween)
        def awardLoginList3 = awards_login.
                find(qAwardLogin3,new BasicDBObject(_id:1))
                .toArray()
        def loginCoin3 =  50L * (awardLoginList3.size())
        Thread.sleep(1000L)

        def qAwardLogin4 = new BasicDBObject(type:4,timestamp:timeBetween)
        def awardLoginList4 = awards_login.
                find(qAwardLogin4,new BasicDBObject(_id:1))
                .toArray()
        def loginCoin4 =  100L * (awardLoginList4.size())
        Thread.sleep(1000L)


        def qSign = new BasicDBObject(award:true,timestamp:timeBetween)
        def day_login = mongo.getDB('xylog').getCollection('day_login')
        def signList = day_login.
                find(qSign,new BasicDBObject(_id:1))
                .toArray()

        def signCoin =  4L * (signList.size())
        Thread.sleep(1000L)

        Long login_coin =  signCoin + loginCoin1 + loginCoin2 + loginCoin3 + loginCoin4
        println "login_coin:---->:${login_coin}"
        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(login_coin:login_coin)))
        */
        def query = new BasicDBObject(timestamp:getTimeBetween(), "award_name": "coin_4", "active_name":'sign_chest')
        def lottery_log = mongo.getDB('xylog').getCollection('lottery_logs')
        Long login_coin = lottery_log.count(query)  * 4l
        println "login_coin:---->:${login_coin}"
        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(login_coin:login_coin)))
    }

    //08.任务加币(首充，首次关注，首次修改昵称等)
    static staticsMissionCoin()
    {
        def timeBetween = getTimeBetween()
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
        def timeBetween = getTimeBetween()
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
        def timeBetween = getTimeBetween()
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
        def timeBetween = getTimeBetween()
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

    //红包送币
    static staticsRedPacketCoin()
    {
        def timeBetween = getTimeBetween()
        def q = new BasicDBObject(timestamp:timeBetween,active_name:'red_packet')
        def lottery_log = mongo.getDB('xylog').getCollection('lottery_logs')
        def coinList = lottery_log.
                find(q,new BasicDBObject(award_coin:1))
                .toArray()
        def redPacket_coin = coinList.sum {it.award_coin?:0} as Long
        if(null == redPacket_coin)
            redPacket_coin = 0L

        println "RedPacket_coin:---->:${redPacket_coin}"
        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(redPacket_coin:redPacket_coin)))
    }

    //12.举报奖励币
    static staticsAccuseAddCoin()
    {
        def timeBetween = getTimeBetween()
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
        def rq = new BasicDBObject(status:[$in:[2,4]],last_update: getTimeBetween())
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
        def rqlabe = new BasicDBObject(status:[$in:[2,4]],last_update: getTimeBetween())
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

        def qWan = new BasicDBObject(type:'football_shoot','session.got':'COIN100000',timestamp:getTimeBetween())
        def room_cost = mongo.getDB('xylog').getCollection('room_cost')
        def coinWanList = room_cost.
                find(qWan,new BasicDBObject(_id:1))
                .toArray()
        def football_coin =  100000L * (coinWanList.size())
        println "football_coin:---->:${football_coin}"
        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(football_coin:football_coin)))

    }

    //16.活动中奖--获得币  活动类型："10month","CardYouxi","Christmas","Fortune","SF","chargeRank","dianliang","laodong", "new_year","qq_wx_wb_share","send_hongbao" ,"worldcup"
    static staticActives(){
        Long award_coin = 0
        def query = new BasicDBObject(timestamp:getTimeBetween(),award_coin:[$gt: 0])
        def lottery_log = mongo.getDB('xylog').getCollection('lottery_logs')
        lottery_log.aggregate(
                new BasicDBObject('$match', query),
                new BasicDBObject('$project', [award_coin: '$award_coin']),
                new BasicDBObject('$group', [_id: null, award_coin: [$sum: '$award_coin']])
        ).results().each { BasicDBObject obj ->
            award_coin = obj.get('award_coin') as Long
        }
        println "award_coin:---->:${award_coin}"
        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(award_coin:award_coin)))
    }

    private final static Map<String, Long> APP_FREE_GIFT = ['100':10, '103':100, '102':10]
    //手机直播免费礼物奖励
    static staticAppFreeGift()
    {
        def q = new BasicDBObject(timestamp:getTimeBetween(),active_name:'app_meme_luck_gift')
        def lottery_log = mongo.getDB('xylog').getCollection('lottery_logs')
        Long award_coin = 0;
        APP_FREE_GIFT.each {String giftId, Integer coin ->
            award_coin += lottery_log.count(q.append('award_name', giftId)) * APP_FREE_GIFT[giftId]
        }

        println "staticAppFreeGift award_coin:---->:${award_coin}"
        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(app_free_gift_coin:award_coin)))
    }

    static final List<String> ACTIVE_NAMES = ['chest20160812']
    //各种活动抽奖获取柠檬
    static staticsActivityAwardCoins()
    {
        def timeBetween = getTimeBetween()
        def query = new BasicDBObject(timestamp:timeBetween,active_name:[$in:ACTIVE_NAMES])
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
        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(activity_award_coin:award_coin)))
    }

    //17.币兑换加币  水果乐园游戏 德州扑克
    static staticGameAddCoin() {
        gameEarned('kunbo', 'kunbo_game_coin')
        gameEarned('texasholdem', 'texasholdem_game_coin')
        gameEarned('fishing', 'fishing_game_coin')
        gameEarned('niuniu', 'niuniu_game_coin')
    }


    static Long gameEarned(String via, String field) {
        def trade_log = mongo.getDB('xylog').getCollection('trade_logs')
        def q = new BasicDBObject(time: getTimeBetween(), via: via, 'resp.result': '0', 'resp.coin': [$gt: 0])
        def coinList = trade_log.find(q, new BasicDBObject(resp: 1)).toArray()
        def game_coin = coinList.sum { it.resp?.coin ?: 0 } as Long
        if (null == game_coin) {
            game_coin = 0L
        }
        println "${field}: AddCoin---->:${game_coin}".toString()
        financeTmpDB.update(new BasicDBObject(_id: myId), new BasicDBObject('$set', new BasicDBObject(field, game_coin)))
        return game_coin;
    }

    //18.点乐分享加币 2016/1/28 改为 百度积分墙
    static staticDianle() {
        //def q = new BasicDBObject(time: timeBetween, via: 'dianle')
        def q = new BasicDBObject(time: getTimeBetween(), via: 'baidu_jf')
        def trade_log = mongo.getDB('xylog').getCollection('trade_logs')
        def coinList = trade_log.find(q, new BasicDBObject(resp: 1)).toArray()
        def share_coin = coinList.sum { it.resp?.coin ?: 0 } as Long
        if (null == share_coin) {
            share_coin = 0L
        }
        println "dianle_share_coin:---->:${share_coin}".toString()
        financeTmpDB.update(new BasicDBObject(_id: myId), new BasicDBObject('$set', new BasicDBObject(dianle_share_coin: share_coin)))
    }

    //===============================add星币end=============================//

     //===============================星豆 begin=============================//
    //TODO 01.包包中的礼物折算成星豆的价值  //性能问题暂停
    static  staticsBagBean(){
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

    //===============================星豆 add  begin=============================//
    //02.奇迹送星豆给主播
    static staticsSpecialBean()
    {
        def q = new BasicDBObject('star1.bonus_time':getTimeBetween())
        def special_gifts = mongo.getDB('xylog').getCollection('special_gifts')
        def beanList = special_gifts.
                find(q,new BasicDBObject('star1':1))
                .toArray()
        def special_bean = beanList.sum {it.star1.earned?:0} as Long
        if(null == special_bean)
            special_bean = 0L
        println "special_bean:---->:${special_bean}"
        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(special_bean:special_bean)))
    }

    //03.房间普通用户送礼给用户或主播获星豆
    static staticsRoomBean()
    {
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

        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(room_bean:room_bean)))
    }

    //TODO 不符合财务要求 需要废弃 04.系统送出礼物到用户包包，折算成星豆的价值
    static staticAddBagBean()
    {
        def q = new BasicDBObject(timestamp:getTimeBetween())
        def obtain_log = mongo.getDB('xylog').getCollection('obtain_gifts')
        def beanList = obtain_log.find(q,new BasicDBObject("gifts_bean":1)).toArray()
        def obtain_gifts_bean = beanList.sum {it.gifts_bean?:0} as Long
        if(null == obtain_gifts_bean)
            obtain_gifts_bean = 0L

        println "obtain_gifts_bean:---->:${obtain_gifts_bean}"
        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(obtain_gifts_bean:obtain_gifts_bean)))
    }
    //===============================星豆 add  end=============================//


    //===============================星豆 减少  begin=============================//
    //01.主播提现
    static staticsCashBean()
    {
        def q = new BasicDBObject(timestamp:getTimeBetween(),exchange:[$gt: 0],status:[$in:[0,1]])
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
    //02.星豆兑换成行星币

    //03.用户从包包中送出礼物给主播 包包的豆减少
    static staticsBagSendBean()
    {

        def rq = new BasicDBObject(timestamp: getTimeBetween(), type:"send_gift",cost:0)
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

        financeTmpDB.update(new BasicDBObject(_id:myId),new BasicDBObject('$set',new BasicDBObject(bag_send_bean:bag_send_bean)))
    }
    //===============================星豆 减少  end=============================//

    //===============================星豆end=============================//


    final static Integer day = 0;

    static void main(String[] args)
    {
        long l = System.currentTimeMillis()
        long begin = l

      /*  userRemain()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},userRemain cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)
*/
        l = System.currentTimeMillis()
        userRemainByAggregate()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},userRemainByAggregate cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)
        //===============================减星币 begin=============================//
        //01.日消耗
        l = System.currentTimeMillis()
        staticsRoomSpendCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticsRoomSpendCoin cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //02.私信消耗
        l = System.currentTimeMillis()
        staticsMailSpendCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticsMailSpendCoin cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //03.在途星币
        l = System.currentTimeMillis()
        staticsTransitCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticsTransitCoin cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //04.点歌扣星币
        l = System.currentTimeMillis()
        staticsSongSpendCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticsSongSpendCoin cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //05.求爱签扣星币
        l = System.currentTimeMillis()
        staticsLabelSpendCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticsLabelSpendCoin cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //06.手工减币
        l = System.currentTimeMillis()
        staticsCutCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticsCutCoin cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //07.举报扣币
        l = System.currentTimeMillis()
        staticsAccuseSubtractCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticsAccuseSubtractCoin cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //08.活动扣币
        l = System.currentTimeMillis()
        staticsActiveSubtractCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticsActiveSubtractCoin cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //09.水果乐园兑换游戏币减币
        l = System.currentTimeMillis()
        staticGameSubtractCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticGameSubtractCoin cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //===============================减星币 end================================//

        //===============================add星币  begin=============================//
        //01.充值-获得币
        l = System.currentTimeMillis()
        staticsChargeTotal()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticsChargeTotal cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //02.星豆换币--获得币
        l = System.currentTimeMillis()
        staticsExchange()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticsExchange cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //03.手工加币--获得币
        l = System.currentTimeMillis()
        staticsAdminTotal()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticsAdminTotal cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //04.送幸运礼物--获得币
        l = System.currentTimeMillis()
        staticsLuck()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticsLunck cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //05.砸金蛋加币
        l = System.currentTimeMillis()
        staticEggCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticEggCoin cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //05.砸金蛋(必中)加币
        l = System.currentTimeMillis()
        staticEggBingoCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticEggBingoCoin cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //06.K歌晋级
        l = System.currentTimeMillis()
        staticsKSongCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticsKSongCoin cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //07.签到加币
        l = System.currentTimeMillis()
        staticsLoginCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticsLoginCoin cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)


        //08.任务加币
        l = System.currentTimeMillis()
        staticsMissionCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticsMissionCoin cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //09.合作加币
        l = System.currentTimeMillis()
        staticsUnionCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticsUnionCoin cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //10.财神加币
        l = System.currentTimeMillis()
        staticsFortuneCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticsFortuneCoin cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //11.宝藏送币
        l = System.currentTimeMillis()
        staticsTreasureCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticsTreasureCoin cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //红包送币
        l = System.currentTimeMillis()
        staticsRedPacketCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticsRedPacketCoin cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //12.举报奖币
        l = System.currentTimeMillis()
        staticsAccuseAddCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticsAccuseAddCoin cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //13.点歌退星币
        l = System.currentTimeMillis()
        staticsSongRefundCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticsSongRefundCoin cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)


        //14.求爱签退星币
        l = System.currentTimeMillis()
        staticsLabelRefundCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticsLabelRefundCoin cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)


        //15.踢足球奖星币
        l = System.currentTimeMillis()
        staticFootballCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticFootballCoin cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //16.活动中奖--获得币
        l = System.currentTimeMillis()
        staticActives()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticActives cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //手机直播礼物
        l = System.currentTimeMillis()
        staticAppFreeGift()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticAppFreeGift cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //17.水果乐园游戏币兑换加币
        l = System.currentTimeMillis()
        staticGameAddCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticGameAddCoin cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //18.点乐/百度积分墙 分享加币
        l = System.currentTimeMillis()
        staticDianle()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticDianle cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //翻牌
        l = System.currentTimeMillis()
        cardStatic()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},cardStatic cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //么么活动抽奖获取柠檬
        l = System.currentTimeMillis()
        staticsActivityAwardCoins()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticsActivityAwardCoins cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)
        //===============================add星币 end=============================//

        //===============================星豆 begin=============================//
        //01.包包中的礼物
        /*
        l = System.currentTimeMillis()
        staticsBagBean()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticsBagBean cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)
        */

        //===============================星豆 add begin=============================//
        //02.奇迹-获星豆
        l = System.currentTimeMillis()
        staticsSpecialBean()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticsSpecialBean cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //03.房间消费获豆
        l = System.currentTimeMillis()
        staticsRoomBean()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticsRoomBean cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //04.系统送出礼物到用户包包，折算成星豆的价值
        l = System.currentTimeMillis()
        staticAddBagBean()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticAddBagBean cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //===============================星豆 add end=============================//

        //===============================星豆 减少 begin=============================//
        //01.主播提现
        l = System.currentTimeMillis()
        staticsCashBean()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticsCashBean cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)


        //02.星豆兑换星币

        //03.用户从包包中送出礼物给主播 包包的豆减少
        l = System.currentTimeMillis()
        staticsBagSendBean()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DailyReport.class.getSimpleName()},staticsBagSendBean cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)
        //===============================星豆 减少 end=============================//

        //===============================星豆 end=============================//

        jobFinish(begin);
    }

    /**
     * 标记任务完成  用于运维监控
     * @return
     */
    private static jobFinish(Long begin){
        def timerName = 'DailyReport'
        Long totalCost = System.currentTimeMillis() - begin
        saveTimerLogs(timerName,totalCost)
        println "${new Date().format('yyyy-MM-dd')}:${DailyReport.class.getSimpleName()}:finish  cost time:  ${System.currentTimeMillis() - begin} ms"
    }

    //落地定时执行的日志
    private static saveTimerLogs(String timerName,Long totalCost)
    {
        def timerLogsDB =  mongo.getDB("xyrank").getCollection("timer_logs")
        def tmp = System.currentTimeMillis()
        def id = timerName + "_"  + new Date().format("yyyyMMdd")
        def update = new BasicDBObject(timer_name:timerName,cost_total:totalCost,cat:'day',unit:'ms',timestamp:tmp)
        timerLogsDB.findAndModify(new BasicDBObject('_id',id), null, null, false,new BasicDBObject('$set',update),true, true)
    }

}


