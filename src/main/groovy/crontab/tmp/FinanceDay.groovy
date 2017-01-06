#!/usr/bin/env groovy
package crontab.st

import com.mongodb.BasicDBObject
import com.mongodb.DB
import com.mongodb.DBCollection
import com.mongodb.DBCursor
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
])
import com.mongodb.Mongo
import com.mongodb.MongoURI
import org.apache.commons.lang.StringUtils
import org.apache.commons.lang.math.NumberUtils

import java.text.SimpleDateFormat
import com.mongodb.DBObject

/**
 * TODO 核对每日进/出柠檬
 */
class FinanceDay {

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
    //static mongo = new Mongo(new MongoURI('mongodb://192.168.1.46:20000/?w=1'))

    static historyMongo = new Mongo(new MongoURI(getProperties('mongo_history.uri', 'mongodb://192.168.31.246:27017/?w=1') as String))

    static DB historyDB = historyMongo.getDB('xylog_history')
    static DB xylog = mongo.getDB('xylog')
    static DBCollection coll = mongo.getDB('xy_admin').getCollection('stat_month')
    static DBCollection finance_dailyReport = mongo.getDB('xy_admin').getCollection('finance_dailyReport')
    static DBCollection finance_dailyEarned = mongo.getDB('xy_admin').getCollection('finance_dailyEarned')
    static DBCollection finance_daily_log = mongo.getDB('xy_admin').getCollection('finance_daily_log')
    static DBCollection finance_monthReport = mongo.getDB('xy_admin').getCollection('finance_monthReport')
    static DBCollection family_award_log = mongo.getDB('xy_family').getCollection('award_log')
    static DBCollection withdrawl_log = mongo.getDB('xy_admin').getCollection('withdrawl_log')
    static DBCollection finance_log  = mongo.getDB('xy_admin').getCollection('finance_log')
    static DBCollection channel_pay = mongo.getDB('xy_admin').getCollection('channel_pay')
    static DBCollection stat_daily = mongo.getDB('xy_admin').getCollection('stat_daily')
    static DBCollection applys = mongo.getDB('xy_admin').getCollection('applys')
    static DBCollection stat_lives = mongo.getDB('xy_admin').getCollection('stat_lives')
    static DBCollection exchange_log = mongo.getDB('xylog').getCollection('exchange_log')
    static MIN_MILLS = 60 * 1000L
    static DAY_MILLON = 24 * 3600 * 1000L

    private static Set typeSet = new HashSet(30);
    static cost(Long begin){
        Long end = begin+DAY_MILLON
        def timeBetween = [$gte: begin, $lt: end]
        String year = new Date(begin).format("yyyy")
        def room_cost = historyDB.getCollection("room_cost_${year}".toString());
        Long cost = 0;
        Long count = 0;
        def cursor = room_cost.find($$(timestamp: timeBetween), $$(type:1, cost:1)).batchSize(5000)
        while (cursor.hasNext()) {
            def obj = cursor.next()
            typeSet.add(obj['type'])
            cost += obj['cost'] as Long
            count++;
        }
        println "${new Date(begin).format("yyyy-MM-dd")} cost : ${cost} count ${count}  type : ${typeSet}"
        //println "${new Date().format('yyyy-MM-dd HH:mm:ss')} from ${new Date(begin).format("yyyy-MM-dd HH:mm:ss")} to ${new Date(end).format("yyyy-MM-dd HH:mm:ss")} cost : ${cost}".toString()
    }

    static costByMonth(Integer month){
        Calendar cal = getCalendar()
        cal.add(Calendar.MONTH, -month)
        long firstDayOfCurrentMonth = cal.getTimeInMillis()  //当月第一天
        cal.add(Calendar.MONTH, -1)
        long firstDayOfLastMonth = cal.getTimeInMillis()  //上月第一天
        String year = new Date(firstDayOfLastMonth).format("yyyy")

        def timeBetween = [$gte: firstDayOfLastMonth, $lt: firstDayOfCurrentMonth]
        def room_cost = historyDB.getCollection("room_cost_${year}".toString());
        Long cost = 0;

        def res = room_cost.aggregate([new BasicDBObject('$match', [timestamp:timeBetween]),
                                       new BasicDBObject('$project', [cost:'$cost']),
                                       new BasicDBObject('$group', [_id:null, cost_total: [$sum: '$cost']])
                                        ]).results().iterator()
        while (res.hasNext()) {
            def cost_log = res.next()
            println cost_log;
            cost = cost_log['cost_total'] as Long
        }
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')} from ${new Date(firstDayOfLastMonth).format("yyyy-MM-dd HH:mm:ss")} to ${new Date(firstDayOfCurrentMonth).format("yyyy-MM-dd HH:mm:ss")} cost : ${cost}"

    }

    /**
     * 获得柠檬
     */
    static earned(Long begin){
        Long end = begin+DAY_MILLON
        def timeBetween = [$gte: begin, $lt: end]
        Map result = new HashMap();
        println "${new Date(begin).format("yyyy-MM-dd HH:mm:ss")} begin ..."
        //充值相关
        result['charge_coin'] = getCharege(timeBetween)
        //签到
        result['old_sign_coin'] = oldSignCoin(timeBetween)

        result['sign_coin'] = signCoin(timeBetween)
        //新手任务
        result['mission_coin'] = staticsMissionCoin(timeBetween)
        //幸运礼物
        result['luck_coin'] = staticsLuck(timeBetween)
        //VC兑换柠檬
        result['exchange_coin'] = staticsExchange(timeBetween)

        //财神
        result['fortune_coin'] = staticsFortuneCoin(timeBetween)
        //宝藏
        result['treasure_coin'] = staticsTreasureCoin(timeBetween)
        //红包
        result['redpacket_coin'] = staticsRedPacketCoin(timeBetween)
        //水果乐园
        result['furit_coin'] = friutCoin(timeBetween)
        //德州兑入
        result['poker_coin'] = pokerCoin(timeBetween)
        //捕鱼兑入
        result['fish_coin'] = fishCoin(timeBetween)

        result['niuniu_coin'] = niuniuCoin(timeBetween)
        //游戏获得 砸蛋 翻牌 点球
        result['card_coin'] = cardStatic(timeBetween)
        result['egg_coin'] = staticEggCoin(timeBetween)
        result['eggBingo_coin'] = staticEggBingoCoin(timeBetween)
        result['football_coin'] = staticFootballCoin(timeBetween)

        //点乐 百度积分墙
        result['dianle_coin'] = staticDianle(timeBetween, 'dianle')
        result['baidu_jf_coin'] = staticDianle(timeBetween, 'baidu_jf')
        //点歌退回柠檬
        result['song_coin'] = staticsSongRefundCoin(timeBetween)

        def data = $$(timestamp:begin);
        data.putAll(result)
        //finance_dailyEarned.save(data);
        finance_dailyEarned.update($$(_id: new Date(begin).format("yyyy-MM-dd")), $$($set:data), true, false)

        ////TODO 不是直接获取柠檬   手机直播免费礼物  第一名家族礼物发放价值
        println "${new Date(begin).format("yyyy-MM-dd HH:mm:ss")} earned result : ${result}"
    }

    static Long oldSignCoin(Map timeBetween)
    {
        def awards_login = xylog.getCollection('awards_login')
        Long total = 0l;
        [1:8L,2:20L,3:50l,4:100l].each {Integer type, Long coin ->
            def qAwardLogin1 = new BasicDBObject(type:type,timestamp:timeBetween)
            def awardLoginList1 = awards_login.count(qAwardLogin1)
            total +=  coin * awardLoginList1
        }
        def qAwardLogin1 = new BasicDBObject(type:1,timestamp:timeBetween)
        def awardLoginList1 = awards_login.
                find(qAwardLogin1,new BasicDBObject(_id:1))
                .toArray()
        def loginCoin1 =  8L * (awardLoginList1.size())

        def qAwardLogin2 = new BasicDBObject(type:2,timestamp:timeBetween)
        def awardLoginList2 = awards_login.
                find(qAwardLogin2,new BasicDBObject(_id:1))
                .toArray()
        def loginCoin2 =  20L * (awardLoginList2.size())
        def qAwardLogin3 = new BasicDBObject(type:3,timestamp:timeBetween)
        def awardLoginList3 = awards_login.
                find(qAwardLogin3,new BasicDBObject(_id:1))
                .toArray()
        def loginCoin3 =  50L * (awardLoginList3.size())

        def qAwardLogin4 = new BasicDBObject(type:4,timestamp:timeBetween)
        def awardLoginList4 = awards_login.
                find(qAwardLogin4,new BasicDBObject(_id:1))
                .toArray()
        def loginCoin4 =  100L * (awardLoginList4.size())


        def qSign = new BasicDBObject(award:true,timestamp:timeBetween)
        def day_login = historyDB.getCollection('day_login_history')
        def signList = day_login.find(qSign,new BasicDBObject(_id:1)).toArray()
        def signCoin =  4L * (signList.size())

        total += signCoin

        Long old_login_coin =  signCoin + loginCoin1 + loginCoin2 + loginCoin3 + loginCoin4
        //println "total : ${total}, old_login_coin : ${old_login_coin}"
        return old_login_coin
    }

    static Long signCoin(Map timeBetween){
        def query = new BasicDBObject(timestamp:timeBetween, "award_name": "coin_4", "active_name":'sign_chest')
        def lottery_log = historyDB.getCollection('lottery_logs_history')
        Long login_coin = lottery_log.count(query)  * 4l
        return login_coin;
    }

    static Long staticsMissionCoin(Map timeBetween){
        def match = new BasicDBObject(timestamp:timeBetween,mission_id:[$ne:'sign_daily'])
        Long coin = aggregate(xylog.getCollection('mission_logs'),[new BasicDBObject('$match', match),
                                           new BasicDBObject('$project', [coin:'$coin']),
                                           new BasicDBObject('$group', [_id:null, total: [$sum: '$coin']])
        ])
        return coin;
    }
    static Long staticsLuck(Map timeBetween){
        def match = new BasicDBObject(timestamp:timeBetween)
        Long coin = aggregate(xylog.getCollection('room_luck'),[new BasicDBObject('$match', match),
                                           new BasicDBObject('$project', [coin:'$got']),
                                           new BasicDBObject('$group', [_id:null, total: [$sum: '$coin']])
        ])
        return coin;
    }

    static Long staticsExchange(Map timeBetween){
        def match = new BasicDBObject(timestamp:timeBetween)
        Long coin = aggregate(xylog.getCollection('exchange_log'),[new BasicDBObject('$match', match),
                                           new BasicDBObject('$project', [coin:'$exchange']),
                                           new BasicDBObject('$group', [_id:null, total: [$sum: '$coin']])
        ])
        return coin;
    }

    static Long getCharege(Map timeBetween){
        Long coin = aggregate(finance_log,[new BasicDBObject('$match', [timestamp:timeBetween]),
                               new BasicDBObject('$project', [coin:'$coin']),
                               new BasicDBObject('$group', [_id:null, total: [$sum: '$coin']])
        ])
        return coin
    }

    static staticsFortuneCoin(Map timeBetween){
        def q = new BasicDBObject(timestamp:timeBetween,active_name:'Fortune')
        def lottery_log = historyDB.getCollection('lottery_logs_history')
        def count = lottery_log.count(q)
        def fortune_coin = 16000L * count
        return fortune_coin
    }

    static staticsTreasureCoin(Map timeBetween){
        def match = new BasicDBObject(timestamp:timeBetween,active_name:'treasure')
        Long coin = aggregate(historyDB.getCollection('lottery_logs_history'),[new BasicDBObject('$match', match),
                                           new BasicDBObject('$project', [coin:'$obtain_coin']),
                                           new BasicDBObject('$group', [_id:null, total: [$sum: '$coin']])
        ])
        return coin
    }


    static staticsRedPacketCoin(Map timeBetween){
        def match = new BasicDBObject(timestamp:timeBetween,active_name:'red_packet')
        Long coin = aggregate(historyDB.getCollection('lottery_logs_history'),[new BasicDBObject('$match', match),
                                                                               new BasicDBObject('$project', [coin:'$award_coin']),
                                                                               new BasicDBObject('$group', [_id:null, total: [$sum: '$coin']])
        ])
        return coin
    }

    static cardStatic(Map timeBetween) {
        def match = new BasicDBObject(type:'open_card',timestamp:timeBetween)
        String year = new Date(timeBetween.get('$gte') as Long).format("yyyy")
        def room_cost = historyDB.getCollection("room_cost_${year}".toString());
        Long coin = aggregate(room_cost,[new BasicDBObject('$match', match),
                               new BasicDBObject('$project', [coin:'$session.got_coin']),
                               new BasicDBObject('$group', [_id:null, total: [$sum: '$coin']])
                              ])
        return coin
    }

    static staticEggCoin(Map timeBetween){
        def qWan = new BasicDBObject(type:'open_egg','session.got':'COIN10000',timestamp:timeBetween)
        String year = new Date(timeBetween.get('$gte') as Long).format("yyyy")
        def room_cost = historyDB.getCollection("room_cost_${year}".toString());
        def coinWanList = room_cost.count(qWan)
        def egg_coin =  10000L * coinWanList
        return egg_coin;
    }

    static staticEggBingoCoin(Map timeBetween){
        def match = new BasicDBObject(type:'open_bingo_egg',timestamp:timeBetween)
        String year = new Date(timeBetween.get('$gte') as Long).format("yyyy")
        def room_cost = historyDB.getCollection("room_cost_${year}".toString());
        Long coin = aggregate(room_cost,[new BasicDBObject('$match', match),
                                         new BasicDBObject('$project', [coin:'$session.got_coin']),
                                         new BasicDBObject('$group', [_id:null, total: [$sum: '$coin']])
        ])
        return coin
    }

    static staticFootballCoin(Map timeBetween){
        def qWan = new BasicDBObject(type:'football_shoot','session.got':'COIN100000',timestamp:timeBetween)
        String year = new Date(timeBetween.get('$gte') as Long).format("yyyy")
        def room_cost = historyDB.getCollection("room_cost_${year}".toString());
        def coinWanList = room_cost.count(qWan)
        def coin =  100000L * coinWanList
        return coin;

    }

    static staticsSongRefundCoin(Map timeBetween){
        def match = new BasicDBObject(status:[$in:[2,4]],last_update: timeBetween)
        Long coin = aggregate(mongo.getDB('xy').getCollection("songs"),[new BasicDBObject('$match', match),
                                         new BasicDBObject('$project', [coin:'$cost']),
                                         new BasicDBObject('$group', [_id:null, total: [$sum: '$coin']])
        ])
        return coin
    }

    static Long friutCoin(Map timeBetween){
        return gameEarned(timeBetween, 'kunbo');
    }

    static Long pokerCoin(Map timeBetween){
        return gameEarned(timeBetween, 'texasholdem');
    }

    static Long fishCoin(Map timeBetween){
        return gameEarned(timeBetween, 'fishing');
    }
    static Long niuniuCoin(Map timeBetween){
        return gameEarned(timeBetween, 'niuniu');
    }

    static Long gameEarned(Map timeBetween, String via) {
        def trade_log = mongo.getDB('xylog').getCollection('trade_logs')
        def q = new BasicDBObject(time: timeBetween, via: via, 'resp.result': '0', 'resp.coin': [$gt: 0])
        def coinList = trade_log.find(q, new BasicDBObject(resp: 1)).toArray()
        def game_coin = coinList.sum { it.resp?.coin ?: 0 } as Long
        if (null == game_coin) {
            game_coin = 0L
        }
        return game_coin;
    }

    static staticDianle(Map timeBetween , String via) {
        def q = new BasicDBObject(time:timeBetween, via: via)
        def trade_log = mongo.getDB('xylog').getCollection('trade_logs')
        def coinList = trade_log.find(q, new BasicDBObject(resp: 1)).toArray()
        def share_coin = coinList.sum { it.resp?.coin ?: 0 } as Long
        if (null == share_coin) {
            share_coin = 0L
        }
        return share_coin
    }

    static Long aggregate(DBCollection col, List<DBObject> pipeline){
        Long total = 0;
        def res = col.aggregate(pipeline).results().iterator()
        while (res.hasNext()) {
            def log = res.next()
            total = log['total'] as Long
        }
        return total
    }

    static lastDayCost(){
        def date = Date.parse("yyyy-MM-dd HH:mm:ss" ,"2014-06-27 00:00:00")
        int month = 0
        Long cost = 0;
        while(month < 100){
            Long begin = getCurrentMonthLastDay(date, month)
            Long end = begin + MIN_MILLS;
            String year = new Date(begin).format("yyyy")
            def timeBetween = [$gte: begin, $lt: end]
            def room_cost = historyDB.getCollection("room_cost_${year}".toString());
            def res = room_cost.aggregate(
                    new BasicDBObject('$match', [timestamp:timeBetween]),
                    new BasicDBObject('$project', [cost:'$cost']),
                    new BasicDBObject('$group', [_id:null,cost: [$sum: '$cost']])).results().iterator();
            while (res.hasNext()) {
                def cost_log = res.next()
                cost = cost_log['cost'] as Long
            }
            println "begin : ${new Date(begin).format("yyyy-MM-dd HH:mm:ss")}  end : ${new Date(end).format("yyyy-MM-dd HH:mm:ss")} cost ${cost}"
            month++
        }

    }

    static final Set<String> cost_keys = ['hand_coin','card_coin','charge_coin','login_coin', 'mission_coin', 'luck_coin', 'exchange_coin', 'fortune_coin', 'treasure_coin','redPacket_coin', 'kunbo_game_coin',
                                        'dianle_share_coin', 'app_free_gift_coin', 'texasholdem_game_coin', 'fishing_game_coin', 'coin_refund_song', 'egg_coin', 'football_coin', 'egg_bingo_coin'] as Set;


    static void earnedByMonth(Date date){
        Calendar cal = getCalendar()
        cal.setTime(date)
        long firstDayOfCurrentMonth = cal.getTimeInMillis()  //当月第一天
        cal.add(Calendar.MONTH, 1)
        long firstDayOfLastMonth = cal.getTimeInMillis()  //下月第一天
        String ym = new Date(firstDayOfLastMonth).format("yyyyMM")
        def timebetween = [timestamp: [$gte: firstDayOfCurrentMonth, $lt: firstDayOfLastMonth]]
        Long total = 0l;
        Map<String, Long> earneds = new HashMap<>();
        def datas = finance_dailyEarned.find($$(timebetween)).toArray()
        datas.each {DBObject data ->
            data.removeField('_id');
            data.removeField('timestamp');
            data.keySet().each {String key ->
                total += data.get(key) as Long
                Long value = earneds.get(key) ?: 0
                earneds.put(key, (value + (data.get(key) as Long)))
            }
        }
        Map<String, Long> map = new HashMap<>();
        Long report_total = 0l;
        def report_datas = finance_dailyReport.find($$(timebetween)).toArray()
        report_datas.each {DBObject report_data ->
            report_data.removeField('_id');
            report_data.removeField('timestamp');
            report_data.keySet().each {String key ->
                if(cost_keys.contains(key) && report_data.get(key) != null){
                    report_total += report_data.get(key) as Long
                    Long value = map.get(key) ?: 0
                    map.put(key, (value + (report_data.get(key) as Long)))
                }
            }
        }
        println " begin : ${new Date(firstDayOfCurrentMonth).format("yyyy-MM-dd HH:mm:ss")}  end : ${new Date(firstDayOfLastMonth).format("yyyy-MM-dd HH:mm:ss")} \n" +
                " count : ${datas.size()}  total ${total} \n" +
                " report_count : ${report_datas.size()}  report_total ${report_total} \n" +
                " daily ${earneds} \n" +
                " report ${map}"
    }

    static void compareDailyEarnedWithDailyReport() {
        def dailyEarned = finance_dailyEarned.find($$(_id: [$ne: null])).sort($$(timestamp: 1)).toArray()

        dailyEarned.each { DBObject data ->
            StringBuffer sb = new StringBuffer()
            data.removeField('_id');
            Long timestamp = data.removeField('timestamp') as Long;
            sb << "earned : "
            data.keySet().each { String key ->
                Long coin = data.get(key) as Long
                if(coin > 0){
                    sb << key << ":" << coin << " "
                }
            }
            sb << System.lineSeparator();
            sb << "report : "
            def dailyReport  = finance_dailyReport.findOne($$('timestamp', timestamp))
            if(dailyReport != null){
                dailyReport.keySet().each { String key ->
                    Long coin = data.get(key) as Long
                    if(coin > 0){
                        sb << key << ":" << coin << " "
                    }
                }
            }

            println " ${new Date(timestamp).format("yyyy-MM-dd HH:mm:ss")} "
            println sb.toString()
        }

    }

    static void compareDailyEndsurplusWithDailyReport() {
        int beginDay = 26
        while(beginDay-- > 0){
            Calendar cal = getCalendar()
            cal.add(Calendar.MONTH, - beginDay)
            long firstDayOfCurrentMonth = cal.getTimeInMillis() - DAY_MILLON  //上月最后一天
            Long charge_coin = sumDailyReportData(firstDayOfCurrentMonth, 'charge_coin')
            def daily_log = finance_daily_log.findOne($$(_id:new Date(firstDayOfCurrentMonth).format("yyyyMMdd")+"_finance"))
            def daily_reprot = finance_monthReport.findOne($$(_id:new Date(firstDayOfCurrentMonth).format("yyyyMM")+"_finance"))
            Long daily_end_surplus = (daily_log?.get('end_surplus') ?: 0) as Long
            Long report_end_surplus = (daily_reprot?.get('end_surplus') ?: 0) as Long
            Map report_inc = daily_reprot?.get('inc') as Map
            Long report_charge_coin = (report_inc?.get('charge_coin') ?: 0) as Long
            println " ${new Date(firstDayOfCurrentMonth).format("yyyy-MM-dd")} \n" +
                    "daily : ${daily_end_surplus}  report : ${report_end_surplus} balance : ${daily_end_surplus-report_end_surplus} \n" +
                    "charge_coin : ${charge_coin}  report_charge_coin:${report_charge_coin} balance: ${charge_coin-report_charge_coin}"
        }

    }


    static void compareDailyWithMonthReport() {
        int beginMonth = 26
        Long end_surplus = 0
        while(beginMonth-- > 0){
            Calendar cal = getCalendar()
            cal.add(Calendar.MONTH, - beginMonth)
            long firstDayOfCurrentMonth = cal.getTimeInMillis()  //当月第一天
            cal.add(Calendar.MONTH, -1)
            long firstDayOfLastMonth = cal.getTimeInMillis()  //上月第一天
            def timebetween = [timestamp: [$gte: firstDayOfLastMonth, $lt: firstDayOfCurrentMonth]]
            Long inc_total = sumDailyLogs(timebetween, 'inc_total') ?: 0
            Long dec_total = sumDailyLogs(timebetween, 'dec_total') ?: 0
            Long total_coin = sumDailyLogs(timebetween, 'charge', 'total_coin') ?: 0
            end_surplus = end_surplus+inc_total-dec_total

            String ym = new Date(firstDayOfLastMonth).format("yyyyMM")
            String ymd = new Date(firstDayOfLastMonth).format("yyyyMMdd")
            def month_reprot = finance_monthReport.findOne($$(_id:ym+"_finance"))
            Map report_inc = month_reprot?.get('inc') as Map
            Long report_inc_total = (report_inc['total']?:0) as Long
            Long report_charge_total = (report_inc['total_coin']?:0) as Long
            Map report_dec = month_reprot?.get('dec') as Map
            Long report_dec_total = (report_dec['total']?:0)as Long
            println " ${new Date(firstDayOfLastMonth).format("yyyy-MM-dd")} to ${new Date(firstDayOfCurrentMonth).format("yyyy-MM-dd")} end_surplus : ${end_surplus}"
            if(inc_total-report_inc_total != 0){
                println "inc_total :${inc_total}  report_inc_total : ${report_inc_total} balance : ${inc_total-report_inc_total}"
            }
            if(dec_total-report_dec_total != 0){
                println "dec_total :${dec_total}  report_dec_total : ${report_dec_total} balance : ${dec_total-report_dec_total}"
            }
            if(total_coin - report_charge_total !=0){
                println "total_charege_coin :${total_coin}  report_charge_total : ${report_charge_total} balance : ${total_coin - report_charge_total} \n"
            }


/*
            Set keys= new HashSet();
            keys.addAll(report_dec.keySet())
            keys.addAll(setAllKeys(timebetween, 'dec'))
            keys.each {String dec_key ->
                Long daily_dec_coin = sumDailyLogs(timebetween, 'dec', dec_key) ?: 0
                Long report_dec_coin = (report_dec[dec_key]?:0)as Long
                //println " ${dec_key} : daily=${daily_dec_coin}  report=${report_dec_coin}"
                if(daily_dec_coin != report_dec_coin){
                    println "inc diff ---------- ${dec_key} : daily=${daily_dec_coin}  report=${report_dec_coin}"
                }
            }
            println();
            Set inckeys= new HashSet();
            inckeys.addAll(report_inc.keySet())
            inckeys.addAll(setAllKeys(timebetween, 'inc'))
            inckeys.each {String dec_key ->
                if(!dec_key.equals('game')){
                    Long daily_inc_coin = sumDailyLogs(timebetween, 'inc', dec_key) ?: 0
                    Long report_inc_coin = (report_inc[dec_key]?:0)as Long
                    //println " ${dec_key} : daily=${daily_inc_coin}  report=${report_dec_coin}"
                    if(daily_inc_coin != 0 && daily_inc_coin != report_inc_coin){
                        println "dec diff ---------- ${dec_key} : daily=${daily_inc_coin}  report=${report_inc_coin}"
                    }
                }

            }
            println();*/
        }

    }

    static printDailyLog(){
        String lastTime = "201406"
        Long surplus = 0
        Long inc_total = 0
        Long dec_total = 0
        finance_daily_log.find().toArray().each {
            /*
            Long currTime = it['timestamp'] as Long
            String currDate = new Date(currTime).format("yyyyMM")
            if(!currDate.equals(lastTime)){
                Integer begin_surplus = surplus
                surplus = surplus + inc_total - dec_total
                println  "${lastTime} : ${surplus} = ${begin_surplus} + ${inc_total} - ${dec_total}"
                lastTime = currDate
                inc_total = 0
                dec_total = 0;
            }
            inc_total += it['inc_total'] as Long
            dec_total += it['dec_total'] as Long
            */
            inc_total = it['inc_total'] as Long
            dec_total = it['dec_total'] as Long
            Number balance = inc_total-dec_total
            Long abs = Math.abs(balance)
            if(abs > 1000000){
                println " ${it['_id']} begin_surplus :${it['begin_surplus']} inc_total:${it['inc_total']}  dec_total:${it['dec_total']}  end_surplus:${it['end_surplus']} balance : ${balance}"
            }

        }
    }

    static Set setAllKeys(Map timebetween, String field){
        Set keys= new HashSet();
        finance_daily_log.find($$(timebetween)).toArray().each {
            keys.addAll((it[field] as Map ).keySet())
        }
        return keys
    }

    static Long sumDailyLogs(Map timebetween, String field, String key){
        finance_daily_log.find($$(timebetween)).toArray().sum {
            ((it[field] as Map )[key]?:0) as Long
        } as Long;
    }


    static Long sumDailyLogs(Map timebetween, String field){
        finance_daily_log.find($$(timebetween)).toArray().sum {
            it[field]?:0
        } as Long;
    }

    static Long sumDailyReportData(Long end, String field){
        Calendar cal = getCalendar()
        cal.setTimeInMillis(end + DAY_MILLON )
        long firstDayOfCurrentMonth = cal.getTimeInMillis()  //当月第一天
        cal.add(Calendar.MONTH, -1)
        long firstDayOfLastMonth = cal.getTimeInMillis()  //上月第一天
        String ym = new Date(firstDayOfLastMonth).format("yyyyMM")
        String year = new Date(firstDayOfLastMonth).format("yyyy")
        def timebetween = [timestamp: [$gte: firstDayOfLastMonth, $lt: firstDayOfCurrentMonth]]
        return getDailyReportList(timebetween, field)
    }

    static Long getDailyReportList(Map timebetween, String field){
        finance_daily_log.find($$(timebetween)).toArray().sum {
            it[field]?:0
        } as Long;
    }

    static compareChargeWithStat(Long begin){
        def charge_logs = stat_daily.findOne($$(_id:new Date(begin).format("yyyyMMdd")+"_finance", type:'finance'))
        Long stat_charge_coin = (charge_logs?.get('total_coin') ?: 0) as Long
        def daily_log = finance_daily_log.findOne($$(_id:new Date(begin).format("yyyyMMdd")+"_finance"))
        Map charge = daily_log?.get('charge') as Map
        Long charge_coin = (charge?.get('total_coin') ?: 0) as Long
        if(charge_coin != stat_charge_coin){
            println " ${new Date(begin).format("yyyy-MM-dd")} \n" +
                    "daily : ${charge_coin}  stat_daily : ${stat_charge_coin} balance : ${charge_coin-stat_charge_coin} \n"
        }

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

    public static Long getCurrentMonthLastDay(Date date, int month){
        Calendar a = Calendar.getInstance();
        a.setTime(date)
        a.add(Calendar.MONTH, month);
        a.set(Calendar.DATE, 1);//把日期设置为当月第一天
        a.roll(Calendar.DATE, -1);//日期回滚一天，也就是最后一天
        return (a.getTime()+1).getTime();
    }

    public static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    public static BasicDBObject $$(Map map) {
        return new BasicDBObject(map)
    }

    static void main(String[] args) {
        long l = System.currentTimeMillis()
        //生成之前历史财务报表
        //financeMonthStatic(month)
/*      TODO 消费柠檬
        int beginDay = 10
        def date = Date.parse("yyyy-MM-dd HH:mm:ss" ,"2014-06-27 00:00:00")
        while(beginDay-- > 0){
            Long begin = date.getTime()
            cost(begin)
            date = date+1;
        }

        int begin = 25;
        while(begin-- > 0){
            costByMonth(begin)
        }
*/
        //TODO 增加柠檬
/*
        int beginDay = 1000
        def date = Date.parse("yyyy-MM-dd HH:mm:ss" ,"2014-11-07 00:00:00")
        while(beginDay-- > 0){
            Long begin = date.getTime()
            if(begin > System.currentTimeMillis()){
                break;
            }
            earned(begin)
            date = date+1;
        }
*/
       // lastDayCost()

        //TODO 统计每月增加柠檬
/*
        earnedByMonth(Date.parse("yyyy-MM" ,"2014-06"))
        earnedByMonth(Date.parse("yyyy-MM" ,"2014-07"))
        earnedByMonth(Date.parse("yyyy-MM" ,"2014-08"))
        earnedByMonth(Date.parse("yyyy-MM" ,"2014-09"))
        earnedByMonth(Date.parse("yyyy-MM" ,"2014-10"))
*/


        //比较每日消费
        //compareDailyEarnedWithDailyReport();

        //比较日表和月表结余
        //compareDailyEndsurplusWithDailyReport();
/*
        int beginDay = 1000
        def date = Date.parse("yyyy-MM-dd HH:mm:ss" ,"2014-11-07 00:00:00")
        while(beginDay-- > 0){
            Long begin = date.getTime()
            if(begin > System.currentTimeMillis()){
                break;
            }
            compareChargeWithStat(begin)
            date = date+1;
        }
*/
        //每日加减
        printDailyLog()
        //compareDailyWithMonthReport();
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   FinanceDay, cost  ${System.currentTimeMillis() - l} ms"
    }

}

