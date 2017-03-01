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

import java.text.SimpleDateFormat
import com.mongodb.DBObject

/**
 * 每月财务消费报表
 */
class FinanceMonth {

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

    static historyMongo = new Mongo(new MongoURI(getProperties('mongo_history.uri', 'mongodb://192.168.31.246:27017/?w=1') as String))

    static DB historyDB = historyMongo.getDB('xylog_history')
    static DBCollection coll = mongo.getDB('xy_admin').getCollection('stat_month')
    static DBCollection finance_dailyReport = mongo.getDB('xy_admin').getCollection('finance_dailyReport')
    static DBCollection finance_monthReport = mongo.getDB('xy_admin').getCollection('finance_monthReport')
    static DBCollection channel_pay = mongo.getDB('xy_admin').getCollection('channel_pay')
    static DBCollection stat_daily = mongo.getDB('xy_admin').getCollection('stat_daily')
    static DBCollection applys = mongo.getDB('xy_admin').getCollection('applys')
    static DBCollection family_award_log = mongo.getDB('xy_family').getCollection('award_log')
    static DBCollection stat_lives = mongo.getDB('xy_admin').getCollection('stat_lives')
    static DBCollection withdrawl_log = mongo.getDB('xy_admin').getCollection('withdrawl_log')
    static DBCollection exchange_log = mongo.getDB('xylog').getCollection('exchange_log')
    static DBCollection games_DB = mongo.getDB('xy_admin').getCollection('games')


    def static List<DBObject> dailyReportList = null;
    def static List<DBObject> statDailyList = null;
    def static Map<String, String> payKeys = new HashMap<>(50);
    def static Set<Integer> stars = new HashSet<>(5000);

    static{
        initChargeKeys();
        initStars();
    }

    //财务每月统计
    def static financeMonthStatic(int i) {
        Calendar cal = getCalendar()
        cal.add(Calendar.MONTH, -i)
        long firstDayOfCurrentMonth = cal.getTimeInMillis()  //当月第一天
        cal.add(Calendar.MONTH, -1)
        long firstDayOfLastMonth = cal.getTimeInMillis()  //上月第一天
        String ym = new Date(firstDayOfLastMonth).format("yyyyMM")
        String year = new Date(firstDayOfLastMonth).format("yyyy")


//        def timebetween = [timestamp: [$gte: 1485878400000, $lt: 1488211200000]]
        def timebetween = [timestamp: [$gte: firstDayOfLastMonth, $lt: firstDayOfCurrentMonth]]

        //初始化工作
        getDailyReportList(timebetween)
        getStatDaily(timebetween)
        println("statDailyList is ${statDailyList},timebetween is ${timebetween}")
        //增加柠檬
        def inc = increase(timebetween)

        //消费柠檬
        def dec = decrease(timebetween)

        //运营后台扣币
        def hand_cut_coin = sumDailyReportData('hand_cut_coin')
        inc.put('hand_cut_coin', hand_cut_coin) //为了后台报表展示方便
        Number cost_total = (inc['total'] as Number)
        //总减少柠檬 = 消费柠檬 - 运营后台减币
        Long inc_total = cost_total - hand_cut_coin
        inc['total'] = inc_total

        //本月期初结余=上月期末结余
        def begin_surplus = lastMonthSurplus(i)
        //期末月结余=上月期末节约 + 总增加的柠檬 - 总减少柠檬
        def end_surplus = (begin_surplus + inc_total) - (dec['total'] as Number)

        println "${new Date(firstDayOfLastMonth).format("yyyy-MM-dd")} begin_surplus:${begin_surplus} + inc_total:${inc['total']} - dec_total:${dec['total']} = ${end_surplus}"

        def obj = new BasicDBObject(
                inc: inc,
                dec: dec,
                begin_surplus : begin_surplus,
                end_surplus : end_surplus,
                type: 'finance',
                date:ym,
                timestamp:firstDayOfLastMonth
        )
        //println "save : ${ym}"
        finance_monthReport.update($$(_id: "${ym}_finance".toString()), new BasicDBObject('$set', obj), true, false)


        //TODO ====  主播VC 提现相关 begin ====//
        def vc_begin_surplus = lastMonthVCSurplus(i)//期初结余
        //本月新增VC
        def star_vc= starEarnedVc(timebetween)
        //主播冻结vc
        def star_frozen_vc= starFrozenVc(timebetween)
        def star_vail_vc = star_vc - star_frozen_vc //本月有效vc
        def star_exchange = starExchange(timebetween) //本月兑换vc (主播VC兑换柠檬)
        def star_withdrawl= starWithdrawl(timebetween)  //本月提现vc
        //期末结余VC = (期初结余 + 本月新增VC) - (本月兑换vc+本月提现vc)
        def vc_end_surplus = (vc_begin_surplus + star_vc) - (star_exchange + star_withdrawl)

        def star = $$(vc_begin_surplus:vc_begin_surplus,
                star_vail_vc:star_vail_vc,
                star_vc:star_vc,
                star_frozen_vc:star_frozen_vc,
                star_exchange:star_exchange,
                star_withdrawl:star_withdrawl,
                vc_end_surplus:vc_end_surplus);
        println star
        def starInfo = new BasicDBObject(star : star)
        finance_monthReport.update($$(_id: "${ym}_finance".toString()), new BasicDBObject('$set', starInfo), true, false)
        //TODO ====  主播VC 提现相关 end ====//

        //TODO === 主播礼物分成统计 === //
        Map gifts = new HashMap();
        giftVC(year, timebetween, gifts);
        def giftInfo = new BasicDBObject(gifts : gifts)
        finance_monthReport.update($$(_id: "${ym}_finance".toString()), new BasicDBObject('$set', giftInfo), true, false)
        //TODO === 主播礼物分成统计 === //


        println "${new Date().format('yyyy-MM-dd HH:mm:ss')} from ${new Date(firstDayOfLastMonth).format("yyyy-MM-dd HH:mm:ss")} to ${new Date(firstDayOfCurrentMonth).format("yyyy-MM-dd HH:mm:ss")}".toString()
    }

    static BasicDBObject increase(Map timebetween){
        Number totalCoin = 0
        Map data = new HashMap();
        //充值相关
        totalCoin += charge(data, timebetween);

        //签到
        totalCoin += warpDataFromDaliyReport('login_coin', data)
        //新手任务
        totalCoin += warpDataFromDaliyReport('mission_coin', data)
        //游戏获得 砸蛋 翻牌 点球
        totalCoin += warpDataFromDaliyReport('game_coin', data)
        //TODO 第一名家族礼物发放价值
        //totalCoin += familyAwardCoin(timebetween, data)

        def incData = $$('total',totalCoin);
        incData.putAll(data);
        return incData
    }

    //礼物 玩游戏

    private static final List<String> COST_FIELDS = ['send_gift']
    static BasicDBObject decrease(Map timebetween){
        Number totalCoin = 0
        Map data = new HashMap();
        COST_FIELDS.each {String field ->
            totalCoin += warpDataFromStatDaily(field, data)
        }

        // todo 要测试 增加了游戏统计的逻辑
        def gameList = games_DB.find()
        gameList.each {
            def field = it._id as String
            totalCoin += warpDataFromStatDaily(field, data)
        }

        def decData = $$('total',totalCoin);
        decData.putAll(data);
        return decData
    }

    /**
     * 充值相关
     * @return
     */
    static Long charge(Map data, Map timebetween){
        def query = $$(timebetween).append('type','finance')
        def charge_logs = stat_daily.find(query).toArray()
        //充值金额
        def total_cny = new BigDecimal(0)
        //充值柠檬
        def total_coin = new BigDecimal(0)
        //代充值金额
        def proxy_total_cny = new BigDecimal(0)
        //代充值柠檬
        def proxy_total_coin = new BigDecimal(0)
        //后台加币柠檬
        def hand_coin = new BigDecimal(0)
        charge_logs.each {DBObject chargeObj ->
            total_cny = total_cny.add(new BigDecimal(chargeObj['total'] as Double))
            total_coin = total_coin.add(new BigDecimal(chargeObj['total_coin'] as Long))
            def admin = chargeObj['admin'] as Map
            if(admin){
                hand_coin = hand_coin.add(new BigDecimal(admin['coin'] as Long))
            }
            //代充
            chargeObj.each {String key, Object val ->
                if(payKeys.containsKey(key.toLowerCase())){
                    try{
                        def detail = val as Map
                        //println(detail)
                        proxy_total_cny = proxy_total_cny.add(new BigDecimal((detail['cny'] ?: 0.0d )as Double))
                        proxy_total_coin = proxy_total_coin.add(new BigDecimal((detail['coin'] ?: 0l) as Long))
                    }catch (Exception e){
                        println  e;
                    }

                }
            }
        }
        //去除手动加币
        def total_coin_without_hand = total_coin.subtract(hand_coin)
        //直充值金额 = 总额减去代充金额
        def direct_total_cny = total_cny.subtract(proxy_total_cny)
        //直充值柠檬 = 总柠檬减去代充柠檬
        def direct_total_coin = total_coin_without_hand.subtract(proxy_total_coin)

        data.put('charge_cny', total_cny.toDouble() as Long)
        data.put('charge_coin', total_coin_without_hand.toLong())
        data.put('hand_coin', hand_coin.toLong())
        data.put('direct_total_cny', direct_total_cny.toDouble()  as Long)
        data.put('direct_total_coin', direct_total_coin.toLong())
        data.put('proxy_total_cny', proxy_total_cny.toDouble()  as Long)
        data.put('proxy_total_coin', proxy_total_coin.toLong())
        data.put('total_coin', total_coin.toLong())
        return total_coin.toLong()
    }

    static Long lastMonthSurplus(int i){
        Calendar cal = getCalendar()
        cal.add(Calendar.MONTH, -i)
        cal.add(Calendar.MONTH, -2)
        long firstDayOfLastMonth = cal.getTimeInMillis()  //上月第一天
        String ym = new Date(firstDayOfLastMonth).format("yyyyMM")
        //println "lastMonthSurplus ${ym} "
        def last_month = finance_monthReport.findOne($$(_id:"${ym}_finance".toString()))
        return (last_month?.get('end_surplus') ?: 0) as Long;
    }

    static Long lastMonthVCSurplus(int i){
        Calendar cal = getCalendar()
        cal.add(Calendar.MONTH, -i)
        cal.add(Calendar.MONTH, -2)
        long firstDayOfLastMonth = cal.getTimeInMillis()  //上月第一天
        String ym = new Date(firstDayOfLastMonth).format("yyyyMM")
        //println "lastMonthSurplus ${ym} "
        def last_month = finance_monthReport.findOne($$(_id:"${ym}_finance".toString()))
        def star = last_month?.get('star') as Map
        return (star?.get('vc_end_surplus') ?: 0) as Long;
    }

    static Long warpDataFromDaliyReport(String field, Map data){
        def  coin = sumDailyReportData(field)
        data[field] = coin
        return coin
    }

    static Long sumDailyReportData(final String field){
        if(dailyReportList == null || dailyReportList.size() <= 0) return 0;
        return dailyReportList.sum {
            it[field]?:0
        } as Long
    }

    static void getDailyReportList(Map timebetween){
        dailyReportList = finance_dailyReport.find($$(timebetween)).toArray();
    }

    //初始化充值pay key
    static void initChargeKeys(){
        //获得充值类型为代充的方式
        channel_pay.find($$(charge_type:'2')).toArray().each {
            String key = it['_id'] as String
            payKeys[key.toLowerCase()] = it['charge_type'] ?: "1"
        }
        println payKeys
    }

    //初始化历史+现役主播
    static void initStars(){
        stars.addAll(applys.find($$(status:[$in:[2,4]]), $$(xy_user_id:1))*.xy_user_id)
    }

    //static final game_types = ['open_egg':'nocar_price','football_shoot':'nocar_price','open_card':'got']
    static final game_types = ['open_card':'got']
    static Long gameInfo(Map timebetween, Map data){
        Long game_total = 0;
        def game = new HashMap();
        //游戏获得 砸蛋 翻牌 点球 铃铛
        /*game_types.each {String type, String got ->
            def query = $$(timebetween).append('type',type)
            def sum = (stat_daily.find(query, $$(got, 1)).toArray().sum {it[got] ?:0} ?: 0 )as Long
            game.put(type, sum)
            game_total += sum
        }*/
        //翻牌柠檬的奖励
        Long card_total = sumDailyReportData('card_coin')
        game.put('open_card', card_total)
        game_total += card_total

        //砸蛋只统计直接获得柠檬的奖励
        Long egg_total = sumDailyReportData('egg_coin')
        game.put('open_egg', egg_total)
        game_total += egg_total

        //砸蛋只统计直接获得柠檬的奖励
        Long egg_bingo_total = sumDailyReportData('egg_bingo_coin')
        game.put('open_bingo_egg', egg_bingo_total)
        game_total += egg_bingo_total

        //点球只统计直接获得柠檬的奖励
        Long football_total = sumDailyReportData('football_coin')
        game.put('football_shoot', football_total)
        game_total += football_total

        data.put('game_total', game_total)
        data.put('game',game)
        return game_total
    }

    private static final Integer GIFT_PRICE = 5000
    static Long familyAwardCoin(Map timebetween, Map data){
        def query = $$(timebetween)
        def sum = (family_award_log.find(query, $$(count:1)).toArray().sum {
            it['count']?:0
        } as Long ?: 0l )
        def total = sum * GIFT_PRICE
        data.put('family_award_price', total)
        return total
    }


    static void getStatDaily(Map timebetween){
        statDailyList = stat_daily.find($$(timebetween).append('type',"allcost")).toArray();
    }

    static Long warpDataFromStatDaily(String field, Map data){
        def total =  (statDailyList.sum {
            def fields = (it[field] ?: Collections.emptyMap()) as Map
            return (fields['cost'] ?: 0 ) as Long
        } as Long ?: 0)
        data.put(field, total)
        return total;
    }

    //主播兑换VC统计
    static Long starExchange(Map timebetween){
        Long total = 0;
        def query = $$('session.priv' : '2')
        query.putAll(timebetween)
        def res = exchange_log.aggregate(
                new BasicDBObject('$match', query),
                new BasicDBObject('$project', [star_id: '$user_id', exchange_coin: '$exchange']),
                new BasicDBObject('$group', [_id: '$star_id', exchange_coin: [$sum: '$exchange_coin']]) //top N 算法
        )
        Iterable objs = res.results()
        objs.each {row ->
            def user_id = row._id
            def exchange_coin = row.exchange_coin
            /*if (user_id){
                //判断是否为签约过的主播
                if(stars.contains(user_id)){
                    total += exchange_coin
                }
            }*/
            total += exchange_coin
        }
        return total;
    }

    //主播赚取vc
    static Long starEarnedVc(Map timebetween){
        Long total = 0;
        //30 * 60

        def res = stat_lives.aggregate(
                new BasicDBObject('$match', timebetween),
                new BasicDBObject('$project', [earned: '$earned']),
                new BasicDBObject('$group', [_id: null, earned: [$sum: '$earned']]) //top N 算法
        )
        Iterable objs = res.results()
        objs.each {row ->
            total += row.earned
        }
        return total;
    }

    //主播未新增超过30,000VC主播冻结VC
    private final static Integer VAIL_VC = 30000
    static Long starFrozenVc(Map timebetween){
        Long total = 0;
        def res = stat_lives.aggregate(
                new BasicDBObject('$match', timebetween),
                new BasicDBObject('$project', [user_id:'$user_id', earned: '$earned']),
                new BasicDBObject('$group', [_id: '$user_id', earned: [$sum: '$earned']]) //top N 算法
        )
        Iterable objs = res.results()
        objs.each {row ->
            Long earned = row.earned
            if(earned < VAIL_VC){
                total += earned
            }
        }
        return total;
    }

    //主播提现
    static Long starWithdrawl(Map timebetween){
        Long total = 0;
        def query = $$(status : 1)
        query.putAll(timebetween)
        def res = withdrawl_log.aggregate(
                new BasicDBObject('$match', query),
                new BasicDBObject('$project', [exchange : '$exchange']),
                new BasicDBObject('$group', [_id: null, exchange: [$sum: '$exchange']]) //top N 算法
        )
        Iterable objs = res.results()
        objs.each {row ->
            total += row.exchange
        }
        return total * 100;
    }

    //主播礼物分成统计
    static void giftVC(String year, Map timebetween, Map data){
        Map<String, Long> giftVcs = new HashMap();//消费柠檬礼物
        Map<String, Long> bagGiftVcs = new HashMap();//背包礼物
        def query = $$(type : 'send_gift')
        query.putAll(timebetween)
        DBCursor cursor = historyDB.getCollection("room_cost_${year}".toString())
                            .find($$(query),$$('session.data':1,star_cost:1,cost:1)).batchSize(10000)
        while (cursor.hasNext()) {
            def cost_log = cursor.next()
            Integer earned = gotStarEarned(cost_log)
            Integer star_cost = (cost_log['star_cost']  ?: 0) as Integer
            Integer cost = (cost_log['cost'] ?: 0) as Integer
            star_cost = (star_cost == 0 ? cost : star_cost)
            if(earned > 0 && star_cost > 0){
                String rate = String.valueOf((String.format("%.2f", (earned / star_cost)).toDouble() * 100) as Integer)
                String g_rate = String.valueOf((String.format("%.2f", (earned / star_cost)).toDouble() * 100) as Integer)
                if(!g_rate.equals("50")  && !g_rate.equals("40") && !g_rate.equals("25") && !g_rate.equals("20") ){
                    rate = String.valueOf((String.format("%.1f", (earned / star_cost)).toDouble() * 100) as Integer)
                    //printCostLog(g_rate, cost_log)
                }
                if(cost > 0){//消费柠檬
                    BigInteger totalCost = new BigInteger((giftVcs.get(rate) ?: 0).toString());
                    totalCost = totalCost.add(new BigInteger(star_cost))
                    giftVcs.put(rate, totalCost.toLong())
                }else{//背包
                    BigInteger totalCost = new BigInteger((bagGiftVcs.get(rate) ?: 0).toString());
                    totalCost = totalCost.add(new BigInteger(star_cost))
                    bagGiftVcs.put(rate, totalCost.toLong())
                }

            }
        }
        data.put('costGifts', giftVcs)
        data.put('bagGifts', bagGiftVcs)
    }

    static Set<Integer> printedGifts = new HashSet<>()
    private static void printCostLog(String g_rate , def cost_log){
        Map session = cost_log['session'] as Map
        Map data = session['data'] as Map
        Integer gift_id = data['_id'] as Integer
        if(printedGifts.add(gift_id)){
            println "rate : ${g_rate}, star_cost:${cost_log['star_cost']}, cost:${cost_log['cost']}, data:${data}"
        }

    }

    private static Integer gotStarEarned(DBObject cost_log){
        Map session = cost_log['session'] as Map
        Map data = session['data'] as Map
        Integer xy_star_id = data['xy_star_id'] as Integer
        xy_star_id = xy_star_id == null ? data['xy_user_id'] as Integer : xy_star_id;
        if(stars.contains(xy_star_id)){//如果是主播
            Integer earned = (data['earned'] ?:0) as Integer
            return earned
        }
        return 0;
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


    public static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    public static BasicDBObject $$(Map map) {
        return new BasicDBObject(map)
    }

    // 0 是上个月
    static final Integer month = 0;
    static void main(String[] args) {
        long l = System.currentTimeMillis()
        //生成之前历史财务报表
/*

        int begin = 2
        while(begin-- > 0){
            financeMonthStatic(begin)
        }
*/

        //生成下月财务报表
        financeMonthStatic(month)

        //导出财务消费报表
        //export();
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   StaticsEveryMonth:parentQdMonthStatic, cost  ${System.currentTimeMillis() - l} ms"
    }

}