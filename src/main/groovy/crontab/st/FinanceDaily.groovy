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
 * 每日充值消费报表（财务-真实柠檬币比例）
 */
class FinanceDaily {

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

    static historyMongo = new Mongo(new MongoURI(getProperties('mongo_history.uri', 'mongodb://192.168.31.246:27017/?w=1') as String))

    static DB historyDB = historyMongo.getDB('xylog_history')
    static DBCollection coll = mongo.getDB('xy_admin').getCollection('stat_month')
    static DBCollection finance_dailyReport = mongo.getDB('xy_admin').getCollection('finance_dailyReport')
    static DBCollection finance_daily_log = mongo.getDB('xy_admin').getCollection('finance_daily_log')
    static DBCollection channel_pay = mongo.getDB('xy_admin').getCollection('channel_pay')
    static DBCollection stat_daily = mongo.getDB('xy_admin').getCollection('stat_daily')
    static DBCollection applys = mongo.getDB('xy_admin').getCollection('applys')
    static DBCollection family_award_log = mongo.getDB('xy_family').getCollection('award_log')
    static DBCollection finance_log  = mongo.getDB('xy_admin').getCollection('finance_log')
    static DAY_MILLON = 24 * 3600 * 1000L
    static long zeroMill = new Date().clearTime().getTime()
    static Long yesTday = zeroMill - DAY_MILLON
    static MIN_MILLS = 60 * 1000L

    def static List<DBObject> dailyReportList = null;
    def static List<DBObject> statDailyList = null;
    def static Map<String, String> payKeys = new HashMap<>(50);
    def static Set<Integer> stars = new HashSet<>(5000);

    //比较每日统计和用户剩余柠檬快照差额 邮件报警
    static Long EMAIL_THRESHOLD = 3500
    //手续费比例
    def static Map<String, Double> PAY_RATES = ['itunes':0.7]
    static{
        //initChargeKeys();
        //initStars();
    }

    //财务每日统计
    def static Boolean financeDayStatic(Long begin) {
        Long end = begin+DAY_MILLON
        String ymd = new Date(begin).format("yyyyMMdd")

        def timebetween = [timestamp: [$gte: begin, $lt: end]]

        //初始化工作
        getDailyReportList(timebetween)
        getStatDaily(timebetween)

        //统计充值相关
        def charge = charge_total(timebetween)
        //扣费前充值金额(用户端充值金额)
        def charge_cny = charge['charge_cny'] as Number
        //扣费后充值金额(公司预收账款)
        def cut_charge_cny = charge['cut_charge_cny'] as Number

        //本月期初结余=上月期末结余
        def begin_surplus = lastDaySurplus(begin)
        //充值新增柠檬
        Long charge_coin = charge['charge_coin'] as Long
        //非充值新增柠檬 (非充值手段新增的柠檬，如玩游戏) + 手动加币
        def inc = increase(timebetween)
        def inc_coin =  (inc['total'] as Number) + (charge['hand_coin'] as Number)

        //运营后台扣币
        def hand_cut_coin = sumDailyReportData('hand_cut_coin')

        //总新增柠檬= 充值新增柠檬 + 非充值新增柠檬- 运营后台减币
        def inc_total = charge_coin + inc_coin  - hand_cut_coin

        //总消费柠檬
        def dec = decrease(timebetween)
        def dec_total = dec['total'] as Number

        //期末柠檬余额=上月期末节约 + 增加的柠檬 - 总消费柠檬
        def end_surplus = begin_surplus + inc_total - dec_total

        //今日消费差额 = 总新增柠檬 - 总消费柠檬
        def today_balance  = inc_total - dec_total
        //今日用户剩余柠檬快照
        Long remain = userRemain(begin);
        //昨日用户剩余柠檬快照
        Long yesterday_remain = userRemain(begin-DAY_MILLON);
        //用户剩余柠檬快照差额 = 今日用户剩余柠檬快照 - /昨日用户剩余柠檬快照
        def remian_balance = remain - yesterday_remain;
        //比较统计和快照差额
        Long balance = today_balance - remian_balance;
        def obj = new BasicDBObject(
                inc: inc,
                dec: dec,
                inc_total:inc_total, //总新增柠檬
                dec_total:dec_total, //总消费柠檬
                hand_cut_coin:hand_cut_coin,//运营后台扣币
                charge_cny:charge_cny, //用户端充值金额

                charge_coin:charge_coin, //充值新增柠檬
                inc_coin:inc_coin, //非充值新增柠檬
                begin_surplus : begin_surplus,
                end_surplus : end_surplus,
                cut_charge_cny:cut_charge_cny,//公司预收账款
                charge : charge,
                today_balance : today_balance, //今日消费差额
                remian_balance : remian_balance, //用户剩余柠檬快照差额
                balance : balance, //比较统计和快照差额
                remain:remain,
                type: 'finance_daily',
                date:ymd,
                timestamp:begin
        )
        //println "save : ${ym}"
        finance_daily_log.update($$(_id: "${ymd}_finance".toString()), new BasicDBObject('$set', obj), true, false)

        println "${new Date().format('yyyy-MM-dd HH:mm:ss')} from ${new Date(begin).format("yyyy-MM-dd HH:mm:ss")} to ${new Date(end).format("yyyy-MM-dd HH:mm:ss")}".toString()
        Boolean flag = Math.abs(balance) > EMAIL_THRESHOLD
        if(flag){
            println "balance : ${balance}".toString()
        }
        return flag
    }

    static BasicDBObject charge_total(Map timebetween){
        Map data = new HashMap();
        charge(data, timebetween);
        def incData = $$(data);
        return incData
    }

    static Long getRemain(Map timebetween){
        return sumDailyReportData('total_coin')
    }

    static BasicDBObject increase(Map timebetween){
        Number totalCoin = 0
        Map data = new HashMap();
        //签到
        totalCoin += warpDataFromDaliyReport('login_coin', data)
        //新手任务
        totalCoin += warpDataFromDaliyReport('mission_coin', data)
        //活动 抽奖等方式获取柠檬
        totalCoin += warpDataFromDaliyReport('activity_award_coin', data)
        //游戏获得
        totalCoin += gameInfo(timebetween, data)

        def incData = $$('total',totalCoin);
        incData.putAll(data);
        return incData
    }

    //礼物 砸蛋 点球	翻牌	铃铛	守护	VIP 座驾	沙发	靓号	财神	宝藏	接生	广播 点歌 家族 一元购 解绑 求爱签 接单

    private static final List<String> COST_FIELDS = ['send_gift','play_game']
    static BasicDBObject decrease(Map timebetween){
        Number totalCoin = 0
        Map data = new HashMap();
        COST_FIELDS.each {String field ->
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
    static charge(Map data, Map timebetween){
        def query = $$(timebetween)
        def financeList = finance_log.find(query).toArray()
        //扣费充值金额
        def total_cny = new BigDecimal(0)
        //扣费充值金额
        def cut_total_cny = new BigDecimal(0)
        //充值柠檬
        def charge_coin = new BigDecimal(0)
        //总柠檬
        def total_coin = new BigDecimal(0)
        //手动加币
        def hand_coin = new BigDecimal(0)
        financeList.each {DBObject finance ->
            String via = (finance['via'] ?: '') as String
            Double cny = new BigDecimal((finance['cny'] ?:0.0d) as Double)
            total_coin = total_coin.add(new BigDecimal((finance['coin'] ?:0) as Long))
            if(!via.equals('Admin')){
                charge_coin = charge_coin.add(new BigDecimal((finance['coin'] ?:0) as Long))
                total_cny = total_cny.add(cny)
            }else{
                hand_coin = hand_coin.add(new BigDecimal((finance['coin'] ?:0) as Long))
            }
            Double rate = PAY_RATES.get(via) ?: 1
            //Double cut_cny = cny - (((cny * 100) * rate)/ 100)
            Double cut_cny = cny * rate
            cut_total_cny = cut_total_cny.add(cut_cny)
        }
        data.put('charge_cny', total_cny.toDouble() as Long)
        data.put('cut_charge_cny', cut_total_cny.toDouble() as Long)
        data.put('charge_coin', charge_coin.toLong())
        data.put('total_coin', total_coin.toLong())
        data.put('hand_coin', hand_coin.toLong())
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

    static Long lastDaySurplus(Long begin){
        long yesterDay = begin - DAY_MILLON
        String ymd = new Date(yesterDay).format("yyyyMMdd")
        def last_day = finance_daily_log.findOne($$(_id:"${ymd}_finance".toString()))
        return (last_day?.get('end_surplus') ?: 0) as Long;
    }

    static Long userRemain(Long begin){
        String ymd = new Date(begin).format("yyyyMMdd")
        def last_day = finance_dailyReport.findOne($$(_id:"finance_${ymd}".toString()))
        return (last_day?.get('total_coin') ?: 0) as Long;
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
    }

    static Long gameInfo(Map timebetween, Map data){
        Long game_total = 0;
        def game = new HashMap();
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

    static void main(String[] args) {
        long l = System.currentTimeMillis()
        //生成历史财务报表

/*        int beginDay = 60
        def date = Date.parse("yyyy-MM-dd HH:mm:ss" ,"2016-10-20 00:00:00")
        while(beginDay-- > 0){
            Long begin = date.getTime()
            if(begin >= new Date().clearTime().getTime()){
                break;
            }
            financeDayStatic(begin)
            date = date+1;
        }*/

        Boolean needEmailNotify = financeDayStatic(yesTday)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}:${FinanceDaily.class.getSimpleName()}:finish  cost time: ${System.currentTimeMillis() - l} ms : needEmailNotify:${needEmailNotify}"
    }

}

