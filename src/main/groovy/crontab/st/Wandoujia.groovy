#!/usr/bin/env groovy
package crontab.st

import com.mongodb.BasicDBObject
import com.mongodb.DBObject
import com.mongodb.Mongo
import com.mongodb.MongoURI
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
])
import groovy.json.JsonSlurper

class Wandoujia {
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
    static DAY_MILLON = 24 * 3600 * 1000L

    static long zeroMill = new Date().clearTime().getTime()
    static Long yesTday = zeroMill - DAY_MILLON
    static String YMD = new Date(yesTday).format("yyyyMMdd")

    static mobiles = mongo.getDB('xy_union').getCollection('mobiles')
    static users = mongo.getDB('xy_user').getCollection('users')
    static xy_users = mongo.getDB('xy').getCollection('users')
    static lottery_logs = mongo.getDB('xylog').getCollection('lottery_logs')

    /**
     * 同步手机号码
     */
    static void fetchData() {
        /*
        def data = new JsonSlurper().parseText(
                new URL("http://data.api.ttpod.com/data_api?app=beauty&type=active_count_by_from&date=${date.format('yyyy-MM-dd')}").getText()
        )

        if (data.size() > 0) {

            data.each { Map row ->
                coll.update(new BasicDBObject('_id', "${day}${row['from']}".toString()), new BasicDBObject('$set', [active: row['new'] as Integer]))
                println row
            }


        }
        */
        readFile();
        //setMobile("15021031149",1)
    }

    private static final List game_names= ['天龙八部3D','梦幻西游']
    private static readFile(){
        def file1 = new File("/empty/crontab/wandoujia1.csv")
        file1.splitEachLine(',') { row ->
            try{
                String game = row[1] as String
                String mobile = row[2] as String
                firstLogin(mobile, game_names.indexOf(game))
            }catch (Exception e){
                println e
            }

         }
        file1.delete()
        def file2 = new File("/empty/crontab/wandoujia2.csv")
        file2.splitEachLine(',') { row ->
            try{
                String mobile = row[2] as String
                Integer count = row[3] as Integer
                setChargeMobile(mobile,count)
            }catch (Exception e){
                println e
            }

        }
        file2.delete()
    }

    /**
     * 首冲登录
     * @param mobile
     */
    private static firstLogin(String mobile, Integer type){
        try{
            //mobiles.insert(new BasicDBObject('_id' : mobile, count: 3))
            String field = "type_"+type
            if(mobiles.count(new BasicDBObject('_id' : mobile).append(field, 1)) == 1){
                return
            }
            mobiles.update(new BasicDBObject('_id' : mobile), new BasicDBObject('$inc':[count: 3]).append('$set',new BasicDBObject(field, 1)) , true, false)
        }catch (Exception e){
            println e
        }

    }

    /**
     * 充值兑换礼物
     * @param mobile
     * @param count
     * @return
     */
    private static setChargeMobile(String mobile, Integer count){
        mobiles.update(new BasicDBObject('_id' : mobile), new BasicDBObject('$inc':[count: count]), true, false)
    }

    static void award(){
        def cursor = mobiles.find(new BasicDBObject('count' : [$gte:1])).batchSize(10000)
        while (cursor.hasNext()) {
            def obj = cursor.next()
            String mobile = obj['_id'] as String
            Integer count = obj['count'] as Integer
            if(add_bag(mobile, count)){
                mobiles.update(new BasicDBObject('_id' : mobile), new BasicDBObject('$inc':[count: -count]))
            }

        }
    }

    final static GIFT_ID = 426
    private static Boolean add_bag(String mobile, Integer count){
        def user = users.findOne(new BasicDBObject('mobile' : mobile), new BasicDBObject('_id' : 1))
        if(user != null){
            def tuid = user['_id']
            if(tuid){
                def user_info = xy_users.findAndModify(new BasicDBObject('tuid',tuid),
                        new BasicDBObject('$set',new BasicDBObject("bag."+GIFT_ID, count)))
                if(user_info == null)
                    return Boolean.FALSE
                Integer userId = user_info.get('_id') as Integer
                def nickName = user_info.get('nick_name') as String
                def lotteryId = userId + "_Wandoujia_" + GIFT_ID + "_" + System.currentTimeMillis()
                saveLotteryLog(lotteryId, "Wandoujia", userId, null, nickName, GIFT_ID.toString(), count, 0, 0)
                return Boolean.TRUE
            }
        }
        return Boolean.FALSE
    }


    public static Boolean saveLotteryLog(String lotteryId, String active_name, Integer userId, Integer star_id, String nickName,
                                  String award_name, int award_count,  long award_coin, int cost_coin){
        Long time = System.currentTimeMillis();
        Map<Object,Object> obj = new HashMap<Object,Object>();
        obj.put('_id', lotteryId);
        obj.put("user_id", userId);
        obj.put("star_id", star_id);
        obj.put("nick_name", nickName);
        obj.put("award_name", award_name);
        obj.put("award_count", award_count);
        obj.put("timestamp", time);
        obj.put("active_name", active_name);
        obj.put("cost_coin", cost_coin);
        obj.put("award_coin", award_coin);
        lottery_logs.save(new BasicDBObject(obj))
    }

    static void main(String[] args) {
        long l = System.currentTimeMillis()
        long begin = l
        l = System.currentTimeMillis()
        fetchData() //从豌豆荚获得数据
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   fetchData, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(5000L)

        l = System.currentTimeMillis()
        award() //奖励网站用户
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   award, cost  ${System.currentTimeMillis() - l} ms"

        //落地定时执行的日志
        l = System.currentTimeMillis()
        def timerName = 'Wandoujia'
        Long totalCost = System.currentTimeMillis() - begin
        saveTimerLogs(timerName, totalCost)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  save timer_logs , cost  ${System.currentTimeMillis() - l} ms"

        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  ${Wandoujia.class.getSimpleName()}, cost  ${System.currentTimeMillis() - begin} ms"

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


