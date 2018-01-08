#!/usr/bin/env groovy
package crontab.st

import com.mongodb.BasicDBObject
import com.mongodb.DBCollection
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
])
import com.mongodb.Mongo
import com.mongodb.MongoURI

/**
 * 新增用户付费统计
 */
class StaticsRegPay {

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

    static mongo = new Mongo(new MongoURI(getProperties('mongo.uri', 'mongodb://192.168.2.27:10000/?w=1') as String))
    static DAY_MILLON = 24 * 3600 * 1000L
    static long zeroMill = new Date().clearTime().getTime()
    static Long yesTday = zeroMill - DAY_MILLON
    static String YMD = new Date(yesTday).format("yyyyMMdd")
    static DBCollection stat_regpay = mongo.getDB('xy_admin').getCollection('stat_regpay')
    static DBCollection users = mongo.getDB('xy').getCollection('users')
    static DBCollection finance_log_DB = mongo.getDB('xy_admin').getCollection('finance_log')
    static DBCollection day_login = mongo.getDB("xylog").getCollection("day_login")

    /**
     * regs:[] //注册IDs
     * reg_count:  注册人数
     * payuser_till_current: //截止到今天付费总人数
     * paycount_till_current: //截止到今天付费总金额
     * pay_last5: //最近5日内充值人数
     * avg_lifecycle: //当日注册用户中付费的 平均登录天数（每个用户登录天数/用户数）
     * repay_count: //复购次数
     * pay_count: //当日新增付费
     * pay_total: //总金额
     * pay_count1: //当日新增付费
     * pay_total1: //总金额
     * pay_count3: //当日新增付费
     * pay_total3: //总金额
     * pay_count7: //当日新增付费
     * pay_total7: //总金额
     * pay_count30: //当日新增付费
     * pay_total30: //总金额
     *
     * @param i
     */
    static totalStatics(int i){
        def begin = yesTday - i * DAY_MILLON
        def end = begin + DAY_MILLON
        def regs = users.count(new BasicDBObject(timestamp: [$gte: begin, $lt: end]))
        def a = true
        users.aggregate([
                $$('$match', [timestamp: [$gte: begin, $lt: end]]),
                $$('$project', [user_id: '$_id', qd: '$qd']),
                $$('$group', [_id: '$qd', user_id: [$addToSet: '$user_id']])
        ]).results().each {BasicDBObject obj->
            if (!a) return
            println obj
            a = false
            //qd信息,  对应的users

        }




    }


    static statics(def regs, def gteMill, def begin, def end) {
        Double total_cny = 0, day1cny = 0, day2cny = 0, day3cny = 0, day4cny = 0
        def total_uids = new HashSet(), day1_uids = new HashSet(),day2_uids = new HashSet(),day3_uids = new HashSet(),day4_uids = new HashSet()
        finance_log_DB.find($$(user_id: [$in: regs], via: [$ne: 'Admin'], timestamp: [$lt: gteMill])).toArray().each {BasicDBObject obj->
            def timestamp = obj['timestamp'] as Long
            def cny = obj.cny as Double
            if (timestamp > end && timestamp < end + DAY_MILLON) {
                day1cny = day1cny + cny
                day1_uids.add(obj.user_id)
            }
            if (timestamp > end + DAY_MILLON && timestamp < end + 2 * DAY_MILLON) {
                day2cny = day2cny + cny
                day2_uids.add(obj.user_id)
            }
            if (timestamp > end + 2 * DAY_MILLON && timestamp < end + 3 * DAY_MILLON) {
                day3cny = day3cny + cny
                day3_uids.add(obj.user_id)
            }
            if (timestamp > end + 3 * DAY_MILLON && timestamp < end + 4 * DAY_MILLON) {
                day4cny = day4cny + cny
                day4_uids.add(obj.user_id)
            }
            total_cny = total_cny + cny
            total_uids.add(obj.user_id)
        }
        def day_login = mongo.getDB("xylog").getCollection("day_login")
        def start = end
        def util = zeroMill

        def days = 0 as Integer

        total_uids.each {Integer id ->
            def obj = day_login.find($$(user_id: id, timestamp: [$gte: begin, $lt: util])).sort($$(timestamp: -1)).limit(1)
            def time = obj[0]['timestamp'] as Long
            days = days + (((time - start) / DAY_MILLON) as Double).toInteger() + 1
        }
        println "${new Date(begin).format('yyyy-MM-dd')}  ${days} / ${total_uids.size()}".toString()

        /*day_login.find($$(user_id: [$in: total_uids], timestamp: [$gte: begin, $lt: util])).toArray().each {BasicDBObject obj ->
        def time = obj['timestamp'] as Long
        days = days + (((time - start) / DAY_MILLON) as Double).toInteger() + 1
        println days
    }*/
    //def avg = (days / total_uids.size()) as Double
    println "${new Date(begin).format('yyyy-MM-dd')}  ${days} / ${total_uids.size()}".toString()


    println "${new Date(begin).format('yyyy-MM-dd')}    ${total_cny}:${total_uids.size()}".toString() +
    "    ${day1cny}:${day1_uids.size()}    ${day2cny}:${day2_uids.size()}    ${day3cny}:${day3_uids.size()}    ${day4cny}:${day4_uids.size()}"
    }









    public static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    public static BasicDBObject $$(Map map) {
        return new BasicDBObject(map)
    }

    static Integer DAY = 0

    static void main(String[] args) {
        try {
            long l = System.currentTimeMillis()
            totalStatics(DAY)
            println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   totalStatics, cost  ${System.currentTimeMillis() - l} ms"
        } catch (Exception e){
            println "Exception : " + e
        }

    }

}

