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
import org.apache.commons.lang.StringUtils

import java.text.SimpleDateFormat
import com.mongodb.DBObject

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/**
 * 付费用户行为统计
 */
class UserBehaviorStatic {

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

    static mongo = new Mongo(new MongoURI(getProperties('mongo.uri', 'mongodb://192.168.31.231:20000,192.168.31.236:20000,192.168.31.231:20001/?w=1&slaveok=true') as String))
    static DAY_MILLON = 24 * 3600 * 1000L
    static long zeroMill = new Date().clearTime().getTime()
    static Long yesTday = zeroMill - DAY_MILLON
    static String YMD = new Date(yesTday).format("yyyyMMdd")
    static DBCollection finance_log  = mongo.getDB('xy_admin').getCollection('finance_log')
    static DBCollection catch_record = mongo.getDB('xy_catch').getCollection('catch_record')
    static DBCollection invitor_logs = mongo.getDB('xylog').getCollection('invitor_logs')

    static class CatchRateUser {
        final user = new HashSet(1000)
        final count = new AtomicInteger()
        final rate = new AtomicInteger()
        final cny = new AtomicInteger()
        final catchCount = new AtomicInteger()

        def toMap() { [user: user.size(), count: count.get(), rate: rate.get()] }
    }

    static Map<Integer, Integer> payPeriodCount = new HashMap<>();
    //static Map<Integer, Integer> catchRateCount = new HashMap<>();
    static Map<Integer, CatchRateUser> catchRateCount =MapWithDefault.<Integer, CatchRateUser> newInstance(new HashMap()) { new CatchRateUser() }
    static staticsPayUser(){
        Integer totalPayUserCount = 0
        Integer catchUserCount = 0
        Integer fromInvitorUserCount = 0
        Integer fromGZHUserCount = 0
        def time = [via: [$ne: 'Admin']]
        def query = new BasicDBObject(time)
        finance_log.aggregate([new BasicDBObject('$match', query),
                                new BasicDBObject('$project', [user_id: '$user_id', cny: '$cny']),
                                new BasicDBObject('$group', [_id: '$user_id',  count: [$sum: '$cny']])]
        ).results().each {
            def obj = $$(it as Map)
            Integer userId = obj?.get('_id') as Integer;
            def catchQuery = new BasicDBObject(time).append('user_id', userId) //抓中次数
            def cny = obj?.get('count') as Integer; //充值总额度
            def count = catch_record.count(catchQuery) as Long;
            def bingoQuery = catchQuery.append('status',true) //抓中次数
            def bingoCount = catch_record.count(bingoQuery) as Long;
            //大于10RMB的
            if(cny >= 2){
                //println obj;
                Integer period = payPeriod(userId)
                //while(period > 0){
                    Integer periodCount = payPeriodCount.get(period) ?: 0
                    payPeriodCount.put(period, ++periodCount);
                //}

            }
            if(freeCatchBingo(userId)){
                catchUserCount++;
            }
            if(fromInvitor(userId)){
                fromInvitorUserCount++;
            }
            if(fromGZH(userId)){
                fromGZHUserCount++;
            }
            Integer cRate = rateCatchBingo(userId)
            def cRateCount = catchRateCount[cRate]
            cRateCount.count.incrementAndGet()
            cRateCount.user.add(userId)
            cRateCount.cny.addAndGet(cny)
            cRateCount.catchCount.addAndGet(catch_record.count($$(user_id:userId)) as Integer)
            totalPayUserCount++;
        }
        println "付费人数:${totalPayUserCount}"
        payPeriodCount.each {Integer period, Integer userCount ->
            println " 第${period+1}天:${userCount}\t占比: ${fmtNumber(userCount/totalPayUserCount * 100)}%"
        }
        println "付费抓中比率"
        println " 命中率 \t用户: \t占比 \t充值总额 \t人均充值:元\t人均抓取次数"
        catchRateCount.each {Integer cRate, CatchRateUser cRateUser ->
            println " ${cRate}%\t\t${cRateUser.count} \t${fmtNumber(cRateUser.count.toInteger()/totalPayUserCount * 100)}%" +
                    "\t ${cRateUser.cny}  \t\t ${fmtNumber(cRateUser.cny.toInteger() / cRateUser.count.toInteger())}" +
                    "\t\t${(cRateUser.catchCount.toInteger()/cRateUser.count.toInteger()) as Integer}"
        }
        println "付费前免费抓中人数:${catchUserCount}\t占比: ${fmtNumber(catchUserCount / totalPayUserCount * 100)}%"
        println "付费用户中为邀请用户 \t占比: ${fmtNumber(fromInvitorUserCount / totalPayUserCount * 100)}%"
        println "付费用户中为公众号用户 \t占比: ${fmtNumber(fromGZHUserCount / totalPayUserCount * 100)}%"
    }

    //付费周期
    static payPeriod(Integer userId){
        Long firstTime = finance_log.find($$(user_id:userId), $$(timestamp:1)).sort($$(timestamp:1)).limit(1).toArray()[0]?.get("timestamp") as Long
        Long lastTime = finance_log.find($$(user_id:userId), $$(timestamp:1)).sort($$(timestamp:-1)).limit(1).toArray()[0]?.get("timestamp") as Long
        Integer period = Integer.valueOf(new Date(lastTime).format("yyyyMMdd")) - Integer.valueOf(new Date(firstTime).format("yyyyMMdd"))
        return period
    }

    //前几次免费是否抓命中
    static freeCatchBingo(Integer userId){
        Long firstTime = finance_log.find($$(user_id:userId), $$(timestamp:1)).sort($$(timestamp:1)).limit(1).toArray()[0]?.get("timestamp") as Long
        return catch_record.count($$(user_id:userId, timestamp:[$lt:firstTime] ,status:true)) >= 1
    }

    //前几次免费是否抓命中
    static fromInvitor(Integer userId){
        return invitor_logs.count($$(user_id:userId)) >= 1
    }

    //来自公众号
    static fromGZH(Integer userId){
        String qd = finance_log.find($$(user_id:userId), $$(timestamp:1,qd:1)).sort($$(timestamp:1)).limit(1).toArray()[0]?.get("qd") as String
        return qd.equals("wawa_kuailai_gzh")
    }

    //付费用户的概率
    static rateCatchBingo(Integer userId){
        Long catchCount = catch_record.count($$(user_id:userId))
        Long catchCountBingo = catch_record.count($$(user_id:userId,'status':true))
        Double rate = catchCountBingo / catchCount;
        //println "rate : ${catchCountBingo},  ${catchCount} = ${Math.rint( rate * 100)}"
        return Math.rint( rate * 100)
    }

    static Map<Integer, Integer> playPeriodCount = new HashMap<>();
    static staticsCatchUser(){
        Integer totalplayUserCount = 0
        catch_record.aggregate([
                                new BasicDBObject('$project', [user_id: '$user_id', toyId: '$toy._id']),
                                new BasicDBObject('$group', [_id: '$user_id',  count: [$sum: 1], users: [$addToSet: '$user_id']])]
        ).results().each {
            def obj = $$(it as Map)
            Integer userId = obj?.get('_id') as Integer;
            def count = obj?.get('count') as Long; //抓取次数
            //大于10RMB的
            if(count > 1){
                Integer period = catchPeriod(userId)
                Integer periodCount = playPeriodCount.get(period) ?: 0
                playPeriodCount.put(period, ++periodCount);
            }
            totalplayUserCount++;
        }
        println "抓取人数:${totalplayUserCount}"
        playPeriodCount.each {Integer period, Integer userCount ->
            println " 第${period+1}天:${userCount}\t占比: ${fmtNumber(userCount/totalplayUserCount * 100)}%"
        }
    }

    //付费周期
    static catchPeriod(Integer userId){
        Long firstTime = catch_record.find($$(user_id:userId), $$(timestamp:1)).sort($$(timestamp:1)).limit(1).toArray()[0]?.get("timestamp") as Long
        Long lastTime = catch_record.find($$(user_id:userId), $$(timestamp:1)).sort($$(timestamp:-1)).limit(1).toArray()[0]?.get("timestamp") as Long
        Integer period = Integer.valueOf(new Date(lastTime).format("yyyyMMdd")) - Integer.valueOf(new Date(firstTime).format("yyyyMMdd"))
        return period
    }

    static fmtNumber(Double num){
        String result = String .format("%.2f", num);
        return result
    }
    public static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    public static BasicDBObject $$(Map map) {
        return new BasicDBObject(map)
    }

    static Integer DAY = 0

    static void main(String[] args) {
        long l = System.currentTimeMillis()
        //统计付费行为
        staticsPayUser()
        //抓取行为
        staticsCatchUser()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   UserBehaviorStatic, cost  ${System.currentTimeMillis() - l} ms"
    }

}
