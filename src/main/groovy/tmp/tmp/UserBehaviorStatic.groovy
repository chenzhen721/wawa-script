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
    static DBCollection order_logs = mongo.getDB('xylog').getCollection('order_logs')
    static DBCollection event_logs = mongo.getDB('xylog').getCollection('event_logs')
    static DBCollection day_login = mongo.getDB('xylog').getCollection('day_login')
    static DBCollection users = mongo.getDB('xy').getCollection('users')
    static DBCollection apply_post_logs = mongo.getDB('xylog').getCollection('apply_post_logs')

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
        Integer totalPayCny = 0
        Integer catchBingoUserCount = 0
        Integer catchUserCount = 0
        Integer fromInvitorUserCount = 0
        Integer fromGZHUserCount = 0

        Integer catchDaizhuaCountUserCount = 0

        def query = new BasicDBObject(via: [$ne: 'Admin'])
        List<Map> top20Users = new ArrayList<>();
        finance_log.aggregate([new BasicDBObject('$match', query),
                                new BasicDBObject('$project', [user_id: '$user_id', cny: '$cny']),
                                new BasicDBObject('$group', [_id: '$user_id',  count: [$sum: '$cny']]),
                                new BasicDBObject('$sort', [count:-1])]
        ).results().each {
            def obj = $$(it as Map)
            Integer userId = obj?.get('_id') as Integer;
            def catchQuery = new BasicDBObject('user_id', userId) //抓中次数
            def cny = obj?.get('count') as Integer; //充值总额度
            def count = catch_record.count(catchQuery) as Long;
            def bingoQuery = catchQuery.append('status',true) //抓中次数
            def bingoCount = catch_record.count(bingoQuery) as Long;
            //大于10RMB的
            if(top20Users.size() < 20){
                top20Users.add(obj);
            }
            if(count > 0){
                catchUserCount++
            }
            if(cny > 0){
                Integer period = payPeriod(userId)
                //while(period > 0){
                    Integer periodCount = payPeriodCount.get(period) ?: 0
                    payPeriodCount.put(period, ++periodCount);
                //}
               if(catch_record.count($$('user_id':userId,'toy.channel':3)) > 0){
                   catchDaizhuaCountUserCount++
               }
            }
            if(freeCatchBingo(userId)){
                catchBingoUserCount++;
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
            totalPayCny += cny
        }
        println "付费人数:${totalPayUserCount}"
        payPeriodCount.each {Integer period, Integer userCount ->
            println " 付费周期${period+1}天:${userCount}\t占比: ${fmtNumber(userCount/totalPayUserCount * 100)}%"
        }
        println "付费抓中比率"
        println " 命中率 \t用户: \t占比 \t充值总额 \t人均充值:元\t人均抓取次数"
        catchRateCount.each {Integer cRate, CatchRateUser cRateUser ->
            println " ${cRate}%\t\t${cRateUser.count} \t${fmtNumber(cRateUser.count.toInteger()/totalPayUserCount * 100)}%" +
                    "\t ${cRateUser.cny}  \t\t ${fmtNumber(cRateUser.cny.toInteger() / cRateUser.count.toInteger())}" +
                    "\t\t${(cRateUser.catchCount.toInteger()/cRateUser.count.toInteger()) as Integer}"
        }
        println "付费前免费抓中人数:${catchBingoUserCount}\t占比: ${fmtNumber(catchBingoUserCount / totalPayUserCount * 100)}%"
        println "付费用户代抓人数:${catchDaizhuaCountUserCount}\t占比: ${fmtNumber(catchDaizhuaCountUserCount / catchUserCount * 100)}%"
        println "付费用户中为邀请用户 \t占比: ${fmtNumber(fromInvitorUserCount / totalPayUserCount * 100)}%"
        println "付费用户中为公众号用户 \t占比: ${fmtNumber(fromGZHUserCount / totalPayUserCount * 100)}%"
        println "付费TOP20用户 : ${top20Users}"
    }


    static staticsTopPayUser(Integer limit){
        def query = new BasicDBObject(via: [$ne: 'Admin'])
        List<Map> top20Users = new ArrayList<>();
        println "用户\t充值\t抓取\t抓中\t命中率\t最近登录时间\t最后付费时间\t付费后抓取次数\t命中率\t最后次充值完毕后活跃天数".toString()
        finance_log.aggregate([new BasicDBObject('$match', query),
                               new BasicDBObject('$project', [user_id: '$user_id', cny: '$cny']),
                               new BasicDBObject('$group', [_id: '$user_id',  count: [$sum: '$cny']]),
                               new BasicDBObject('$sort', [count:-1]),
                               new BasicDBObject('$limit',limit)
                              ]
        ).results().each {
            def obj = $$(it as Map)
            Integer userId = obj?.get('_id') as Integer;
            def catchQuery = new BasicDBObject('user_id', userId) //抓中次数
            def cny = obj?.get('count') as Integer; //充值总额度
            def count = catch_record.count(catchQuery) as Long;//抓取次数
            def bingoQuery = catchQuery.append('status',true) //抓中次数
            def bingoCount = catch_record.count(bingoQuery) as Long;
            Long last_login= users.findOne(userId, $$(last_login:1))?.get('last_login') as long
            Long lastTime = finance_log.find($$(user_id:userId, via: [$ne: 'Admin']), $$(timestamp:1)).sort($$(timestamp:-1)).limit(1).toArray()[0]?.get("timestamp") as Long
            Long countAfterLastPay = catch_record.count($$(user_id:userId, timestamp:[$gte:lastTime]))
            Long bingoCountAfterLastPay = catch_record.count($$(user_id:userId, timestamp:[$gte:lastTime] ,status:true ))
            Double rate = 0
            if(bingoCountAfterLastPay > 0 && countAfterLastPay > 0)
                rate = bingoCountAfterLastPay/countAfterLastPay

            Integer day = ((last_login - lastTime) / DAY_MILLON )as Integer
            println "${userId}\t${cny}\t${count}\t${bingoCount}\t${fmtNumber(bingoCount/count * 100)}%" +
                    "\t${new Date(last_login).format("yyyy-MM-dd")}\t${new Date(lastTime).format("yyyy-MM-dd")}" +
                    "\t${countAfterLastPay}  \t\t${fmtNumber(rate * 100)}% \t\t${day}"
        }
    }

    //付费周期
    static payPeriod(Integer userId){
        Long firstTime = finance_log.find($$(user_id:userId), $$(timestamp:1)).sort($$(timestamp:1)).limit(1).toArray()[0]?.get("timestamp") as Long
        Long lastTime = finance_log.find($$(user_id:userId), $$(timestamp:1)).sort($$(timestamp:-1)).limit(1).toArray()[0]?.get("timestamp") as Long
        Integer period = (new Date(lastTime).clearTime().getTime() - new Date(firstTime).clearTime().getTime()) / DAY_MILLON
        return period
    }

    //是否在充值前命中
    static freeCatchBingo(Integer userId){
        Long firstTime = finance_log.find($$(user_id:userId, via: [$ne: 'Admin']), $$(timestamp:1)).sort($$(timestamp:1)).limit(1).toArray()[0]?.get("timestamp") as Long

        if(firstTime == null || firstTime<=0) firstTime = System.currentTimeMillis()
         Long count = catch_record.count($$(user_id:userId, timestamp:[$lt:firstTime] ,status:true))
        return count >= 1
    }

    //是否在充值前 三次内命中
    static free3CatchBingo(Integer userId){
        Long firstTime = finance_log.find($$(user_id:userId, via: [$ne: 'Admin']), $$(timestamp:1)).sort($$(timestamp:1)).limit(1).toArray()[0]?.get("timestamp") as Long
        if(firstTime == null || firstTime<=0) firstTime = System.currentTimeMillis()
        def catchList = catch_record.find($$(user_id:userId, timestamp:[$lt:firstTime])).sort($$(timestamp:1)).limit(3).toArray()
        Boolean status = Boolean.FALSE
        for (DBObject c : catchList){
            Boolean status1 = c.get('status') as Boolean
            if(status1.equals(Boolean.TRUE)){
                status = Boolean.TRUE
            }
        }
        return status
    }

    //是否为被邀请用户
    static fromInvitor(Integer userId){
        return invitor_logs.count($$(user_id:userId)) >= 1
    }

    //获得邀请人ID
    static Integer getInvitor(Integer userId){
        return (invitor_logs.findOne($$(user_id:userId))?.get('invitor') ?:0) as Integer
    }

    //是否成功邀请过用户
    static isInvitor(Integer userId){
        return invitor_logs.count($$(invitor:userId)) >= 1
    }

    //是否成功邀请过用户
    static isInvitor(Integer userId, Long begin, Long end){
        return invitor_logs.count($$(invitor:userId,timestamp:[$gte:begin,$lt:end])) >= 1
    }

    //邀请过来的用户
    static List invitiedUsers(Integer userId){
        return invitor_logs.find($$(invitor:userId)).toArray()*.user_id
    }

    //邀请过来的用户
    static List invitiedUsers(Integer userId, Long begin, Long end){
        return invitor_logs.find($$(invitor:userId,timestamp:[$gte:begin,$lt:end])).toArray()*.user_id
    }

    //来自公众号用户
    static fromGZH(Integer userId){
        String userQd = users.findOne(userId, $$(qd:1))?.get("qd")
        return userQd.equals("wawa_kuailai_gzh")
    }

    //付费用户的抓中的概率
    static rateCatchBingo(Integer userId){
        Long catchCount = catch_record.count($$(user_id:userId))
        Long catchCountBingo = catch_record.count($$(user_id:userId,'status':true)) ?: 0
        //println "rate : ${userId} : ${catchCountBingo},  ${catchCount}"
        Double rate = catchCountBingo > 0 ? catchCountBingo / catchCount: 0;
        //println "rate : ${catchCountBingo},  ${catchCount} = ${Math.rint( rate * 100)}"
        return Math.rint( rate * 100)
    }

    static Map<Integer, Integer> playPeriodCount = new HashMap<>();
    static staticsCatchUser(String date){
        Integer totalplayUserCount = 0
        Integer catchUserCount = 0
        Integer catchUserPayCount = 0
        Integer catchUserInvitedCount = 0
        Integer uncatchUserCount = 0
        Integer uncatchUserPayCount = 0
        Integer uncatchUserInvitedCount = 0

        Long begin = Date.parse("yyyy-MM-dd", date).clearTime().getTime()
        def query = $$( timestamp: [$gte: begin, $lt: begin + DAY_MILLON])
        catch_record.aggregate([new BasicDBObject('$match', query),
                                new BasicDBObject('$project', [user_id: '$user_id', toyId: '$toy._id']),
                                new BasicDBObject('$group', [_id: '$user_id',  count: [$sum: 1], users: [$addToSet: '$user_id']])]
        ).results().each {
            def obj = $$(it as Map)
            Integer userId = obj?.get('_id') as Integer;
            def count = obj?.get('count') as Long; //抓取次数
            if(count > 1){
                /*Integer period = catchPeriod(userId)
                Integer periodCount = playPeriodCount.get(period) ?: 0
                playPeriodCount.put(period, ++periodCount);*/
                if(freeCatchBingo(userId)){
                    catchUserCount++;
                    if(isPay(userId)){
                        catchUserPayCount++;
                    }

                }else{
                    uncatchUserCount++
                    if(isPay(userId)){
                        uncatchUserPayCount++;
                    }
                }
            }
            totalplayUserCount++;
        }
        println "${date}: 抓取人数:${totalplayUserCount} \t充值前免费抓中人数:${catchUserCount}" +
                "\t充值前免费抓中的付费用户:${catchUserPayCount} 占比: ${fmtNumber(catchUserPayCount / catchUserCount * 100)}%" +
                "\t免费抓中未付费用户:${catchUserCount-catchUserPayCount} 占比: ${fmtNumber( (catchUserCount-catchUserPayCount) / catchUserCount * 100)}%"+
                "\t未抓中用户${uncatchUserCount}\t 未抓中付费用户:${uncatchUserPayCount} 占比: ${fmtNumber( uncatchUserPayCount / uncatchUserCount * 100)}%"
        /*
        playPeriodCount.each {Integer period, Integer userCount ->
            println " 第${period+1}天:${userCount}\t占比: ${fmtNumber(userCount/totalplayUserCount * 100)}%"
        }*/
    }


    /**
     *
     抓取的人数
     前三次抓取到娃娃的人数
     前三次抓取到娃娃的付费人数
     前三次抓取到娃娃且成功邀请了至少一个好友的人数
     前三次未抓取到娃娃的付费人数
     前三次未抓取到娃娃且成功邀请了至少一个好友的人数
     * @return
     */
    static staticsChannelCatchUser(String qd, String date) {
        Integer userCount = 0
        Integer totalplayUserCount = 0
        Integer totalFollowerUserCount = 0

        Integer catchUserCount = 0
        Integer catchUserPayCount = 0
        Integer catchUserInvitedCount = 0
        Integer uncatchUserCount = 0
        Integer uncatchUserPayCount = 0
        Integer uncatchUserInvitedCount = 0

        Integer catchCount = 0
        Integer catchBingoCount = 0
        Integer invitedUserCount = 0
        Integer fromInvitorUserCount = 0
        Integer fromInvitorCatchCount = 0
        Integer totalPayUserCount = 0

        Map<Long, Integer> userCatchCounts = new HashMap<>();
        Map<Integer, Integer> userInvitorCounts = new HashMap<>();
        Map<String, Integer> userInvitorChannelCounts = new HashMap<>();

        Long begin = Date.parse("yyyy-MM-dd", date).clearTime().getTime()
        Long end =  begin + DAY_MILLON
        def query = $$(timestamp: [$gte: begin, $lt:end])
        if(StringUtils.isNotBlank(qd)){
            def channel_db = mongo.getDB('xy_admin').getCollection('channels')
            List<String> qds = channel_db.find($$(parent_qd: qd), $$(_id: 1)).toArray()*._id
            qds.add(qd);
            query.append('qd',[$in: qds])
        }
        def cur = users.find(query).batchSize(200)
        while (cur.hasNext()) {
            def row = cur.next()
            Integer userId = row['_id'] as Integer
            Long weixin_focus_timestamp = row['weixin_focus_timestamp'] as Long
            userCount++
            def catchQuery = new BasicDBObject('user_id', userId)//抓取次数
            def count = catch_record.count(catchQuery) as Long;
            catchCount += count
/*            def bingoQuery = catchQuery.append('status', true) //抓中次数
            def bingoCount = catch_record.count(bingoQuery) as Long;
            catchBingoCount += bingoCount*/

            Integer userCounts = userCatchCounts.get(count) ?: 0
            userCatchCounts.put(count, ++userCounts)
            Integer invitorUid = getInvitor(userId)
            if (invitorUid > 0) {
                fromInvitorUserCount++;
                Integer counts = userInvitorCounts.get(invitorUid) ?: 0
                userInvitorCounts.put(invitorUid, ++counts)
            }

            if (count > 0) {
                //邀请来的用户 抓取率
                if (invitorUid > 0) {
                    fromInvitorCatchCount++
                }
                totalplayUserCount++;
                if (free3CatchBingo(userId)) {
                    catchUserCount++;
                    if (isPay(userId, begin, end)) {
                        catchUserPayCount++;
                    }
                    if (isInvitor(userId, begin, end)) {
                        catchUserInvitedCount++;
                    }
                } else {
                    uncatchUserCount++
                    if (isPay(userId, begin, end)) {
                        uncatchUserPayCount++;
                    }
                    if (isInvitor(userId, begin, end)) {
                        uncatchUserInvitedCount++;
                    }
                }
            }
            invitedUserCount += invitiedUsers(userId, begin, end).size()
            if (weixin_focus_timestamp != null && weixin_focus_timestamp > 0) {
                totalFollowerUserCount++;
            }
            if(isPay(userId, begin, end)){
                totalPayUserCount++;
            }

        }

        println "${date}:${qd}: 总人数:${userCount}\t抓取人数:${totalplayUserCount}\t付费人数:${totalPayUserCount}\t关注人数:${totalFollowerUserCount}"
        println "抓中人数:${catchUserCount}\t抓中付费用户:${catchUserPayCount} \t占比: ${fmtNumber(catchUserPayCount / catchUserCount * 100)}%" +
                "\t抓中用户邀请了至少一个好友的人数:${catchUserInvitedCount}"
       println  "未抓中用户${uncatchUserCount}\t 未抓中付费用户:${uncatchUserPayCount} \t占比: ${fmtNumber(uncatchUserPayCount / uncatchUserCount * 100)}%" +
                "\t未抓中用户邀请了至少一个好友的人数:${uncatchUserInvitedCount}"

        println "通过邀请来的人数:${fromInvitorUserCount} \t通过邀请来的抓取人数:${fromInvitorCatchCount}\t邀请他们来的人数:${userInvitorCounts.size()} \t邀请人数:${invitedUserCount}"

        /*      println "邀请人数TOP用户:";
              userInvitorCounts.sort {a, b ->
                   b.value - (int) a.value
              }.each {
                  if(it.value > 5)
                      print "${it.key}:${it.value},"

                  String userQd = users.findOne(it.key as Integer, $$(qd:1)).get("qd")
                  Integer userQdCounts = userInvitorChannelCounts.get(userQd) ?: 0
                  userInvitorChannelCounts.put(userQd, ++userQdCounts)
              }
              println ""

              println "邀请渠道TOP:";
              userInvitorChannelCounts.sort {a, b ->
                  b.value - (int) a.value
              }.each {
                  if(it.value > 1)
                      print "${it.key}:${it.value},"

              }
              println ""
      */

    }

    //付费周期
    static catchPeriod(Integer userId){
        Long firstTime = catch_record.find($$(user_id:userId), $$(timestamp:1)).sort($$(timestamp:1)).limit(1).toArray()[0]?.get("timestamp") as Long
        Long lastTime = catch_record.find($$(user_id:userId), $$(timestamp:1)).sort($$(timestamp:-1)).limit(1).toArray()[0]?.get("timestamp") as Long
        Integer period = (new Date(lastTime).clearTime().getTime() - new Date(firstTime).clearTime().getTime()) / DAY_MILLON
        return period
    }

    //收到娃娃用户的付费占比
    static staticsDeliverUserOfPay(String date){
        Long begin = Date.parse("yyyy-MM-dd HH:mm:ss","${date} 00:00:00".toString()).getTime()
        def uids = apply_post_logs.distinct("user_id", $$(post_type:3,timestamp:[$gte:begin,$lt:begin+DAY_MILLON]))
        Integer payUserCount = 0
        Integer payUserToyCount = 0
        Integer toyCount = 0
        Integer toyPay = 0
        def cur =apply_post_logs.find($$(post_type:3,timestamp:[$gte:begin,$lt:begin+DAY_MILLON])).batchSize(100)
        Set<Integer> users =new HashSet<>();
        while (cur.hasNext()) {
            def row = cur.next()
            Integer userId = row['user_id'] as Integer
            List toys = row['toys'] as List
            if(isPay(userId)){
                if(users.add(userId)){
                    payUserCount++;
                    toyPay += totalPay(userId)
                }
                payUserToyCount += toys.size()
            }
            toyCount += toys.size()
        }
        println "${date}: 发货用户数:${uids.size()}\t充值用户数:${payUserCount}\t充值额度:${toyPay}" +
                "\t娃娃数量:${toyCount}\t充值用户娃娃数量: ${payUserToyCount}\t未充值用户娃娃数量: ${toyCount-payUserToyCount}"
    }

    //是否付费用户
    static Boolean isPay(Integer userId){
        return finance_log.count($$(user_id:userId,via: [$ne: 'Admin'])) > 0
    }

    //是否付费用户
    static Boolean isPay(Integer userId, Long begin, Long end){
        return finance_log.count($$(user_id:userId,via: [$ne: 'Admin'], timestamp:[$gte:begin,$lt:end])) > 0
    }

    static DBCollection  weixin_msgs = mongo.getDB('xy_union').getCollection('weixin_msgs')
    static DBCollection  diamond_add_logs = mongo.getDB('xylog').getCollection('diamond_add_logs')
    static DBCollection red_packets = mongo.getDB('xy_activity').getCollection('red_packets')
    //红包相关数据统计 产生红包数/发送用户人数/领取红包人数/领取后抓取人数/领取后充值人数/充值金额
    static void msgPushStatistic(String eventName, String eventId,String date){
        Long begin = Date.parse("yyyy-MM-dd HH:mm:ss","${date} 00:00:00".toString()).getTime()
        def msgs = weixin_msgs.distinct("to_id", $$(success_send:1,timestamp:[$gte:begin,$lt:begin+DAY_MILLON],event:eventId)).size()
        def cur = event_logs.find($$(event:eventId,timestamp:[$gte:begin,$lt:begin+DAY_MILLON])).batchSize(10)
        Integer userCount = 0
        Integer userCatchCount = 0
        Integer catchCountTotal = 0
        Integer userCatchGotCount = 0
        Integer userCatchGotpayCount = 0
        Integer payUserCount = 0
        Integer payCount = 0
        Set<Integer> users =new HashSet<>();
        while (cur.hasNext()) {
            def row = cur.next()
            Integer userId = row['user_id'] as Integer
            Long timestamp = row['timestamp'] as Long
            if(users.add(userId)){
                userCount++;
                Long end = timestamp + 1 * 60 * 60 *1000l
                Long catchCount = catch_record.count($$($$(user_id:userId, timestamp:[$gte:timestamp, $lt:end])));
                if(catchCount > 0){
                    userCatchCount++;
                    catchCountTotal += catchCount
                }
                Boolean catched = catch_record.count($$($$(user_id:userId, status:true,timestamp:[$gte:timestamp, $lt:end]))) > 0
                if(catched){
                    userCatchGotCount++;
                }

                if(finance_log.count($$($$(user_id:userId,via: [$ne: 'Admin'], timestamp:[$gt:timestamp, $lt:end]))) > 0){
                    payUserCount++;
                    if(catched)
                        userCatchGotpayCount++
                }
                List<Integer> cnys = finance_log.find($$(user_id:userId,via: [$ne: 'Admin'], timestamp:[$gt:timestamp, $lt:end])).toArray()*.cny
                if(cnys != null && cnys.size() >0){
                    payCount += cnys.sum() as Integer
                }
            }

        }
        println "${date}:${eventName}推送 >> 推送用户数:${msgs}\t 点击人数: ${userCount}\t 1小时内抓取人数: ${userCatchCount}\t抓取次数: ${catchCountTotal}" +
                " \t充值人数: ${payUserCount}\t总充值金额: ${payCount} 元\t抓中人数: ${userCatchGotCount}\t抓中后充值人数: ${userCatchGotpayCount}"
    }

    static List<String> guangfangs = ['wawa_default','wawa_kuailai_gzh','wawa_share_erweima','wawa_amiao_gzh','guanfangweibo','gongzhonghao']
    static List<String> channels = ['tjwlz','haibo','qxyl','wawa_liangziyun']
    static void staticChannlePay(){
        Long begin = Date.parse("yyyy-MM-dd HH:mm:ss","2018-01-01 00:00:00").getTime()
        int i = 0
        while (++i < 15){
            Integer payTotalCount = 0
            Integer payguanfangTotalCount = 0
            Integer payChannelTotalCount = 0
            List<Integer> cnys = finance_log.find($$(via: [$ne: 'Admin'], timestamp:[$gte:begin, $lt:begin+DAY_MILLON])).toArray()*.cny
            if(cnys != null && cnys.size() >0){
                payTotalCount += cnys.sum() as Integer
            }
            List<Integer> guanfangCnys = finance_log.find($$(qd:[$in:guangfangs],via: [$ne: 'Admin'], timestamp:[$gte:begin, $lt:begin+DAY_MILLON])).toArray()*.cny
            if(guanfangCnys != null && guanfangCnys.size() >0){
                payguanfangTotalCount += guanfangCnys.sum() as Integer
            }
            List<Integer> channelCnys = finance_log.find($$(qd:[$in:channels],via: [$ne: 'Admin'], timestamp:[$gte:begin, $lt:begin+DAY_MILLON])).toArray()*.cny
            if(channelCnys != null && channelCnys.size() >0){
                payChannelTotalCount += channelCnys.sum() as Integer
            }
            println "${new Date(begin).format('yyyy-MM-dd HH:mm:ss')} : 总:${payTotalCount} " +
                    "\t官方:${payguanfangTotalCount} \t占比:${fmtNumber(payguanfangTotalCount/payTotalCount * 100)}%" +
                    "\t渠道:${payTotalCount-payguanfangTotalCount} \t占比:${fmtNumber((payTotalCount-payguanfangTotalCount)/payTotalCount * 100)}%"
            begin = begin+DAY_MILLON
        }

    }

    //统计用户点击充值按钮后完成支付的情况
    static void staticPaiedAfterClickButton(String date){
        Long begin = Date.parse("yyyy-MM-dd HH:mm:ss",date).getTime()
        Integer userCount = 1
        Integer payUserCount = 1
        Integer payTotalCount = 1
        Integer OrderTotalCount = 1
        Integer OrderPayTotalCount = 1
        Set<Integer> users =new HashSet<>();
        Map<Integer, Integer> userClickCount = new HashMap<>();
        order_logs.find($$( timestamp:[$gte:begin, $lt:begin+DAY_MILLON])).toArray().each {DBObject order ->
            OrderTotalCount++;
            Long timestamp = order['timestamp'] as Long
            String _id = order['_id'] as String
            String[] ids = _id.split("_")
            Integer userId = ids[0] as Integer
            Long end = timestamp + 4 * 60 * 60 *1000l
            if(users.add(userId)){
                userCount++;
                if(finance_log.count($$(user_id:userId,via: [$ne: 'Admin'], timestamp:[$gte:timestamp, $lt:end])) > 0){
                    payUserCount++;
                }
                if(finance_log.count($$($$(user_id:userId,via: [$ne: 'Admin']))) > 0){
                    payTotalCount++;
                    OrderPayTotalCount++
                }
            }
            Integer clickCount = userClickCount.get(userId) ?: 0
            userClickCount.put(userId, ++clickCount);
        }
        def uids = finance_log.distinct("to_id", $$(via: [$ne: 'Admin'], timestamp:[$gte:begin, $lt:begin+DAY_MILLON]))
        Integer maxCount=1;
        Integer maxUserID=1;
        Integer minCount=1;
        userClickCount.each {Integer userId, Integer count ->
            if(count >= maxCount){
                maxCount = count
                maxUserID = userId
            }
            minCount = count <= minCount ? count : minCount

        }
        userClickCount.sort {

        }
        println "${new Date(begin).format('yyyy-MM-dd HH:mm:ss')} : " +
                "\t生成支付订单人数${uids.size()+userCount}\t 未支付人数:${userCount} " +
                "\t4小时内支付成功过的人数:${payUserCount} \t占比:${fmtNumber(payUserCount/userCount * 100)}%" +
                "\t历史上支付成功过的人数:${payTotalCount} \t占比:${fmtNumber(payTotalCount/userCount * 100)}%" +
                "\t从未支付成功过的人数:${userCount-payTotalCount} \t占比:${fmtNumber((userCount-payTotalCount) / userCount * 100)}%" +
                "\t从未支付成功订单:${OrderTotalCount} \t历史上支付成功的人所产生的未支付订单:${OrderPayTotalCount}\t占比:${fmtNumber(OrderPayTotalCount/ OrderTotalCount * 100)}%"

        println "最大点击数:${maxUserID}:${maxCount}"
    }

    //统计用户邀请
    static staticInvitorUser(String date){
        Long begin = Date.parse("yyyy-MM-dd HH:mm:ss","${date} 00:00:00".toString()).getTime()
        Integer userInvitedCount = 0;
        Integer invitedUserCount = 0;
        Integer invitedUserPayCount = 0;

        Integer newuserInvitedCount = 0;
        Integer newInvitedUserCount = 0;
        Integer newInvitedUserPayCount = 0;
        //活跃用户
        List<Integer> actives = day_login.find($$(timestamp:[$gte:begin, $lt:begin+DAY_MILLON])).toArray()*.user_id
        Integer activeUserCount = actives.size()
        //新增用户
        List<Integer> news = users.find($$(timestamp:[$gte:begin, $lt:begin+DAY_MILLON])).toArray()*._id
        //非新增用户
        actives.removeAll(news)

        actives.each {Integer userId ->
            if(isInvitor(userId,begin,begin+DAY_MILLON)){
                userInvitedCount++;
                List invitedUsers = invitiedUsers(userId,begin,begin+DAY_MILLON)
                invitedUserCount += invitedUsers.size()
                invitedUsers.each {Integer inviUserId ->
                    if(finance_log.count($$(user_id:inviUserId,via: [$ne: 'Admin'], timestamp:[$gte:begin, $lt:begin+DAY_MILLON])) > 0){
                        invitedUserPayCount++;
                    }
                }

            }
        }
        news.each {Integer userId ->
            if(isInvitor(userId,begin,begin+DAY_MILLON)){
                newuserInvitedCount++;
                List invitedUsers = invitiedUsers(userId,begin,begin+DAY_MILLON)
                newInvitedUserCount += invitedUsers.size()
                invitedUsers.each {Integer inviUserId ->
                    if(finance_log.count($$(user_id:inviUserId,via: [$ne: 'Admin'], timestamp:[$gte:begin, $lt:begin+DAY_MILLON])) > 0){
                        newInvitedUserPayCount++;
                    }
                }
            }
        }
        println "${new Date(begin).format('yyyy-MM-dd')} : " +
                "\t活跃人数:${activeUserCount}" +
                "\t[新增人数:${news.size()} \t邀请好友成功的人数:${newuserInvitedCount}\t 邀请好友数量:${newInvitedUserCount} \t付费人数:${invitedUserPayCount}]" +
                "\t[非新增人数:${actives.size()} \t邀请好友成功的人数:${userInvitedCount}\t 邀请好友数量:${invitedUserCount}\t付费人数:${newInvitedUserPayCount}]" +
                ""
    }

    //统计抓取和代抓次数
    static staticsCatch(String date){
        Long begin = Date.parse("yyyy-MM-dd HH:mm:ss","${date} 00:00:00".toString()).getTime()
        int i = 0
        Long total = 0;
        Long daizhua_total = 0;
        Long zigeo_total = 0;

        Long userTotalCount = 0
        Long payUserTotalCount = 0
        Long payUserCatchTotalCount = 0

        while (i++ <= 23){
            long end = begin+ 1 * 60 * 60 * 1000
            def catchList = catch_record.find($$(timestamp:[$gte:begin, $lt:end])).sort($$(timestamp:1)).toArray()
            Long count = 1;
            Long zego_count = 0;
            Long daizhua_count = 0
            Long userCount = 1
            Long payUserCount = 1
            Long payUserCatchCount = 1
            Set<Integer> users =new HashSet<>();
            catchList.each {DBObject c ->
                Integer userId = c.get('user_id') as Integer
                Integer device_type = c.get('device_type') as Integer
                Integer channel = (c.get('toy') as Map).get('channel') as Integer
                count++
                if(device_type.equals(2)){
                    zego_count++
                }
                if(channel.equals(3)){
                    daizhua_count++
                }
                if(users.add(userId)){
                    userCount++;
                    if(isPay(userId)){
                        payUserCount++;
                    }
                }
                if(isPay(userId)){
                    payUserCatchCount++;
                }
            }
            zigeo_total+=zego_count
            total+=count
            daizhua_count+=zego_count
            daizhua_total+=daizhua_count
            userTotalCount+=userCount
            payUserTotalCount+=payUserCount
            payUserCatchTotalCount+=payUserCatchCount
            println "${new Date(begin).format('yyyy-MM-dd HH:mm:ss')} :" +
                    "下抓用户数:${userCount}\t下抓付费用户数:${payUserCount}\t占比:${fmtNumber(payUserCount/userCount * 100)}%, " +
                    "\t下抓次数:${count},\t付费用户下抓数:${payUserCatchCount} \t占比:${fmtNumber(payUserCatchCount/count * 100)}%, " +
                    "\tzego机器下抓次数:${zego_count}\t代抓:${daizhua_count}\t非代抓:${count-daizhua_count}"
            begin = end
        }
        println "总计下抓次数 : ${total},zego机器下抓次数:${zigeo_total}, " +
                "下抓用户数:${userTotalCount}, 下抓付费用户数:${payUserTotalCount}, 占比:${fmtNumber(payUserTotalCount/userTotalCount * 100)}%, " +
                "付费用户下抓数:${payUserCatchTotalCount},  占比:${fmtNumber(payUserCatchTotalCount/total * 100)}%, " +
                "代抓${daizhua_total} , 代抓占比:${fmtNumber(daizhua_total/total * 100)}%, 非代抓${total-daizhua_total}"
    }

    static staticsUserRegister(String qd,String date){
        Long begin = Date.parse("yyyy-MM-dd HH:mm:ss","${date} 00:00:00".toString()).getTime()
        int i = 0
        println "${qd} : ${date}:"
        while (i++ <= 23){
            long end = begin+ 1 * 60 * 60 * 1000
            def count = users.count($$(qd:qd, timestamp:[$gte:begin, $lt:end]))
            println "${new Date(begin).format('HH:mm:ss')} : ${count}"
            begin = end
        }
    }

    //统计分享层级
    static staticsShareLevel(){
        def allUsers = invitor_logs.distinct("user_id")
        Map<Integer, Integer> userCatchCounts = new HashMap<>();
        allUsers.each {Integer userId ->
            Integer level = findLevel(1, userId)
            Integer userCounts = userCatchCounts.get(level) ?: 0
            userCatchCounts.put(level, ++userCounts)
        }
        println "发出邀请人数:${allUsers.size()}"
        println "邀请层级"
        userCatchCounts.each {Integer level, Integer count->
            println "${level} : ${count}  占比:${fmtNumber(count/allUsers.size() * 100)}%"
        }
    }

    static Integer findLevel(Integer level, Integer userId){
        List invitorUsers = invitor_logs.find($$(user_id:userId)).toArray()*.invitor
        if(invitorUsers.size() == 0) return level;
        return findLevel(level+1, invitorUsers[0] as Integer);
    }

    static Integer totalPay(Integer userId){
        Integer payCount = 0
        List<Integer> cnys = finance_log.find($$(user_id:userId,via: [$ne: 'Admin'])).toArray()*.cny
        if(cnys != null && cnys.size() >0){
            payCount += cnys.sum() as Integer
        }
        return payCount
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
        //staticsPayUser()
        //staticsTopPayUser(50)
        //抓取行为
        /*staticsCatchUser('2018-01-27')
        staticsCatchUser('2018-01-28')
        staticsCatchUser('2018-01-29')
        staticsCatchUser('2018-01-30')*/
        //邮寄用户
        //staticsDeliverUserOfPay('2018-01-30')
/*

        msgPushStatistic('娃娃过期','ToyExpire',"2018-02-01");
        msgPushStatistic('积分过期','PointsExpire',"2018-02-01");
        msgPushStatistic('邀请好友','Inviter',"2018-02-01");
        msgPushStatistic('发货通知','DeliverInfo',"2018-02-01");
        msgPushStatistic('红包发送','redpacket',"2018-02-01");
        msgPushStatistic('上新商品','ToyRenew',"2018-02-01");
*/

        //统计渠道充值
        //staticChannlePay();

        //统计用户点击充值按钮后完成支付的情况
/*        staticPaiedAfterClickButton('2018-01-28 00:00:00')
        staticPaiedAfterClickButton('2018-01-29 00:00:00')
        staticPaiedAfterClickButton('2018-01-30 00:00:00')*/

        //统计渠道数据统计
        //staticsChannelCatchUser('amds1411565', "2018-01-24")
/*
        staticsChannelCatchUser(null,"2018-02-01")
        staticsChannelCatchUser('wawa_share_lianjie',"2018-02-01")
        staticsChannelCatchUser('wawa_amiao_gzh',"2018-02-01")
        staticsChannelCatchUser('wawa_share_erweima',"2018-02-01")
     */
        staticsChannelCatchUser('wawa_share_lianjie',"2018-02-10")
        staticsChannelCatchUser('wawa_share_lianjie',"2018-02-11")
        staticsChannelCatchUser('wawa_share_lianjie',"2018-02-12")
        staticsChannelCatchUser('wawa_share_lianjie',"2018-02-13")
        staticsChannelCatchUser('wawa_share_lianjie',"2018-02-14")
        staticsChannelCatchUser('wawa_share_lianjie',"2018-02-15")
        staticsChannelCatchUser('wawa_share_lianjie',"2018-02-16")
        staticsChannelCatchUser('wawa_share_lianjie',"2018-02-17")
        staticsChannelCatchUser('wawa_share_lianjie',"2018-02-18")
        staticsChannelCatchUser('wawa_share_lianjie',"2018-02-19")
        staticsChannelCatchUser('wawa_share_lianjie',"2018-02-20")
        staticsChannelCatchUser('wawa_share_lianjie',"2018-02-21")
        staticsChannelCatchUser('wawa_share_lianjie',"2018-02-22")
        staticsChannelCatchUser('wawa_share_lianjie',"2018-02-23")
        /*
        staticInvitorUser('2018-01-20');
        */
        //staticsShareLevel();
        //staticsCatch('2018-02-03');
        //staticsCatch('2018-02-04');
        //staticsUserRegister('wawa_share_lianjie',"2018-02-10")
        //staticsUserRegister('wawa_share_lianjie',"2018-02-11")

        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   UserBehaviorStatic, cost  ${System.currentTimeMillis() - l} ms"
    }

}

