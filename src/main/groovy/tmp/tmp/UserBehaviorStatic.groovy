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
        println "付费前免费抓中人数:${catchUserCount}\t占比: ${fmtNumber(catchUserCount / totalPayUserCount * 100)}%"
        println "付费用户中为邀请用户 \t占比: ${fmtNumber(fromInvitorUserCount / totalPayUserCount * 100)}%"
        println "付费用户中为公众号用户 \t占比: ${fmtNumber(fromGZHUserCount / totalPayUserCount * 100)}%"
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
        return catch_record.count($$(user_id:userId, timestamp:[$lt:firstTime] ,status:true)) >= 1
    }

    //是否为被邀请用户
    static fromInvitor(Integer userId){
        return invitor_logs.count($$(user_id:userId)) >= 1
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
        String qd = finance_log.find($$(user_id:userId), $$(timestamp:1,qd:1)).sort($$(timestamp:1)).limit(1).toArray()[0]?.get("qd") as String
        return qd.equals("wawa_kuailai_gzh")
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
    static staticsCatchUser(){
        Integer totalplayUserCount = 0
        Integer catchUserCount = 0
        Integer catchUserPayCount = 0
        Integer catchUserInvitedCount = 0
        Integer uncatchUserCount = 0
        Integer uncatchUserPayCount = 0
        Integer uncatchUserInvitedCount = 0
        catch_record.aggregate([
                                new BasicDBObject('$project', [user_id: '$user_id', toyId: '$toy._id']),
                                new BasicDBObject('$group', [_id: '$user_id',  count: [$sum: 1], users: [$addToSet: '$user_id']])]
        ).results().each {
            def obj = $$(it as Map)
            Integer userId = obj?.get('_id') as Integer;
            def count = obj?.get('count') as Long; //抓取次数
            if(count > 1){
                Integer period = catchPeriod(userId)
                Integer periodCount = playPeriodCount.get(period) ?: 0
                playPeriodCount.put(period, ++periodCount);
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
        println "抓取人数:${totalplayUserCount} \t充值前免费抓中人数:${catchUserCount}" +
                "\t充值前免费抓中的付费用户:${catchUserPayCount} 占比: ${fmtNumber(catchUserPayCount / catchUserCount * 100)}%" +
                "\t免费抓中未付费用户:${catchUserCount-catchUserPayCount} 占比: ${fmtNumber( (catchUserCount-catchUserPayCount) / catchUserCount * 100)}%"+
                "\t未抓中用户${uncatchUserCount}\t 未抓中付费用户:${uncatchUserPayCount} 占比: ${fmtNumber( uncatchUserPayCount / uncatchUserCount * 100)}%"
        playPeriodCount.each {Integer period, Integer userCount ->
            println " 第${period+1}天:${userCount}\t占比: ${fmtNumber(userCount/totalplayUserCount * 100)}%"
        }
    }

    //统计量子云
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
    static staticsLiangziyunCatchUser(){
        Integer userCount = 0
        Integer totalplayUserCount = 0
        Integer catchUserCount = 0
        Integer catchUserPayCount = 0
        Integer catchUserInvitedCount = 0
        Integer uncatchUserCount = 0
        Integer uncatchUserPayCount = 0
        Integer uncatchUserInvitedCount = 0

        Integer catchCount = 0
        Integer catchBingoCount = 0
        Integer invitedUserCount = 0
        Map<Long, Integer> userCatchCounts = new HashMap<>();
        def cur = users.find($$(qd:"wawa_liangziyun", timestamp:[$gte:1516118400000,$lt:1516204800000])).batchSize(100)
        while (cur.hasNext()) {
            def row = cur.next()
            Integer userId = row['_id'] as Integer
            userCount++
            def catchQuery = new BasicDBObject('user_id', userId)//抓取次数
            def count = catch_record.count(catchQuery) as Long;
            catchCount += count
            def bingoQuery = catchQuery.append('status',true) //抓中次数
            def bingoCount = catch_record.count(bingoQuery) as Long;
            catchBingoCount += bingoCount

            Integer userCounts = userCatchCounts.get(count) ?: 0
            userCatchCounts.put(count, ++userCounts)
            if(count > 0){
                totalplayUserCount++;
                if(freeCatchBingo(userId)){
                    catchUserCount++;
                    if(isPay(userId)){
                        catchUserPayCount++;
                    }
                    if(isInvitor(userId)){
                        catchUserInvitedCount++;
                        invitedUserCount += invitiedUsers(userId).size()
                    }
                }else{
                    uncatchUserCount++
                    if(isPay(userId)){
                        uncatchUserPayCount++;
                    }
                    if(isInvitor(userId)){
                        uncatchUserInvitedCount++;
                        invitedUserCount += invitiedUsers(userId).size()
                    }
                }
            }

        }
        println "总人数:${userCount} \t抓取人数:${totalplayUserCount} \t抓中人数:${catchUserCount}\t平均命中率${fmtNumber(catchBingoCount / catchCount * 100)}%" +
                "\t抓中付费用户:${catchUserPayCount} 占比: ${fmtNumber(catchUserPayCount / catchUserCount * 100)}%" +
                "\t抓中用户邀请了至少一个好友的人数:${catchUserInvitedCount}" +
                "\t未抓中用户${uncatchUserCount}\t 未抓中付费用户:${uncatchUserPayCount} 占比: ${fmtNumber( uncatchUserPayCount / uncatchUserCount * 100)}%" +
                "\t未抓中用户邀请了至少一个好友的人数:${uncatchUserInvitedCount}\t一共邀请:${invitedUserCount}"


        println userCatchCounts;
    }

    //付费周期
    static catchPeriod(Integer userId){
        Long firstTime = catch_record.find($$(user_id:userId), $$(timestamp:1)).sort($$(timestamp:1)).limit(1).toArray()[0]?.get("timestamp") as Long
        Long lastTime = catch_record.find($$(user_id:userId), $$(timestamp:1)).sort($$(timestamp:-1)).limit(1).toArray()[0]?.get("timestamp") as Long
        Integer period = (new Date(lastTime).clearTime().getTime() - new Date(firstTime).clearTime().getTime()) / DAY_MILLON
        return period
    }

    //收到娃娃用户的付费占比
    static staticsDeliverUserOfPay(){
        def uids = apply_post_logs.distinct("user_id", $$(post_type:3))
        Integer payUserCount = 0
        Integer payUserToyCount = 0
        Integer toyCount = 0
        Integer toyPay = 0
        def cur =apply_post_logs.find($$(post_type:3)).batchSize(100)
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
        println " 发货用户数:${uids.size()}\t充值用户数:${payUserCount}\t充值用户占比: ${fmtNumber(payUserCount/uids.size() * 100)}%\t充值额度:${toyPay}" +
                "\t娃娃数量:${toyCount}\t充值用户娃娃数量: ${payUserToyCount}\t未充值用户娃娃数量: ${toyCount-payUserToyCount}"
    }

    //是否付费用户
    static Boolean isPay(Integer userId){
        return finance_log.count($$(user_id:userId,via: [$ne: 'Admin'])) > 0
    }

    static DBCollection  weixin_msgs = mongo.getDB('xy_union').getCollection('weixin_msgs')
    static DBCollection  diamond_add_logs = mongo.getDB('xylog').getCollection('diamond_add_logs')
    static DBCollection red_packets = mongo.getDB('xy_activity').getCollection('red_packets')
    //红包相关数据统计 产生红包数/发送用户人数/领取红包人数/领取后抓取人数/领取后充值人数/充值金额
    static void msgPushStatistic(String eventName, String eventId){
        Long begin = Date.parse("yyyy-MM-dd HH:mm:ss","2018-01-12 00:00:00").getTime()
        def msgs = weixin_msgs.distinct("to_id", $$(success_send:1,timestamp:[$gte:begin],event:eventId)).size()
        def cur = event_logs.find($$(event:eventId,timestamp:[$gte:begin])).batchSize(10)
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
        println "${eventName}推送 >> 推送用户数:${msgs}\t 点击人数: ${userCount}\t 1小时内抓取人数: ${userCatchCount}\t抓取次数: ${catchCountTotal}" +
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
    static void staticPaiedAfterClickButton(){
        Long begin = Date.parse("yyyy-MM-dd HH:mm:ss","2018-01-11 00:00:00").getTime()
        int i = 0
        while (++i <= 7){
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
            begin = begin+DAY_MILLON
        }

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
        //抓取行为
        //staticsCatchUser()
        //邮寄用户
        //staticsDeliverUserOfPay()
        /*msgPushStatistic('娃娃过期','ToyExpire');
        msgPushStatistic('积分过期','PointsExpire');
        msgPushStatistic('邀请好友','Inviter');
        msgPushStatistic('发货通知','DeliverInfo');
        msgPushStatistic('红包发送','redpacket');*/
        //msgPushStatistic('上新商品','ToyRenew');
        //统计渠道充值
        //staticChannlePay();
        //统计用户点击充值按钮后完成支付的情况
        //staticPaiedAfterClickButton()
        //量子云相关数据统计
        //staticsLiangziyunCatchUser()
        staticInvitorUser('2018-01-12');
        staticInvitorUser('2018-01-13');
        staticInvitorUser('2018-01-19');
        staticInvitorUser('2018-01-20');
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   UserBehaviorStatic, cost  ${System.currentTimeMillis() - l} ms"
    }

}

