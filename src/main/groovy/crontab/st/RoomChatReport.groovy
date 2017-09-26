#!/usr/bin/env groovy
package crontab.st

import com.mongodb.BasicDBObject
import com.mongodb.DB
import com.mongodb.DBCollection
import com.mongodb.DBObject
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
])

import com.mongodb.Mongo
import com.mongodb.MongoURI
import org.apache.commons.lang.StringUtils

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/**
 * 房间内聊天数据统计
 */
class RoomChatReport {

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

    static DB chat_log = mongo.getDB('chat_log')
    static DBCollection users = mongo.getDB('xy').getCollection('users')

    static List<Integer> needCounts = [1, 2, 5, 10, 50, 100, 200]
    static roomChatStatics(int i) {
        // yesTday 昨天凌晨 - 1天 = 前天凌晨
        def begin = yesTday - i * DAY_MILLON
        def end = begin + DAY_MILLON
        def YMD = new Date(begin).format('yyyyMMdd')
        DBCollection chat_logs = chat_log.getCollection("${new Date(begin).format('yyyy_M')}_room_publish")
        def query = new BasicDBObject("action":"room.chat.pub","data.ts": [$gte: begin, $lt: end])
        Map<Integer, UserChatCounts> chats = MapWithDefault.<Integer, UserChatCounts> newInstance(new TreeMap()) { new UserChatCounts() }
        chat_logs.aggregate([new BasicDBObject('$match', query),
                             new BasicDBObject('$project', ["uid":'$data.from_id']),
                             new BasicDBObject('$group', [_id: '$uid', total:['$sum' : 1]])
          ]).results().each {
            def obj = new BasicDBObject(it as Map)
            Integer total = obj.total
            Integer uid = obj._id
            needCounts.each {
                countUser(total, uid, it as Integer, chats)
            }

        }
        print YMD + ": ";
        chats.each {Integer count, UserChatCounts userChat ->
            print count + "条以上数量:" + userChat.count + ", "
        };
        println ""
    }

    static countUser(Integer total, Integer uid, Integer needCount, Map<Integer, UserChatCounts> chats){
        if(total >= needCount){
            def userChatCounts = chats[needCount]
            userChatCounts.count.incrementAndGet()
            userChatCounts.user.add(uid)
        }
    }
    static class UserChatCounts {
        final user = new HashSet(1000)
        final count = new AtomicInteger()
        def toMap() { [user: user, count: count.get()] }
    }


    static Integer DAY = 0;

    static void main(String[] args) { //待优化，可以到历史表查询记录
        long l = System.currentTimeMillis()
        long begin = l

        //用户聊天每日统计
        l = System.currentTimeMillis()
        7.times {
            roomChatStatics(DAY++)
        }

        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticTotalReport, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //落地定时执行的日志
        jobFinish(begin)
    }

    /**
     * 标记任务完成  用于运维监控
     * @return
     */
    private static jobFinish(Long begin) {
        def timerName = 'RoomChatReport'
        Long totalCost = System.currentTimeMillis() - begin
        saveTimerLogs(timerName, totalCost)
        println "${new Date().format('yyyy-MM-dd')}:${RoomChatReport.class.getSimpleName()}:finish  cost  ${System.currentTimeMillis() - begin} ms"
    }

    //落地定时执行的日志
    private static saveTimerLogs(String timerName, Long totalCost) {
        def timerLogsDB = mongo.getDB("xyrank").getCollection("timer_logs")
        def tmp = System.currentTimeMillis()
        def id = timerName + "_" + new Date().format("yyyyMMdd")
        def update = new BasicDBObject(timer_name: timerName, cost_total: totalCost, cat: 'day', unit: 'ms', timestamp: tmp)
        timerLogsDB.findAndModify(new BasicDBObject('_id', id), null, null, false, new BasicDBObject('$set', update), true, true)
    }

    static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    static BasicDBObject $$(Map map) {
        return new BasicDBObject(map)
    }

}