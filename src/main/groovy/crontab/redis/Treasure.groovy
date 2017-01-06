#!/usr/bin/env groovy
package crontab

import com.mongodb.BasicDBObject
import com.mongodb.DBCollection
import com.mongodb.DBObject
@Grapes([
@Grab('org.mongodb:mongo-java-driver:2.14.2'),
@Grab('commons-lang:commons-lang:2.6'),
@Grab('redis.clients:jedis:2.1.0'),
])
import com.mongodb.Mongo
import com.mongodb.MongoURI
import groovy.json.JsonBuilder
import redis.clients.jedis.Jedis
import redis.clients.jedis.TransactionBlock
import redis.clients.jedis.exceptions.JedisException

/**
 * 宝藏
 *
 * date: 14-5-13 下午2:46
 */
class Treasure {


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

    static final String jedis_host = getProperties("main_jedis_host", "192.168.31.249")
    static final String chat_jedis_host = getProperties("chat_jedis_host", "192.168.31.249")
    static final Integer main_jedis_port = getProperties("main_jedis_port",6379) as Integer
    static final Integer chat_jedis_port = getProperties("chat_jedis_port",6379) as Integer

    static final String user_jedis_host = getProperties("user_jedis_host", "192.168.31.246")
    static final Integer user_jedis_port = getProperties("user_jedis_port",6379) as Integer
    static userRedis = new Jedis(user_jedis_host,user_jedis_port)

    static M  = new Mongo(new MongoURI(getProperties('mongo.uri','mongodb://192.168.31.249:27017/?w=1') as String))
    static redis = new Jedis(jedis_host, main_jedis_port)
    static chatRedis = new Jedis(chat_jedis_host, chat_jedis_port)

    static mongo = M.getDB("xy")
    static rooms = mongo.getCollection("rooms")
    static users = mongo.getCollection("users")

    static lottery_logs = M.getDB('xylog').getCollection('lottery_logs')
    static final long delay = 45 * 1000L

    static main(arg) {
        final Treasure task = new Treasure()
        //
        long l = System.currentTimeMillis()
        long begin = l
        task.treasureAward()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  treasureAward----->cost:  ${System.currentTimeMillis() - l} ms"

        //落地定时执行的日志
        l = System.currentTimeMillis()
        def timerName = 'Treasure'
        Long totalCost = System.currentTimeMillis() - begin
        saveTimerLogs(timerName,totalCost)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  save timer_logs , cost  ${System.currentTimeMillis() - l} ms"

        Thread.sleep(30*1000)

        l = System.currentTimeMillis()
        task.treasureAward()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  treasureAward----->cost:  ${System.currentTimeMillis() - l} ms"

        totalCost = System.currentTimeMillis() - l
        saveTimerLogs(timerName,totalCost)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  save timer_logs , cost  ${System.currentTimeMillis() - l} ms"

    }

    //落地定时执行的日志
    private static saveTimerLogs(String timerName,Long totalCost)
    {
        def timerLogsDB =  M.getDB("xyrank").getCollection("timer_logs")
        def tmp = System.currentTimeMillis()
        def id = timerName + "_"  + new Date().format("yyyyMMdd")
        def update = new BasicDBObject(timer_name:timerName,cost_total:totalCost,cat:'minute',unit:'ms',timestamp:tmp)
        timerLogsDB.findAndModify(new BasicDBObject('_id',id), null, null, false,new BasicDBObject('$set',update),true, true)
    }

    static Integer index = 0
    static final Long NOTICE_SEC = 330
    static final Long AWARD_SEC = 210
    static final Integer ROBOT_ID = 1024000   //机器人

    static final Long FIRST_AWARD = 3000
    static final Long SEC_AWARD = 2000
    static final Long THIRD_AWARD = 1000
    static final Long AVG_AWARD = 24300
    //远古宝藏分奖
    def treasureAward(){
        index = 0
        String treasureWaitKey = "treasure:roomIds:notice"
        String treasureAwardKey = "treasure:roomIds:award"
        //判断是否存等待通知宝藏
        if(redis.scard(treasureWaitKey) > 0 ){
            redis.smembers(treasureWaitKey).each {String roomId->
                String roomKey = "treasure:roomId:${roomId}".toString();
                Long sec = redis.ttl(roomKey)
                println "notice roomKey : ${roomKey}  ttl: ${sec}"
                if(sec <= NOTICE_SEC){
                    println "notice:${roomKey}"
                    //publish to all channel
                    chatRedis.publish("ALLchannel", new JsonBuilder([action:'treasure.notice',
                            "data_d": [room:rooms.findOne(new BasicDBObject("_id", roomId as Integer),
                                    new BasicDBObject(["_id":1,"nick_name":1,"bean":1]))]] ).toString())
                    redis.multi(new TransactionBlock(){
                        void execute() throws JedisException {
                            this.srem(treasureWaitKey, roomId)
                            this.sadd(treasureAwardKey,roomId)
                        }
                    })
                }
            }
        }

        //判断是否存等待发奖宝藏
        if(redis.scard(treasureAwardKey) > 0 ){
            redis.smembers(treasureAwardKey).each {String roomId->
                String roomKey = "treasure:roomId:${roomId}".toString();
                Integer count =(redis.get(roomKey) ?: 0) as Integer
                Long sec = redis.ttl(roomKey)
                println "award roomKey : ${roomKey}  ttl: ${sec} count: ${count}"
                if(sec <= AWARD_SEC){
                    chatRedis.publish("ALLchannel", new JsonBuilder([action:'treasure.award',
                            "data_d": [room:rooms.findOne(new BasicDBObject("_id", roomId as Integer),
                                    new BasicDBObject(["_id":1,"nick_name":1,"bean":1]))]] ).toString())
                    //发奖
                    awardProcess(roomId, count)
                    redis.multi(new TransactionBlock(){
                        void execute() throws JedisException {
                            this.del(roomKey)
                            this.srem(treasureAwardKey, roomId)
                        }
                    })
                }
            }
            println "award room count:"+index
        }

    }

    def awardProcess(def roomId, Integer count){
        if(count <= 0) return

        println "awardProcess: "+roomId
        String room_viewers = "room:${roomId}:users"
        Set<String> sets = userRedis.smembers(room_viewers)?: Collections.emptySet()
        List viewers =  sets.asList()

        viewers = viewers.findAll {
            Integer userId = it as Integer
            userId > ROBOT_ID
        }
        println "all viewers:" + viewers.size()
        DBObject lotterys = new BasicDBObject()
        Long obtain_coin = 0L
        Integer visitor_count = viewers.size()
        if(visitor_count > 5){
            def pushInfo = new ArrayList();
            Integer first_user = viewers.remove(new Random().nextInt(viewers.size())) as Integer
            Long firstAward = (FIRST_AWARD *count)
            if(addCoins(first_user, firstAward)){
                lotterys.append("first",[user_id:first_user, coin:firstAward])
                pushInfo.add(awardPushInfo("first_user",first_user,firstAward,roomId))
                obtain_coin += firstAward
            }

            Integer sec_user1 = viewers.remove(new Random().nextInt(viewers.size())) as Integer
            Integer sec_user2 = viewers.remove(new Random().nextInt(viewers.size())) as Integer
            Long secAward = (SEC_AWARD *count)
            if(addCoins(sec_user1, secAward)){
                lotterys.append("sec1",[user_id:sec_user1, coin: secAward])
                pushInfo.add(awardPushInfo("sec_user1",sec_user1,secAward,roomId))
                obtain_coin += secAward
            }
            if(addCoins(sec_user2, secAward)){
                lotterys.append("sec2",[user_id:sec_user2, coin: secAward])
                pushInfo.add(awardPushInfo("sec_user2",sec_user2,secAward,roomId))
                obtain_coin += secAward
            }

            Integer third_user1 = viewers.remove(new Random().nextInt(viewers.size())) as Integer
            Integer third_user2 = viewers.remove(new Random().nextInt(viewers.size())) as Integer
            Long thirdAward = (THIRD_AWARD *count)
            if(addCoins(third_user1, thirdAward)){
                lotterys.append("third1",[user_id:third_user1, coin: thirdAward])
                pushInfo.add(awardPushInfo("third_user1",third_user1,thirdAward,roomId))
                obtain_coin += thirdAward
            }

            if(addCoins(third_user2, thirdAward)){
                lotterys.append("third2",[user_id:third_user2, coin: thirdAward])
                pushInfo.add(awardPushInfo("third_user2",third_user2,thirdAward,roomId))
                obtain_coin += thirdAward
            }


            Integer[] avguserIDs = viewers as Integer[]
            Long avg_coins = ((AVG_AWARD * count)/avguserIDs.size()) as Integer
            println "avguserIDs: ${avguserIDs.size()}  avg_coins:${avg_coins}"
            if(users.updateMulti(new BasicDBObject('_id', new BasicDBObject('$in', avguserIDs)),
                    new BasicDBObject('$inc', new BasicDBObject("finance.coin_count", avg_coins))).getN() >= 1){
                lotterys.append("average",[user_id:avguserIDs,coin:avg_coins])
                pushInfo.add(["other":avg_coins])
                obtain_coin += avg_coins * avguserIDs.size()
            }
            //中奖信息推送
            def room_key = "ROOMchannel:"+roomId;
            chatRedis.publish(room_key, new JsonBuilder([action:'treasure.room',
                    "data_d": pushInfo] ).toString())
        }
        saveLottery(roomId as Integer, lotterys, visitor_count, obtain_coin);
    }

    def Boolean addCoins(Integer userId, Long coins){
       println "addCoins: ${coins}"
       return  users.update(new BasicDBObject('_id',userId),
                new BasicDBObject('$inc',new BasicDBObject("finance.coin_count",coins))
        ).getN() == 1
    }

    def awardPushInfo(String user, Integer user_id, Long coins, def room_id){
        Map<String, Object> obj = new HashMap()
        obj.put(user , users.findOne(user_id, new BasicDBObject(["finance.coin_spend_total":1,"nick_name":1])))
        obj.put('obtain_coin', coins)
        obj.put('room_id', room_id)
        obj.put('t', System.currentTimeMillis())
        return obj
    }

    def saveLottery(Integer room, DBObject lotterys, Integer visitor_count, Long obtain_coin)
    {
        def time = System.currentTimeMillis()

        def lotteryId = room + "_treasure_" + time
        Map obj = new HashMap()
        obj.put("_id", lotteryId)
        obj.put("room", room)
        obj.put("awards",lotterys)
        obj.put("visitor_count",visitor_count)
        obj.put("timestamp", time)
        obj.put("active_name", "treasure")
        obj.put("obtain_coin", obtain_coin)
        lottery_logs.save(new BasicDBObject(obj))
        index++;
    }
}
