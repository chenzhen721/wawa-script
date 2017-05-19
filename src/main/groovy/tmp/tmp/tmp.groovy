#!/usr/bin/env groovy
package tmp

@GrabResolver(name = 'restlet', root = 'http://210.22.151.242:8081/nexus/content/groups/public')
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
        @Grab('com.ttpod:https-util:1.0'),
])
import com.mongodb.*
import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import org.apache.commons.lang.StringUtils
import org.apache.commons.lang.math.NumberUtils
import redis.clients.jedis.Jedis
import com.https.HttpsUtil
import java.text.SimpleDateFormat


import groovy.json.JsonSlurper
import org.apache.commons.lang.StringUtils
import java.security.MessageDigest
import com.https.HttpsUtil
import com.mongodb.BasicDBObject
import com.mongodb.DBObject
import com.mongodb.Mongo
import com.mongodb.MongoURI
import groovy.json.JsonBuilder
class tmp {

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

    static final String jedis_host = getProperties("main_jedis_host", "192.168.31.246")
    static final String chat_jedis_host = getProperties("chat_jedis_host", "192.168.31.246")
    static final String live_jedis_host = getProperties("live_jedis_host", "192.168.31.246")
    static final String user_jedis_host = getProperties("user_jedis_host", "192.168.31.246")

    static final Integer main_jedis_port = getProperties("main_jedis_port",6379) as Integer
    static final Integer chat_jedis_port = getProperties("chat_jedis_port",6379) as Integer
    static final Integer live_jedis_port = getProperties("live_jedis_port",6379) as Integer
    static final Integer user_jedis_port = getProperties("user_jedis_port",6379) as Integer

    static mongo  = new Mongo(new MongoURI(getProperties('mongo.uri','mongodb://192.168.31.249:27017/?w=1') as String))
    static historyMongo = new Mongo(new MongoURI(getProperties('mongo_history.uri', 'mongodb://192.168.31.246:27017/?w=1') as String))
    static historyDB = historyMongo.getDB('xylog_history')
    static xyHistoryDB = historyMongo.getDB('xy_history')
    static mainRedis = new Jedis(jedis_host,main_jedis_port)
    static userRedis = new Jedis(user_jedis_host,user_jedis_port, 50000)


    def final Long DAY_MILL = 24*3600*1000L
    static DAY_MILLON = 24 * 3600 * 1000L
    static finance_log = mongo.getDB('xy_admin').getCollection('finance_log')
    static DBCollection room_cost =  mongo.getDB("xylog").getCollection("room_cost")
    static DBCollection user_bet =  mongo.getDB("game_log").getCollection("user_bet")
    static DBCollection smelter =  mongo.getDB("xyactive").getCollection("smelter")

    static users = mongo.getDB('xy').getCollection('users')
    static xy_users = mongo.getDB('xy_user').getCollection('users')
    static rooms = mongo.getDB('xy').getCollection('rooms')
    static day_login = mongo.getDB('xylog').getCollection('day_login')
    static lottery_logs = mongo.getDB('xylog').getCollection('lottery_logs')


    static DBCollection order_logs =  mongo.getDB("xylog").getCollection("order_logs")
    static DBCollection room_feather =  mongo.getDB("xylog").getCollection("room_feather")
    static DBCollection week_stars =  mongo.getDB("xyrank").getCollection("week_stars")
    static channels = mongo.getDB('xy_admin').getCollection('channels')

    static long zeroMill = new Date().clearTime().getTime()
    static Long yesTday = zeroMill - DAY_MILLON

    public static BasicDBObject $$(String key,Object value){
        return new BasicDBObject(key,value);
    }

    public static BasicDBObject $$(Map map){
        return new BasicDBObject(map);
    }

    static void recoverFinanceLogToId(){
        def finance_log = mongo.getDB('xy_admin').getCollection('finance_log')
        finance_log.setReadPreference(ReadPreference.secondary())
        def list = finance_log.find($$(via:'weixin_m')) .toArray()
        println "size : ${list.size()}";
        list.each { BasicDBObject obj ->
            def feeType = obj.get('feeType') as String
            def transactionId = obj.get('transactionId') as String
            obj.put('feeType', transactionId)
            obj.put('transactionId', feeType)
            finance_log.save(obj)
        }
    }

    //获取核心用户手机号
    static void getKeyUserMobile(){
        Long begin = yesTday - DAY_MILLON
        def timeBetween = [$gte: begin, $lt: begin + DAY_MILLON]

        //充值用户
        Set users1 = aggregateUsers(finance_log, [via:[$ne:'Admin'], timestamp: timeBetween], [user_id: '$to_id'])
        //送礼用户
        Set users2 = aggregateUsers(room_cost, ['session.data.xy_star_id': [$ne: null], timestamp: timeBetween], [user_id: '$session._id'])
        // 且在玩游戏用户
        Set users3 = aggregateUsers(user_bet, [user_id:[$ne:null], timestamp: timeBetween], [user_id: '$user_id'])
        /*
               println "充值:"
               List<String> m1 = getMobile(users1.toList())
               println m1

               println "送礼:"
               List<String> m2 = getMobile(users2.toList())
               println m2

               println "玩游戏:"
               List<String> m3 = getMobile(users3.toList())
               println m3

               Set<String> userMobile = new HashSet()
               userMobile.addAll(m1)
               userMobile.addAll(m2)
               userMobile.addAll(m3)
               println "所有手机号:"
               println userMobile
               println userMobile.size()

               Set<Integer> userSet = new HashSet()
               userSet.addAll(users1)
               userSet.addAll(users2)
               userSet.addAll(users3)

               println "所有用户id:"
               println userSet
               println userSet.size()
       */
        println "昨日新增充值用户id+mobile : "
        println getMobileOfUsers(users1.toList())
        println "昨日新增送礼用户id+mobile: "
        println getMobileOfUsers(users2.toList())
        println "昨日新增玩游戏用户id+mobile: "
        println getMobileOfUsers(users3.toList())
    }

    static Set<Integer> aggregateUsers(DBCollection coll, Map match, Map project){
        Set<Integer> userSet = new HashSet()
        def results = coll.aggregate(
                new BasicDBObject('$match', match),
                new BasicDBObject('$project', project),
                new BasicDBObject('$group', [_id: null, users: [$addToSet: '$user_id']])
        ).results()
        results.each { BasicDBObject data ->
            userSet = new HashSet(data.users)
        }
        return userSet.collect{it as Integer}
    }

    static List<String> getMobile(List<Integer> userIds){
        //普通用户
        def tuids = users.find($$(_id: [$in: userIds] , priv:3), $$(tuid:1)).toArray()*.tuid as List<Integer>
        def mobiles = xy_users.find($$(_id:[$in: tuids], mobile:[$ne:null]), $$(mm_no:1,mobile:1)).toArray()*.mobile as List<String>
        return mobiles
    }

    static Map<String,String> getMobileOfUsers(List<Integer> userIds){
        Map<String, String> datas = new HashMap<>();
        def tuids = users.find($$(_id: [$in: userIds] , priv:3, timestamp:[$gte:yesTday - DAY_MILLON]), $$(tuid:1)).toArray()*.tuid as List<Integer>
        xy_users.find($$(_id:[$in: tuids], mobile:[$ne:null]), $$(mm_no:1,mobile:1)).toArray().each {DBObject user ->
            datas.put(user['mm_no'] as String, user['mobile']  as String)
        }
        return datas
    }

    static insetsmelter(){
        smelter.insert([
                        $$("_id":1,"type":1,"value":200,"status":true,"name":"一级导弹","price":5000,"cool_down":600,"min":100),
                        $$("_id":2,"type":1,"value":250,"status":true,"name":"二级导弹","min":150,"price":15000,"cool_down":900),
                        $$("_id":3,"type":1,"value":300,"status":true,"min":200,"name":"三级导弹","price":20000,"cool_down":1200),
                        $$("_id":4,"type":1,"value":350,"status":true,"min":250,"name":"四级导弹","price":25000,"cool_down":1800),
                        $$("_id":5,"type":2,"min":100,"value":200,"status":true,"name":"一级盾牌","price":5000,"cool_down":300),
                        $$("_id":6,"type":2,"value":250,"min":150,"status":true,"name":"二级盾牌","price":15000,"cool_down":450),
                        $$("_id":7,"type":2,"value":300,"min":200,"status":true,"name":"三级盾牌","price":20000,"cool_down":600),
                        $$("_id":8,"type":2,"value":350,"min":250,"status":true,"name":"四级盾牌","price":25000,"cool_down":900),
                        ])
    }

    static initUserInfo(){
        users.updateMulti($$(_id:[$gte:0]), $$('$set':[exp:0,level:0]))
    }
    static void main(String[] args){
        def l = System.currentTimeMillis()
        //recoverFinanceLogToId();
        //getKeyUserMobile();
        //insetsmelter()
        initUserInfo();
        println " cost  ${System.currentTimeMillis() -l} ms".toString()
    }

}
