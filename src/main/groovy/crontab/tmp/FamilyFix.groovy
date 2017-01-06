#!/usr/bin/env groovy
import com.mongodb.DBCollection
import com.mongodb.DBObject
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
])
import com.mongodb.MongoURI
import groovy.json.JsonSlurper
import com.mongodb.BasicDBObject
import com.mongodb.Mongo
import redis.clients.jedis.Jedis
/**
 *家族数据修复
 */
class FamilyFix {
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
    static final String live_jedis_host = getProperties("live_jedis_host", "192.168.31.249")

    static final Integer main_jedis_port = getProperties("main_jedis_port",6379) as Integer
    static final Integer chat_jedis_port = getProperties("chat_jedis_port",6379) as Integer
    static final Integer live_jedis_port = getProperties("live_jedis_port",6379) as Integer

    static mongo  = new Mongo(new MongoURI(getProperties('mongo.uri','mongodb://192.168.31.249:27017/?w=1') as String))
    static historyMongo = new Mongo(new MongoURI(getProperties('mongo_history.uri', 'mongodb://192.168.31.246:27017/?w=1') as String))
    static  historyDB = historyMongo.getDB('xylog_history')
    static mainRedis = new Jedis(jedis_host,main_jedis_port)



    def final Long DAY_MILL = 24*3600*1000L
    static DAY_MILLON = 24 * 3600 * 1000L
    static apply = mongo.getDB('xy_admin').getCollection('applys')
    static members = mongo.getDB('xy_family').getCollection('members')
    static users = mongo.getDB('xy').getCollection('users')
    static rooms = mongo.getDB('xy').getCollection('rooms')
    static DBCollection room_cost =  mongo.getDB("xylog").getCollection("room_cost")
    static long zeroMill = new Date().clearTime().getTime()
    static Long yesTday = zeroMill - DAY_MILLON

    static def _begin =  Date.parse("yyyy-MM-dd HH:mm" ,"2016-02-29 00:00").getTime()
    static def _end =  Date.parse("yyyy-MM-dd HH:mm" ,"2016-03-07 00:00").getTime()

    static family_member_last_cost_fix(){
        def cursor = members.find(new BasicDBObject(last_week:[$lte:0]), //30天内开播的主播
                new BasicDBObject("fid" : 1, "uid":1)).batchSize(5000)
        while (cursor.hasNext()) {
            def member = cursor.next()
            String uid = member.uid as String
            Integer fid = member.fid as Integer
            String _id = member._id as String
            //计算上周用户消费总额
            Long cost = getUserCostTotal(uid, fid)
            println "uid : ${uid}  fid : ${fid}   cost : ${cost}"
            if(cost > 0){
                members.update(new BasicDBObject(_id:_id),
                        new BasicDBObject('$set',new BasicDBObject('last_week',cost)), false, false)
            }
        }
    }



    static Long getUserCostTotal(String userId, Integer fid){
        def res = room_cost.aggregate(
                new BasicDBObject('$match', ['session._id':userId,'family_id': fid,
                                             timestamp: [$gte: _begin, $lt: _end]]),
                new BasicDBObject('$project', [_id: '$session._id',cost:'$star_cost']),
                new BasicDBObject('$group', [_id: '$_id', num: [$sum: '$cost']]),
                new BasicDBObject('$sort', [num:-1]),
                new BasicDBObject('$limit',1) //top N 算法
        )
        Iterable objs = res.results()
        Long total = 0l;
        objs.each { row ->
            total = row.num
        }
        return total
    }


    static void main(String[] args)
    {
        long l = System.currentTimeMillis()
        family_member_last_cost_fix()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   FamilyFix, cost  ${System.currentTimeMillis() -l} ms"
    }



}