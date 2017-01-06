package tmp.one

import com.mongodb.BasicDBObject
import com.mongodb.Mongo
import redis.clients.jedis.Jedis

import java.text.SimpleDateFormat

def redis = new Jedis('10.0.5.201')
def mongo = new Mongo("10.0.5.33",10000)
//mongo.getDB('xylog').getCollection('day_login').find(new BasicDBObject("uid",'A00000303EA389')).each {
//    redis.srem("room:1240248:users",it.get('user_id').toString())
//    println it
//
//    //redis.publish("USERchannel:${it.get('user_id')}",'{"action":"sys.freeze"}')
//
//}
def sf = new SimpleDateFormat("yyyy-MM-dd")

def begin = sf.parse("2013-07-01").getTime()
def end = sf.parse("2013-08-01").getTime()
System.setOut(new PrintStream(new File("d:/07.txt")))
mongo.getDB('xy_admin').getCollection('finance_log').aggregate(
        new BasicDBObject('$match', [via: "Ipay",timestamp: [$gte: begin,$lt:end ]]),
        new BasicDBObject('$project', [_id: '$user_id',cny:'$cny']),
        new BasicDBObject('$group', [_id: '$_id',cny: [$sum: '$cny'],times:[$sum: 1]]),
        new BasicDBObject('$sort', [cny:-1,times:1])
).results().iterator().each {
    println it
}