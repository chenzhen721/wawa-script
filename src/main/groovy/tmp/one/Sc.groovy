package tmp.one

import com.mongodb.BasicDBObject
import com.mongodb.Mongo
import redis.clients.jedis.Jedis
def redis = new Jedis('10.0.5.201',6380)
def mongo = new Mongo("10.0.5.33",10000)
//mongo.getDB('xylog').getCollection('day_login').find(new BasicDBObject("uid",'A00000303EA389')).each {
//    redis.srem("room:1240248:users",it.get('user_id').toString())
//    println it
//
//    //redis.publish("USERchannel:${it.get('user_id')}",'{"action":"sys.freeze"}')
//
//}

mongo.getDB('xy_admin').getCollection('finance_log').distinct('user_id',new BasicDBObject('cny',[$gte:10])).each {
    redis.sadd('tenyuan_userset',it.toString())
}
println 'OK'