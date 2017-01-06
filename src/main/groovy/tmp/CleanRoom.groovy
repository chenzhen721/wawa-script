package tmp

import com.mongodb.BasicDBObject
import com.mongodb.Mongo
import redis.clients.jedis.Jedis
def redis = new Jedis('10.0.5.201')
def mongo = new Mongo("10.0.5.33",10000)
//mongo.getDB('xylog').getCollection('day_login').find(new BasicDBObject("uid",'A00000303EA389')).each {
//    redis.srem("room:1240248:users",it.get('user_id').toString())
//    println it
//
//    //redis.publish("USERchannel:${it.get('user_id')}",'{"action":"sys.freeze"}')
//
//}

def edits = mongo.getDB('xylog').getCollection('room_edit')
def ids = new ArrayList(500)
mongo.getDB('xy').getCollection('rooms').find(new BasicDBObject("live",false)).each {room->
    ids << room.get('_id')
}
    println edits.remove(new BasicDBObject(type:'live_on',room:[$in:ids],etime:null)).getN()

    //redis.publish("USERchannel:${it.get('user_id')}",'{"action":"sys.freeze"}')

//}