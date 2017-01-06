#!/usr/bin/env groovy

import com.mongodb.BasicDBObject
import com.mongodb.DBCollection
import com.mongodb.DBObject
@Grapes([
@Grab('org.mongodb:mongo-java-driver:2.14.2'),
@Grab('commons-lang:commons-lang:2.6'),
@Grab('redis.clients:jedis:2.1.0'),
])
import com.mongodb.Mongo
import  redis.clients.jedis.Jedis
import  redis.clients.jedis.Tuple

def String jedis_host = "192.168.1.100"
def redis = new Jedis(jedis_host,6379)

println redis.get("kgs:users:nextId")
redis.set("kgs:users:nextId", "0")
Set<Tuple> ranks = redis.zrevrangeWithScores("race:star:rank:2",0,31)
ranks.each {Tuple it ->
    println it.getElement() + ":" + it.getScore().toLong()
}
redis.expire("medal:progress:1201589:124", 192365)
redis.srem("race:star:wins:2",'1789110','1201323')
redis.sadd("race:star:wins:2", '1202471','1202507')
redis.zadd("race:star:rank:3", 266, '3454077')
redis.del("medal:progress:9497793:121")
redis.del("sign:chest:prettnum:list")
def list = [100010,100100,100101,100222,107408,115717,121123,121343,123132,131445,144564,147741,149788,159951,165165,174175,177595,179848,198366,198441,198478,198798,200088,201234,201577,209708,224488,225588,233900,233904,233999,258852,300000,311113,335577,336699,341314,363699,369963,404849,408050,448844,464488,465465,465564,471980,478744,479847,480845,484585,488884,494811,498041,498494,498984,517517,520203,520285,520465,520480,520581,520681,520798,525252,550055,556655,567765,641685,655556,663333,666611,668844,685644,718415,741879,747411,749842,776655,776677,778877,781005,787848,789070,789797,798879,800054,809855,841985,842248,855668,870807,879784,879787,879789,879879,881111,882222,883333,885533,885614]
list.each {
    redis.rpush("sign:chest:prettnum:list", it.toString())
}
println redis.llen("sign:chest:prettnum:list")

//删除守护榜
Set<String> guard_cars = redis.keys("room:guarder:rank:week:*".toString())
guard_cars.each {String key->
    println key

}
Set<Tuple> guard_ranks =  redis.zrevrangeWithScores("room:guarder:rank:week:34:1204399",0,10)
guard_ranks.each {Tuple it ->
    println it.getElement() + ":" + it.getScore().toLong()
}
int total_count = 0;
Set<String> hushis = redis.keys("hushi_2015:*".toString())
hushis.each {String key->

    if(key.contains(':total')){
        long total = redis.get(key) as Long
        Integer lettory_count = (total / 191200) as Integer
        total_count += lettory_count
        println redis.ttl(key)
    }
}
println total_count

redis.hincrBy("qixi_20150820:1205983:total", "47", 990);
/*def Mongo  = new Mongo(new com.mongodb. MongoURI('mongodb://10.0.5.32:10000,10.0.5.33:10000,10.0.5.34:10000/?w=1&slaveok=true'))
def rooms =Mongo.getDB("xy").getCollection("rooms")

rooms.find(new BasicDBObject('live',false)).each {
    liveRedis.keys("live:${it._id}:*".toString()).each{liveRedis.del(it)}
}*/

//
//liveRedis0.keys("*").each {String key->
//    Set<Tuple> set =  liveRedis1.zrangeWithScores(key,0,-1)
//    def map = new HashMap<Double,String>(set.size())
//
//    set.each {Tuple tu->
//        map.put(tu.getScore(),tu.getElement())
//    }
//
//    liveRedis0.zadd(key,map)
//}
