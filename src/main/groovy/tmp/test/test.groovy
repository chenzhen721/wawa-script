#!/usr/bin/env groovy
package crontab.test

/**
 * 定时更新房间的在线人数
 *
 * date: 13-2-28 下午2:46
 * @author: yangyang.cong@ttpod.com
 */
@Grapes([
@Grab('org.mongodb:mongo-java-driver:2.14.2'),
@Grab('commons-lang:commons-lang:2.6'),
@Grab('redis.clients:jedis:2.1.0'),
])
import com.mongodb.Mongo
import com.mongodb.BasicDBObject
import org.apache.commons.lang.StringUtils
import redis.clients.jedis.Jedis


def mongo = new Mongo("192.168.1.22",27017)

def rooms = mongo.getDB("xy").getCollection("rooms")

def redis = new Jedis("192.168.1.22")


long delay = 30 * 1000L
mongo.getDB("xylog").getCollection("room_cost").aggregate(
        new BasicDBObject('$match',[timestamp:[$gt : 1362290558300, $lte : 1362290635580],type:[$in:['send_gift','song']]]),
        new BasicDBObject('$project',[star_id:'$session.data.xy_star_id',earned:'$session.data.earned']),
        new BasicDBObject('$group',[_id:'$star_id',earned:[$sum : '$earned']])
)
def query = new BasicDBObject(timestamp:[$gte:1366992000000])
query['user_id'] = 1240134
Double cny_total = 0
def res = mongo.getDB("xy_admin").getCollection('stat_brokers').find(query,new BasicDBObject('sale.cny',1))
        .toArray()
if(! res.isEmpty()){
    cny_total   +=( res.sum {it.getAt('sale')?.getAt('cny')?:0} as Number)
}
println cny_total
def arr = mongo.getDB("xy_admin").getCollection('finance_log').find(
        new BasicDBObject(timestamp: [$gte:new Date().clearTime().getTime()],ext:'1240134'),
        new BasicDBObject('cny',1)).toArray()
if(! arr.isEmpty()){
    cny_total += (arr.sum {it.getAt('cny')} as Number)
}
println cny_total