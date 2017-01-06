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
import redis.clients.jedis.Jedis


def mongo = new Mongo("192.168.1.22",27017)

def rooms = mongo.getDB("xy").getCollection("rooms")

def redis = new Jedis("192.168.1.22")


long delay = 30 * 1000L

while (true){
    long l = System.currentTimeMillis()
    int i = 0
    rooms.find().toArray().each { dbo ->
        i++
        Integer room_id = dbo.get("_id") as Integer
        rooms.update(new BasicDBObject("_id",room_id),
                new BasicDBObject('$set',new BasicDBObject("visiter_count",
                        redis.scard("room:${room_id}:users".toString())
                ))
        )
    }

    println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  update ${i} rows , cost  ${System.currentTimeMillis() -l} ms"

    Thread.sleep(delay)
}

