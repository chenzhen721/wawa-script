#!/usr/bin/env groovy
package crontab.tmp

import com.mongodb.BasicDBObject
import com.mongodb.DBCollection
@Grapes([
@Grab('org.mongodb:mongo-java-driver:2.14.2'),
@Grab('commons-lang:commons-lang:2.6'),
@Grab('redis.clients:jedis:2.1.0'),
@Grab(group = 'net.sf.json-lib', module = 'json-lib', version = '2.3', classifier = 'jdk15')
]) import com.mongodb.Mongo

import java.text.SimpleDateFormat
import com.mongodb.DBObject

/**
 *
 * 临时文件
 *
 * date: 13-2-28 下午2:46
 * @author: haigen.xiong@ttpod.com
 */
class InitRoomSongFilling
{
   // static mongo = new Mongo("127.0.0.1", 10000)
    static mongo  = new Mongo(new com.mongodb. MongoURI('mongodb://10.0.5.32:10000,10.0.5.33:10000,10.0.5.34:10000/?w=1&slaveok=true'))
    static DAY_MILLON = 24 * 3600 * 1000L
    static DBCollection room_cost_history_DB =  mongo.getDB("xylog_history").getCollection("room_cost_song_tmp")
    static DBCollection room_cost_DB =  mongo.getDB("xylog").getCollection("room_cost")

    //每日主播收到点歌类型的数据压缩
    static void staticRoomSongDayStar(Long begin,Long starId, DBCollection room_cost_DB)
    {
        def type = 'song'
        Long end = begin + DAY_MILLON
        def res = room_cost_DB.aggregate(
                new BasicDBObject('$match', ['session.data.xy_star_id':starId,timestamp: [$gte: begin, $lt: end], type: type]),
                new BasicDBObject('$project', [star_id: '$session.data.xy_star_id']),
                new BasicDBObject('$group', [_id: '$star_id', count: [$sum: 1]]))

        Iterable objs =  res.results()
        def day = new Date(begin).format("yyyyMMdd")
        def coll = mongo.getDB("xylog").getCollection("room_cost_day_star")
        objs.each {row ->
            def star_id = row._id as Integer
            def count = row.count as Integer
            println "star_id----------->:${star_id}"
            println "song count----------->:${count}"
            def update = new BasicDBObject(count:count)
            def id = star_id+"_" + day + "_" + type
            println "id----------->:${id}"
            coll.update(new BasicDBObject('_id',id),new BasicDBObject('$set',update))
        }
    }

    static void main(String[] args)
    {
        def l = System.currentTimeMillis()
        def lbegin = l
        def  roomCost =  mongo.getDB("xylog_history").getCollection("room_cost_history")
        def songLst =  roomCost.find(new BasicDBObject(type:'song')).toArray()
        room_cost_history_DB.insert(songLst)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   room_cost_song_tmp insert, cost  ${System.currentTimeMillis() -l} ms"

        l = System.currentTimeMillis()
        def list = mongo.getDB("xylog").getCollection("room_cost_day_star").find(new BasicDBObject(timestamp:['$lt': 1398334148405]),new BasicDBObject(star_id:1,timestamp:1)).sort(new BasicDBObject(timestamp:1)).toArray()
        for(DBObject obj : list)
        {
           Long  begin = obj.get("timestamp") as Long
           Integer starId = obj.get("star_id") as Integer
           staticRoomSongDayStar(begin,starId,room_cost_history_DB)
        }
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   room_cost_history_DB, cost  ${System.currentTimeMillis() -l} ms"

        Thread.sleep(1000L)

        def list2 = mongo.getDB("xylog").getCollection("room_cost_day_star").find(new BasicDBObject(timestamp:['$gte': 1398334148405]),new BasicDBObject(star_id:1,timestamp:1)).sort(new BasicDBObject(timestamp:1)).toArray()
        for(DBObject obj : list2)
        {
            Long  begin = obj.get("timestamp") as Long
            Integer starId = obj.get("star_id") as Integer
            staticRoomSongDayStar(begin,starId,room_cost_DB)
        }
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   room_cost, cost  ${System.currentTimeMillis() -l} ms"

        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   InitRoomSongFilling, cost  ${System.currentTimeMillis() -lbegin} ms"

    }
}