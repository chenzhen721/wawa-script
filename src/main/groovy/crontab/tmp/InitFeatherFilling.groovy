#!/usr/bin/env groovy
package crontab.tmp

import com.mongodb.BasicDBObject
import com.mongodb.DBObject
@Grapes([
@Grab('org.mongodb:mongo-java-driver:2.14.2'),
@Grab('commons-lang:commons-lang:2.6'),
@Grab('redis.clients:jedis:2.1.0'),
@Grab(group = 'net.sf.json-lib', module = 'json-lib', version = '2.3', classifier = 'jdk15')
]) import com.mongodb.Mongo
import java.text.SimpleDateFormat

/**
 *
 * 临时文件
 *
 * date: 13-2-28 下午2:46
 * @author: haigen.xiong@ttpod.com
 */
class InitFeatherFilling
{
    //static mongo = new Mongo("127.0.0.1", 10000)
    static mongo  = new Mongo(new com.mongodb. MongoURI('mongodb://10.0.5.32:10000,10.0.5.33:10000,10.0.5.34:10000/?w=1&slaveok=true'))
    static DAY_MILLON = 24 * 3600 * 1000L
    static long zeroMill = new Date().clearTime().getTime()




    //DelRoomfeather 删除 room_feather 数据
    static del_room_feather(long begin,int cat)
    {
        long end = begin + cat * DAY_MILLON
        long zeroMill = new Date().clearTime().getTime()
        def room_feather= mongo.getDB('xylog').getCollection('room_feather')
        room_feather.remove(new BasicDBObject(timestamp:[$gte:begin,$lt:end]))
    }

    static void main(String[] args)
    {
        long l = System.currentTimeMillis()
        long _begin0 = new SimpleDateFormat("yyyyMMdd").parse("20140301").getTime()
        del_room_feather(_begin0,10)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitFeatherFilling.class.getSimpleName()} del_room_feather1, cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)



        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitFeatherFilling.class.getSimpleName()} InitFeatherFilling----------->:finish "

    }

}