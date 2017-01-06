#!/usr/bin/env groovy
package crontab.tmp

import com.mongodb.BasicDBObject
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
class InitSingTmp
{
   // static mongo = new Mongo("127.0.0.1", 10000)
    static mongo  = new Mongo(new com.mongodb. MongoURI('mongodb://10.0.5.32:10000,10.0.5.33:10000,10.0.5.34:10000/?w=1&slaveok=true'))
    static DAY_MILLON = 24 * 3600 * 1000L
    static long zeroMill = new Date().clearTime().getTime()

    static initGiftsLog()
    {
        def giftLogsDB = mongo.getDB('xy_sing').getCollection('gift_logs')
        def roundDB = mongo.getDB('xy_sing').getCollection('rounds')
        List<DBObject> giftLogs = giftLogsDB.find(new BasicDBObject(award:[$exists:true]),new BasicDBObject(_id:1,award:1,award_total:1,timestamp:1)).toArray()
        for(DBObject log:giftLogs)
        {
           String id = log.get("_id") as String

           def  roundObj =  roundDB.findOne(new BasicDBObject('_id':id),new BasicDBObject('pid':1))
           def pid =  roundObj?.get("pid") as String
           def zhuRound = giftLogsDB.findOne(new BasicDBObject(_id:pid),new BasicDBObject(_id:1,cost:1,count:1))
           if(null != zhuRound)
           {
               giftLogsDB.update(new BasicDBObject('_id',pid),new BasicDBObject('$set', new BasicDBObject(pid:pid,award:log.get("award"),award_total: log.get("award_total"),timestamp:log.get("timestamp"))))
           }
        }
    }

    static initPointsLog()
    {
        def pointsLogsDB = mongo.getDB('xy_sing').getCollection('point_logs')
        def roundDB = mongo.getDB('xy_sing').getCollection('rounds')
        List<DBObject> pointsLogs = pointsLogsDB.find(new BasicDBObject(rid:[$exists:true]),new BasicDBObject(_id:1,rid:1)).toArray()
        for(DBObject log :pointsLogs)
        {
            String rid = log.get("rid") as String

            def  roundObj =  roundDB.findOne(new BasicDBObject('_id',rid),new BasicDBObject('pid':1))

            def pid =  roundObj?.get("pid") as String

            pointsLogsDB.update(new BasicDBObject('_id', log.get("_id")),new BasicDBObject('$set', new BasicDBObject(pid:pid)))
        }
    }


    static void main(String[] args)
    {
        long l = System.currentTimeMillis()

        initGiftsLog()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitSingTmp.class.getSimpleName()} initGiftsLog, cost  ${System.currentTimeMillis() -l} ms"
        //Thread.sleep(1000L)

       /* initPointsLog()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitSingTmp.class.getSimpleName()} initPointsLog, cost  ${System.currentTimeMillis() -l} ms"*/

    }

}