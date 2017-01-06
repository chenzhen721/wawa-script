#!/usr/bin/env groovy
package crontab.rank

import com.mongodb.BasicDBObject
@Grapes([
@Grab('org.mongodb:mongo-java-driver:2.14.2'),
@Grab('commons-lang:commons-lang:2.6'),
@Grab('redis.clients:jedis:2.1.0'),
])
import com.mongodb.Mongo
import com.mongodb.MongoURI

/**
 *
 * date: 13-2-28 下午2:46
 * @author: yangyang.cong@ttpod.com
 */
class RankFeather {
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

    static mongo  = new Mongo(new MongoURI(getProperties('mongo.uri','mongodb://192.168.31.246:27017/?w=1') as String))
    static rooms = mongo.getDB('xy').getCollection('rooms')
    static DAY_MILLON = 24 * 3600 * 1000L

   static bulidRank(int day) {

        long now = System.currentTimeMillis()
        long zeroMill = new Date().clearTime().getTime()
        def  query=[timestamp:[$gt: zeroMill - day * DAY_MILLON, $lte: now]]

        def res = mongo.getDB("xylog").getCollection("room_feather_day").aggregate(
                new BasicDBObject('$match',query),
                new BasicDBObject('$project', [_id: '$star_id',num: '$num']),
                new BasicDBObject('$group', [_id: '$_id', num: [$sum: '$num']]),
                new BasicDBObject('$sort', [num:-1]),
                new BasicDBObject('$limit',20) //top N 算法
        )
        return res.results()
    }

    static cats = [day:0,week : 6, month:28]
   static void main(String[] args)
   {
        long l = System.currentTimeMillis()
        long begin = l
        rooms.updateMulti(new BasicDBObject(test: [$ne: true]), new BasicDBObject('$unset', new BasicDBObject('feather_rank':1)))
        cats.each {cat,day->
            Iterable objs = bulidRank(day)
            def list = new ArrayList(20)
            int i = 0
            objs.each {row ->
                if (i++ < 20){
                    list.add(new BasicDBObject(_id:"${cat}_${row._id}".toString(),
                            cat:cat,user_id:row._id as Integer,num:row.num,rank:i,sj:new Date())
                    )
                    if(cat.equals('day')){
                        rooms.update(new BasicDBObject(_id:row._id, test: [$ne: true]), new BasicDBObject('$set', new BasicDBObject('feather_rank':i)))
                    }
                }
            }
            def coll = mongo.getDB("xyrank").getCollection("feather")
            coll.remove(new BasicDBObject("cat",cat))
            if(list.size() > 0)
                coll.insert(list)
        }
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   RankDay,RankWeek,RankMonth, cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)


        //落地定时执行的日志
        l = System.currentTimeMillis()
        def timerName = 'RankFeather'
        Long totalCost = System.currentTimeMillis() - begin
        saveTimerLogs(timerName,totalCost)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  save timer_logs , cost  ${System.currentTimeMillis() - l} ms"

        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${RankFeather.class.getSimpleName()}, cost  ${System.currentTimeMillis() -begin} ms"

    }

    //落地定时执行的日志
    private static saveTimerLogs(String timerName,Long totalCost)
    {
        def timerLogsDB =  mongo.getDB("xyrank").getCollection("timer_logs")
        def tmp = System.currentTimeMillis()
        def id = timerName + "_"  + new Date().format("yyyyMMdd")
        def update = new BasicDBObject(timer_name:timerName,cost_total:totalCost,cat:'hour',unit:'ms',timestamp:tmp)
        timerLogsDB.findAndModify(new BasicDBObject('_id',id), null, null, false,new BasicDBObject('$set',update),true, true)
    }

}