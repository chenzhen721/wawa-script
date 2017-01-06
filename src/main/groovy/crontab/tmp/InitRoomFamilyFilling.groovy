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

/**
 *
 * 临时文件
 *
 * date: 13-2-28 下午2:46
 * @author: haigen.xiong@ttpod.com
 */
class InitRoomFamilyFilling
{
    //static mongo = new Mongo("127.0.0.1", 10000)
    static mongo  = new Mongo(new com.mongodb. MongoURI('mongodb://10.0.5.32:10000,10.0.5.33:10000,10.0.5.34:10000/?w=1&slaveok=true'))
    static DAY_MILLON = 24 * 3600 * 1000L

    static staticRoomCostFamilyDay(Long begin)
    {
        Long _end =  begin + DAY_MILLON
        String ymd = new Date(begin).format("yyyyMMdd")
        def l = System.currentTimeMillis()
        mongo.getDB("xy").getCollection("familys").find(new BasicDBObject("status",2),new BasicDBObject("_id",1)).toArray().each
        {
            Integer familyId = it._id as Integer
            def query = new BasicDBObject(family_id: familyId,timestamp:[$gte: begin, $lt: _end])

            def costList = mongo.getDB("xylog").getCollection("room_cost").
                    find(query,new BasicDBObject(cost:1))
                    .toArray()
            def cost = costList.sum {it.cost?:0} as Long
            def id = familyId + "_" + ymd
            if(cost>0)
            {
                def update = new BasicDBObject(num:cost,family_id:familyId,timestamp:begin)
                def memberDB = mongo.getDB("xylog").getCollection("room_cost_family")
                memberDB.findAndModify(new BasicDBObject('_id',id), null, null, false,
                        new BasicDBObject('$set':update),true, true)
            }
        }
    }



    static void main(String[] args)
    {
        long l = System.currentTimeMillis()
        //201405月
        for(int i=4;i<31;i++)
        {
            StringBuilder  sBegin = new StringBuilder()
            if(i<10)
              sBegin.append("2014050").append(i)
            else
               sBegin.append("201405").append(i)
            println "201405 begin------->:${sBegin.toString()}"
            Long _begin = new SimpleDateFormat("yyyyMMdd").parse(sBegin.toString()).getTime()
            staticRoomCostFamilyDay(_begin)
        }
        //201406月
        for(int i=1;i<5;i++)
        {
            StringBuilder  sBegin = new StringBuilder()
            if(i<10)
                sBegin.append("2014060").append(i)
            else
                sBegin.append("201406").append(i)
            println "201406 begin------->:${sBegin.toString()}"
            Long _begin = new SimpleDateFormat("yyyyMMdd").parse(sBegin.toString()).getTime()
            staticRoomCostFamilyDay(_begin)
        }

        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitRoomFamilyFilling.class.getSimpleName()} InitRoomFamilyFilling , cost  ${System.currentTimeMillis() -l} ms"

        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitRoomFamilyFilling.class.getSimpleName()} InitRoomFamilyFilling----------->:finish "

    }

}