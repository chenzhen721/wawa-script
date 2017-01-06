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
class InitFamilyMemberFilling
{
   // static mongo = new Mongo("127.0.0.1", 10000)
    static mongo  = new Mongo(new com.mongodb. MongoURI('mongodb://10.0.5.32:10000,10.0.5.33:10000,10.0.5.34:10000/?w=1&slaveok=true'))
    static DAY_MILLON = 24 * 3600 * 1000L

    static staticFamilyMemberDay(Long begin)
    {
        Long _end =  begin + DAY_MILLON
        String ymd = new Date(begin).format("yyyyMMdd")
        def l = System.currentTimeMillis()
        mongo.getDB("xy").getCollection("familys").find(new BasicDBObject("status",2),new BasicDBObject("_id",1)).toArray().each
        {
            Integer familyId = it._id as Integer
            println "familyId------>:${familyId}"
            def query = [family_id: familyId,timestamp:[$gte: begin, $lt: _end]]

            def res = mongo.getDB("xylog").getCollection("room_cost").aggregate(
                    new BasicDBObject('$match', query),
                    new BasicDBObject('$project', [_id: '$session._id',cost:'$cost']),
                    new BasicDBObject('$group', [_id: '$_id', num: [$sum: '$cost']]),
                    new BasicDBObject('$sort', [num:-1]),
                    new BasicDBObject('$limit',500) //top N 算法
            )

            Iterator objs = res.results().iterator()
            def memberDB = mongo.getDB("xylog").getCollection("family_member_cost")
            def userDB =  mongo.getDB("xy").getCollection("users")
            objs.each{ row ->
               def user_id = row._id
               def user = userDB.findOne(new BasicDBObject(_id:user_id),new BasicDBObject(priv:1))
               Integer priv = user?.get("priv") as Integer
               def id = user_id + "_" + familyId + "_" + ymd
               def num = row.num as Integer
               if(num>0 && (2!=priv))
               {
                   def incObj = new BasicDBObject(num:row.num)
                   def update = new BasicDBObject(user_id:user_id as Integer,family_id:familyId,timestamp:begin)
                   memberDB.findAndModify(new BasicDBObject('_id',id), null, null, false,
                           new BasicDBObject('$inc':incObj,'$set':update),true, true)
               }
            }
        }
    }



    static void main(String[] args)
    {
        long l = System.currentTimeMillis()
        //201404月
        for(int i=13;i<30;i++)
        {
            StringBuilder  sBegin = new StringBuilder()
            if(i<10)
              sBegin.append("2014040").append(i)
            else
               sBegin.append("201404").append(i)
            println "201404 begin------->:${sBegin.toString()}"
            Long _begin = new SimpleDateFormat("yyyyMMdd").parse(sBegin.toString()).getTime()
            staticFamilyMemberDay(_begin)
        }
        //201405月
        for(int i=1;i<8;i++)
        {
            StringBuilder  sBegin = new StringBuilder()
            if(i<10)
                sBegin.append("2014050").append(i)
            else
                sBegin.append("201405").append(i)
            println "201405 begin------->:${sBegin.toString()}"
            Long _begin = new SimpleDateFormat("yyyyMMdd").parse(sBegin.toString()).getTime()
            staticFamilyMemberDay(_begin)
        }

        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitFamilyMemberFilling.class.getSimpleName()} InitFamilyMemberFilling , cost  ${System.currentTimeMillis() -l} ms"

        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitFamilyMemberFilling.class.getSimpleName()} InitFamilyMemberFilling----------->:finish "

    }

}