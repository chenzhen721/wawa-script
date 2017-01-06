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
import com.mongodb.DBCollection
import com.mongodb.DBObject

/**
 *
 * 临时文件
 *
 * date: 13-2-28 下午2:46
 * @author: haigen.xiong@ttpod.com
 */
class InitRoomUsrFilling
{
   //static mongo = new Mongo("127.0.0.1", 10000)
    static mongo  = new Mongo(new com.mongodb. MongoURI('mongodb://10.0.5.32:10000,10.0.5.33:10000,10.0.5.34:10000/?w=1&slaveok=true'))
    static DAY_MILLON = 24 * 3600 * 1000L

    static long zeroMill = new Date().clearTime().getTime()

    static DBCollection room_cost_DB =  mongo.getDB("xylog").getCollection("room_cost")

    static staticRoomCostUsr(Long begin)
    {
        long end = begin + DAY_MILLON
        def res = room_cost_DB.aggregate(
                new BasicDBObject('$match', [timestamp: [$gte: begin, $lt: end]]),
                new BasicDBObject('$project', [_id: '$session._id',cost:'$cost']),
                new BasicDBObject('$group', [_id: '$_id', num: [$sum: '$cost']]),
                new BasicDBObject('$sort', [num:-1]),
                new BasicDBObject('$limit',100))//top N 算法

        String cat = new Date(begin).format("yyyyMMdd")

        Iterator objs = res.results().iterator()
        def list = new ArrayList(100)
        objs.each {row ->
            def cost = row.num as Long
            def uid = row._id as Integer
            if(cost>0&&uid>0)
            {
                def my = new BasicDBObject(_id:"${row._id}_${cat}".toString(), user_id:row._id as Integer,num:row.num,timestamp:begin)
                list.add(my)
            }
        }

        def coll = mongo.getDB("xylog").getCollection("room_cost_usr")
        coll.insert(list)
    }

    //明星获豆日压缩
    static staticRoomCostStar(Long begin)
    {
        long end = begin + DAY_MILLON
        def res = room_cost_DB.aggregate(
                new BasicDBObject('$match', [timestamp: [$gte:begin, $lt: end], type: [$in: ['send_gift', 'song']]]),
                new BasicDBObject('$project', [star_id: '$session.data.xy_star_id', earned: '$session.data.earned']),
                new BasicDBObject('$group', [_id: '$star_id', num: [$sum: '$earned']]),
                new BasicDBObject('$sort', [num:-1]),
                new BasicDBObject('$limit',100))//top N 算法

        Iterator objs = res.results().iterator()
        def list = new ArrayList(100)
        String cat = new Date(begin).format("yyyyMMdd")
        objs.each {row ->
            def cost = row.num as Long
            def uid = row._id as Integer
            if(cost>0&&uid>0)
            {
                list.add(new BasicDBObject(_id:"${row._id}_${cat}".toString(), star_id:row._id as Integer,num:row.num,timestamp:begin))
            }
        }
        def coll = mongo.getDB("xylog").getCollection("room_cost_star")
        coll.insert(list)
    }





    static void main(String[] args)
    {
        long l = System.currentTimeMillis()
        def roomUsrDB =   mongo.getDB("xylog").getCollection("room_cost_usr")
        def list =roomUsrDB.find(null,new BasicDBObject(user_id:1)).sort(new BasicDBObject(timestamp:1)).toArray()
        def usersDB = mongo.getDB("xy").getCollection("users")
        for(DBObject obj : list)
        {
            Integer user_id = obj.get("user_id") as Integer
            def user = usersDB.findOne(new BasicDBObject('_id',user_id),new BasicDBObject(family:1,priv:1))
            def family = user?.get("family")
            if(family)
            {
                def priv = user?.get("priv") as Integer
                if(2 != priv)
                {
                   def family_id = ((Map)family).get("family_id") as Integer
                   if(family_id)
                       roomUsrDB.update(new BasicDBObject('_id',obj.get("_id")),new BasicDBObject('$set',new BasicDBObject(family_id:family_id)))
                }
            }
        }

        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitRoomUsrFilling.class.getSimpleName()} InitRoomUsrFilling----------->:finish "

    }

}