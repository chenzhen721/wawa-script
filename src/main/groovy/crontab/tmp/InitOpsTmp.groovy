#!/usr/bin/env groovy
package crontab.tmp

import com.mongodb.BasicDBObject
@Grapes([
@Grab('org.mongodb:mongo-java-driver:2.14.2'),
@Grab('commons-lang:commons-lang:2.6'),
@Grab('redis.clients:jedis:2.1.0'),
]) import com.mongodb.Mongo
import com.mongodb.DBObject

/**
 *
 *
 * date: 13-2-28 下午2:46
 * @author: yangyang.cong@ttpod.com
 */
class InitOpsTmp {

    //static mongo = new Mongo("127.0.0.1", 10000)
    static mongo  = new Mongo(new com.mongodb. MongoURI('mongodb://10.0.5.32:10000,10.0.5.33:10000,10.0.5.34:10000/?w=1&slaveok=true'))
    static DAY_MILLON = 24 * 3600 * 1000L
    static long zeroMill = new Date().clearTime().getTime()

    static ops() {
        def q = new BasicDBObject('session._id': [$in: ["1",
                                                        "12868",
                                                        "12901",
                                                        "12904",
                                                        "12905",
                                                        "12906",
                                                        "12907",
                                                        "12910",
                                                        "12915",
                                                        "12916",
                                                        "12917"]], timestamp: [$gte: 1383235200000, $lt: 1396281600000], type: [$in: [
                'finance_add',
                'finance_cut_coin']])
        def clist = mongo.getDB("xy_admin").getCollection("ops").
                find(q, null)
                .toArray()
        //  邮箱	注册IP地址	上一次登陆IP地址	昵称	时间	谁改的

        for (DBObject obj : clist) {
            def data = (Map) obj.get("data")
            def user_id = data?.get("user_id") as Integer
            def coin = data?.get("coin") as Integer

            def user = mongo.getDB("xy").getCollection("users").findOne(new BasicDBObject("_id", user_id), new BasicDBObject(user_name: 1, nick_name: 1))
            def user_name = user?.get("user_name")
            def nick_name = user?.get("nick_name")
            //"user_id": NumberInt(9827694)
            def registerLog = mongo.getDB("xylog").getCollection("day_login").find(new BasicDBObject(user_id: user_id), new BasicDBObject(ip: 1)).sort(new BasicDBObject(timestamp: 1)).limit(1).toArray()
            def register_ip = ""
            for (DBObject log : registerLog) {
                register_ip = log.get("ip")
            }

            def dayLog = mongo.getDB("xylog").getCollection("day_login").find(new BasicDBObject(user_id: user_id), new BasicDBObject(ip: 1)).sort(new BasicDBObject(timestamp: -1)).limit(1).toArray()
            def login_ip = ""
            for (DBObject log : dayLog) {
                login_ip = log.get("ip")
            }
            def ops = (Map) obj.get("session")
            def ops_Id = ops?.get("_id")
            def ops_name = ops?.get("name")
            def tmp = obj.get("timestamp") as Long
            def today = new Date(tmp).format("yyyy-MM-dd HH:mm:ss")
            def type = obj.get("type")

            def myObj = user_name + "|" + register_ip + "|" + login_ip + "|" + nick_name + "|" + today + "|" + ops_Id + "|" + ops_name + "|" + "|" + coin + "|" + type

            println "${myObj}"
        }
    }

    static roomCost()
    {
        def query = new BasicDBObject(type: "send_gift",timestamp:[$gte:1401897600000],
                'session.data.xy_star_id':[$exists:true],
                'session.data.xy_user_id':[$exists:true])
        def roomCostDB = mongo.getDB("xylog").getCollection("room_cost")
        List<DBObject> costLst = roomCostDB.find(query, new BasicDBObject('session.data.xy_star_id':1,room:1)).toArray()

        for(DBObject obj: costLst)
        {
            def _id =  obj.get("_id")
            def room = obj.get("room") as Integer
            def xy_star_id =((Map)((Map)obj.get("session"))?.get("data"))?.get("xy_star_id") as Integer
            if(room.intValue() != xy_star_id.intValue())
                roomCostDB.update(new BasicDBObject('_id',_id),new BasicDBObject('$unset',new BasicDBObject('session.data.xy_star_id',1)))
        }

    }

    static void main(String[] args)
    {
        long l = System.currentTimeMillis()

        ops();

        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitOpsTmp.class.getSimpleName()},ops cost  ${System.currentTimeMillis() -l} ms"

        roomCost()

        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitOpsTmp.class.getSimpleName()},roomCost cost  ${System.currentTimeMillis() -l} ms"

        roomCost()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitOpsTmp.class.getSimpleName()},roomCost cost  ${System.currentTimeMillis() -l} ms"

    }

}