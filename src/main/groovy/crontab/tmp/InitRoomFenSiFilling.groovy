#!/usr/bin/env groovy
package crontab.tmp

import com.mongodb.BasicDBObject
@Grapes([
@Grab('org.mongodb:mongo-java-driver:2.14.2'),
@Grab('commons-lang:commons-lang:2.6'),
@Grab('redis.clients:jedis:2.1.0'),
@Grab(group = 'net.sf.json-lib', module = 'json-lib', version = '2.3', classifier = 'jdk15')
]) import com.mongodb.Mongo
import com.mongodb.MongoURI

import java.text.SimpleDateFormat

/**
 *
 * 临时文件
 *
 */
class InitRoomFenSiFilling
{
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
    static mongo  = new Mongo(new MongoURI(getProperties('mongo.uri','mongodb://192.168.31.249:27017/?w=1') as String))
    static fenSiDB = mongo.getDB("xylog").getCollection("room_fensi_cost")
    static DAY_MILLON = 24 * 3600 * 1000L

    static staticRoomFenSiDay(List stars, long zeroMill)
    {
        Long yesTday = zeroMill - DAY_MILLON
        String YMD = new Date(yesTday).format("yyyyMMdd")
        stars.each
                {

                    Integer roomId = it._id as Integer
                    def query = ['session.data.xy_star_id': roomId,timestamp:[$gte: yesTday, $lt: zeroMill]]

                    def res = mongo.getDB("xylog").getCollection("room_cost").aggregate(
                            new BasicDBObject('$match', query),
                            new BasicDBObject('$project', [_id: '$session._id',earned:'$session.data.earned',cost:'$star_cost']),
                            new BasicDBObject('$group', [_id: '$_id', num: [$sum: '$earned'], cost: [$sum: '$cost']]),
                            new BasicDBObject('$sort', [num:-1]),
                            new BasicDBObject('$limit',1000) //top N 算法
                    )

                    Iterator objs = res.results().iterator()
                    if(objs.hasNext()){
                        def obj = objs.next()
                        println obj
                    }
                    //fenSiDB.remove(new BasicDBObject(room:roomId,timestamp:[$gt:yesTday,$lt:zeroMill])).getN()
                    objs.each {row ->
                        def user_id = row._id
                        if(user_id)
                        {
                            def id = user_id + "_" + roomId + "_" + YMD
                            def update = new BasicDBObject(user_id:user_id as Integer,num:row.cost, bean:row.num,room:roomId,timestamp:yesTday)
                            fenSiDB.findAndModify(new BasicDBObject('_id',id), null, null, false,
                                    new BasicDBObject('$set',update),true, true)
                        }
                    }
                }
    }

    static staticFamilyRoomFenSiDay(List roomslst, long zeroMill)
    {
        Long yesTday = zeroMill - DAY_MILLON
        String today = new Date(yesTday).format("yyyyMMdd")
        roomslst.each{
            Integer roomId = it._id as Integer
            def query = ['session.data.xy_star_id': roomId,timestamp:[$gte: yesTday, $lt: zeroMill]]
            //家族房间粉丝消费
            Integer room_family_id = it?.family_id as Integer
            if(room_family_id != null) {
                query = ['room_family_id': room_family_id, timestamp: [$gte: yesTday, $lt: zeroMill]]
            }
            def res = mongo.getDB("xylog").getCollection("room_cost").aggregate(
                    new BasicDBObject('$match', query),
                    new BasicDBObject('$project', [_id: '$session._id',earned:'$session.data.earned',cost:'$star_cost']),
                    new BasicDBObject('$group', [_id: '$_id', num: [$sum: '$earned'], cost: [$sum: '$cost']]),
                    new BasicDBObject('$sort', [num:-1]),
                    new BasicDBObject('$limit',1000) //top N 算法
            )
            Iterator objs =   res.results().iterator()
            objs.each {row ->
                def user_id = row._id
                if(user_id)
                {
                    def id = user_id + "_" + roomId + "_" + today
                    def update = new BasicDBObject(user_id:user_id as Integer,num:row.cost, bean:row.num,room:roomId,timestamp:yesTday)
                    fenSiDB.findAndModify(new BasicDBObject('_id',id), null, null, false,
                            new BasicDBObject('$set':update),true, true)
                }
            }

        }
    }

    //恢复粉丝背包送礼
    static staticAddRoomFenSiDay(List stars, Long begin)
    {
        Long _end =  begin + DAY_MILLON
        String ymd = new Date(begin).format("yyyyMMdd")
        stars.each
                {

                    Integer roomId = it._id as Integer
                    println "roomId------>:${roomId}"
                    def query = ['session.data.xy_star_id': roomId, 'cost': 0, 'star_cost':null,'type':"send_gift",
                                 timestamp:[$gte: begin, $lt: _end]]

                    def res = mongo.getDB("xylog").getCollection("room_cost").aggregate(
                            new BasicDBObject('$match', query),
                            new BasicDBObject('$project', [_id: '$session._id',earned:'$session.data.earned']),
                            new BasicDBObject('$group', [_id: '$_id', num: [$sum: '$earned']]),
                            new BasicDBObject('$sort', [num:-1]),
                            new BasicDBObject('$limit',500) //top N 算法
                    )

                    Iterator objs = res.results().iterator()

                    def fenSiDB = mongo.getDB("xylog").getCollection("room_fensi_cost")
                    objs.each{ row ->
                        def user_id = row._id
                        def id = user_id + "_" + roomId + "_" + ymd
                        Long earned = (row.num ?: 0) as Long
                        if(earned>0){
                            Long bag_cost =  (earned / 0.4) as Long
                            fenSiDB.update(new BasicDBObject('_id',id), new BasicDBObject('$inc',new BasicDBObject(num:bag_cost)), true,false)
                        }
                    }

                }
    }


    static recoverFamilyRoomCost(List roooms){
        mongo.getDB("xylog").getCollection("room_cost")
        roooms.each
        {
            Integer roomId = it._id as Integer
            println "roomId------>:${roomId}"
            Integer room_family_id = it?.family_id as Integer
            if(room_family_id != null) {
                mongo.getDB("xylog").getCollection("room_cost").updateMulti(new BasicDBObject(room:roomId), new BasicDBObject('$set', new BasicDBObject(room_family_id:roomId)))
            }

        }
    }

    static void main(String[] args)
    {
        long l = System.currentTimeMillis()
        //fenSiDB.remove(new BasicDBObject())
        def timeLimit = new BasicDBObject(timestamp:[$gte:l - 60*DAY_MILLON],pic_url:[$exists:true], type:2)
        def family_rooms = mongo.getDB("xy").getCollection("rooms").find(timeLimit,new BasicDBObject("live_id":1,"family_id":1)).toArray()
        def rooms = mongo.getDB("xy").getCollection("rooms").find(new BasicDBObject(timestamp:[$gte:l - 30*DAY_MILLON],pic_url:[$exists:true], type:1),
                        new BasicDBObject("live_id":1,"family_id":1)).toArray()
        //恢复家族房间room room_family_id 字段
        //recoverFamilyRoomCost(family_rooms);

        //恢复粉丝消费
        /*Date date = Date.parse('yyyyMMdd','20150616')
        int i = 30
        while(i-- >= 0){
            date = date + 1
            println date.format('yyyyMMdd')
            long _begin1 = date.clearTime().getTime()
            staticFamilyRoomFenSiDay(family_rooms, _begin1)

        }*/


        long _begin1 = new SimpleDateFormat("yyyyMMdd").parse("20160119").getTime()
        staticRoomFenSiDay(rooms, _begin1)

        //staticRoomFenSiDay()
        //println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitRoomFenSiFilling.class.getSimpleName()} InitRoomFenSiFilling 20140625 , cost  ${System.currentTimeMillis() -l} ms"

        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitRoomFenSiFilling.class.getSimpleName()} InitRoomFenSiFilling----------->:finish "

    }

}