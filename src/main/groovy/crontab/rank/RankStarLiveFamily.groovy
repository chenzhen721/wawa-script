#!/usr/bin/env groovy

@Grapes([
@Grab('org.mongodb:mongo-java-driver:2.14.2'),
@Grab('commons-lang:commons-lang:2.6'),
@Grab('redis.clients:jedis:2.1.0'),
])
import com.mongodb.MongoURI
import groovy.json.JsonSlurper
import com.mongodb.BasicDBObject
import com.mongodb.Mongo
import redis.clients.jedis.Jedis

/**
 * TODO 暂停使用
 * 统计主播本周家族消费排行榜
 * @author: jiao.li@ttpod.com
 * Date: 13-10-29 下午6:45
 */

@Deprecated
class RankStarLiveFamily{

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

    static final String jedis_host = getProperties("main_jedis_host", "192.168.31.249")
    static final String chat_jedis_host = getProperties("chat_jedis_host", "192.168.31.249")
    static final String live_jedis_host = getProperties("live_jedis_host", "192.168.31.249")

    static final Integer main_jedis_port = getProperties("main_jedis_port",6379) as Integer
    static final Integer chat_jedis_port = getProperties("chat_jedis_port",6379) as Integer
    static final Integer live_jedis_port = getProperties("live_jedis_port",6379) as Integer

    static mongo  = new Mongo(new MongoURI(getProperties('mongo.uri','mongodb://192.168.31.249:27017/?w=1') as String))
    static mainRedis = new Jedis(jedis_host, main_jedis_port)

    static final String  CAT_WEEK =  "week"   //本周
    static final String  TYPE_LIVE_COST =  "live_cost"   //消费榜

    //统计周一开始到现在
    static Map getThisWeek(){
        def c =  Calendar.getInstance()
        long end = c.getTime().getTime()
        c.setFirstDayOfWeek(Calendar.MONDAY)
        c.set(Calendar.DAY_OF_WEEK, c.getFirstDayOfWeek()); // Monday
        def beginDate = c.getTime().clearTime()
        long begin = beginDate.getTime()
        [$gte: begin,$lt: end]
    }
    static timestamp_between = getThisWeek();

    static buildCostRank(starId, fids) {
        def res = mongo.getDB("xylog").getCollection("room_cost").aggregate(
                new BasicDBObject('$match', [timestamp: timestamp_between,
                        "session.data.xy_star_id": starId,
                            "family_id": [$in: fids]]),
                new BasicDBObject('$project', [fid : '$family_id',cost: '$cost']),
                new BasicDBObject('$group', [_id: '$fid',
                        total_num:[$sum: '$cost']
                ]),
                new BasicDBObject('$sort', [total_num:-1]),
                new BasicDBObject('$limit',10)
        )
        return res.results().iterator()
    }

    static complete(Iterator objs, star_id, room_id, String type, String cat){
        def list = new ArrayList(10)
        def coll = mongo.getDB("xyrank").getCollection("family")
        int i = 0
        while (objs.hasNext()){
            def obj =  objs.next()
            i++
            obj.put("fid", obj.get("_id"))
            obj.put("_id", "${cat}_${i}_${type}_${star_id}".toString())
            obj.put("cat" , cat)
            obj.put("type" , type)
            obj.put("rank", i)
            obj.put("xy_star_id", star_id)
            obj.put("room_id", room_id)
            obj.put("sj",new Date().format("yyyyMMdd"))
            list << obj
            //println obj;
        }
        if(list.size() > 0)
            coll.insert(list)
        String familyRankKey = "room:" + room_id + ":familys:rank:string";
        mainRedis.del(familyRankKey)

    }



    static void main(String[] args)
    {
        long l = System.currentTimeMillis()
        long begin = l
        def rooms = mongo.getDB("xy").getCollection("rooms").find(new BasicDBObject(pic_url: [$exists: true], test: [$ne: true])).toArray()
        def fids = mongo.getDB("xy_family").getCollection("familys").find(new BasicDBObject("status", 2),
                new BasicDBObject("_id", 1)).toArray()*._id
        if(rooms != null){
            mongo.getDB("xyrank").getCollection("family").remove(new BasicDBObject("type",TYPE_LIVE_COST))

            rooms.each{
                def star_id = it.xy_star_id
                def room_id = it._id
                complete(buildCostRank(star_id, fids), star_id, room_id, TYPE_LIVE_COST , CAT_WEEK)
            }
        }
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  star family rank , cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //落地定时执行的日志
        l = System.currentTimeMillis()
        def timerName = 'RankStarLiveFamily'
        Long totalCost = System.currentTimeMillis() - begin
        saveTimerLogs(timerName,totalCost)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  save timer_logs , cost  ${System.currentTimeMillis() - l} ms"

        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${RankStarLiveFamily.class.getSimpleName()}, cost  ${System.currentTimeMillis() -begin} ms"
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