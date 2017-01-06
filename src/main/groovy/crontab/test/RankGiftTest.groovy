#!/usr/bin/env groovy


@Grapes([
@Grab('org.mongodb:mongo-java-driver:2.14.2'),
@Grab('commons-lang:commons-lang:2.6'),
@Grab('redis.clients:jedis:2.1.0'),
])
import com.mongodb.Mongo
import com.mongodb.MongoURI
import com.mongodb.BasicDBObject

/**
 * 周星定时任务
 */
class RankGiftTest {
    //static mongo = new Mongo("127.0.0.1", 10000)
    //static mongo = new Mongo("192.168.1.156", 10000)
    static mongo  = new Mongo(new MongoURI('mongodb://192.168.1.36:10000,192.168.1.37:10000,192.168.1.38:10000/?w=1&slaveok=true'))
    static Map getThisSunWeek(){
        /*def cal =  Calendar.getInstance()
        cal.set(Calendar.DAY_OF_WEEK,2)
        if (cal.getTimeInMillis() > System.currentTimeMillis()){
            cal.add(Calendar.DAY_OF_YEAR,-7)
        }
        [$gte: cal.getTime().clearTime().getTime()]*/
        [$gte: 1414944000000, $lt:1415548800000]
    }
    static timestamp_between = getThisSunWeek();

    static bulidRank(def giftId) {

        def res = mongo.getDB("xylog").getCollection("room_cost").aggregate(
                new BasicDBObject('$match', [type: "send_gift",
                        "session.data._id":giftId,'session.data.xy_star_id':[$ne:null],
                        timestamp: timestamp_between]),
                new BasicDBObject('$project', [_id: '$session.data.xy_star_id',timestamp:'$timestamp',
                        count:'$session.data.count',earned:'$session.data.earned'
                ]),
                new BasicDBObject('$group', [_id: '$_id',
                        count: [$sum: '$count'],
                        earned:[$sum: '$earned'],
                        timestamp:[$max:'$timestamp']
                ]),
                new BasicDBObject('$sort', [count:-1,timestamp:-1])
        )
        return res.results().iterator()
    }

    static void main(String[] args)
    {
        String cat = "week"
        long l = System.currentTimeMillis()
        long begin = l
        def list = new ArrayList(50)
        mongo.getDB("xy_admin").getCollection("gifts").find(new BasicDBObject("status":true, star:true)).each
        {
            Iterator objs=  bulidRank(it.get("_id"))
            int i = 0
            while (objs.hasNext())
            {
                def obj =  objs.next()
                i++
                def gift_id = it.get("_id")
                obj.put("user_id",obj.get("_id"))
                obj.put("_id","${gift_id}_${i}_${cat}_${obj.get('user_id')}".toString())
                obj.put("gift_id",gift_id)
                obj.put("pic_url",it.get("pic_url"))
                obj.put("name",it.get("name"))
                obj.put("cat",cat)
                obj.put("rank",i)
                obj.put("sj",new Date())
                list<< obj
            }
        }
        //def coll = mongo.getDB("xyrank").getCollection("gift")
        //coll.remove(new BasicDBObject("cat",cat))
        //coll.insert(list)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  gift rank, cost  ${System.currentTimeMillis() -l} ms"
        //Thread.sleep(1000L)

        //落地定时执行的日志
        //l = System.currentTimeMillis()
        //def timerName = 'RankGift'
        //Long totalCost = System.currentTimeMillis() - begin
        //saveTimerLogs(timerName,totalCost)
        //println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  save timer_logs , cost  ${System.currentTimeMillis() - l} ms"

        //println "${new Date().format('yyyy-MM-dd HH:mm:ss')}    ${RankGift.class.getSimpleName()}, cost  ${System.currentTimeMillis() -begin} ms"

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