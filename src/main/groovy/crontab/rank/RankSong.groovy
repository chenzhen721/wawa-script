#!/usr/bin/env groovy

@Grapes([
@Grab('org.mongodb:mongo-java-driver:2.14.2'),
@Grab('commons-lang:commons-lang:2.6'),
@Grab('redis.clients:jedis:2.1.0'),
])
import com.mongodb.Mongo
import com.mongodb.MongoURI
import com.mongodb.BasicDBObject
import com.mongodb.DBCollection

/**
 * 定时更新房间的在线人数
 *
 * date: 13-2-28 下午2:46
 * @author: yangyang.cong@ttpod.com
 */
class RankSong {
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

    static DAY_MILLON = 24 * 3600 * 1000L
    static long zeroMill = new Date().clearTime().getTime()
    static starRoomDB = mongo.getDB("xylog").getCollection("room_cost_day_star")
    static DBCollection room_cost_DB =  mongo.getDB("xylog").getCollection("room_cost")

    //当日主播收到点歌类型的数据压缩
    static void staticDay()
    {
        def type = 'song'
        long now = System.currentTimeMillis()
       def timestamp = [$gt: zeroMill, $lte: now]
        def res = room_cost_DB.aggregate( //
                new BasicDBObject('$match', [timestamp: timestamp, type: type]),
                new BasicDBObject('$project', [star_id: '$session.data.xy_star_id', earned: '$session.data.earned']),
                new BasicDBObject('$group', [_id: '$star_id', num: [$sum: '$earned'], count: [$sum: 1]]),
                new BasicDBObject('$sort', [count:-1]),
                new BasicDBObject('$limit',500))

        Iterable objs =  res.results()
        def day = new Date().format("yyyyMMdd")

        def list = new ArrayList(100)
        String cat = 'day'
        int i = 0
        objs.each {row ->
            def earned = row.num as Long
            def star_id = row._id as Integer
            def count = row.count as Integer

            if (i++ < 100)
                list.add(new BasicDBObject(_id:"${cat}_${star_id}".toString(),
                        cat:cat,user_id:star_id,num:earned,rank:i,sj:new Date())
                )

            def update = new BasicDBObject(type:type,star_id:star_id as Integer,earned:earned,count:count,timestamp:now)
            def id =star_id+"_" + day + "_" + type
            starRoomDB.findAndModify(new BasicDBObject('_id',id), null, null, false,
                    new BasicDBObject('$set',update),true, true)
        }
        def coll = mongo.getDB("xyrank").getCollection("song")
        coll.remove(new BasicDBObject("cat",cat))
        if(list.size() > 0)
            coll.insert(list)

    }

    static void staticWeek()
    {
        String cat = "week"
        Integer iDay =  6
        Integer limit = 100
        saveRank(cat,iDay,limit)
    }

    static void staticMonth()
    {
        String cat = "month"
        Integer iDay =  28
        Integer limit = 100
        saveRank(cat,iDay,limit)
    }

    static void staticTotal()
    {
        String cat = "total"
        Integer iDay =  -1
        Integer limit = 100
        saveRank(cat,iDay,limit)
    }

    static void saveRank(String cat ,Integer day,Integer limit)
    {
        long now = System.currentTimeMillis()
        def query = [type : 'song']
        if(!"total".equals(cat))
            query.timestamp = [$gt: zeroMill - day * DAY_MILLON, $lte: now]

        def res = starRoomDB.aggregate(
                new BasicDBObject('$match', query),
                new BasicDBObject('$project', [_id: '$star_id',count:'$count']),
                new BasicDBObject('$group', [_id: '$_id', num: [$sum: '$count']]),
                new BasicDBObject('$sort', [num:-1]),
                new BasicDBObject('$limit',limit) //top N 算法
        )

        Iterable objs = res.results()
        def list = new ArrayList(100)
        objs.each {row ->
            def user_id =  row._id
            if(user_id)
                list.add(new BasicDBObject(_id:"${cat}_${user_id}".toString(),cat:cat,user_id:user_id as Integer,num:row.num,sj:new Date()))
        }
        def coll = mongo.getDB("xyrank").getCollection("song")
        coll.remove(new BasicDBObject("cat",cat))
        if(list.size() > 0)
            coll.insert(list)
    }

    static void main(String[] args)
    {
        long l = System.currentTimeMillis()
        long begin =  l
        //01.点歌日榜
        staticDay()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticDay , cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //02.点歌周榜
        l = System.currentTimeMillis()
        staticWeek()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticWeek , cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //03.点歌月榜
        l = System.currentTimeMillis()
        staticMonth()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticMonth , cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //04.点歌总榜
        l = System.currentTimeMillis()
        staticTotal()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticTotal , cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)


        //落地定时执行的日志
        l = System.currentTimeMillis()
        def timerName = 'RankSong'
        Long totalCost = System.currentTimeMillis() - begin
        saveTimerLogs(timerName,totalCost)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  save timer_logs , cost  ${System.currentTimeMillis() - l} ms"

        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${RankSong.class.getSimpleName()} , cost  ${System.currentTimeMillis() -begin} ms"
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