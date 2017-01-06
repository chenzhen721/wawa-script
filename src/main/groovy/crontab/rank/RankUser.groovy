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
 * 富豪日，周，月榜
 *
 * date: 13-2-28 下午2:46
 * @author: yangyang.cong@ttpod.com
 */
class RankUser {

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

   static HOUR_MILLON = 3600 * 1000L
   static DAY_MILLON = 24 * HOUR_MILLON
   static long zeroMill = new Date().clearTime().getTime()
   static roomUserDB = mongo.getDB("xylog").getCollection("room_cost_usr")
   static userDB = mongo.getDB("xy").getCollection("users")
   static coll = mongo.getDB("xyrank").getCollection("user")
   static final Integer size = 100


    private final static Long SLEEP_TIME = 2 * 1000l
    private final static Integer MAX_THRESHOLD = 50

    //待优化 调优成 每个小时累加
    static void staticHour()
    {
        String cat = "hour"
        long now = System.currentTimeMillis()
        def roomCostDb =  mongo.getDB("xylog").getCollection("room_cost")
        def res = roomCostDb.aggregate(
                new BasicDBObject('$match', [timestamp: [$gt: now - HOUR_MILLON, $lte: now]]),
                new BasicDBObject('$project', [_id: '$session._id',cost:'$star_cost']),
                new BasicDBObject('$group', [_id: '$_id', num: [$sum: '$cost']]),
                new BasicDBObject('$sort', [num:-1])
        )
        Iterable objs = res.results()
        def list = new ArrayList(size)
        int i = 0
        def today = new Date()
        objs.each{row ->
            def user_id =  row._id
            if(user_id){
                if (i++ < size){
                    list.add(new BasicDBObject(_id:"${cat}_${user_id}".toString(),cat:cat,user_id:user_id as Integer,num:row.num,rank:i,sj:today))
                }
            }
        }
        coll.remove(new BasicDBObject("cat",cat))
        if(list.size() > 0)
            coll.insert(list)
    }

    //待优化 调优成 每个小时累加
    static void staticDay()
    {
        String cat = "day"
        long now = System.currentTimeMillis()
        def roomCostDb =  mongo.getDB("xylog").getCollection("room_cost")
        def res = roomCostDb.aggregate(
                new BasicDBObject('$match', [timestamp: [$gt: zeroMill, $lte: now]]),
                new BasicDBObject('$project', [_id: '$session._id',cost:'$star_cost']),
                new BasicDBObject('$group', [_id: '$_id', num: [$sum: '$cost']]),
                new BasicDBObject('$sort', [num:-1])
        )
        int threshold = 0;
        Iterable objs = res.results()
        def list = new ArrayList(size)
        int i = 0
        def today = new Date()
        objs.each{row ->
            def user_id =  row._id
            if(user_id){
                if (i++ < size){
                    list.add(new BasicDBObject(_id:"${cat}_${user_id}".toString(),cat:cat,user_id:user_id as Integer,num:row.num,sj:today))
                }
                def id = user_id + "_"  + today.format("yyyyMMdd")
                def update = new BasicDBObject(user_id:user_id as Integer,num:row.num,timestamp:now)
                roomUserDB.update(new BasicDBObject('_id',id), new BasicDBObject('$set',update), true, false)
                //roomUserDB.findAndModify(new BasicDBObject('_id',id), null, null, false,new BasicDBObject('$set',update),true, true)
            }
            if(threshold++ >= MAX_THRESHOLD){
                Thread.sleep(SLEEP_TIME)
                threshold = 0;
            }
        }
        coll.remove(new BasicDBObject("cat",cat))
        if(list.size() > 0)
            coll.insert(list)
    }

    static void staticWeek()
    { //财周星
        String cat = "week"
        Integer iDay =  6
        saveWeekRank(cat,iDay)
    }

    static void staticMonth()
    {
        String cat = "month"
        Integer iDay =  28
        Integer limit = size
        saveRank(cat,iDay,limit)
    }

    static final Integer WEEK_SPEND_COST = 20000 //财星周消费200元人民币
    static void saveWeekRank(String cat ,Integer day)
    {
        long now = System.currentTimeMillis()
        def res = roomUserDB.aggregate(
                new BasicDBObject('$match', [timestamp: [$gt: zeroMill - day * DAY_MILLON, $lte: now]]),
                new BasicDBObject('$project', [_id: '$user_id',cost:'$num']),
                new BasicDBObject('$group', [_id: '$_id', num: [$sum: '$cost']]),
                new BasicDBObject('$match', [num: [$gte: WEEK_SPEND_COST]]),
                new BasicDBObject('$sort', [num:-1])
        )

        Iterable objs = res.results()
        def list = new ArrayList(500)
        objs.each {row ->
            def user_id =  row._id
            if(user_id)
                list.add(new BasicDBObject(_id:"${cat}_${user_id}".toString(),cat:cat,user_id:row._id as Integer,num:row.num,sj:new Date()))
        }
        coll.remove(new BasicDBObject("cat",cat))
        if(list.size() > 0)
            coll.insert(list)
    }

    static void staticTotal()
    {
       String cat = "total"
       def spendQ = new BasicDBObject('finance.coin_spend_total':[$gt:0])
       def lst = userDB.find(spendQ, new BasicDBObject(["_id":1,"finance.coin_spend_total":1]))
                .sort(new BasicDBObject("finance.coin_spend_total",-1)).limit(10000).toArray()
        def list = new ArrayList(10000)
        int index = 0;
        lst.each {row ->
            def user_id =  row._id
            def finance = row['finance'] as Map
            list.add(new BasicDBObject(_id:"${cat}_${user_id}".toString(),cat:cat,rank:++index,
                    user_id:row._id as Integer,num:finance['coin_spend_total'] as Long,sj:new Date()))

        }
        coll.remove(new BasicDBObject("cat",cat))
        if(list.size() > 0)
            coll.insert(list)
    }

    static void saveRank(String cat ,Integer day,Integer limit)
    {
        long now = System.currentTimeMillis()
        def res = roomUserDB.aggregate(
                new BasicDBObject('$match', [timestamp: [$gt: zeroMill - day * DAY_MILLON, $lte: now]]),
                new BasicDBObject('$project', [_id: '$user_id',cost:'$num']),
                new BasicDBObject('$group', [_id: '$_id', num: [$sum: '$cost']]),

                new BasicDBObject('$sort', [num:-1]),
                new BasicDBObject('$limit',limit) //top N 算法
        )

        Iterable objs = res.results()
        def list = new ArrayList(500)
        objs.each {row ->
            def user_id =  row._id
            if(user_id)
                list.add(new BasicDBObject(_id:"${cat}_${user_id}".toString(),cat:cat,user_id:row._id as Integer,num:row.num,sj:new Date()))
        }
        coll.remove(new BasicDBObject("cat",cat))
        if(list.size() > 0)
            coll.insert(list)
    }

    static void main(String[] args)
    {
        long l = System.currentTimeMillis()
        long begin =  l
        //富豪时榜
        staticHour()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticHour , cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)
        //富豪日榜
        staticDay()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticDay , cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)
        //富豪周榜
        l = System.currentTimeMillis()
        staticWeek()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticWeek , cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)
        //富豪月榜
        l = System.currentTimeMillis()
        staticMonth()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticMonth , cost  ${System.currentTimeMillis() -l} ms"
        //富豪总榜
        l = System.currentTimeMillis()
        staticTotal()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticTotal , cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //落地定时执行的日志
        l = System.currentTimeMillis()
        def timerName = 'RankUser'
        Long totalCost = System.currentTimeMillis() - begin
        saveTimerLogs(timerName,totalCost)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  save timer_logs , cost  ${System.currentTimeMillis() - l} ms"

        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}    ${RankUser.class.getSimpleName()} , cost  ${System.currentTimeMillis() -begin} ms"
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