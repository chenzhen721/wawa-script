#!/usr/bin/env groovy


@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
])
import com.mongodb.Mongo
import com.mongodb.BasicDBObject
import com.mongodb.MongoURI
import com.mongodb.DBObject

/**
 * TODO 每小时累加
 * 房间粉丝 月榜，总榜
 *
 * date: 13-2-28 下午2:46
 */
class RankUserRoom {


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
    // static long zeroMill = new Date().clearTime().getTime()
    static fenSiDB = mongo.getDB("xylog").getCollection("room_fensi_cost")
    static familyFenSiDB = mongo.getDB("xylog").getCollection("family_fensi_cost")
    static long zeroMill = new Date().clearTime().getTime()

    private final static Long SLEEP_TIME = 3 * 1000l
    private final static Integer MAX_THRESHOLD = 10

    //每个小时聚合当天用户的消费
    static void staticDay(List<DBObject> roomslst)
    {
        long now = System.currentTimeMillis()
        def today = new Date().format("yyyyMMdd")
        def timeBetween = [$gte:zeroMill , $lt: now]
        int threshold = 0;
        roomslst.each{
            Integer roomId = it._id as Integer

            def query = ['session.data.xy_star_id': roomId,timestamp:timeBetween]

            //家族房间粉丝消费
            Integer room_family_id = it?.family_id as Integer
            if(room_family_id != null) {
                query = ['room_family_id': room_family_id, timestamp: timeBetween]
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
                    def update = new BasicDBObject(user_id:user_id as Integer,num:row.cost, bean:row.num,room:roomId,timestamp:now)
                    fenSiDB.update(new BasicDBObject('_id',id), new BasicDBObject('$set':update), true, false)
                    //fenSiDB.findAndModify(new BasicDBObject('_id',id), null, null, false,new BasicDBObject('$set':update),true, true)
                }
            }
            if(threshold++ >= MAX_THRESHOLD){
                Thread.sleep(SLEEP_TIME)
                threshold = 0;
            }

        }
    }


    //月
    static void staticMonth(List<DBObject> roomslst)
    {
        String cat = "month"
        saveRank(cat,roomslst)
    }

    //总
    static staticTotal(List<DBObject> roomslst)
    {
        String cat = "total"
        saveRank(cat,roomslst)
    }


    static bulidRank(Integer roomId, String cat)
    {
        long now = System.currentTimeMillis()
        long zeroMill = new Date().clearTime().getTime()
        def query = [room : roomId]
        if("month".equals(cat))
            query.timestamp = [$gt: zeroMill - 28 * DAY_MILLON, $lte: now]

        def res = fenSiDB.aggregate(
                new BasicDBObject('$match', query),
                new BasicDBObject('$project', [_id: '$user_id',cost:'$num',bean:'$bean']),
                new BasicDBObject('$group', [_id: '$_id', num: [$sum: '$cost'], bean: [$sum: '$bean']]),
                new BasicDBObject('$sort', [num:-1]),
                new BasicDBObject('$limit',10) //top N 算法
        )
        return res.results().iterator()
    }


    static void saveRank(String cat, List<DBObject> roomslst){
        //println "room size : " + roomslst.size()
        int threshold = 0;
        for (DBObject room : roomslst){
            Integer roomId = room._id as Integer
            //long l = System.currentTimeMillis()
            Iterator objs = bulidRank(roomId,cat)
            //println "cat : " + cat + " room:" + roomId +" cost: " + (System.currentTimeMillis() - l)
            def list = new ArrayList(10)
            int rank = 0
            objs.each {row ->
                def user_id = row._id
                if(user_id){
                    rank++
                    list.add(new BasicDBObject(_id:"${roomId}_${cat}_${user_id}".toString(),
                            cat:cat,user_id:user_id as Integer,num:row.num,bean:row.bean,rank:rank,room:roomId,sj:new Date()))
                }
            }
            def coll = mongo.getDB("xyrank").getCollection("user_room")
            coll.remove(new BasicDBObject("cat":cat,room:roomId))
            if(list.size() > 0)
                coll.insert(list)
            if(threshold++ >= MAX_THRESHOLD){
                Thread.sleep(SLEEP_TIME)
                threshold = 0;
            }
        }
    }


    static void main(String[] args)
    {
        long l = System.currentTimeMillis()
        long begin = l

        def timeLimit = new BasicDBObject(timestamp:[$gte:l - DAY_MILLON]) // 最近一天开播过的
        def roomslst = mongo.getDB("xy").getCollection("rooms").find(timeLimit,new BasicDBObject("live_id":1, "family_id":1)).toArray()

        //01.粉丝日榜
        staticDay(roomslst)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticDay, cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //02.粉丝月榜
        l = System.currentTimeMillis()
        staticMonth(roomslst)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticMonth, cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //03.粉丝总榜
        l = System.currentTimeMillis()
        staticTotal(roomslst)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticTotal, cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //落地定时执行的日志
        l = System.currentTimeMillis()
        def timerName = 'RankUserRoom'
        Long totalCost = System.currentTimeMillis() - begin
        saveTimerLogs(timerName,totalCost)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  save timer_logs , cost  ${System.currentTimeMillis() - l} ms"

        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}    ${RankUserRoom.class.getSimpleName()}, cost  ${System.currentTimeMillis() -begin} ms"
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

    //记录上次任务执行时间戳
    private static Long getAndSetTaskTimestamp(long timestamp){
        def task_logs =  mongo.getDB("xylog").getCollection("task_logs")
        def id = "RankUserRoom"

        def update = new BasicDBObject(timestamp:timestamp)
        def task = task_logs.findAndModify(new BasicDBObject('_id',id), null, null, false,new BasicDBObject('$set',update),true, true)
        if(task.get('timestamp')){
            long last_time = task.get('timestamp') as Long
            //如果上次执行时间为昨天则从当天凌晨开始计算
            last_time = Math.max(zeroMill, last_time)
            return last_time
        }
        return timestamp
    }
}