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
 * 明星日，周，月榜
 *
 * date: 13-2-28 下午2:46
 * @author: yangyang.cong@ttpod.com
 */
class RankStar {
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
    static roomStarDB = mongo.getDB("xylog").getCollection("room_cost_star")
    static familyRoomStarDB = mongo.getDB("xylog").getCollection("family_room_cost_star")
    static familyStar = mongo.getDB("xyrank").getCollection("family_star")
    static coll = mongo.getDB("xyrank").getCollection("star")
    static userDB = mongo.getDB("xy").getCollection("users")
    static rooms = mongo.getDB("xy").getCollection("rooms")
    static final Integer size = 100

    //每小时主播排行 一个小时内的主播排行
    static void staticHour(){
        String cat = "hour"
        long now = System.currentTimeMillis()
        def res = mongo.getDB("xylog").getCollection("room_cost").aggregate(
                new BasicDBObject('$match', [timestamp: [$gt: now - HOUR_MILLON, $lte: now]]),
                new BasicDBObject('$project', [star_id: '$session.data.xy_star_id', earned: '$session.data.earned']),
                new BasicDBObject('$group', [_id: '$star_id', num: [$sum: '$earned']]),
                new BasicDBObject('$sort', [num: -1]),
                new BasicDBObject('$limit',100) //top N 算法
        )
        Iterable objs = res.results()
        def list = new ArrayList(size)
        int i = 0
        def today = new Date()
        //更新实时榜单，用于房间列表排序
        rooms.updateMulti(new BasicDBObject("hour_cost", [$gt:0]),
                new BasicDBObject('$unset', new BasicDBObject("hour_cost",1)));
        objs.each {row ->
            def user_id = row._id
            if (user_id){
                if (i++ < size){
                    def starId = user_id as Integer
                    def cost = row.num;
                    list.add(new BasicDBObject(_id:"${cat}_${starId}".toString(),cat:cat,user_id:starId,num:cost,rank:i,sj:today))
                    //更新实时榜单，用于房间列表排序
                    rooms.update(new BasicDBObject("_id", starId), new BasicDBObject('$set', new BasicDBObject("hour_cost",cost)))
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
        def res = mongo.getDB("xylog").getCollection("room_cost").aggregate(
                new BasicDBObject('$match', [timestamp: [$gt: zeroMill , $lte: now]]),
                new BasicDBObject('$project', [star_id: '$session.data.xy_star_id', earned: '$session.data.earned']),
                new BasicDBObject('$group', [_id: '$star_id', num: [$sum: '$earned']]),
                new BasicDBObject('$sort', [num:-1]),
                new BasicDBObject('$limit',100) //top N 算法
        )
        Iterable objs = res.results()

        def list = new ArrayList(size)
        int i = 0
        def rooms = mongo.getDB('xy').getCollection('rooms')
        def today = new Date()
        objs.each {row ->
            def user_id = row._id
            if (user_id)
            {
                if (i++ < size)
                {
                    def starId = user_id as Integer
                    list.add(new BasicDBObject(_id:"${cat}_${user_id}".toString(),cat:cat,user_id:starId,num:row.num,rank:i,sj:today))
                    rooms.update(new BasicDBObject('xy_star_id',starId),new BasicDBObject('$set',[rank:-i]))
                }

                def id = user_id + "_"  + today.format("yyyyMMdd")
                def update = new BasicDBObject(star_id:user_id as Integer,num:row.num,timestamp:now)
                roomStarDB.findAndModify(new BasicDBObject('_id',id), null, null, false,
                        new BasicDBObject('$set',update),true, true)
            }
        }
        coll.remove(new BasicDBObject("cat",cat))
        if(list.size() > 0)
            coll.insert(list)
    }

    static void staticWeek()
    {
        String cat = "week"
        Integer day = 6
        saveRank(cat, day)
    }

    static void staticMonth()
    {
        String cat = "month"
        Integer day = 28
        saveRank(cat, day)
    }

    static void staticTotal()
    {
        String cat = "total"
        def spendQ = new BasicDBObject('finance.bean_count_total':[$gt:0])
        spendQ.append("priv",2)
        def lst = userDB.find(spendQ, new BasicDBObject(["_id":1,"finance.bean_count_total":1]))
                .sort(new BasicDBObject("finance.bean_count_total",-1)).limit(1000).toArray()
        def list = new ArrayList(1000)
        int index = 0;
        lst.each {row ->
            def user_id =  row._id
            def finance = row['finance'] as Map
            list.add(new BasicDBObject(_id:"${cat}_${user_id}".toString(),cat:cat,rank:++index,
                    user_id:row._id as Integer,num:finance['bean_count_total'] as Long,sj:new Date()))

        }
        coll.remove(new BasicDBObject("cat",cat))
        if(list.size() > 0)
            coll.insert(list)
    }

    static void saveRank(String cat,Integer day)
    {
        long now = System.currentTimeMillis()
        def res = roomStarDB.aggregate(
                new BasicDBObject('$match', [timestamp: [$gt: zeroMill - day * DAY_MILLON, $lte: now]]),
                new BasicDBObject('$project', [_id: '$star_id',earned:'$num']),
                new BasicDBObject('$group', [_id: '$_id', num: [$sum: '$earned']]),
                new BasicDBObject('$sort', [num:-1]),
                new BasicDBObject('$limit',size) //top N 算法
        )
        Iterable objs = res.results()
        def list = new ArrayList(size)
        int i = 0
        objs.each {row ->
            def user_id = row._id
            if (user_id)
            {
                i++
                user_id = user_id as Integer
                list.add(new BasicDBObject(_id:"${cat}_${user_id}".toString(),cat:cat,user_id:user_id,num:row.num,rank:i,sj:new Date()) )
            }
        }
        coll.remove(new BasicDBObject("cat",cat))
        if(list.size() > 0)
            coll.insert(list)
    }

    static void updateRooms()
    {
        def rooms = mongo.getDB('xy').getCollection('rooms')
        def users = mongo.getDB('xy').getCollection('users')
        rooms.find().toArray().each { BasicDBObject obj ->
            rooms.update(new BasicDBObject('_id',obj.get('_id')),new BasicDBObject('$set',[bean:
                    users.findOne(obj.get('xy_star_id'),
                            new BasicDBObject('finance.bean_count_total',1))?.get('finance')?.getAt('bean_count_total')?:0
            ]))
        }
    }

    static staticFamilyTotal(){
        try{
            def timeLimit = new BasicDBObject(timestamp:[$gte:System.currentTimeMillis() - 2*DAY_MILLON],pic_url:[$exists:true], type:2) // 最近一天开播过家族直播间的
            def roomslst = mongo.getDB("xy").getCollection("rooms").find(timeLimit,new BasicDBObject("live_id":1, "family_id":1)).toArray()
            String cat = "total"
            roomslst.each{
                Integer roomId = it._id as Integer
                Integer family_id = it?.family_id as Integer
                long now = System.currentTimeMillis()
                def res = mongo.getDB("xylog").getCollection("room_cost").aggregate(
                        new BasicDBObject('$match', [timestamp: [$gt: zeroMill , $lte: now], room:roomId]),
                        new BasicDBObject('$project', [star_id: '$session.data.xy_star_id', earned: '$session.data.earned',cost:'$star_cost']),
                        new BasicDBObject('$group', [_id: '$star_id', num: [$sum: '$earned'], cost: [$sum: '$cost']]),
                        new BasicDBObject('$sort', [num:-1]),
                        new BasicDBObject('$limit',100) //top N 算法
                )
                Iterable objs = res.results()
                def today = new Date()
                objs.each {row ->
                    def star_id = row._id
                    if (star_id)
                    {
                        def id = star_id + "_"  + family_id +"_"+today.format("yyyyMMdd")
                        def update = new BasicDBObject(family_id:family_id,star_id:star_id as Integer, room:roomId,cost:row.cost,num:row.num,timestamp:now)
                        familyRoomStarDB.findAndModify(new BasicDBObject('_id',id), null, null, false,
                                new BasicDBObject('$set',update),true, true)
                    }
                }

                def query = [room : roomId]
                res = familyRoomStarDB.aggregate(
                        new BasicDBObject('$match', query),
                        new BasicDBObject('$project', [_id: '$star_id',cost:'$cost',bean:'$num']),
                        new BasicDBObject('$group', [_id: '$_id', num: [$sum: '$cost'], bean: [$sum: '$bean']]),
                        new BasicDBObject('$sort', [num:-1]),
                        new BasicDBObject('$limit',10) //top N 算法
                )

                objs = res.results()
                def list = new ArrayList(10)
                int i = 0
                objs.each {row ->
                    def star_id = row._id
                    if(star_id)
                    {
                        i++
                        list.add(new BasicDBObject(_id:"${roomId}_${cat}_${star_id}".toString(),family_id:family_id,
                                cat:cat,star_id:star_id as Integer,num:row.num,bean:row.bean,rank:i,room:roomId,sj:new Date()))
                    }
                }
                familyStar.remove(new BasicDBObject("cat":cat,room:roomId))
                familyStar.insert(list)

            }
        }catch (Exception e){
            println " staticFamilyTotal Exception " + e
        }

    }

    static void main(String[] args)
    {
        long l = System.currentTimeMillis()
        long begin = l
        //更新房间
        updateRooms()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticRoom, cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)
        //明星时榜
        l = System.currentTimeMillis()
        staticHour()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticHour, cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)
        //明星日榜
        l = System.currentTimeMillis()
        staticDay()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticDay, cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)
        //明星周榜
        l = System.currentTimeMillis()
        staticWeek()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticWeek, cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)
         //明星月榜
        l = System.currentTimeMillis()
        staticMonth()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticMonth, cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)
        //明星总排名
        l = System.currentTimeMillis()
        staticTotal()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticTotal, cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)
        //主播家族直播间贡献总榜
        l = System.currentTimeMillis()
        staticFamilyTotal()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticFamilyTotal, cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)
        //落地定时执行的日志
        l = System.currentTimeMillis()
        def timerName = 'RankStar'
        Long totalCost = System.currentTimeMillis() - begin
        saveTimerLogs(timerName,totalCost)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  save timer_logs , cost  ${System.currentTimeMillis() - l} ms"

        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${RankStar.class.getSimpleName()}, cost  ${System.currentTimeMillis() -begin} ms"
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