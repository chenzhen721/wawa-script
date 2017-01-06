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
 * TODO 本文件线上未使用 暂时和房间统计RankUserRoom.groovy合并
 * 包厢消费总榜
 *
 * date: 13-2-28 下午2:46
 */
class RankUserBox {


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
   static box_cost = mongo.getDB("xylog").getCollection("box_cost")

    //总
    static staticBoxTotal(List<DBObject> boxlst)
    {
        String cat = "total"
        saveBoxRank(cat,boxlst)
    }

    static void saveBoxRank(String cat,List<DBObject> boxlst)
    {
        boxlst.each{
            Integer boxId = it._id as Integer
            Iterator objs = bulidBoxRank(boxId,cat)
            def list = new ArrayList(10)
            int i = 0
            objs.each {row ->
                def user_id = row._id
                if(user_id)
                {
                    i++
                    list.add(new BasicDBObject(_id:"${boxId}_${cat}_${user_id}".toString(),
                            cat:cat,user_id:user_id as Integer,num:row.total,rank:i,box:boxId,sj:new Date()))
                }
            }
            def coll = mongo.getDB("xyrank").getCollection("user_box")
            coll.remove(new BasicDBObject("cat":cat,room:boxId))
            if(list.size() > 0)
                coll.insert(list)
        }
    }

    static bulidBoxRank(Integer boxId, String cat)
    {
        long now = System.currentTimeMillis()
        long zeroMill = new Date().clearTime().getTime()
        def query = [box : boxId]
        def res = box_cost.aggregate(
                new BasicDBObject('$match', query),
                new BasicDBObject('$project', [_id: '$session._id',cost:'$cost']),
                new BasicDBObject('$group', [_id: '$_id', total: [$sum: '$cost']]),
                new BasicDBObject('$sort', [total:-1]),
                new BasicDBObject('$limit',10) //top N 算法
        )
        return res.results().iterator()
    }


    static void main(String[] args)
    {
        long l = System.currentTimeMillis()
        long begin = l

        def query = new BasicDBObject(status:2)
        def boxlst = mongo.getDB("xy").getCollection("boxes").find(query,new BasicDBObject("_id",1)).toArray()

       //总榜
        l = System.currentTimeMillis()
        staticBoxTotal(boxlst)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticBoxTotal, cost  ${System.currentTimeMillis() -l} ms"

        //落地定时执行的日志
        l = System.currentTimeMillis()
        def timerName = 'RankUserBox'
        Long totalCost = System.currentTimeMillis() - begin
        saveTimerLogs(timerName,totalCost)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  save timer_logs , cost  ${System.currentTimeMillis() - l} ms"

        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}    ${RankUserBox.class.getSimpleName()}, cost  ${System.currentTimeMillis() -begin} ms"
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