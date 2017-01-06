#!/usr/bin/env groovy

@Grapes([
@Grab('org.mongodb:mongo-java-driver:2.14.2'),
@Grab('commons-lang:commons-lang:2.6'),
@Grab('redis.clients:jedis:2.1.0'),
@Grab(group = 'net.sf.json-lib', module = 'json-lib', version = '2.3', classifier = 'jdk15'),
]) import com.mongodb.BasicDBObject
import com.mongodb.DBObject
import com.mongodb.Mongo
import com.mongodb.MongoURI
import net.sf.json.JSONObject
import redis.clients.jedis.Jedis

/**
 * TODO 暂停使用
 */

@Deprecated
class RankMemberFamily
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
   static DAY_MILLON = 24 * 3600 * 1000L
   static long zeroMill = new Date().clearTime().getTime()
    //调优成 每个小时累加
    static staticDay(List<DBObject>  familyLst)
    {
        def tmp = System.currentTimeMillis()
        def begin = tmp - 3600 * 1000L
        def timeBetween = [$gte:begin , $lt: tmp]
        familyLst.each
        {
            Integer familyId = it._id as Integer
            def query = [family_id:familyId,timestamp:timeBetween]
            def res = mongo.getDB("xylog").getCollection("room_cost").aggregate(
                    new BasicDBObject('$match', query),
                    new BasicDBObject('$project', [_id: '$session._id',cost:'$cost']),
                    new BasicDBObject('$group', [_id: '$_id', num: [$sum: '$cost']]),
                    new BasicDBObject('$sort', [num:-1]),
                    new BasicDBObject('$limit',500) //top N 算法
            )
            Iterator objs = res.results().iterator()
            def memberDB = mongo.getDB("xylog").getCollection("family_member_cost")
            def userDB =  mongo.getDB("xy").getCollection("users")
            objs.each{ row ->
                def user_id = row._id
                def user = userDB.findOne(new BasicDBObject(_id:user_id),new BasicDBObject(priv:1))
                Integer priv = user?.get("priv") as Integer
                def id = user_id + "_" + familyId + "_" + new Date().format("yyyyMMdd")
                def num = row.num as Integer
                if(num>0 && (2!=priv))
                {
                    def update = new BasicDBObject(user_id:user_id as Integer,family_id:familyId,timestamp:tmp)
                    def incObject = new BasicDBObject(num:num)
                    memberDB.findAndModify(new BasicDBObject('_id',id), null, null, false,
                            new BasicDBObject('$inc':incObject,'$set':update),true, true)
                }
            }
        }
    }

    static final String  CAT_MONTH =  "month"  //按月统计
    static final String  TYPE_MEM_COST =  "mem_cost" //家族成员消费榜
    //家族成员消费月
    static staticMonth(List<DBObject>  familyLst)
    {
        def tmp = System.currentTimeMillis()
        def timeBetween = [$gte:30*DAY_MILLON, $lt: tmp]
        familyLst.each
        {
            Integer familyId = it._id as Integer
            def query = [family_id:familyId,timestamp:timeBetween]
            def res = mongo.getDB("xylog").getCollection("family_member_cost").aggregate(
                    new BasicDBObject('$match', query),
                    new BasicDBObject('$project', [_id: '$user_id',cost:'$num']),
                    new BasicDBObject('$group', [_id: '$_id', num: [$sum: '$cost']]),
                    new BasicDBObject('$sort', [num:-1]),
                    new BasicDBObject('$limit',10) //top N 算法
            )
            Iterator objs = res.results().iterator()
            completeMem(objs, familyId, TYPE_MEM_COST , CAT_MONTH)
        }
    }

    static completeMem(Iterator objs,Integer fid, String type, String cat){
        def list = new ArrayList(100)
        def coll = mongo.getDB("xyrank").getCollection("family_users")
        def users = mongo.getDB("xy").getCollection("users");
        int i = 0
        while (objs.hasNext()){
            def obj =  objs.next()
            if(users.count(new BasicDBObject([_id : obj["_id"] as Integer, "family.family_id" : fid as Integer ])) <= 0){
                continue;
            }
            i++
            obj.put("user_id", obj.get("_id"))
            obj.put("fid", fid)
            obj.put("_id", "${cat}_${type}_${fid}_${obj['user_id']}".toString())
            obj.put("cat" , cat)
            obj.put("type" , type)
            obj.put("rank", i)
            obj.put("sj",new Date().format("yyyyMMdd"))
            if(i < 50){
                list << obj
            }
        }
        coll.remove(new BasicDBObject("fid", fid))
        if(list.size() > 0)
            coll.insert(list)
    }

    static void main(String[] args)
    {
        long l = System.currentTimeMillis()
        long begin = l
        List<DBObject>  familyLst = mongo.getDB("xy_family").getCollection("familys")
                .find(new BasicDBObject("status",2),new BasicDBObject("_id",1)).toArray()

        //01.日排行
        staticDay(familyLst)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  staticDay, cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //02.月排行
        l = System.currentTimeMillis()
        staticMonth(familyLst)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  staticMonth, cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //落地定时执行的日志
        l = System.currentTimeMillis()
        def timerName = 'RankMemberFamily'
        Long totalCost = System.currentTimeMillis() - begin
        saveTimerLogs(timerName,totalCost)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  save timer_logs , cost  ${System.currentTimeMillis() - l} ms"


        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  ${RankMemberFamily.class.getSimpleName()}, cost  ${System.currentTimeMillis() -begin} ms"
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