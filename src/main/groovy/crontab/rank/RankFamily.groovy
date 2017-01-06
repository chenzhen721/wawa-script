#!/usr/bin/env groovy

@Grapes([
@Grab('org.mongodb:mongo-java-driver:2.14.2'),
@Grab('commons-lang:commons-lang:2.6'),
@Grab('redis.clients:jedis:2.1.0'),
@Grab(group = 'net.sf.json-lib', module = 'json-lib', version = '2.3', classifier = 'jdk15'),
]) import com.mongodb.BasicDBObject
import com.mongodb.DBObject
import com.mongodb.Mongo
import net.sf.json.JSONObject
import com.mongodb.MongoURI

/**
 * TODO 暂停使用
 */

@Deprecated
class RankFamily
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
    static WEEK_MILLON = 7 * 24 * 3600 * 1000L
    static long zeroMill = new Date().clearTime().getTime()
    //待调优成 每个小时累加
    static staticDay(def familyId)
    {
        def now = System.currentTimeMillis()
        String ymd = new Date().format("yyyyMMdd")
        def begin = now - 3600 * 1000L
        def timeBetween = [$gte:begin , $lt: now]
        def query = new BasicDBObject(family_id: familyId,timestamp:timeBetween)

        def costList = mongo.getDB("xylog").getCollection("room_cost").
                find(query,new BasicDBObject(cost:1))
                .toArray()

        def cost = costList.sum {it.cost?:0} as Long
        def id = familyId + "_" + ymd
        if(cost>0)
        {
            def update = new BasicDBObject(family_id:familyId,timestamp:now)
            def incObject = new BasicDBObject(num:cost)
            def memberDB = mongo.getDB("xylog").getCollection("room_cost_family")
            memberDB.findAndModify(new BasicDBObject('_id',id), null, null, false,
                    new BasicDBObject('$inc':incObject,'$set':update),true, true)
        }
    }
/*
    *//**
     *
     *
     * 家族消费榜 统计家族总的消费
     * @return
     *//*
    static buildCostRank(fids , day)
    {
        long now = System.currentTimeMillis()
        long zeroMill = new Date().clearTime().getTime()
        def res = mongo.getDB("xylog").getCollection("room_cost").aggregate(
                new BasicDBObject('$match', [timestamp: [$gt: zeroMill - day * DAY_MILLON, $lte: now], "family_id": [$in: fids]]),
                new BasicDBObject('$project', [fid : '$family_id',cost: '$cost']),
                new BasicDBObject('$group', [_id: '$fid',total_num:[$sum: '$cost']]),
                new BasicDBObject('$sort', [total_num:-1]))
        return res.results().iterator()
    }*/


    /**
     *
     *
     * 家族消费榜 统计家族总的消费
     * @return
     */
    static buildCostRankNew(int day)
    {
        long now = System.currentTimeMillis()
        long zeroMill = new Date().clearTime().getTime()
        def res = mongo.getDB("xylog").getCollection("room_cost_family").aggregate(
                new BasicDBObject('$match', [timestamp: [$gt: zeroMill - day * DAY_MILLON, $lte: now]]),
                new BasicDBObject('$project', [fid : '$family_id',cost: '$num']),
                new BasicDBObject('$group', [_id: '$fid',total_num:[$sum: '$cost']]),
                new BasicDBObject('$sort', [total_num:-1]))
        return res.results().iterator()
    }


    static latestStarIds()
    {
        List<Integer> ids = new ArrayList<Integer>();
        long now = System.currentTimeMillis()
        long begin = now - WEEK_MILLON;
        def res = mongo.getDB("xylog").getCollection("room_edit").aggregate(
                new BasicDBObject('$match', [timestamp: [$gte: begin, $lt: now]]),
                new BasicDBObject('$project', [id : '$session._id']),
                new BasicDBObject('$group', [_id: '$id']))

        def objs = res.results().iterator()
        while (objs.hasNext())
        {
            def obj =  objs.next()
            ids.add(obj["_id"] as Integer);
        }

        return ids
    }

    /**
     * 家族人数榜 统计家族总人数 -> 改为家族实力榜 根据家族成员的财富值进行排名
     * @return
     */
    static buildCountRank(fids)
    {
        def res = mongo.getDB("xy").getCollection("users").aggregate(
                new BasicDBObject('$match', ["priv" : [$ne: 2],"family.family_id": [$in: fids],"finance.coin_spend_total": [$ne: null]]),
                new BasicDBObject('$project', [_id: '$family.family_id',coin : '$finance.coin_spend_total']),
                new BasicDBObject('$group', [_id: '$_id', total_num: [$sum: '$coin']]),
                new BasicDBObject('$sort', [total_num:-1]))

        return res.results().iterator()
    }

    static complete(Iterator objs, String type, String cat)
    {
        def list = new ArrayList(50)
        def coll = mongo.getDB("xyrank").getCollection("family")
        def family = mongo.getDB("xy_family").getCollection("familys")
        int i = 0
        //清除
        family.update(new BasicDBObject("rank_${type}", new BasicDBObject('$gt', 0)),
                new BasicDBObject('$set',  new BasicDBObject("rank_${type}" , 0)), false, true);
        while (objs.hasNext())
        {
            def obj =  objs.next()
            i++
            obj.put("fid", obj.get("_id"))
            obj.put("_id", "${cat}_${i}_${type}_".toString() + obj.get("fid"))
            obj.put("cat" , cat)
            obj.put("type" , type)
            obj.put("rank", i)
            obj.put("sj",new Date().format("yyyyMMdd"))
            obj.put("total_num", obj.get("total_num") ?: obj.get("family_level") ?: 0)
            obj.removeField("family_level")
            if(i < 50){
                list << new BasicDBObject(obj)
            }
            family.findAndModify(new BasicDBObject("_id", obj.get("fid") as Integer),
                    new BasicDBObject('$set' , new BasicDBObject("rank_${type}", i)));
        }

        coll.remove(new BasicDBObject("type",type))
        if(list.size() > 0)
            coll.insert(list)
    }

    //统计每个家族中所有的主播的星光值及家族的星光总值
    static void familyStarXingGuang(def fid)
    {
        def family_star =  mongo.getDB("xyrank").getCollection("family_star")
        def users = mongo.getDB("xy").getCollection("users")
        def familys  = mongo.getDB("xy_family").getCollection("familys")

        def starIds = latestStarIds(); //最近7天有过直播的主播

        def user_field = new BasicDBObject("finance.bean_count_total": 1)
        def stars = users.find(new BasicDBObject('family.family_id': fid).append("priv",2), user_field).sort(new BasicDBObject("finance.bean_count_total": -1)).toArray()
        Integer family_level = 0
        def list = new ArrayList(100)
        for(DBObject star : stars)
        {
            Integer star_id = star.get("_id") as Integer
            Long beanTotal = ((Map)star.get("finance"))?.get("bean_count_total")
            if(null == beanTotal)
                beanTotal = 0L
            Integer starLevel  = 0
            if(starIds.contains(star_id)&& beanTotal>0L)
            {
                def jsonText = new URL("http://api.memeyule.com/public/user_level/"+beanTotal).getText("utf-8")
                JSONObject obj  =  JSONObject.fromObject(jsonText);
                String sStarlevel = (String)obj.get("data")
                starLevel  = Integer.parseInt(sStarlevel)
            }

            family_level = family_level +  starLevel
            BasicDBObject star_xg = new BasicDBObject();
            star_xg.put("_id",fid+"_"+star_id+"_"+starLevel)
            star_xg.put("fid",fid)
            star_xg.put("bean_xg_total",beanTotal)
            star_xg.put("xg_level",starLevel)
            star_xg.put("xy_star_id",star_id)

            star_xg.put("type","xg")
            list << star_xg
        }
        family_star.remove(new BasicDBObject("fid":fid ,"type":"xg"))
        if(list.size() > 0)
            family_star.insert(list)
        familys.update(new BasicDBObject('_id',fid),new BasicDBObject('$set':new BasicDBObject('family_level',family_level)))
    }

    static buildSupportRankByLevel()
    {
        def familys = mongo.getDB("xy_family").getCollection("familys").find(new BasicDBObject("status", 2)
            .append('family_level', new BasicDBObject('$gt', 0)),new BasicDBObject("_id", 1)
            .append("family_level", 1)).sort(new BasicDBObject("family_level", -1)).limit(50).iterator()

        return familys
    }

    static final String  CAT_TOTAL =  "total"   //总计
    static final String  CAT_MONTH =  "month"  //按月统计

    static final String  TYPE_COST =  "cost"   //消费榜
    static final String  TYPE_SUPPORT =  "support"   //拥护榜
    static final String  TYPE_NUM =  "num"   //成员人数榜 -> 改为家族实力榜

    static void main(String[] args)
    {
        long l = System.currentTimeMillis()
        long begin = l
        def fids = mongo.getDB("xy_family").getCollection("familys").find(new BasicDBObject("status", 2),
                    new BasicDBObject("_id", 1)).toArray()*._id
        //01.当天的家族消费汇总
        fids.each
        {
            staticDay(it)
        }
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticDay, cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //02.家族消费榜 统计家族总的消费
        l = System.currentTimeMillis()
        complete(buildCostRankNew(30), TYPE_COST , CAT_MONTH) //按月统计
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   buildCostRank, cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

       //03.家族人数榜 统计家族总人数 -> 改为家族实力榜 根据家族成员的财富值进行排名
        l = System.currentTimeMillis()
        complete(buildCountRank(fids), TYPE_NUM , CAT_TOTAL)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   buildCountRank, cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //04.统计每个家族中所有的主播的星光值及家族的星光总值
        l = System.currentTimeMillis()
        fids.each
        {
            familyStarXingGuang(it)
        }
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   familyStarXingGuang, cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //05.家族拥护榜
        l = System.currentTimeMillis()
        complete(buildSupportRankByLevel(), TYPE_SUPPORT , CAT_TOTAL)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   buildSupportRank, cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //落地定时执行的日志
        l = System.currentTimeMillis()
        def timerName = 'RankFamily'
        Long totalCost = System.currentTimeMillis() - begin
        saveTimerLogs(timerName,totalCost)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  save timer_logs , cost  ${System.currentTimeMillis() - l} ms"

        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}    ${RankFamily.class.getSimpleName()}, total cost  ${System.currentTimeMillis() -begin} ms"
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