#!/usr/bin/env groovy
package crontab.rank

@Grapes([
@Grab('org.mongodb:mongo-java-driver:2.14.2'),
@Grab('commons-lang:commons-lang:2.6'),
@Grab('redis.clients:jedis:2.1.0'),
@Grab(group = 'net.sf.json-lib', module = 'json-lib', version = '2.3', classifier = 'jdk15'),
])

import com.mongodb.BasicDBObject
import com.mongodb.Mongo
import redis.clients.jedis.Jedis
import net.sf.json.JSONObject
import com.mongodb.DBObject
import com.mongodb.MongoURI

/**
 * 每周计算主播所属家族  计算方式：主播属于每周消费最高的家族
 * @author: jiao.li@ttpod.com
 * Date: 13-10-21 下午4:45
 */

class RankStarFamily
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

    static final String jedis_host = getProperties("main_jedis_host", "192.168.31.249")
    static final String chat_jedis_host = getProperties("chat_jedis_host", "192.168.31.249")
    static final String live_jedis_host = getProperties("live_jedis_host", "192.168.31.249")

    static final Integer main_jedis_port = getProperties("main_jedis_port",6379) as Integer
    static final Integer chat_jedis_port = getProperties("chat_jedis_port",6379) as Integer
    static final Integer live_jedis_port = getProperties("live_jedis_port",6379) as Integer

    static final String user_jedis_host = getProperties("user_jedis_host", "192.168.31.246")
    static final Integer user_jedis_port = getProperties("user_jedis_port",6379) as Integer
    static userRedis = new Jedis(user_jedis_host,user_jedis_port)


    static mongo  = new Mongo(new MongoURI(getProperties('mongo.uri','mongodb://192.168.31.249:27017/?w=1') as String))
    static mainRedis = new Jedis(jedis_host, main_jedis_port)
    static chatRedis = new Jedis(chat_jedis_host, chat_jedis_port)

    static WEEK_MILLON = 7 * 24 * 3600 * 1000L


    //统计上一周
    static Map getThisSunWeek(){
        def cal =  Calendar.getInstance()
        cal.setFirstDayOfWeek(Calendar.MONDAY);

        cal.set(Calendar.DAY_OF_WEEK,2)
        cal.add(Calendar.DAY_OF_YEAR,-7)
        def beginDate = cal.getTime().clearTime()
        long begin = beginDate.getTime()
        [$gte: begin,$lt: begin + 7*24*3600*1000L]

    }
    static timestamp_between = getThisSunWeek();

    /**
     * 统计上周消费最高的家族
     * @return
     */
    static buildCostTopRank() {
        def res = mongo.getDB("xylog").getCollection("room_cost").aggregate(
                new BasicDBObject('$match', [timestamp: timestamp_between,
                        "session.data.xy_star_id": [$ne: null],
                        "family_id": [$ne: null]]),

                new BasicDBObject('$project', [fid : '$family_id',
                        uid : '$session.data.xy_star_id', cost: '$cost']),

                new BasicDBObject('$group', [
                        _id: [ fid :'$fid', uid: '$uid'],
                        sum_cost:[$sum: '$cost']
                ]),
                new BasicDBObject('$sort', [sum_cost:-1]),
                new BasicDBObject('$group', [
                        _id: '$_id.uid',
                        fid : [$first : '$_id.fid'],
                        cost:[$first: '$sum_cost']
                ])
        )
        return res.results().iterator()
    }

    //每周记录拥护榜第一名家族，并且推送全场
    private  static void createFisrtFamily()
    {
        def familyQuery =  new BasicDBObject("status", 2).append('family_level', new BasicDBObject('$gt', 0))
        def familyField =  new BasicDBObject(leader_id:1,badge_name:1,family_name:1,family_pic:1,)
        def support_family = mongo.getDB("xy_family").getCollection("familys").find(familyQuery,familyField).sort(new BasicDBObject("family_level", -1)).limit(1).toArray().get(0)
        def type =  "support"
        def familyId = support_family.get("_id")
        def support_num1 = [fid: familyId,
                cat: 'total',
                type: type,
                rank:1,
                sj: new Date().format("yyyyMMdd"),
                total_num:support_family?.get("family_level")]


        //记录上一周拥护榜第一名家族，并且推送全场
        if(support_num1 && support_family)
        {
            def cal =  Calendar.getInstance()
            cal.setFirstDayOfWeek(Calendar.MONDAY)
            def week = cal.get(Calendar.WEEK_OF_YEAR)
            def log_id =  type + "_" + new Date().format("yyyy") + week

            def curr =  cal.getTime().format("yyyy-MM-dd")
            cal.set(Calendar.DAY_OF_WEEK,Calendar.MONDAY);
            //the day of the week   Monday
            def firstday = cal.getTime().format("yyyy-MM-dd")
            def family_rank = mongo.getDB("xylog").getCollection("family_rank");
            if( curr.equals(firstday) && family_rank.findOne(new BasicDBObject("_id", log_id)) == null)
            {
                support_num1["_id"] = log_id;
                support_num1["timestamp"] = System.currentTimeMillis();
                family_rank.insert(new BasicDBObject((Map)support_num1));
                String family_champion_key = "family:support:champion"
                mainRedis.del(family_champion_key)
                //mark the first family
                def family = mongo.getDB("xy_family").getCollection("familys")
                family.update(new BasicDBObject("week_${type}", new BasicDBObject('$gt', 0)),
                        new BasicDBObject('$set',  new BasicDBObject("week_${type}" , 0)), false, true);

                family.findAndModify(new BasicDBObject("_id", support_family["_id"] as Integer),
                        new BasicDBObject('$set' , new BasicDBObject("week_${type}", 1)));

                //TODO cancel default car of leader
                def users = mongo.getDB("xy").getCollection("users");
                Integer uid = support_family.get("leader_id") as Integer
                users.update(new BasicDBObject('_id',uid),
                        new BasicDBObject('$unset',new BasicDBObject("car.curr",1)))
                //设置荣耀家族族长获得座驾
                setCar(uid)

                //publish to all channel
                def json = '{"action": "family.support","data_d":{' +
                        '"fid":'+ support_family["_id"] +
                        ',"badge_name":"'+ support_family["badge_name"] +'"'+
                        ',"family_name":"'+ support_family["family_name"] +'"'+
                        ',"family_pic":"'+ support_family["family_pic"] +'"'+
                        ',"leader_id":'+ support_family["leader_id"] +
                        '}}';
                chatRedis.publish("ALLchannel", json)
            }
        }
    }

    //荣耀家族族长获得一周座驾
    private static setCar(Integer userId){
        //天狼座驾
        String carId = "130"
        String entryKey = "car."+carId;
        Long now = System.currentTimeMillis();
        def users = mongo.getDB("xy").getCollection("users");
        if(users.update(new BasicDBObject('_id',userId).append(entryKey,new BasicDBObject('$not', new BasicDBObject('$gte',now))),
                new BasicDBObject('$set',new BasicDBObject(entryKey,now + WEEK_MILLON)) ).getN() == 1
                || 1 == users.update(new BasicDBObject('_id',userId).append(entryKey,new BasicDBObject('$gt',now)),
                        new BasicDBObject('$inc',new BasicDBObject(entryKey,WEEK_MILLON))).getN() ){

            userRedis.set("user:${userId}:car".toString(), carId.toString());
            userRedis.expire("user:${userId}:car".toString(),(WEEK_MILLON/1000) as Integer);
            users.update(new BasicDBObject('_id', userId), new BasicDBObject('$set', new BasicDBObject("car.curr", carId)));
        }
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
            Integer starLevel  = 0
            if(null == beanTotal)
                beanTotal = 0L
            if(starIds.contains(star_id) && beanTotal> 0L)
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

    static latestStarIds(){
        List<Integer> ids = new ArrayList<Integer>();
        long now = System.currentTimeMillis()
        long begin = now - WEEK_MILLON;
        def res = mongo.getDB("xylog").getCollection("room_edit").aggregate(
                new BasicDBObject('$match', [timestamp: [$gte: begin, $lt: now]]),
                new BasicDBObject('$project', [id : '$session._id']),
                new BasicDBObject('$group', [_id: '$id'])
        )
        def objs = res.results().iterator()
        while (objs.hasNext()){
            def obj =  objs.next()
            ids.add(obj["_id"] as Integer);
        }
        return ids
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
        while (objs.hasNext()){
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

    static buildSupportRankByLevel(){
        def familys = mongo.getDB("xy_family").getCollection("familys").find(new BasicDBObject("status", 2)
                .append('family_level', new BasicDBObject('$gt', 0)),new BasicDBObject("_id", 1)
                .append("family_level", 1)).sort(new BasicDBObject("family_level", -1)).limit(50).iterator()

        return familys
    }

    static final String  CAT_TOTAL =  "total"   //总计
    static final String  TYPE_SUPPORT =  "support"   //拥护榜

    static void main(String[] args)
    {
        long l = System.currentTimeMillis()
        long begin = l
        def users = mongo.getDB("xy").getCollection("users");
        def family = mongo.getDB("xy_family").getCollection("familys")
        Iterator objs=  buildCostTopRank()
        //清空所有主播所属家族
        users.update(new BasicDBObject('priv', 2), new BasicDBObject('$unset',new BasicDBObject('family', 1)), true, true);
        //初始化家族主播数量
        family.update(new BasicDBObject('star_count', new BasicDBObject('$gt', 0)),
                        new BasicDBObject('$set',  ["star_count" : 0]), false, true);
        while (objs.hasNext())
        {
            def obj =  objs.next()
            def uid = obj.get("_id")
            def fid = obj.get("fid")
            if(family.count(new BasicDBObject("_id", fid).append("status", 2)) > 0 )
            {
                // 设置主播所属家族
                def query = new BasicDBObject('priv', 2).append("_id", uid)
                users.findAndModify(query, new BasicDBObject('$set',  [family: [family_id:fid,family_priv:3,timestamp:System.currentTimeMillis()]]))
                // 设置家族主播数量
                family.findAndModify(new BasicDBObject('_id', fid), new BasicDBObject('$inc',  ["star_count" : 1]))
            }
        }
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   select star to family , cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //星光榜
        l = System.currentTimeMillis()
        def fids = mongo.getDB("xy_family").getCollection("familys").find(new BasicDBObject("status", 2),
                new BasicDBObject("_id", 1)).toArray()*._id

        if(fids == null || fids.size() <= 0)
            return;

        fids.each{
            familyStarXingGuang(it)
        }
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   familyStarXingGuang, cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //家族拥护榜
        l = System.currentTimeMillis()
        complete(buildSupportRankByLevel(), TYPE_SUPPORT , CAT_TOTAL)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   buildSupportRank, cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //设置荣耀家族
        l = System.currentTimeMillis()
        createFisrtFamily()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   create lastWeek FisrtFamily , cost  ${System.currentTimeMillis() -l} ms"
        Thread.sleep(1000L)

        //落地定时执行的日志
        l = System.currentTimeMillis()
        def timerName = 'RankStarFamily'
        Long totalCost = System.currentTimeMillis() - begin
        saveTimerLogs(timerName,totalCost)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  save timer_logs , cost  ${System.currentTimeMillis() - l} ms"


        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}    ${RankStarFamily.class.getSimpleName()} , cost  ${System.currentTimeMillis() - begin} ms"
    }

    //落地定时执行的日志
    private static saveTimerLogs(String timerName,Long totalCost)
    {
        def timerLogsDB =  mongo.getDB("xyrank").getCollection("timer_logs")
        def tmp = System.currentTimeMillis()
        def id = timerName + "_"  + new Date().format("yyyyMMdd")
        def update = new BasicDBObject(timer_name:timerName,cost_total:totalCost,cat:'week',unit:'ms',timestamp:tmp)
        timerLogsDB.findAndModify(new BasicDBObject('_id',id), null, null, false,new BasicDBObject('$set',update),true, true)
    }
}