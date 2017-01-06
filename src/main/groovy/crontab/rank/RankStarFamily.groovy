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
 * 家族荣耀榜
    统计家族旗下主播每周维C收益总数,排名第一的家族下周携带黄金家族徽章
    第一家族族长拥有天狼坐骑,并携带<王>标志
    第一家族族长拥有发放贡献礼包的权利
    每周家族富豪榜排名前3的家族,族内成员根据家族贡献值可以额外获得当周家族贡献的15%,10%,5%的财富等级加成
 * @author: jiao.li@ttpod.com
 *
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

    //上周weekofyear
    static int getLastWeek() {
        Calendar cal =  Calendar.getInstance()
        //cal.setTime(Date.parse("yyyy-MM-dd HH:mm" ,"2016-03-14 01:00"))
        cal.setFirstDayOfWeek(Calendar.MONDAY)
        def week = (~cal.get(Calendar.WEEK_OF_YEAR)&1)
        return week;
    }

    //统计上周贡献榜
    static buildCostRank() {
        def week = getLastWeek()
        def cal =  Calendar.getInstance()
        //cal.setTime(Date.parse("yyyy-MM-dd HH:mm" ,"2016-03-14 01:00"))
        cal.setFirstDayOfWeek(Calendar.MONDAY)
        def curr =  cal.getTime().format("yyyy-MM-dd")
        cal.set(Calendar.DAY_OF_WEEK,Calendar.MONDAY);
        def firstday = cal.getTime().format("yyyy-MM-dd")
        def family_log = mongo.getDB("xylog").getCollection("family_log");
        def log_id =  new Date().format("yyyy") + cal.get(Calendar.WEEK_OF_YEAR)
        if(curr.equals(firstday) &&  family_log.count(new BasicDBObject("_id", log_id)) <= 0) {
            String redisKey = "family:"+week +":rank"
            Set set = mainRedis.zrevrange(redisKey,0, -1)
            if(set == null || set.size() <= 0)
                return
            //println set

            def first_log = [_id : log_id,
                             sj  : new Date().format("yyyyMMdd"),
                             familys:rankRankData(redisKey),
                             timstemp: System.currentTimeMillis()]

            //冠军家族
            try{
                Integer fid = topOneReward(set[0] as Integer)
                // for 接口 /public/family_support_champion
                String family_champion_key = "family:support:champion"
                mainRedis.del(family_champion_key)
                if(fid != null){
                    first_log.put('champion_fid',fid)
                }

                //前三名家族
                rewardTopThree(set.take(3))
                //保存排行榜
                save_last_cost(set.toList())
                //记录定时日志
                family_log.insert(new BasicDBObject((Map) first_log));

                //删除redis上周家族贡献榜
                mainRedis.del(redisKey)
            }catch(Exception e){
                println "buildCostRank Exception ${e.getMessage()}"
            }

        }
    }

    private static Map<Object, Integer> rankRankData(String redisKey){
        def all_ranks = mainRedis.zrevrangeWithScores(redisKey,0, -1)
        Map<Object, Integer> last_rank = new HashMap<>()
        all_ranks.each {redis.clients.jedis.Tuple it ->
            last_rank.put(it.getElement(), it.getScore().toInteger())
        }
        return last_rank
    }

    static final Double[] RATES = [0, 0.15, 0.1, 0.05]
    //奖励前三名家族 贡献值可以额外获得当周家族贡献的15%,10%,5%的财富等级加成
    static rewardTopThree(List<String> fids) {
        try{
            println "rewardTopThree fids : "+ fids
            def members = mongo.getDB("xy_family").getCollection("members")
            def exp_logs = mongo.getDB("xy_family").getCollection("exp_logs")
            def users = mongo.getDB("xy").getCollection("users");
            def week = getLastWeek()
            def ymd = new Date().format("yyyyMMdd")
            int rank = 0;
            def list = new ArrayList(1000)
            fids.each {
                Integer fid = it as Integer
                rank++
                members.find(new BasicDBObject("fid", fid).append("user_priv",3),
                        new BasicDBObject("uid", 1).append("week_cost_1",1).append("week_cost_0",1)).toArray()
                        .each {BasicDBObject obj ->
                    Integer uid = obj['uid']
                    Long cost = (obj["week_cost_${week}"]  ?: 0) as Long
                    Long exp = cost * RATES[rank]
                    String log_id = fid+"_"+uid+"_"+ymd
                    //println uid + ":" + exp
                    if(exp > 0 && exp_logs.count(new BasicDBObject(_id: log_id)) <= 0){
                        users.update(new BasicDBObject('_id',uid),
                                new BasicDBObject('$inc',new BasicDBObject('finance.coin_spend_total',exp)))
                    }
                    members.update(new BasicDBObject('_id',fid+"_"+uid),
                            new BasicDBObject('$inc',new BasicDBObject('exp_total',exp))
                                    .append('$set',new BasicDBObject('last_exp',exp)))
                    BasicDBObject expLog = new BasicDBObject(_id: log_id)
                    expLog.append('uid', uid)
                    expLog.append('fid', fid)
                    expLog.append('cost', cost)
                    expLog.append('exp', exp)
                    expLog.append('send_msg', 0)
                    expLog.append('timestamp', System.currentTimeMillis())
                    list << expLog
                }
            }
            if(list.size() > 0)
                exp_logs.insert(list)
            //发送经验加成消息
            //new URL("http://test.api.2339.com/java/send_family_msg").getText("utf-8")
            new URL("http://api.memeyule.com/java/send_family_msg").getText("utf-8")
        }catch (Exception e){
            println "Exception : " + e
        }

    }

    private static final Integer AWRAD_BAGS = 10
    //第一家族奖励
    static Integer topOneReward(Integer fid){
        println "first fid : "+ fid
        def family = mongo.getDB("xy_family").getCollection("familys")
        def familyField =  new BasicDBObject(leader_id:1,badge_name:1,family_name:1,family_pic:1,)
        def first_family = family.findOne(fid,familyField)
        if(first_family){
            family.update(new BasicDBObject("week_support", new BasicDBObject('$gt', 0)),
                    new BasicDBObject('$set',  new BasicDBObject("week_support" , 0)), false, true);
            //获得贡献礼包10个
            family.findAndModify(new BasicDBObject("_id", first_family["_id"] as Integer),
                    new BasicDBObject('$set' , new BasicDBObject("week_support", 1))
                            .append('$inc', new BasicDBObject("award_bags", AWRAD_BAGS))
                    );

            //cancel default car of leader
            def users = mongo.getDB("xy").getCollection("users");
            Integer uid = first_family.get("leader_id") as Integer
            users.update(new BasicDBObject('_id',uid),
                    new BasicDBObject('$unset',new BasicDBObject("car.curr",1)))

            //设置荣耀家族族长获得座驾 拥有天狼坐骑
            setCar(uid)

            //publish to all channel
            def json = '{"action": "family.support","data_d":{' +
                    '"fid":'+ first_family["_id"] +
                    ',"badge_name":"'+ first_family["badge_name"] +'"'+
                    ',"family_name":"'+ first_family["family_name"] +'"'+
                    ',"family_pic":"'+ first_family["family_pic"] +'"'+
                    ',"leader_id":'+ first_family["leader_id"] +
                    '}}';
            chatRedis.publish("ALLchannel", json)
            return fid;
        }
        return null
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

    //保存上周贡献值和排名
    static save_last_cost(List<String> fids){
        def members = mongo.getDB("xy_family").getCollection("members")
        def week = getLastWeek()
        def family = mongo.getDB("xy_family").getCollection("familys")
        //保存家族上周贡献榜
        def family_last =  mongo.getDB("xyrank").getCollection("family")
        family_last.remove(new BasicDBObject())
        def list = new ArrayList(fids.size())
        int rank = 1;
        family.update(new BasicDBObject(),
                new BasicDBObject('$set',  new BasicDBObject("rank_support" , 99999)), false, true);
        fids.each {
            def fid = it as Integer
            def obj = new BasicDBObject(_id:fid)
            obj['timestamp'] = System.currentTimeMillis()
            obj['type'] = 'cost'
            obj['cat'] = 'week'
            obj['fid'] = fid
            obj['sj'] = new Date().format("yyyyMMdd")
            obj['rank'] = rank++
            list << obj;
            family.update(new BasicDBObject("_id", fid),
                    new BasicDBObject('$set' , new BasicDBObject("rank_support", obj['rank'])));

            //同步个人贡献榜
            members.find(new BasicDBObject('fid':fid),new BasicDBObject("uid", 1).append("week_cost_1",1).append("week_cost_0",1)).toArray()
                    .each {BasicDBObject member ->
                Integer uid = member['uid']
                Long cost = (member["week_cost_${week}"]  ?: 0) as Long

                members.update(new BasicDBObject('uid',uid),
                        new BasicDBObject('$inc',new BasicDBObject('cost_total',cost)) //设置总贡献值
                                .append('$set',new BasicDBObject('last_week',cost) //同步上周贡献值 用于上周贡献榜排名
                                .append("week_cost_${week}".toString(),0)) //清除上周贡献值
                )
            }
        }
        if(list.size() > 0)
            family_last.insert(list);
    }

    static void main(String[] args)
    {
        long l = System.currentTimeMillis()
        long begin = l

        //家族贡献榜
        l = System.currentTimeMillis()
        buildCostRank()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   buildCostRank, cost  ${System.currentTimeMillis() -l} ms"

        //落地定时执行的日志
        jobFinish(begin)
    }

    /**
     * 标记任务完成  用于运维监控
     * @return
     */
    private static jobFinish(Long begin){
        def timerName = 'RankStarFamily'
        Long totalCost = System.currentTimeMillis() - begin
        saveTimerLogs(timerName, totalCost)
        println "${new Date().format('yyyy-MM-dd')}:${RankStarFamily.class.getSimpleName()}:finish  cost  ${System.currentTimeMillis() - begin} ms"
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