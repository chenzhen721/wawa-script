#!/usr/bin/env groovy
import com.mongodb.DBObject
@Grapes([
@Grab('org.mongodb:mongo-java-driver:2.14.2'),
@Grab('commons-lang:commons-lang:2.6'),
@Grab('redis.clients:jedis:2.1.0'),
])
import com.mongodb.MongoURI
import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import com.mongodb.BasicDBObject
import com.mongodb.Mongo
import org.apache.commons.lang.StringUtils
import redis.clients.jedis.Jedis

import java.text.SimpleDateFormat

/**
 * 周一执行
 * 1.定格周星
 * 2.上周房间守护排名
 * 3.自动设置周星
 */
class RankGiftMonDay {


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
    static final String api_domain = getProperties("api.domain", "http://localhost:8080/")

    static final Integer main_jedis_port = getProperties("main_jedis_port",6379) as Integer
    static final Integer chat_jedis_port = getProperties("chat_jedis_port",6379) as Integer
    static final Integer live_jedis_port = getProperties("live_jedis_port",6379) as Integer

    static final String user_jedis_host = getProperties("user_jedis_host", "192.168.31.246")
    static final Integer user_jedis_port = getProperties("user_jedis_port",6379) as Integer
    static userRedis = new Jedis(user_jedis_host,user_jedis_port)
    static mainRedis = new Jedis(jedis_host,main_jedis_port, 50000)
    static chatRedis = new Jedis(chat_jedis_host, chat_jedis_port)

    static mongo  = new Mongo(new MongoURI(getProperties('mongo.uri','mongodb://192.168.31.249:27017/?w=1') as String))
    static users = mongo.getDB('xy').getCollection('users')
    static lottery_logs = mongo.getDB('xylog').getCollection('lottery_logs')


    static String pic_folder = "/empty/www.2339.com/gift_rank/"
    static rank_url = api_domain+"rank/all_rank_Json"
    static refresh_gift_url = api_domain+"java/refresh_gift_cache"

    static DAY_MILLON = 24 * 3600 * 1000L
    static WEEK_MILLON = 7 * DAY_MILLON

    //需要排除的礼物ID 铃铛礼物ID
    static List EXCLUTION_GIFTS_IDS = [586,587,588]

    private static final Integer TIME_OUT = 20 * 1000;

    static Map getThisSunWeek(){
        def cal =  Calendar.getInstance()
        //cal.setTime(Date.parse("yyyy-MM-dd HH:mm" ,"2016-12-12 01:00"))
        cal.set(Calendar.DAY_OF_WEEK,2)
        cal.add(Calendar.DAY_OF_YEAR,-7)
        def beginDate = cal.getTime().clearTime()
        long begin = beginDate.getTime()
        long end =  begin + WEEK_MILLON
        [$gte:begin ,$lt:end]
    }

    /**
     * amount
     * 当前日期: 0
     * 倒退日期: -1 倒退一周
     * 前进: 1 前进一周
     * @param amount
     * @return
     */
    private static int geWeekOfYear(int amount) {
        Calendar cal =  Calendar.getInstance()
        //cal.setTime(Date.parse("yyyy-MM-dd HH:mm" ,"2016-12-12 01:00"))
        cal.setFirstDayOfWeek(Calendar.MONDAY)
        cal.add(Calendar.WEEK_OF_YEAR, amount)
        return cal.get(Calendar.WEEK_OF_YEAR);
    }

    static timestamp_between = getThisSunWeek();

    static staticWeek()
    {
        try{
            String lock_key = 'crontab:week:lock'
            mainRedis.set(lock_key, "1");
            mainRedis.expire(lock_key, 60);


            //保持上周礼物排行榜 记录周星主播 方法奖励
            saveGiftRank();
            //缓存上周礼物排行榜到静态文件
            def last_week_rank_url = new URL(rank_url)
            HttpURLConnection conn = null;
            def jsonText = "";
            try{
                conn = (HttpURLConnection)last_week_rank_url.openConnection()
                conn.setRequestMethod("GET")
                conn.setDoOutput(true)
                conn.setConnectTimeout(TIME_OUT);
                conn.setReadTimeout(TIME_OUT);
                conn.connect()
                jsonText = conn.getInputStream().getText("UTF-8")
            }catch (Exception e){
                println "staticWeek Exception : " + e;
            }finally{
                if (conn != null) {
                    conn.disconnect();
                    conn = null;
                }
            }
            if(StringUtils.isEmpty(jsonText)) return
            //def jsonText = new URL(rank_url).getText("utf-8")
            //coll.remove(new BasicDBObject("cat",cat))
            def fold = new File(pic_folder)

            new File(fold,"latest.js").setText(jsonText,"utf-8")
            new File(fold,new Date().format('yyyy-MM-dd')+'.js').setText(jsonText,"utf-8")

            /*
            println "remove star.gift_week:" + users.updateMulti(new BasicDBObject('priv',2),new BasicDBObject('$unset',["star.gift_week":1, "star.gift_week_award":1])).getN()

           new JsonSlurper().parseText(jsonText).gift_week.each{
                try{
                    if(! it.star){
                        return
                    }
                    def rank_list = it.rank as List
                    Integer limit = (it.star_limit ?: 0) as Integer
                    def gid =  it._id as Integer
                    for(Object rank_star : rank_list){
                        Integer rank =  (rank_star?.getAt("rank") ?: 0) as Integer
                        if(rank != 1) break;
                        Integer uid =  rank_star?.getAt("user_id") as Integer
                        if(uid){
                            def count = rank_star?.getAt("count") as Integer
                            if(count && count >= limit){
                                users.update(new BasicDBObject('_id',uid),new BasicDBObject('$addToSet',["star.gift_week":gid]))
                                //.append('$set',new BasicDBObject("star.gift_week_award."+gid,1))) //领取的奖励方式 暂停
                                //自动颁发周星奖励
                                //awardWeekStar(uid, gid)
                                //println "${uid}  -> gift ${gid}"
                            }
                        }
                    }

                }catch (e){
                    e.printStackTrace()
                }
            }*/
            //删除2周前周周星历史记录
            mongo.getDB("xyrank").getCollection("week_stars").remove(new BasicDBObject('week',geWeekOfYear(-2)))
            //定格完毕
            mainRedis.del(lock_key)
            //推送给NODEjs
            chatRedis.publish("systemChannel", new JsonBuilder([action:'week_star.completed',
                                                             "data_d": [t:System.currentTimeMillis()]]).toString())
        }catch (Exception e){
            println "staticWeek Exception " + e
            throw  e
        }
    }


    static saveGiftRank(){
        String cat = "week"
        def list = new ArrayList(100)
        //删除上周周星和贡献者
        delLastWeekStarAndUser();

        mongo.getDB("xy_admin").getCollection("gifts").find(new BasicDBObject("status",true)
                    .append('_id', new BasicDBObject('$nin',EXCLUTION_GIFTS_IDS))).each{DBObject gift ->

            Iterator objs = bulidRank(gift.get("_id"))
            int i = 0
            while (objs.hasNext()){
                def obj =  objs.next()
                def count = obj.get("count") as Integer
                def gift_id = gift.get("_id") as Integer
                if(obj.get("user_id") == null){
                    obj.put("user_id",obj.get("_id"))
                }
                def star_id = obj.get('user_id') as Integer
                def rank = ++i
                obj.put("rank",rank)
                saveWeekStar(gift, star_id, count, obj)

                obj.put("real_count", count)
                obj.put("_id","${gift_id}_${cat}_${obj.get('user_id')}".toString())
                obj.put("gift_id",gift_id)
                obj.put("pic_url",gift.get("pic_url"))
                obj.put("name",gift.get("name"))
                obj.put("cat",cat)

                obj.put("sj",new Date())
                list<< obj
            }
        }
        def coll = mongo.getDB("xyrank").getCollection("gift")
        coll.remove(new BasicDBObject("cat",cat))
        if(list.size() > 0)
            coll.insert(list)
        Thread.sleep(5000L)
    }

    private final static String WEEK_STAR_AWARD_LIST_REDIS_KEY = 'week:star:award:list'

    static saveWeekStar(DBObject gift, Integer star_id, Integer count, DBObject obj){
        Boolean star =(gift.get("star") ?: false) as Boolean //是否为周星礼物
        if(!star) return;
        //周星礼物上限判断
        def star_count =(gift.get("star_count") ?: 9999999) as Integer //周星礼物上限
        def gift_id = gift.get("_id") as Integer
        if((star_count > 0 && count > star_count)){
            obj.put("count", star_count)
            obj.put("rank",1)
        }
        try{
            //周星赠送最多用户
            Iterator userRanks = bulidUserRank(star_id, gift_id)
            while (userRanks.hasNext()) {
                def userank = userRanks.next()
                def most_user_id = userank.get("_id") as Integer
                def most_count = userank.get("count") as Integer
                def most_user = users.findOne(most_user_id,new BasicDBObject(nick_name: 1, pic: 1, "finance.bean_count_total": 1,
                        "finance.coin_spend_total": 1, "mm_no": 1))
                most_user.put("most_count", most_count)
                obj.put('most_user',  most_user)
                //发放周星奖励 标记周星和贡献者 和 奖励座驾
                if(star && obj.get('rank') == 1){
                    println  star_id  +' -> gift:'+gift_id
                    awardWeekStar(star_id, gift_id)
                    recordWeekStarAndUser(star_id, most_user_id)
                    setCar(star_id, '163')
                    setCar(most_user_id, '163')
                }
            }
        }catch (Exception e){
            println "bulidUserRank Exception " + e
        }
    }

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
                new BasicDBObject('$sort', [count:-1,timestamp:1])
        )
        return res.results().iterator()
    }

    /**
     * 周星奖励自动发放
     * @param star_id
     * @param gift_id
     */
    static awardWeekStar(Integer star_id, Integer gift_id){
        //记录主播周星对应礼物
        users.update(new BasicDBObject('_id',star_id),new BasicDBObject('$addToSet',["star.gift_week":gift_id]))
        //获取周星礼物奖励VC
        def gift = mongo.getDB("xy_admin").getCollection("gifts")
                .findOne(gift_id as Integer, new BasicDBObject(star_award:1,name:1))
        def star_award = (gift?.get('star_award') ?: 0) as Integer
        def gift_name = gift?.get('name') as String
        try{
            def time = System.currentTimeMillis()
            //主播增加VC
            def user_info = users.findAndModify(new BasicDBObject(_id:star_id),
                    new BasicDBObject('$inc', new BasicDBObject('finance.bean_count':star_award)))
            if(user_info != null){
                def id = "${star_id}_${gift_id}_${new Date().format('yyyyMMdd')}".toString()
                mongo.getDB("xylog").getCollection("weekstar_bonus_logs")
                        .insert(new BasicDBObject(_id : id, star_id : star_id, gift_id : gift_id, star_award : star_award,
                        gift_name : gift_name, timestamp:time))
                def nickName = user_info.get('nick_name') as String
                def lotteryId = star_id + "_star_week_award_" + star_award + "_" + time
                saveLotteryLog(lotteryId, 'star_week_award', star_id, star_id, nickName, gift_id.toString(), 0, star_award, 0)
            }
        }catch (Exception e){
            println "awardWeekStar star_id"+ star_id+ "  Exception : " + e
        }
    }


    //奖励座驾一周
    private static setCar(Integer userId, String carId){
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

    /**
     * 删除上次周星主播和用户和送礼用户
     */
    private static delLastWeekStarAndUser(){
        println "remove star.gift_week:" + users.updateMulti(new BasicDBObject('priv',2),new BasicDBObject('$unset',["star.gift_week":1, "star.gift_week_award":1])).getN()

        Map<String, String> stars = userRedis.hgetAll(WEEK_STAR_AWARD_LIST_REDIS_KEY)
        stars.each {String starId, String type ->
            userRedis.del("room:week:star:users:${starId}".toString())
        }
        userRedis.del(WEEK_STAR_AWARD_LIST_REDIS_KEY)
    }

    /**
     * 记录上周周星主播和用户和送礼用户
     */
    private static recordWeekStarAndUser(Integer starId, Integer userId){
        println starId+":"+userId
        userRedis.hset(WEEK_STAR_AWARD_LIST_REDIS_KEY, starId.toString(),"1")//周星获得者
        userRedis.hset(WEEK_STAR_AWARD_LIST_REDIS_KEY, userId.toString(),"2")//周星贡献者

        Integer weekSec = (WEEK_MILLON/1000) as Integer;
        if(userRedis.ttl(WEEK_STAR_AWARD_LIST_REDIS_KEY) <= 0)
            userRedis.expire(WEEK_STAR_AWARD_LIST_REDIS_KEY, weekSec)

        String room_week_star_key = "room:week:star:users:${starId}".toString()
        userRedis.sadd(room_week_star_key, userId.toString())
        if(userRedis.ttl(room_week_star_key) <= 0)
            userRedis.expire(room_week_star_key, weekSec)
    }

    public static Boolean saveLotteryLog(String lotteryId, String active_name, Integer userId, Integer star_id, String nickName,
                                         String award_name, int award_count,  long award_coin, int cost_coin){
        Long time = System.currentTimeMillis();
        Map<Object,Object> obj = new HashMap<Object,Object>();
        obj.put('_id', lotteryId);
        obj.put("user_id", userId);
        obj.put("star_id", star_id);
        obj.put("nick_name", nickName);
        obj.put("award_name", award_name);
        obj.put("award_count", award_count);
        obj.put("timestamp", time);
        obj.put("active_name", active_name);
        obj.put("cost_coin", cost_coin);
        obj.put("award_coin", award_coin);
        lottery_logs.save(new BasicDBObject(obj))
    }

    /**
     * 周星用户贡献榜
     * @param star_id
     * @param gift_id
     * @return
     */
    static bulidUserRank(Integer star_id, Integer gift_id) {

        def res = mongo.getDB("xylog").getCollection("room_cost").aggregate(
                new BasicDBObject('$match', [type: "send_gift",
                                             "session.data._id":gift_id,'session.data.xy_star_id':star_id,
                                             timestamp: timestamp_between]),
                new BasicDBObject('$project', [_id: '$session._id', count:'$session.data.count'
                ]),
                new BasicDBObject('$group', [_id: '$_id',count: [$sum: '$count']
                ]),
                new BasicDBObject('$sort', [count:-1]),
                new BasicDBObject('$limit',1)
        )
        return res.results().iterator()
    }

    /**
     * 生成上周守护榜
     * @param roomId
     * @return
     */

    static staticWeekGuard(){
        try{
            long now = System.currentTimeMillis()
            def guard_users = mongo.getDB("xylog").getCollection("guard_users")
            def roomslst = mongo.getDB("xy").getCollection("rooms").find(new BasicDBObject(timestamp:[$gte:now - 60 *DAY_MILLON]), //30天内开播的主播
                    new BasicDBObject("_id",1)).toArray()
            //删除座驾
            Set<String> car_keys = mainRedis.smembers(CAR_USER_LIST_KEY)
            if(car_keys != null && car_keys.size() >0){
                userRedis.del((String[])car_keys.toArray())
            }
            roomslst.each {
                Integer roomId = it._id as Integer
                if(guard_users.count(new BasicDBObject("room":roomId,status:true)) > 0){
                    guard_users.updateMulti(new BasicDBObject(room:roomId),new BasicDBObject($set:[last_rank:9999, modify:System.currentTimeMillis()]))
                    //List<String> objs = bulidGuardRankFromRedis(roomId)
                    List<String> objs = bulidGuardWeekRankFromRedis(roomId)
                    saveRank(roomId, objs)
                }
            }
            //删除redis 2周前守护贡献榜
            def history_week = geWeekOfYear(-2)
            String history_redisKey = "room:guarder:rank:week:${history_week}:*".toString()
            mainRedis.keys(history_redisKey).each{mainRedis.del(it)};
        }catch (Exception e){
            println "staticWeekGuard Exception:"+ e
            println "staticWeekGuard Exception:"+ e.getStackTrace()
        }

    }

    //前四名座驾ID
    private static CAR_IDS = [133,134,135,136]
    private static CAR_USER_LIST_KEY = "guard:car:room:user:keys"
    private static saveRank(Integer roomId, List<String> uids){
        def guard_users = mongo.getDB("xylog").getCollection("guard_users")
        //设置前四名座驾
        if(uids == null || uids.size() <= 0) return
        int rank = 1
        uids.each {user_id ->
            if(user_id && rank <= 4){//前四名
                def id = "${roomId}_${user_id}".toString()
                if(guard_users.update(new BasicDBObject(_id:id, expire:[$gte:System.currentTimeMillis()]), //有效期内的守护
                                new BasicDBObject($set:[last_rank:rank, modify:System.currentTimeMillis()]), false ,false).getN() ==1){
                    //前四名座驾
                    def key = "guard:car:${roomId}:${user_id}".toString()
                    //println key
                    userRedis.set(key, CAR_IDS[rank-1].toString());
                    //mainRedis.expire(key,(WEEK_MILLON/1000) as Integer);
                    userRedis.expireAt(key,(new Date()+7).getTime())
                    rank++
                    mainRedis.sadd(CAR_USER_LIST_KEY, key)
                }
            }
        }
    }



    //
    private static bulidGuardWeekRankFromRedis(Integer roomId){
        int last_week = geWeekOfYear(-1)
        //获取上一周前几名
        String redisKey = "room:guarder:rank:week:${last_week}:${roomId}".toString()
        Set set = mainRedis.zrevrange(redisKey,0, 9)
        if(set == null || set.size() <= 0)
            return null
        List<String> uids = set.take(10)
        try{
            recordGuardRankLog(roomId, redisKey)
        }catch (Exception e){
            println "bulidGuardWeekRankFromRedis Exception :" + e
            return uids
        }
        return uids
    }

    private static recordGuardRankLog(Integer roomId, String redisKey){
        try{
            //保存前一周守护榜
            def all_ranks = mainRedis.zrevrangeWithScores(redisKey,0, -1)
            List<String> last_rank = new ArrayList()
            all_ranks.each {redis.clients.jedis.Tuple it ->
                last_rank.add(it.getElement()+':'+ it.getScore().toInteger())
            }
            def task_logs =  mongo.getDB("xylog").getCollection("task_logs")
            def id = "WeekGuardRank_"+new Date().format('yyyy_MM_dd')
            def update = new BasicDBObject(timestamp:System.currentTimeMillis())
            update.append("last_week_rank.${roomId}".toString(), last_rank)
            //task_logs.remove(new BasicDBObject('_id',id))
            task_logs.update(new BasicDBObject('_id',id),new BasicDBObject('$set',update),true,false)
        }catch (Exception e){
            println "recordGuardRankLog Exception :" + e
        }
    }

    static final String NEXT_WEEK_STAR_GIFT_LIST = 'next_week_star_gift_list'
    static final Integer WEEK_STAR_GIFT_CATEID = 210
    /**
     * 自动设置周星礼物
     */
    static setWeekStarGifts(){
        try{
            //获得本周待设置周星礼物
            def next_gifts = mongo.getDB("xy_admin").getCollection("config").findOne(new BasicDBObject('_id',NEXT_WEEK_STAR_GIFT_LIST))
            if(next_gifts == null) return
            def next_gift_ids = next_gifts['gift_ids'] as List

            if(next_gift_ids != null && next_gift_ids.size() > 0){
                def gifts = mongo.getDB("xy_admin").getCollection("gifts")
                def last_week_gifts = mongo.getDB("xylog").getCollection("last_week_gifts")
                //回滚上周礼物状态
                def last_gifts = last_week_gifts.find().toArray() //获得上周周星快照
                if(last_gifts != null){
                    last_gifts.each {BasicDBObject gift ->
                        def giftid = gift.remove('_id') as Integer
                        gifts.update(new BasicDBObject(_id:giftid),new BasicDBObject('$set',gift))
                    }
                    last_week_gifts.remove(new BasicDBObject(_id:new BasicDBObject($gt:0)))
                }
                //设置本周周星 周星分类，周星状态
                List<DBObject> giftList = new ArrayList<DBObject>(10)
                def update = new BasicDBObject('category_id': WEEK_STAR_GIFT_CATEID, star:Boolean.TRUE)
                def fields = new BasicDBObject('ratio': 1,'category_id':1,star:1)
                def unset = new BasicDBObject('ratio': 1)
                next_gift_ids.each {Integer giftId ->
                    // findAndModify(DBObject query, DBObject fields, DBObject sort, boolean remove, DBObject update, boolean returnNew, boolean upsert)
                    def gift = gifts.findAndModify(new BasicDBObject('_id',giftId),fields, null, false, new BasicDBObject('$set',update).append('$unset',unset), false, false)
                    if(gift != null){
                        giftList.add(gift)
                    }
                }
                //记录礼物快照
                if(giftList.size() > 0)
                    last_week_gifts.insert(giftList)
                Thread.sleep(5000)
                //更新缓存
                String gifts_key = "all:ttxiuchang:gifts"
                mainRedis.del(gifts_key)
                new URL(refresh_gift_url).getText("utf-8")
            }
        }catch(Exception e){
            println "setWeekStarGifts Exception ${e}".toString()
        }

    }


    static void main(String[] args)
    {
        long l = System.currentTimeMillis()
        long begin = l
        //周星
        staticWeek()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticWeek, cost  ${System.currentTimeMillis() -l} ms"

        //周星切换
        l = System.currentTimeMillis()
        setWeekStarGifts()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   setWeekStarGifts, cost  ${System.currentTimeMillis() -l} ms"

        //守护周榜
        l = System.currentTimeMillis()
        staticWeekGuard()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticWeekGuard, cost  ${System.currentTimeMillis() -l} ms"

        //落地定时执行的日志
        jobFinish(begin)

    }

    /**
     * 标记任务完成  用于运维监控
     * @return
     */
    private static jobFinish(Long begin){
        def timerName = 'RankGiftMonDay'
        Long totalCost = System.currentTimeMillis() - begin
        saveTimerLogs(timerName, totalCost)
        println "${new Date().format('yyyy-MM-dd')}:${RankGiftMonDay.class.getSimpleName()}:finish  cost  ${System.currentTimeMillis() - begin} ms"
    }


    //落地定时执行的日志
    private static saveTimerLogs(String timerName,Long totalCost)
    {
        def timerLogsDB =  mongo.getDB("xyrank").getCollection("timer_logs")
        def tmp = System.currentTimeMillis()
        def id = timerName + "_"  + new Date().format("yyyyMM")
        def update = new BasicDBObject(timer_name:timerName,cost_total:totalCost,cat:'week',unit:'ms',timestamp:tmp)
        timerLogsDB.findAndModify(new BasicDBObject('_id',id), null, null, false,new BasicDBObject('$set',update),true, true)
    }
}