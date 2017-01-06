#!/usr/bin/env groovy

@Grapes([
@Grab('org.mongodb:mongo-java-driver:2.14.2'),
@Grab('commons-lang:commons-lang:2.6'),
@Grab('redis.clients:jedis:2.1.0'),
])
import com.mongodb.Mongo
import com.mongodb.MongoURI
import com.mongodb.BasicDBObject
import redis.clients.jedis.Jedis
import groovy.json.JsonBuilder
import java.text.SimpleDateFormat
import java.util.concurrent.atomic.AtomicLong

/**
 * 排行榜定时任务
 * 半个小时执行一次
 */
class ActivityRank {
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

    static final Integer main_jedis_port = getProperties("main_jedis_port",6379) as Integer
    static final Integer chat_jedis_port = getProperties("chat_jedis_port",6379) as Integer

    static mongo  = new Mongo(new MongoURI(getProperties('mongo.uri','mongodb://192.168.31.246:27017/?w=1') as String))

    static DAY_MILLON = 24 * 3600 * 1000L
    static MIN_MILLON = 1800 * 1000L

    static long zeroMill = new Date().clearTime().getTime()
    static room_cost = mongo.getDB("xylog").getCollection("room_cost")
    static active_award_logs = mongo.getDB('xyactive').getCollection('active_award_logs')
    static users = mongo.getDB("xy").getCollection("users");
    static rooms = mongo.getDB("xy").getCollection("rooms");
    static lottery_logs = mongo.getDB('xylog').getCollection('lottery_logs')
    static lives = mongo.getDB('xylog').getCollection('room_edit')
    static room_feather = mongo.getDB('xylog').getCollection('room_feather')
    static rooms_rank = mongo.getDB('xyrank').getCollection('rooms')
    static chatRedis = new Jedis(chat_jedis_host, chat_jedis_port)

    static final Integer size = 100

    //皇城争夺战 2015.07.16-07.22
    public static final Long _begin = new SimpleDateFormat("yyyyMMdd").parse("20150716").getTime()
    public static final Long _end = new SimpleDateFormat("yyyyMMddHHmm").parse("201507230010").getTime()
    public final static String ACTIVE_NAME = "Castle_2015"
    public static final List<Integer> GIFT_LIST = [533,534,535]
    static List<Integer> exps = [20000, 10000, 5000, 3000, 2000, 1000, 1000, 1000, 1000, 1000]

    //每30分钟发放一次财富值奖励，发放的对象为该30分钟最后一秒霸占榜单的10个用户
    /**
     * 财富经验奖励详细
     第一名：3000；
     第二-四名：1500；
     第五-十名：1000；
     */
    static void award_exp()
    {
        Long currentTime = System.currentTimeMillis()
        if (currentTime < _begin || currentTime > _end)
            return

        if(active_award_logs.count(new BasicDBObject(['type': ACTIVE_NAME,
                              timestamp: [$gt: currentTime- 29 * 60 * 1000L, $lte:currentTime]])) >= 1){
            println "already award in 30 mins"
            return
        }

        def now = Math.min(currentTime, _end)
        def filter = $$([type: "send_gift", timestamp: [$gte: _begin, $lt: now], "session.data.xy_star_id": [$ne: null],
                         'session.data._id': [$in: GIFT_LIST]])
        //找到排名第一的主播
        def res = room_cost.aggregate($$('$match', filter),
                $$('$project', [_id: '$session.data.xy_star_id', cost: '$star_cost']),
                $$('$group', [_id: '$_id', cost: [$sum: '$cost']]),
                $$('$sort', [cost: -1]),
                $$('$limit', 1) //top N 算法
        ).results().iterator()
        if(!res.hasNext()) return

        def obj = res.next()
        def star_id = obj.get('_id') as Integer

        def user_filter = $$(["session.data.xy_star_id":star_id, timestamp: [$gte: _begin, $lt: now], "session._id": [$ne: null],
                         type: "send_gift", 'session.data._id': [$in: GIFT_LIST]])
        def iter = room_cost.aggregate($$('$match', user_filter),
                $$('$project', [_id: '$session._id',earned: '$session.data.earned', cost:'$star_cost',count:'$session.data.count']),
                $$('$group', [_id: '$_id', cost: [$sum: '$cost']]),
                $$('$sort', [cost:-1]),
                $$('$limit',10) //top N 算法
        ).results().iterator()
        int index = 0;
        Integer top_user = 0;
        while (iter.hasNext() && index < exps.size())
        {
            def uid = Integer.valueOf(iter.next().get('_id') as String)
            Integer exp = exps[index++]
            if(index == 1){
                top_user = uid
            }

            def user_info = users.findAndModify(new BasicDBObject('_id',uid),
                    new BasicDBObject('$inc',new BasicDBObject('finance.coin_spend_total',exp)))
            if(user_info == null)
                continue;

            Integer userId = user_info.get('_id') as Integer
            def nickName = user_info.get('nick_name') as String
            def lotteryId = userId + "_" + ACTIVE_NAME + "_" + System.currentTimeMillis()
            saveLotteryLog(lotteryId, ACTIVE_NAME, userId, null, nickName, exp.toString(), 0, 0, 0)
        }

        def log_id = "${star_id}_${ACTIVE_NAME}_${currentTime}".toString()
        if(active_award_logs.save(new BasicDBObject([_id : log_id, star_id: star_id, user_id: top_user,
                                                  'type': ACTIVE_NAME, timestamp: currentTime])).getN() == 1){
            //推送跑道
            chatRedis.publish("ALLchannel", new JsonBuilder([action:'castle.award',
                                                             "data_d": [star:users.findOne(new BasicDBObject("_id", star_id as Integer),
                                                                     new BasicDBObject(["_id":1,"nick_name":1,"finance":1]))]] ).toString())
        }

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
     * 首页直播间排序更新
     */
    static void room_rank(){
        Long end = System.currentTimeMillis()
        Long begin = end - MIN_MILLON
        def timeBetween = Collections.unmodifiableMap([$gte: begin, $lt: end])
        //清除上个周期内的分值
        rooms_rank.updateMulti($$(_id : [$gte: 0]), $$($unset: [curr:1]))
        rooms_rank.updateMulti($$(_id : [$gte: 0]), $$( $set: ["curr.timestamp":end]))
        //今日周期内主播收益与用户数量
        def earnIter = room_cost.aggregate(
                new BasicDBObject('$match', ['session.data.xy_star_id': [$ne: null], timestamp: timeBetween]),
                new BasicDBObject('$project', [_id: '$session.data.xy_star_id', user_id: '$session._id',earned: '$session.data.earned']),
                new BasicDBObject('$group', [_id: '$_id', earned: [$sum: '$earned'], users: [$addToSet: '$user_id']])
        ).results()
        earnIter.each { BasicDBObject earndObj ->
            def curr = new HashMap();
            def starId = earndObj.get('_id') as Integer
            def userSet = new HashSet(earndObj.users)

            Integer earned = earndObj.earned  as Long
            Integer earned_points = cal_earned_points(earned)
            curr.earned = earned
            curr.earned_points = earned_points

            Integer users = userSet.size()
            Integer users_points = cal_users_points(users)
            curr.users = users
            curr.users_points = users_points
            curr.timestamp = end
            rooms_rank.update($$(_id : starId), $$($set: ["curr" : curr]), true, false)
        }

        //今日周期内主播么么数量
        def followerIter = room_feather.aggregate(
                new BasicDBObject('$match', [timestamp: timeBetween]),
                new BasicDBObject('$project', [_id: '$room', num: '$num']),
                new BasicDBObject('$group', [_id: '$_id', nums: [$sum: '$num']])
        ).results()
        followerIter.each { BasicDBObject followObj ->
            def starId = followObj.get('_id') as Integer
            Integer feathers = followObj.nums as Integer
            Integer feathers_points = cal_feathers_points(feathers)
            rooms_rank.update($$(_id : starId), $$($set: ['curr.feathers' : feathers, 'curr.feathers_points' : feathers_points, 'curr.timestamp' : end]), true, false)
        }

        //计算总分并更新直播间
        cal_room_rank_total();
    }

    static Integer cal_earned_points(Integer earned){
        if(earned < 1000) return 0;
        Integer points = (earned/1000) as Integer
        return points
    }

    static Integer cal_users_points(Integer users){
        if(users < 1) return 0;
        Integer points = users
        return points > 10 ? 10 : points
    }

    static Integer cal_feathers_points(Integer feathers){
        if(feathers < 10) return 0;
        Integer points = (feathers/10) as Integer
        return points > 10 ? 10 : points
    }

    static void cal_room_rank_total(){
        rooms_rank.find().toArray().each { BasicDBObject roomRankObj ->
            Integer starId = roomRankObj._id as Integer
            def yesterday = roomRankObj?.get('yesterday') as Map
            Integer total = 0;

            Integer yes_total = 0;
            //昨日积分
            if(yesterday)
                yes_total = (yesterday?.get('total_points') ?: 0 ) as Integer


            def curr = roomRankObj?.get('curr') as Map
            Integer earned_points = 0;
            Integer users_points = 0;
            Integer feathers_points = 0;
            //当前周期积分
            if(curr){
                earned_points = (curr?.get('earned_points') ?: 0 ) as Integer
                users_points = (curr?.get('users_points') ?: 0 ) as Integer
                feathers_points = (curr?.get('feathers_points') ?: 0 ) as Integer
            }
            //新人扶持积分
            Integer new_points = (roomRankObj?.get('new_points') ?: 0 ) as Integer
            //运营加成积分
            Integer op_points = (roomRankObj?.get('op_points') ?: 0 ) as Integer

            //总分
            total = yes_total + earned_points + users_points + feathers_points + new_points + op_points
            rooms_rank.update($$(_id : starId), $$($set: ["total" : total]), true, false)

            //更新直播间积分
            rooms.update($$(xy_star_id: starId), $$($set:["rank_value": total]))
        }
    }

    public static BasicDBObject $$(String key,Object value){
        return new BasicDBObject(key,value);
    }

    public static BasicDBObject $$(Map map){
        return new BasicDBObject(map);
    }

    static void main(String[] args)
    {
        long l = System.currentTimeMillis()
        long begin = l
        //award_exp()
        //首页主播间排序统计
        room_rank();

        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${ActivityRank.class.getSimpleName()}, cost  ${System.currentTimeMillis() -begin} ms"
    }



}