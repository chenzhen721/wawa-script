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
 * 直播间排行榜定时任务
 * 半个小时执行一次
 */
class RankRoom {
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
    static users = mongo.getDB("xy").getCollection("users");
    static rooms = mongo.getDB("xy").getCollection("rooms");
    static lottery_logs = mongo.getDB('xylog').getCollection('lottery_logs')
    static rooms_rank = mongo.getDB('xyrank').getCollection('rooms')

    static final Integer size = 100

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
            //当前周期积分
            if(curr){
                earned_points = (curr?.get('earned_points') ?: 0 ) as Integer
                users_points = (curr?.get('users_points') ?: 0 ) as Integer
            }
            //新人扶持积分
            Integer new_points = (roomRankObj?.get('new_points') ?: 0 ) as Integer
            //运营加成积分
            Integer op_points = (roomRankObj?.get('op_points') ?: 0 ) as Integer

            //总分
            total = yes_total + earned_points + users_points + new_points + op_points
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
        room_rank();
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${RankRoom.class.getSimpleName()}, cost  ${System.currentTimeMillis() -begin} ms"
    }



}