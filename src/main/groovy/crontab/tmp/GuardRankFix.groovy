#!/usr/bin/env groovy
import com.mongodb.DBObject
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
])
import com.mongodb.MongoURI
import groovy.json.JsonSlurper
import com.mongodb.BasicDBObject
import com.mongodb.Mongo
import redis.clients.jedis.Jedis
/**
 *
 *
 * date: 13-10-18 下午18:46
 * @author: haigen.xiong@ttpod.com
 */
class GuardRankFix {
    def final static String jedis_host = "192.168.1.100"
    def static mongo = new Mongo(new MongoURI('mongodb://192.168.1.36:10000,192.168.1.37:10000,192.168.1.38:10000/?w=1&slaveok=true'))


    def static mainRedis = new Jedis(jedis_host,6379)
    def static liveRedis = new Jedis(jedis_host,6380)

    def static apply = mongo.getDB('xy_admin').getCollection('applys')
    def static finance_log = mongo.getDB('xy_admin').getCollection('finance_log')
    def static users = mongo.getDB('xy').getCollection('users')
    def static rooms = mongo.getDB('xy').getCollection('rooms')
    def static day_login = mongo.getDB('xylog').getCollection('day_login')
    def static lottery_logs = mongo.getDB('xylog').getCollection('lottery_logs')
    def static forbidden_logs = mongo.getDB('xylog').getCollection('forbidden_logs')
    def static room_costs = mongo.getDB('xylog').getCollection('room_cost')
    def static xy_user = mongo.getDB('xy_user').getCollection('users')
    def static guard_users = mongo.getDB('xylog').getCollection('guard_users')
    def static day_login_history = mongo.getDB('xylog_history').getCollection('day_login_history')

    static long DAY_MILLON = 24 * 3600 * 1000L

    static def guard_begin =  Date.parse("yyyy-MM-dd HH:mm" ,"2015-05-04 00:00").getTime()
    static def guard_end =  Date.parse("yyyy-MM-dd HH:mm" ,"2015-05-05 00:02").getTime()

    static guard_rank_fix(){
        def roomslst = rooms.find(new BasicDBObject(timestamp:[$gte:System.currentTimeMillis() - 30 *DAY_MILLON],pic_url:[$exists:true]), //30天内开播的主播
                new BasicDBObject("_id",1)).toArray()
        roomslst.each {
            Integer starId = it._id as Integer
            setStarUserListRank(starId)
        }
    }

    static void setStarUserListRank(Integer starId){
        List user_list = guard_users.find(new BasicDBObject("room":starId,expire:['$gte':1429984800000])).toArray()
        //获得守护用户列表
        if(user_list != null && user_list.size() > 0){

            guard_users.updateMulti(new BasicDBObject(room:starId),new BasicDBObject($set:[last_rank:9999, modify:System.currentTimeMillis()]))
            Set<String> guard_cars = mainRedis.keys("guard:car:${starId}:*".toString())
            if(guard_cars != null && guard_cars.size() > 0){
                mainRedis.del((String[])guard_cars.toArray())
            }

            Map<String, Long> user_rank = new HashMap<String, Long>()
            user_list.each {DBObject user ->
                String user_id = user.get('user_id') as String
                //获得用户购买时间
                Long btime = getUserBeginTime(user_id, starId)

                //计算上周用户消费总额
                Long cost = getUserCostTotal(user_id, starId, btime)

                //记录用户总额
                if(cost > 0)
                    user_rank.put(user_id.toString(), cost)

            }
            //用户消费排序
            user_rank = user_rank.sort{ a, b ->
                b.value - a.value
            }
            //设置排行榜和座驾
            List<String> top4_uids = user_rank.keySet().take(4);
            saveRank(starId, top4_uids)
        }
    }

    static Long getUserBeginTime(String userId, Integer starId){
        Long begin_time = guard_begin
        def user_buy = room_costs.find(new BasicDBObject(type:'buy_guard', 'session._id':userId,'session.data.xy_star_id':starId), new BasicDBObject(timestamp:1))
                .sort(new BasicDBObject('timestamp':-1)).limit(1).toArray()

        if(user_buy != null && user_buy.size() > 0){
            begin_time = user_buy[0]?.get('timestamp') as Long
        }
        begin_time = Math.max(begin_time, guard_begin)
        return begin_time

    }

    static Long getUserCostTotal(String userId, Integer starId, Long begin_time){
        def res = room_costs.aggregate(
                new BasicDBObject('$match', ['session._id':userId,'session.data.xy_star_id':starId, type:['$in':['grab_sofa','send_gift','level_up','song']],
                                             timestamp: [$gt: begin_time, $lt: guard_end]]),
                new BasicDBObject('$project', [_id: '$session._id',cost:'$star_cost']),
                new BasicDBObject('$group', [_id: '$_id', num: [$sum: '$cost']]),
                new BasicDBObject('$sort', [num:-1]),
                new BasicDBObject('$limit',1) //top N 算法
        )
        Iterable objs = res.results()
        Long total = 0l;
        objs.each { row ->
            total = row.num
        }
        return total
    }

    //前四名座驾ID
    private static CAR_IDS = [133,134,135,136]
    private static saveRank(Integer roomId, List<String> uids){
        //设置前四名座驾
        if(uids == null || uids.size() <= 0) return
        def guard_users = mongo.getDB("xylog").getCollection("guard_users")
        int rank = 1
        uids.each {user_id ->
            if(user_id && rank <= 4){//前四名
                def id = "${roomId}_${user_id}".toString()
                if(guard_users.update(new BasicDBObject(_id:id, expire:[$gte:System.currentTimeMillis()]), //有效期内的守护
                        new BasicDBObject($set:[last_rank:rank, modify:System.currentTimeMillis()]), false ,false).getN() ==1){
                    //前四名座驾
                    def key = "guard:car:${roomId}:${user_id}".toString()
                    mainRedis.set(key, CAR_IDS[rank-1].toString());
                    //mainRedis.expire(key,(WEEK_MILLON/1000) as Integer);
                    mainRedis.expireAt(key,(new Date()+7).clearTime().getTime())
                    rank++
                }
            }
        }
    }


    static guard_cost_fix(){
        def roomslst = rooms.find(new BasicDBObject(timestamp:[$gte:System.currentTimeMillis() - 30 *DAY_MILLON],pic_url:[$exists:true]), //30天内开播的主播
                new BasicDBObject("_id",1)).toArray()
        roomslst.each {
            Integer starId = it._id as Integer
            List user_list = guard_users.find(new BasicDBObject("room":starId,expire:['$gte':System.currentTimeMillis()])).toArray()
            //获得守护用户列表
            if(user_list != null && user_list.size() > 0){
                user_list.each {DBObject user ->
                    String user_id = user.get('user_id') as String
                    //mainRedis.del("room:guarder:rank:${starId}_1".toString())
                    //计算上周用户消费总额
                    Long cost = getUserCostTotal(user_id, starId, guard_begin)

                    //重新设置用户消费榜
                    if(cost > 0){
                        println starId + ":" + user_id + ":" + cost

                        mainRedis.zadd("room:guarder:rank:${starId}_1".toString(), cost, user_id)
                    }


                }
            }
        }
    }

    static void main(String[] args)
    {
        long l = System.currentTimeMillis()

        //guard_rank_fix()
        guard_cost_fix()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   GuardRankFix, cost  ${System.currentTimeMillis() -l} ms"
    }



}