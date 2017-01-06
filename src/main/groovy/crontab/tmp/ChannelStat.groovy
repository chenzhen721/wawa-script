#!/usr/bin/env groovy
package tmp

@Grapes([
@Grab('org.mongodb:mongo-java-driver:2.14.2'),
@Grab('commons-lang:commons-lang:2.6'),
@Grab('redis.clients:jedis:2.1.0'),
])
import com.mongodb.*
import org.apache.commons.lang.StringUtils
import redis.clients.jedis.Jedis

import java.text.SimpleDateFormat

class ChannelStat {

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

    static mongo  = new Mongo(new MongoURI(getProperties('mongo.uri','mongodb://192.168.31.249:27017/?w=1') as String))
    static historyMongo = new Mongo(new MongoURI(getProperties('mongo_history.uri', 'mongodb://192.168.31.246:27017/?w=1') as String))
    static historyDB = historyMongo.getDB('xylog_history')
    static mainRedis = new Jedis(jedis_host,main_jedis_port)



    def final Long DAY_MILL = 24*3600*1000L
    static DAY_MILLON = 24 * 3600 * 1000L
    static applys = mongo.getDB('xy_admin').getCollection('applys')
    static finance_log = mongo.getDB('xy_admin').getCollection('finance_log')
    static config = mongo.getDB('xy_admin').getCollection('config')
    static star_recommends = mongo.getDB('xy_admin').getCollection('star_recommends')
    static channel_pay = mongo.getDB('xy_admin').getCollection('channel_pay')
    static users = mongo.getDB('xy').getCollection('users')
    static rooms = mongo.getDB('xy').getCollection('rooms')
    static day_login = mongo.getDB('xylog').getCollection('day_login')
    static lottery_logs = mongo.getDB('xylog').getCollection('lottery_logs')
    static channels = mongo.getDB('xy_admin').getCollection('channels')
    static stat_daily = mongo.getDB('xy_admin').getCollection('stat_daily')
    static long zeroMill = new Date().clearTime().getTime()
    static Long yesTday = zeroMill - DAY_MILLON

    /**
     * 检测渠道注册用户重复
     * @return
     */
    static vaildateChannelDuplicationOfUser (String qd){
        def startStr = "2016-04-29 000000"
        def endStr = "2016-04-30 000000"
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HHmmss")
        def stime = 0L
        def etime = 0L
        try {
            stime = sdf.parse(startStr).getTime()
            etime = sdf.parse(endStr).getTime()
        } catch (Exception e) {
            println e
        }
        /**
         * {aggregate : "users", pipeline : [{$match : {qd:'lishihong2_h5',timestamp:{$gte:1461859200000,$lt:1461945600000} }},
         {$project:{name:'$user_name', uid:'$_id' }},
         {$group:{_id:'$name', ids: { $addToSet: "$uid" }, count:{$sum:1}},
         {$match : {count:{$gt:1} }}
         }
         ]}
         */
        BasicDBObject query = new BasicDBObject(qd:qd,timestamp:[$gte: stime, $lt:etime])
        users.setReadPreference(ReadPreference.secondaryPreferred())
        def res = users.aggregate(
                new BasicDBObject('$match', query),
                new BasicDBObject('$project', [name: '$user_name',uid:'$_id', pic:'$pic']),
                new BasicDBObject('$group', [_id: '$name', ids: [$addToSet: '$uid'] , count:[$sum:1]]),
                new BasicDBObject('$limit',20000) //top N 算法
        )
        Iterator objs = res.results().iterator()
        def allUsers = new HashSet();
        def duplications = new HashMap();
        Integer dupli_total = 0;
        Integer total = 0;
        while (objs.hasNext()){
            def user = objs.next();
            Integer count = user['count'] as Integer
            String name = user['_id'] as String
            total += count;
            allUsers.addAll(user['ids'] as Set)
            if(count > 1){
                dupli_total += count;
                duplications.put(name, user['ids'] as Set)
                DuplicationInfos(user)
            }
        }
        //println duplications
        println "-- ${qd} :  total: ${total}, ips: ${ipsRemoveDuplicat(query)} duplications name : ${dupli_total} --"
    }

    static Integer ipsRemoveDuplicat(BasicDBObject query){
        def ips = new HashSet();
        users.find(query, new BasicDBObject(timestamp:1,pic:1)).toArray().each {
            def user_id = it['_id'] as Integer
            String ip = day_login.findOne(new BasicDBObject(user_id:user_id))?.get('ip') as String
            if(StringUtils.isNotEmpty(ip)){
                ip = StringUtils.remove(ip, '192.168.1.34')
                ip = StringUtils.remove(ip, '192.168.1.35')
                ips.add(ip)
            }
        }
        return ips.size()
    }

    static DuplicationInfos(DBObject user){
        def ids = user['ids'] as Set
        String name = user['_id'] as String
        println name + "================>"
        Set<String> pics = new HashSet();
        users.find(new BasicDBObject(_id: [$in:ids.collect {it as Integer}]), new BasicDBObject(timestamp:1,pic:1)).toArray().each {
            def user_id = it['_id'] as Integer
            def ip = day_login.findOne(new BasicDBObject(user_id:user_id))?.get('ip') as String
            if(StringUtils.isNotEmpty(ip)){
                ip = StringUtils.remove(ip, '192.168.1.34')
                ip = StringUtils.remove(ip, '192.168.1.35')
            }
            pics.add(it['pic'] as String)
            println "${user_id},  register: ${new Date(it['timestamp'] as Long).format("yyyy-MM-dd HH:mm:ss")}, ${ip}, ${it['pic']} "
        }
        println "pics: ${pics.size()}"
    }

    static void main(String[] args){
        def l = System.currentTimeMillis()
        String qd = args.size() == 0 ? "" : args[0] ?: '';
        if(StringUtils.isEmpty(qd)){
            println('must input channel id')
            return
        }
        vaildateChannelDuplicationOfUser(qd)
        println " cost ${System.currentTimeMillis() -l} ms".toString()
    }
}
