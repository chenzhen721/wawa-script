#!/usr/bin/env groovy
import com.mongodb.DBCollection
import com.mongodb.DBObject
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
])
import com.mongodb.Mongo
import com.mongodb.MongoURI
import com.mongodb.BasicDBObject


/**
 * 家族榜单
 */
class RankFamily {

    static Properties props = null;
    static String profilepath = "/empty/crontab/db.properties";

    static getProperties(String key, Object defaultValue) {
        try {
            if (props == null) {
                props = new Properties();
                props.load(new FileInputStream(profilepath));
            }
        } catch (Exception e) {
            println e;
        }
        return props.get(key, defaultValue)
    }

    static mongo = new Mongo(new MongoURI(getProperties('mongo.uri', 'mongodb://192.168.31.231:20000,192.168.31.236:20000,192.168.31.231:20001/?w=1&slaveok=true') as String))

    static HOUR_MILLON = 3600 * 1000L
    static DAY_MILLON = 24 * HOUR_MILLON

    static familys = mongo.getDB("xy_family").getCollection("familys")
    static member_contributions = mongo.getDB("xy_family").getCollection("member_contributions")
    static family_user_rank = mongo.getDB("xyrank").getCollection("family_user")
    static final Integer size = 100

    /**
     * 用户昨日贡献榜
     */
    static void familyUserRankYesterday() {
        familys.find($$(status:2)).toArray().each {DBObject family ->
            saveRank("yesterday",  family['_id'] as Integer)
        }
    }

    static void saveRank(String cat, Integer familyId) {
        long now = System.currentTimeMillis()
        def list = new ArrayList(500)
        Date yesterday = new Date()-1;
        member_contributions.find($$(family_id:familyId, date:yesterday.format('yyyyMMdd'))).sort($$(coin:-1)).limit(10).toArray().each {DBObject contri->
            Integer user_id = contri['user_id'] as Integer
            Long coin = contri['coin'] as Long
            list.add($$(_id: "${cat}_${familyId}_${user_id}".toString(), cat: cat, family_id:familyId, user_id: user_id, num: coin, sj: new Date()))
        }

        family_user_rank.remove($$("cat": cat, family_id:familyId,))
        if (list.size() > 0)
            family_user_rank.insert(list)
    }

    public static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    public static BasicDBObject $$(Map map) {
        return new BasicDBObject(map);
    }

    static void main(String[] args) {
        long l = System.currentTimeMillis()
        long begin = l
        //用户昨日贡献榜
        familyUserRankYesterday()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   familyUserRankYesterday , cost  ${System.currentTimeMillis() - l} ms"

        //落地定时执行的日志
        l = System.currentTimeMillis()
        def timerName = 'RankUser'
        Long totalCost = System.currentTimeMillis() - begin
        saveTimerLogs(timerName, totalCost)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  save timer_logs , cost  ${System.currentTimeMillis() - l} ms"

        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}    ${RankFamily.class.getSimpleName()} , cost  ${System.currentTimeMillis() - begin} ms"
    }

    //落地定时执行的日志
    private static saveTimerLogs(String timerName, Long totalCost) {
        def timerLogsDB = mongo.getDB("xyrank").getCollection("timer_logs")
        def tmp = System.currentTimeMillis()
        def id = timerName + "_" + new Date().format("yyyyMMdd")
        def update = new BasicDBObject(timer_name: timerName, cost_total: totalCost, cat: 'hour', unit: 'ms', timestamp: tmp)
        timerLogsDB.findAndModify(new BasicDBObject('_id', id), null, null, false, new BasicDBObject('$set', update), true, true)
    }

}