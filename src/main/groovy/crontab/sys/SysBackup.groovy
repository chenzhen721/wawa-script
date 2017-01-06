#!/usr/bin/env groovy
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
 * 关键数据备份
 */
class SysBackup {
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

    static long zeroMill = new Date().clearTime().getTime()
    static DAY_MILLON = 24 * 3600 * 1000L
    def static mongo  = new Mongo(new MongoURI(getProperties('mongo.uri','mongodb://192.168.31.249:27017/?w=1') as String))
    def static historyMongo = new Mongo(new MongoURI(getProperties('mongo_history.uri', 'mongodb://192.168.31.246:27017/?w=1') as String))

    def static users = mongo.getDB("xy").getCollection("users");
    def static user_bak = historyMongo.getDB("xylog_history").getCollection("user_bak");

    def static keyUsersBackUp(){
        //余额大于1000用户
        def coinsUser = users.find(new BasicDBObject("last_login":[$gt: zeroMill - 30 * DAY_MILLON],"finance.coin_count":[$gte:1000])).toArray()
        save(coinsUser);
        //消费大于200000
        def costUser = users.find(new BasicDBObject("last_login":[$gt: zeroMill - 30 * DAY_MILLON],"finance.coin_spend_total":[$gte:200000])).toArray()
        save(costUser);
        //主播
        def stars = users.find(new BasicDBObject("last_login":[$gt: zeroMill - 30 * DAY_MILLON],priv:2)).toArray()
        save(stars);
    }

    private static save(List<DBObject> users){
        println "count:"+users.size();
        users.each {BasicDBObject user ->
            if(user.size() > 10){
                user.put('bak_time', System.currentTimeMillis());
                user_bak.save(user)
            }
        }
    }

    static void main(String[] args)
    {
        long l = System.currentTimeMillis()
        long begin = l
        //关键用户数据备份
        keyUsersBackUp();
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${SysBackup.class.getSimpleName()}, cost  ${System.currentTimeMillis() -begin} ms"
    }



}