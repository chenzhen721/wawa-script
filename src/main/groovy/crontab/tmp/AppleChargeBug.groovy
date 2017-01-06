#!/usr/bin/env groovy
package crontab.st

import com.mongodb.BasicDBObject
import com.mongodb.DB
import com.mongodb.DBCollection
import com.mongodb.DBCursor
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
])
import com.mongodb.Mongo
import com.mongodb.MongoURI
import org.apache.commons.lang.StringUtils
import org.apache.commons.lang.math.NumberUtils

import java.text.SimpleDateFormat
import com.mongodb.DBObject

/**
 * 苹果充值漏洞扫描
 */
class AppleChargeBug {

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

    static mongo = new Mongo(new MongoURI(getProperties('mongo.uri', 'mongodb://192.168.31.246:27017/?w=1') as String))
    static historyMongo = new Mongo(new MongoURI(getProperties('mongo_history.uri', 'mongodb://192.168.31.246:27017/?w=1') as String))

    static DB historyDB = historyMongo.getDB('xylog_history')
    static DBCollection finance_log  = mongo.getDB('xy_admin').getCollection('finance_log')
    static DBCollection finance_log_bak  = mongo.getDB('xy_admin').getCollection('finance_log_bak')
    static DBCollection ops  = mongo.getDB('xy_admin').getCollection('ops')
    static users = mongo.getDB('xy').getCollection('users')
    static DBCollection day_login_history  = historyDB.getCollection('day_login_history')
    static MIN_MILLS = 60 * 1000L
    static DAY_MILLON = 24 * 3600 * 1000L


    static scanBugUser(){
        Map<Integer,Map<Double, Integer>> countUsers = new HashMap();
        Map<Integer,Integer> countTotalUsers = new HashMap();
        Map<Integer,Double> cnyTotalUsers = new HashMap();
        Map<Integer,Map<Double, Double>> cnyUsers = new HashMap();
        Long begin = Date.parse("yyyy-MM-dd HH:mm:ss" ,"2016-09-01 00:00:00").getTime()
        Long end = Date.parse("yyyy-MM-dd HH:mm:ss" ,"2016-11-01 00:00:00").getTime()
        def timeBetween = [$gte: begin, $lt: end]

        finance_log.find($$(via: 'itunes',  timestamp: timeBetween)).toArray().each {DBObject charge ->
            Integer userId = charge.get('user_id') as Integer
            Double cny = charge.get('cny') as Double
            Integer totalCount = (countTotalUsers.get(userId) ?:0) as Integer
            countTotalUsers.put(userId, (totalCount+1))

            Integer totalCny= (cnyTotalUsers.get(userId) ?:0) as Integer
            cnyTotalUsers.put(userId, (totalCny+cny))

            Map<Double, Integer>  countMap = (countUsers.get(userId) ?: new HashMap()) as Map
            Integer count = (countMap.get(cny) ?:0) as Integer
            countMap.put(cny, (count+1))
            countUsers.put(userId, countMap)

            Map<Double, Double> cnyMap =(cnyUsers.get(userId) ?: new HashMap()) as Map
            Integer cnyTotal = (cnyMap.get(cny) ?:0) as Double
            cnyMap.put(cny, (cnyTotal+cny))
            cnyUsers.put(userId, cnyMap)
        }
        countTotalUsers = countTotalUsers.sort {a,   b ->
            b.value <=> a.value
        }
        countTotalUsers.each {Integer userId, Integer countTotal ->
            def countMap =countUsers.get(userId)
            def cnyMap =cnyUsers.get(userId)
            print("${userId} : 总次数: ${countTotal}, ")
            countMap.each {Double cny, Integer count ->
                print(cny + "金额  次数: " + count +", 总额: " + cnyMap.get(cny) +" | ")
            }
            println " "
        }
        //8445, 98.0金额  次数: 2, 总额: 196.0 | 298.0金额  次数: 1, 总额: 298.0 | 1.0金额  次数: 4807, 总额: 4807.0 | 50.0金额  次数: 2, 总额: 100.0 | 188.0金额  次数: 3, 总额: 564.0 | 8.0金额  次数: 3630, 总额: 29040.0 |
        StringBuffer sb = new StringBuffer()
        sb.append('UID,1RMB,8RMB,50RMB,98RMB,188RMB,298RMB,totalCount,totalCny,coin,status').append(System.lineSeparator())
        countTotalUsers.each {Integer userId, Integer countTotal ->
            def countMap =countUsers.get(userId)
            def cnyMap =cnyUsers.get(userId)
            def user = users.findOne($$(_id:userId), $$('finance.coin_count':1,status:1))
            Long coin_count = ((user.get('finance') as Map)?.get('coin_count') ?:0) as Long
            Boolean status = user.get('status') as Boolean
            sb.append(userId).append(',')
            sb.append(countMap.get(1.0d) + "|" + cnyMap.get(1.0d)).append(',')
            sb.append(countMap.get(8.0d) + "|" + cnyMap.get(8.0d)).append(',')
            sb.append(countMap.get(50.0d) + "|" + cnyMap.get(50.0d)).append(',')
            sb.append(countMap.get(98.0d) + "|" + cnyMap.get(98.0d)).append(',')
            sb.append(countMap.get(188.0d) + "|" + cnyMap.get(188.0d)).append(',')
            sb.append(countMap.get(298.0d) + "|" + cnyMap.get(298.0d)).append(',')
            sb.append(countTotal).append(',')
            sb.append(cnyTotalUsers.get(userId)).append(',')
            sb.append(coin_count).append(',')
            sb.append(status)
            sb.append(System.lineSeparator())
        }

        def folder_path = '/empty/static/'
        File folder = new File(folder_path)
        if (!folder.exists()) {
            folder.mkdirs()
        }

        File file = new File(folder_path + "/applestore.csv");
        if (!file.exists()) {
            file.createNewFile()
        }
        file.withWriterAppend { Writer writer ->
            writer.write(sb.toString())
            writer.flush()
            writer.close()
        }
    }

    /**
     * bug用户扣币
     */
    static subCoinOfBugUser(List<Integer> uids){
        uids.each {Integer userId ->
            //扣除用户ID
            def user = users.findOne($$(_id:userId), $$('finance.coin_count':1))
            Long coin = ((user.get('finance') as Map)?.get('coin_count') ?:0) as Long
            println user
            users.update($$(_id : userId).append('finance.coin_count', [$gte: coin])
                    , new BasicDBObject('$inc', ['finance.coin_count': 0 - coin]))
            //记录运营扣币流水
            Long timestamp = System.currentTimeMillis()
            def seesion = $$(   "nick_name": "郝瑞",
                    "_id": "12917",
                    "name": "haorui",
                    "ip": "210.22.151.242")

            def data = $$("user_id":userId,
                    "coin": coin,
                    "remark": "苹果充值漏洞扣币")
            def opsInfo = $$("_id":timestamp,"type": "finance_cut_coin", session:seesion, data:data,timestamp:timestamp)
            ops.save(opsInfo)
        }

    }

    public static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    public static BasicDBObject $$(Map map) {
        return new BasicDBObject(map)
    }

    static void main(String[] args) {
        long l = System.currentTimeMillis()
        //scanBugUser();

        //扣除bug用户币
        subCoinOfBugUser([1310933])
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   FinanceClean, cost  ${System.currentTimeMillis() - l} ms"
    }

}

