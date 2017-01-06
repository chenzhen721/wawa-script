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
 * 充值数据整理
 */
class FinanceClean {

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
    static DBCollection day_login_history  = historyDB.getCollection('day_login_history')
    static MIN_MILLS = 60 * 1000L
    static DAY_MILLON = 24 * 3600 * 1000L

    static List<Integer> bug_ids = [16583767,16474574,15593546,15150349]
    static List<Integer> other_bug_ids = [23080547, 22435805, 19606516, 22624411, 7632330, 19616371, 15627837, 8135667]
    static Integer AUDIT_USER_ID = 26072985

    //历史测试账号充值流水转换为ADMIN加币
    static appleTestIdFinanceClean(Integer user_id, Long begin, Long end){
        def timeBetween = [$gte: begin, $lt: end]
        def testChargeLogs = finance_log.find($$(user_id:user_id, via: 'itunes', timestamp:timeBetween)).toArray()
        Long totalCoins = 0l
        BigDecimal cnyTotal = new BigDecimal(0.0)
        testChargeLogs.each {DBObject finance ->
            chargeLogToAdminAdd(finance)
            cnyTotal = cnyTotal.add(new BigDecimal(finance.get('cny').toString()))
        }
        println user_id +" : " + cnyTotal.toDouble()
    }

    //寻找和匹配修正金额的充值流水
    static findUserChargeByCny(Integer cny, Long begin, Long end){
        def timeBetween = [$gte: begin, $lt: end]
        //获取剩余充值用户ID
        def iter = finance_log.aggregate(
                new BasicDBObject('$match', [via: 'itunes',  timestamp: timeBetween]),
                new BasicDBObject('$project', [cny: '$cny', user_id: '$user_id']),
                new BasicDBObject('$group', [_id: null, cny: [$sum: '$cny'], users: [$addToSet: '$user_id']])
        ).results().iterator()
        List<Integer> users = null
        if (iter.hasNext()) {
            def obj = iter.next()
            users = obj.get('users') as List
        }
        //排除漏洞账号
        if(users == null || users.size() == 0){
            println "there is no others to charge"
            return;
        }
        users.removeAll(bug_ids)
        users.removeAll(other_bug_ids)
        users.removeAll(AUDIT_USER_ID)
        println users
        //寻找最近未登录的用户
        List<DBObject> needChangeFinances = new ArrayList<>();
        for(Integer uid : users){
            def finance = finance_log.findOne($$(user_id:uid, via: 'itunes',  timestamp: timeBetween))
            //匹配金额
            Integer userCny = finance.get('cny') as Integer
            if(cny - userCny <= 0){
                break;
            }
            cny = cny - userCny
            needChangeFinances.add(finance)
        }
        //转换充值为手动加币
        println needChangeFinances;

        /*needChangeFinances.each {DBObject finance ->
            chargeLogToAdminAdd(finance)
        }*/
    }

    //转换充值流水为手动加币
    static chargeLogToAdminAdd(DBObject finance){
        //备份充值流水
        finance_log_bak.save(finance)
        //转换为手动加币
        finance.put('via', 'Admin')
        finance.put('remark', '苹果测试充值加币')
        finance.put('transform', 1)
        //记录运营加币流水
        Long timestamp = finance.get('timestamp') as Long
        def seesion = $$("nick_name": "郝瑞",
                "_id": "12917",
                "name": "haorui",
                "ip": "210.22.151.242")

        def data = $$("user_id":finance.get('user_id') as Integer,
                "coin": finance.get('coin') as Long,
                "remark": "苹果测试充值加币")
        def opsInfo = $$("_id":timestamp,"type": "finance_add", session:seesion, data:data,timestamp:timestamp)

        finance_log.save(finance)
        ops.save(opsInfo)
    }

    static saveAddCoinOps(String order_id, Integer userId, Long coin, Long timestamp){
        def seesion = $$("nick_name": "郝瑞",
                "_id": "12917",
                "name": "haorui",
                "ip": "210.22.151.242")

        def data = $$("user_id":userId,
                order_id:order_id,
                "coin": coin,
                "remark": "苹果测试充值加币")
        def opsInfo = $$("_id":timestamp,"type": "finance_add", session:seesion, data:data,timestamp:timestamp)
        println opsInfo
        ops.save(opsInfo)
    }
    //回滚修改的手动加币为充值流水
    static rollbackCharge(){

    }

    static scanBugUser(){
        //收集问题账号IP
        def ips = new HashSet();
        def uids = new HashSet();

        Map<Integer,Double> otherUsers = new HashMap();
        Map<Integer,Double> otherBugOfIpUsers = new HashMap();
        Map<Integer,Double> otherBugOfUidUsers = new HashMap();
        Long begin = Date.parse("yyyy-MM-dd HH:mm:ss" ,"2015-09-01 00:00:00").getTime()
        Long end = Date.parse("yyyy-MM-dd HH:mm:ss" ,"2016-04-01 00:00:00").getTime()
        def timeBetween = [$gte: begin, $lt: end]
        bug_ids.each {Integer user_id ->
            List<DBObject> ipList = day_login_history.find($$(user_id:user_id, timestamp:timeBetween)).toArray()
            ipList.each {DBObject login ->
                String ip = login?.get('ip')
                if(StringUtils.isNotEmpty(ip)){
                    ip = StringUtils.remove(ip, '192.168.1.34')
                    ip = StringUtils.remove(ip, '192.168.1.35')
                    ips.add(ip)
                }
                String uid = login?.get('uid')
                if(StringUtils.isNotEmpty(uid)){
                    uids.add(uid)
                }
            }

        }
        println ips
        //统计本月充值前50名用户IP
        def iter = finance_log.aggregate(
                new BasicDBObject('$match', [via: 'itunes',  timestamp: timeBetween]),
                new BasicDBObject('$project', [cny: '$cny', user_id: '$user_id']),
                new BasicDBObject('$group', [_id: '$user_id', cny: [$sum: '$cny']]),
                new BasicDBObject('$sort', [cny: -1]),
                new BasicDBObject('$limit', 50)
        ).results().iterator()
        while (iter.hasNext()) {
            def obj = iter.next()
            Double cny = obj.get('cny') as Double
            Integer _id = obj.get('_id') as Integer
            otherUsers.put(_id, cny)
        }
        println otherUsers

        otherUsers.each {Integer user_id, Double cny->
            List<DBObject> ipList = day_login_history.find($$(user_id:user_id, timestamp:timeBetween)).toArray()
            ipList.each {DBObject login ->
                String ip = login?.get('ip')
                if(StringUtils.isNotEmpty(ip)){
                    ip = StringUtils.remove(ip, '192.168.1.34')
                    ip = StringUtils.remove(ip, '192.168.1.35')
                    //判断是否有和重复用户IP相同
                    if(ips.contains(ip)){
                        otherBugOfIpUsers.put(user_id, cny)
                    }

                    String uid = login?.get('uid')
                    if(StringUtils.isNotEmpty(uid)){
                        if(uids.contains(uid)){
                            otherBugOfUidUsers.put(user_id, cny)
                        }
                    }
                }
            }
        }
        bug_ids.each { Integer user_id ->
            otherBugOfIpUsers.remove(user_id)
            otherBugOfUidUsers.remove(user_id)
        }
        println "other users of ip :" + otherBugOfIpUsers.keySet()
        println "other users of uid :" + otherBugOfUidUsers.keySet()

    }

    //充值流水比对
    static void FinanceLogMatch(){
        File file = new File('C:\\Users\\James\\Desktop\\神州付11.1-11.29.csv')
        file.splitEachLine(","){
            String date =  it[0]
            String orderId =  it[1]
            String type =  it[2]
            String cny =  it[5]
        }
    }

    //运营加币充值流水和操作流水对比
    static void handAddCoinFinanceLogMatchOpsLogs(){
        Long begin = Date.parse("yyyy-MM-dd HH:mm:ss","2016-10-01 00:00:00").getTime();
        Long end = Date.parse("yyyy-MM-dd HH:mm:ss","2016-11-01 00:00:00").getTime();
        Map <Long,Long> opsMap = new HashMap<>()
        Long opsTotal = 0;
        ops.find($$( "type": "finance_add",timestamp:[$gte:begin, $lt:end])).toArray().each { DBObject op ->
            opsTotal += (op['data'] as Map)['coin'] as Long
            String remark = (op['data'] as Map)['remark'] as String
            Long timestamp = op['timestamp'] as Long
            if(remark.equals('苹果测试充值加币')){
                opsMap.put(timestamp, 1)
            }
        }

        Long total = 0;
        Long apple_total = 0;
        finance_log.find($$(via:'Admin', timestamp:[$gte:begin, $lt:end])).toArray().each {DBObject finance ->
            total += finance['coin'] as Long
            String remark = finance['remark'] as String
            Long timestamp = finance['timestamp'] as Long
            if(remark.equals('苹果测试充值加币') && !opsMap.containsKey(timestamp)){
                apple_total += finance['coin'] as Long
                //saveAddCoinOps(finance['_id'] as String, finance['to_id'] as Integer, finance['coin'] as Long, timestamp)
            }
        }

        println "${total} - ${opsTotal} = ${total-opsTotal} : ${apple_total}"
    }

    public static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    public static BasicDBObject $$(Map map) {0
        return new BasicDBObject(map)
    }

    static void main(String[] args) {
        long l = System.currentTimeMillis()
        //scanBugUser();
        //appleTestIdFinanceClean(AUDIT_USER_ID,Date.parse("yyyy-MM-dd HH:mm:ss" ,"2015-08-01 00:00:00").getTime(),Date.parse("yyyy-MM-dd HH:mm:ss" ,"2016-11-01 00:00:00").getTime());
        /*other_bug_ids.each {Integer userId ->
            appleTestIdFinanceClean(userId,Date.parse("yyyy-MM-dd HH:mm:ss" ,"2015-09-01 00:00:00").getTime(),Date.parse("yyyy-MM-dd HH:mm:ss" ,"2016-5-01 00:00:00").getTime());
        }*/
        //FinanceLogMatch();
        handAddCoinFinanceLogMatchOpsLogs();
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   FinanceClean, cost  ${System.currentTimeMillis() - l} ms"
    }

}

