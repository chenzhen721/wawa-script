#!/usr/bin/env groovy
package crontab.st

import com.mongodb.BasicDBObject
import com.mongodb.DBCollection
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
])
import com.mongodb.Mongo
import com.mongodb.MongoURI

/**
 * 每天统计一份数据
 *
 * date: 13-2-28 下午2:46
 * @author: yangyang.cong@ttpod.com
 */
class Family {
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
    static DBCollection familyDB = mongo.getDB('xy').getCollection('familys')
    static DAY_MILLON = 24 * 3600 * 1000L

    static long zeroMill = new Date().clearTime().getTime()
     static final long[] family_level = [0,
        20000,
        80000,
        180000,
        320000,
        500000,
        720000,
        980000,
        1280000,
        1620000,
        2000000,
        2420000,
        2880000,
        3380000,
        3920000,
        4500000,
        5120000,
        5780000,
        6480000,
        7220000,
        8000000,
        8820000,
        9680000,
        10580000,
        11520000,
        12500000,
        13520000,
        14580000,
        15680000,
        16820000,
        18000000,
        19220000,
        20480000,
        21780000,
        23120000,
        24500000,
        25920000,
        27380000,
        28880000,
        30420000,
        32000000,
        33620000,
        35280000,
        36980000,
        38720000,
        40500000,
        42320000,
        44180000,
        46080000,
        48020000,
        50000000
    ]

    private static final int max_family_level = family_level.length;

    public static int familyLevel(long prestige) {
        for (int i = 1; i < max_family_level; i++) {
            if (prestige < (family_level[i])) {
                return i - 1;
            }
        }
        return max_family_level - 1;
    }

    static awardCoin(){
        Random random = new Random()
        def list = familyDB.find()
        list.each {
            BasicDBObject obj ->
                Integer coin
                def prestige = obj.containsField('prestige') ? obj['prestige'] as Long : 0L
                def id = obj['_id'] as Integer
                def level = familyLevel(prestige)
                println("level is ${level}")
                if(level >= 0 && level <=10){
                    coin = random.nextInt(5000) + 10001
                }else if(level >=11 && level <=20){
                    coin = random.nextInt(7500) + 15001
                }else if(level >=21 && level <=30){
                    coin = random.nextInt(10000) + 20001
                }else if (level >=31 && level <=40){
                    coin = random.nextInt(12500) + 25001
                }else if(level >=41 && level <=50){
                    coin = random.nextInt(15000) + 30001
                }else{
                    coin = random.nextInt(17500) + 35001
                }
                coin = coin/60
                println("award coin family ${id} is ${coin}")
                familyDB.update($$('_id',id),$$('$inc':['gold':coin]))
        }
    }

    static void main(String[] args) {
        long l = System.currentTimeMillis()
        //代理汇总
        awardCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   awardCoin, cost  ${System.currentTimeMillis() - l} ms"
    }
    /**
     * 标记任务完成  用于运维监控
     * @return
     */
    private static jobFinish(long begin) {
        def timerName = 'BrokerStat'
        Long totalCost = System.currentTimeMillis() - begin
        saveTimerLogs(timerName, totalCost)
        println "${new Date().format('yyyy-MM-dd')}:${Family.class.getSimpleName()}:finish  cost  ${System.currentTimeMillis() - begin} ms"
    }

    //落地定时执行的日志
    private static saveTimerLogs(String timerName, Long totalCost) {
        def timerLogsDB = mongo.getDB("xyrank").getCollection("timer_logs")
        def tmp = System.currentTimeMillis()
        def id = timerName + "_" + new Date().format("yyyyMMdd")
        def update = new BasicDBObject(timer_name: timerName, cost_total: totalCost, cat: 'day', unit: 'ms', timestamp: tmp)
        timerLogsDB.findAndModify(new BasicDBObject('_id', id), null, null, false, new BasicDBObject('$set', update), true, true)
    }

    private static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    private static BasicDBObject $$(Map map) {
        return new BasicDBObject(map)
    }
}


