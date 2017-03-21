#!/usr/bin/env groovy
package crontab.st

import com.mongodb.BasicDBObject
import com.mongodb.DBCollection
import com.mongodb.DBObject
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
])
import com.mongodb.Mongo
import com.mongodb.MongoURI

/**
 * 关于商城的统计
 * 可以包含商品买卖，订单等
 */
class ShopStat {
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
    static DBCollection orders = mongo.getDB('shop').getCollection('orders')
    static DBCollection product_stats = mongo.getDB('xy_admin').getCollection('product_stat')
    static DAY_MILLON = 24 * 3600 * 1000L
    static long zeroMill = new Date().clearTime().getTime()
    static Long yesTday = zeroMill - DAY_MILLON

    private static Map getTimeBetween() {
        def gteMill = yesTday - day * DAY_MILLON
        return [$gte: gteMill, $lt: gteMill + DAY_MILLON]
    }

    /**
     * 统计商品的买卖
     * 统计已发货，完成的订单
     * status in [4,5]
     */
    private static void statics_products() {
        def res = orders.aggregate(
                $$('$match', ['status': ['$in': [4, 5]], timestamp: getTimeBetween()]),
                $$('$project', [_id: '$product_id', 'count': '$item_count', 'item_name': '$item_name', 'user_id': '$user_id', 'price': '$price']),
                $$('$group', ['product_id': '$_id', 'count': [$sum: '$count'], 'name': '$name', 'user_id': ['$addToSet': '$user_id'], 'price': ['$sum': '$price']])
        ).results().iterator()
        res.each {
            DBObject obj ->
                def curr_date = new Date(yesTday - day * DAY_MILLON)
                def myId = "product_stat_" + curr_date.format("yyyyMMdd")
                obj.put('_id', myId)
                obj.put('timestamp', curr_date.getTime())
                product_stats.save(obj)
        }
    }

    final static Integer day = 0;

    static void main(String[] args) {
        long l = System.currentTimeMillis()
        long begin = l

        statics_products()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${ShopStat.class.getSimpleName()},statics_products cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        jobFinish(begin);
    }

    /**
     * 标记任务完成  用于运维监控
     * @return
     */
    private static jobFinish(Long begin) {
        def timerName = 'ShopStat'
        Long totalCost = System.currentTimeMillis() - begin
        saveTimerLogs(timerName, totalCost)
        println "${new Date().format('yyyy-MM-dd')}:${ShopStat.class.getSimpleName()}:finish  cost time:  ${System.currentTimeMillis() - begin} ms"
    }

    //落地定时执行的日志
    private static saveTimerLogs(String timerName, Long totalCost) {
        def timerLogsDB = mongo.getDB("xyrank").getCollection("timer_logs")
        def tmp = System.currentTimeMillis()
        def id = timerName + "_" + new Date().format("yyyyMMdd")
        def update = new BasicDBObject(timer_name: timerName, cost_total: totalCost, cat: 'day', unit: 'ms', timestamp: tmp)
        timerLogsDB.findAndModify(new BasicDBObject('_id', id), null, null, false, new BasicDBObject('$set', update), true, true)
    }

    protected static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    protected static BasicDBObject $$(Map map) {
        return new BasicDBObject(map)
    }

}