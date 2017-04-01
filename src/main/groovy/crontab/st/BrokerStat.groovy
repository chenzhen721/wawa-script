#!/usr/bin/env groovy
package crontab.st

import com.mongodb.BasicDBObject
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
class BrokerStat {
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

    static DAY_MILLON = 24 * 3600 * 1000L

    static long zeroMill = new Date().clearTime().getTime()


    static statics(int i) {
        def coll = mongo.getDB('xy_admin').getCollection('stat_brokers')
        def star_award_logs = mongo.getDB('game_log').getCollection('star_award_logs')

        def users = mongo.getDB('xy').getCollection('users')
        def room_cost = mongo.getDB("xylog").getCollection("room_cost")
        def flog = mongo.getDB('xy_admin').getCollection('finance_log')

        Long begin = zeroMill - i * DAY_MILLON
        def timeBetween = [$gte: begin, $lt: begin + DAY_MILLON]

        users.find(new BasicDBObject('priv', 5), new BasicDBObject('status', 1)).toArray().each { BasicDBObject broker ->

            Integer bid = broker.get('_id') as Integer

            def uids = new HashSet(
                    users.find(new BasicDBObject('star.broker', bid), new BasicDBObject('star', 1)).toArray().collect {
                        it.get('_id')
                    })
            def star = [count: uids.size()]
            def res = room_cost.aggregate(
                    new BasicDBObject('$match', ['session.data.xy_star_id': [$in: uids], timestamp: timeBetween]),
                    new BasicDBObject('$project', [earned: '$session.data.earned']),
                    new BasicDBObject('$group', [_id: null, earned: [$sum: '$earned']])
            )
            Iterator records = res.results().iterator();
            def live_earned = 0
            if (records.hasNext()) {
                live_earned = records.next().earned
            }

            def game_award_res = star_award_logs.aggregate(
                    new BasicDBObject('$match', [timestamp: timeBetween, 'room_id': [$in: uids]]),
                    new BasicDBObject('$project', [earned: '$award_earned']),
                    new BasicDBObject('$group', [_id: null, earned: [$sum: '$earned']])
            ).results().iterator()
            def game_earned = 0
            if (game_award_res.hasNext()) {
                game_earned = game_award_res.next().earned
            }
            star.bean_count =live_earned + game_earned

            def sale = [:]
            res = flog.aggregate(
                    new BasicDBObject('$match', [ext: bid.toString(), timestamp: timeBetween]),
                    new BasicDBObject('$project', [cny: '$cny']),
                    new BasicDBObject('$group', [_id: null, cny: [$sum: '$cny'], count: [$sum: 1]])
            )
            records = res.results().iterator();
            if (records.hasNext()) {
                def rec = records.next()
                sale.cny = rec.cny
                sale.count = rec.count
            }

            def YMD = new Date(begin).format("yyyyMMdd")
            coll.save(new BasicDBObject(
                    _id: "${bid}_${YMD}".toString(),
                    star: star,
                    sale: sale,
                    user_id: bid,
                    timestamp: begin,
            ))

            res = flog.aggregate(
                    new BasicDBObject('$match', [ext: bid.toString()]),
                    new BasicDBObject('$project', [cny: '$cny']),
                    new BasicDBObject('$group', [_id: null, cny: [$sum: '$cny'], count: [$sum: 1]])
            )
            records = res.results().iterator();
            sale = [:]
            if (records.hasNext()) {//字符多位 double 精度
                def rec = records.next()
                sale.cny = rec.cny
                sale.count = rec.count
            }

            users.update(broker.append('broker.flag', [$ne: YMD]), new BasicDBObject(
                    // TODO day add
                    $inc: ['broker.bean_total': star.bean_count ?: 0],
                    $set: [
                            'broker.cny_total': sale.cny ?: 0,
                            'broker.sale_count': sale.count ?: 0,
                            //'broker.star_total':star.count,
                            'broker.flag': YMD
                    ]
            ))
        }
    }


    static void main(String[] args) {
        long l = System.currentTimeMillis()
        //代理汇总
        long begin = l
        statics(1)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   BrokerStat, cost  ${System.currentTimeMillis() - l} ms"
        jobFinish(begin)
    }
    /**
     * 标记任务完成  用于运维监控
     * @return
     */
    private static jobFinish(long begin) {
        def timerName = 'BrokerStat'
        Long totalCost = System.currentTimeMillis() - begin
        saveTimerLogs(timerName, totalCost)
        println "${new Date().format('yyyy-MM-dd')}:${BrokerStat.class.getSimpleName()}:finish  cost  ${System.currentTimeMillis() - begin} ms"
    }

    //落地定时执行的日志
    private static saveTimerLogs(String timerName, Long totalCost) {
        def timerLogsDB = mongo.getDB("xyrank").getCollection("timer_logs")
        def tmp = System.currentTimeMillis()
        def id = timerName + "_" + new Date().format("yyyyMMdd")
        def update = new BasicDBObject(timer_name: timerName, cost_total: totalCost, cat: 'day', unit: 'ms', timestamp: tmp)
        timerLogsDB.findAndModify(new BasicDBObject('_id', id), null, null, false, new BasicDBObject('$set', update), true, true)
    }

}


