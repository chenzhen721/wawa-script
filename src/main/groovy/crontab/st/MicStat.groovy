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
 * 房间发言数统计
 */
class MicStat {
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
    static mic_log = mongo.getDB('xylog').getCollection('mic_log')
    //写log
    static DBCollection mic_logs = mongo.getDB('xy_admin').getCollection('stat_mic')

    static DAY_MILLON = 24 * 3600 * 1000L
    static long zeroMill = new Date().clearTime().getTime()
    static Long yesTday = zeroMill - DAY_MILLON
    static BEGIN = '$gte'
    static END = '$lt'
    private static Map getTimeBetween() {
        def gteMill = yesTday - day * DAY_MILLON
        return [$gte: gteMill, $lt: gteMill + DAY_MILLON]
    }

    static statics() {
        def timebetween = getTimeBetween()
        def YMD = new Date(timeBetween.get(BEGIN)).format("yyyyMMdd")
        def mics = 0 //总上麦人次
        def duration = new Duration()
        mic_log.aggregate([
                $$('$match', ['timestamp': timebetween]),
                $$('$project', [roomId: '$room', userId: '$data.mic_user', type: '$data.type', timestamp: '$timestamp']),
                $$('$sort', ['timestamp': -1]),
                $$('$group', [_id: [roomId: '$roomId', userId: '$userId'], total_count: [$sum: 1], type: ['$push': '$type'], timestamp: ['$push': '$timestamp']]),
        ]).results().each { row ->
            def types = row['type'] as ArrayList
            def timestamps = row['timestamp'] as ArrayList
            if (types != null && timestamps != null && types.size() > 0 && types?.size() == timestamps?.size()) {
                if ('close_mic' == types[0]) {
                    types.add(0, 'on_mic')
                    timestamps.add(0, timebetween.get(BEGIN))
                }
                if ('on_mic' == types[types.size() - 1]) {
                    types.add('close_mic')
                    timestamps.add((timebetween.get(END) as Long) - 1)
                }
                duration.rooms.add(row['_id']['roomId'])
                duration.users.add(row['_id']['userId'])
                for (int i = 0; i < types.size() - 1; i++) {
                    duration.filter([type: types[i], timestamp: timestamps[i]])
                }
            }
            mics += row['total_count']?:0
        }
        def result = [total_count: mics, type: 'on_mic', timestamp: timebetween.get(BEGIN)] as Map
        result.putAll(duration.toMap())
        mic_logs.update($$('_id', "${YMD}_mic".toString()), $$(result), true, false)
    }

    static class Duration {
        def rooms = [] as Set
        def users = [] as Set
        def duration = 0 as Long

        Long begin

        void filter(def map) {
            if ('on_mic' == map['type']) {
                begin = map['timestamp'] as Long
            }
            if ('close_mic' == map['type'] && begin != null) {
                Long end = map['timestamp'] as Long
                duration += end - begin
                begin = null
            }
        }

        Map toMap() {
            [room_count: rooms.size(), user_count: users.size(), duration: duration]
        }
    }

    static Integer day = 0

    static void main(String[] args) {
        long l = System.currentTimeMillis()
        long begin = l

        //房间发言数统计
        l = System.currentTimeMillis()
        statics()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${MicStat.class.getSimpleName()},statics cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //落地定时执行的日志
        jobFinish(begin)
    }

    /**
     * 标记任务完成  用于运维监控
     * @return
     */
    private static jobFinish(Long begin) {
        def timerName = 'MicStat'
        Long totalCost = System.currentTimeMillis() - begin
        saveTimerLogs(timerName, totalCost)
        println "${new Date().format('yyyy-MM-dd')}:${MicStat.class.getSimpleName()}:finish  cost  ${System.currentTimeMillis() - begin} ms"
    }

    //落地定时执行的日志
    private static saveTimerLogs(String timerName, Long totalCost) {
        def timerLogsDB = mongo.getDB("xyrank").getCollection("timer_logs")
        def tmp = System.currentTimeMillis()
        def id = timerName + "_" + new Date().format("yyyyMMdd")
        def update = $$(timer_name: timerName, cost_total: totalCost, cat: 'day', unit: 'ms', timestamp: tmp)
        timerLogsDB.findAndModify($$('_id', id), null, null, false, $$('$set', update), true, true)
    }

    static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    static BasicDBObject $$(Map map) {
        return new BasicDBObject(map)
    }
}