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
import org.apache.commons.lang.StringUtils

import java.text.SimpleDateFormat
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong


/**
 * 修复大额用户消费统计
 */
class MvpRecover {

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

    static DAY_MILLON = 24 * 3600 * 1000L

    static long zeroMill = new Date().clearTime().getTime()
    static Long yesTday = zeroMill - DAY_MILLON
    static String YMD = new Date(yesTday).format("yyyyMMdd")

    static DBCollection coll = mongo.getDB('xy_admin').getCollection('stat_daily')
    static DBCollection room_cost_DB = mongo.getDB("xylog").getCollection("room_cost")
    static DBCollection medal_award_logs = mongo.getDB("xylog").getCollection("medal_award_logs")
    static DBCollection finance_log_DB = mongo.getDB('xy_admin').getCollection('finance_log')
    static DBCollection users = mongo.getDB('xy').getCollection('users')
    static DBCollection channel_pay_DB = mongo.getDB('xy_admin').getCollection('channel_pay')
    static DBCollection active_award_logs = mongo.getDB('xyactive').getCollection('active_award_logs')


    /**
     * 特殊用户消费统计，该用户送礼物的主播及主播获得的维C
     * @param i
     */
    static staticMvp(int i, List<String> ids) {
        def begin = yesTday - i * DAY_MILLON
        def timebetween = [$gte: begin, $lt: begin + DAY_MILLON]

        def stat_mvp = mongo.getDB('xy_admin').getCollection('stat_mvps')
        def YMD = new Date(begin).format('yyyyMMdd_')

        if(ids == null || ids.size() == 0)
            ids = getAllMvps()

        if (ids.size() == 0) return

        println YMD
        room_cost_DB.aggregate(
                new BasicDBObject('$match', ['session._id'            : [$in: ids],
                                             'session.data.xy_star_id': [$ne: null],
                                             timestamp                : timebetween]),
                new BasicDBObject('$project', [id    : '$session._id',
                                               star  : '$session.data.xy_star_id',
                                               cost  : '$star_cost',
                                               earned: '$session.data.earned']),
                new BasicDBObject('$group', [_id   : [id: '$id', star: '$star'],
                                             cost  : ['$sum': '$cost'],
                                             earned: ['$sum': '$earned']])
        ).results().each { BasicDBObject obj ->
            def id = obj.get('_id')['id'] as Integer
            def star = obj.get('_id')['star'] as Integer
            def cost = obj.get('cost') as Integer
            def earned = obj.get('earned') as Integer
            def update = new BasicDBObject()
            update.putAll(user_id: id, star_id: star, cost: cost, star_earned: earned, type: 'mvp_cost', timestamp: begin)
            stat_mvp.update(new BasicDBObject(_id: "${YMD}${id}_${star}".toString()), update, true, false)
        }


    }

    //日充值大户
    static chargeMvp(int i){
        def begin = yesTday - i * DAY_MILLON
        def timebetween = [$gte: begin, $lt: begin + DAY_MILLON]

        def stat_mvp = mongo.getDB('xy_admin').getCollection('stat_mvps')
        def YMD = new Date(begin).format('yyyyMMdd_')
        //每日消费大于1000的用户
        finance_log_DB.aggregate(
                new BasicDBObject('$match', [via: [$ne: 'Admin'], timestamp: timebetween]),
                new BasicDBObject('$project', [_id: '$user_id', cny: '$cny', coin: '$coin']),
                new BasicDBObject('$group', [_id: '$_id', cny: [$sum: '$cny'], coin: [$sum: '$coin']]),
                new BasicDBObject('$match', [cny: [$gte: 1000]]),
                new BasicDBObject('$sort', [cny: -1])
        ).results().each { BasicDBObject obj ->
            def id = obj.removeField('_id') as Integer
            def update = new BasicDBObject()
            def cny = obj.get('cny')
            def coin = obj.get('coin')
            update.putAll(user_id: id, cny: cny, coin: coin, type: 'mvp_pay_most', timestamp: begin)
            stat_mvp.update(new BasicDBObject(_id: "${YMD}${id}_vip".toString()), update, true, false)
        }
    }

    private static List<String> getAllMvps(){
        List<String> ids = []
        def mvp = mongo.getDB('xy_admin').getCollection('mvps')
        mvp.find(new BasicDBObject(type: 1), new BasicDBObject(timestamp: 0))
                .toArray().each { BasicDBObject obj ->
            ids.add(obj.get('user_id') as String)
        }
        return ids;
    }
    static void main(String[] args) {
        long l = System.currentTimeMillis()
        long begin = l
        //特殊用户消费统计，该用户送礼物的主播及主播获得的维C
        List<String> ids = ['22710677']
        //13天前到今天
        int i = 30;
        while (i > 0){
            staticMvp(i--, ids)
            //chargeMvp(i)
        }

        println "${new Date().format('yyyy-MM-dd')}:${MvpRecover.class.getSimpleName()}:finish  cost  ${System.currentTimeMillis() - begin} ms"

    }


}

