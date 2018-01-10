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
import org.apache.commons.lang.StringUtils

import java.text.SimpleDateFormat
import com.mongodb.DBObject

/**
 * 娃娃相关统计
 */
class StaticsDoll {

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

    static mongo = new Mongo(new MongoURI(getProperties('mongo.uri', 'mongodb://192.168.2.27:10000/?w=1') as String))
    static DAY_MILLON = 24 * 3600 * 1000L
    static long zeroMill = new Date().clearTime().getTime()
    static Long yesTday = zeroMill - DAY_MILLON
    static String YMD = new Date(yesTday).format("yyyyMMdd")
    static DBCollection coll = mongo.getDB('xy_admin').getCollection('stat_doll')
    static DBCollection catch_record = mongo.getDB('xy_catch').getCollection('catch_record')
    static DBCollection catch_toy = mongo.getDB('xy_catch').getCollection('catch_toy')
    static DBCollection users = mongo.getDB('xy').getCollection('users')
    static DBCollection catch_success_log = mongo.getDB('xylog').getCollection('catch_success_logs')
    static DBCollection apply_post_logs = mongo.getDB('xylog').getCollection('apply_post_logs')
    static DBCollection user_award_logs = mongo.getDB('xylog').getCollection('user_award_logs')

    // 每日抓取人数，次数，抓中次数
    /**
     * toy_id:娃娃编号
     * name:娃娃名称
     * head_pic: 娃娃图片
     * 进入次数（目前没有）
     * count:抓取次数
     * bingo_count:抓中次数
     * 下抓率（没有 依赖进入次数）
     * rate: 实际抓中概率
     * winrate: 设定概率
     * price: 单次抓取价格
     * cost: 娃娃成本
     * post_count: 已寄出数量
     * post_total: 已申请数量(需要计算)
     * stock: 库存
     * exchange_count: 兑换积分数量(需要计算)
     * remaining: 剩余商品个数(需要计算)
     *
     * @param i
     * @return
     */
    static dollStatics(int i){
        def begin = yesTday - i * DAY_MILLON
        def end = begin + DAY_MILLON
        def YMD = new Date(begin).format('yyyyMMdd')
        def time = [timestamp: [$gte: begin, $lt: end], is_delete: [$ne: true]]
        def query = new BasicDBObject(time)
        def regs = users.find(new BasicDBObject(time))*._id
        catch_record.aggregate([new BasicDBObject('$match', query),
                                new BasicDBObject('$project', [user_id: '$user_id', toyId: '$toy._id']),
                                new BasicDBObject('$group', [_id: '$toyId',  count: [$sum: 1], users: [$addToSet: '$user_id']])]
        ).results().each {
            def obj = $$(it as Map)
            def toyId = obj?.get('_id') as Long;
            def count = obj?.get('count') as Long; //抓取次数
            def bingoQuery = new BasicDBObject(time).append('toy._id', toyId).append('status',true) //抓中次数
            def bingo = catch_record.count(bingoQuery) as Long;
            def userSet = new HashSet(obj?.get('users') as Set) //抓取人数
            def new_user = new HashSet()
            //新增用户抓取该娃娃的数据
            regs.each {
                if (userSet.contains(it as Integer)) {
                    new_user.add(it)
                }
            }
            //新增用户抓取次数
            def q = $$(time)
            q.putAll(['toy._id': toyId, user_id: [$in: new_user]])
            def reg_count = catch_record.count($$(q))
            def toy = catch_toy.findOne($$(_id: toyId))
            def log = $$(type:'day',toy_id:toyId, count:count, bingo_count:bingo, user_count:userSet.size(), users:userSet,
                    reg_count: reg_count, regs: new_user, winrate: toy['winrate'], rate: toy['rate'], price: toy['price'], cost: toy['cost'],
                    stock: toy['stock']['total'] ?: 0, timestamp: begin)
            coll.update($$(_id: "${YMD}_${toyId}_doll".toString()), new BasicDBObject('$set': log), true, false)
        }
    }

    //每日抓取人数
    static dollTotalDay(int i) {
        def begin = yesTday - i * DAY_MILLON
        def end = begin + DAY_MILLON
        def date = new Date(begin)
        def YMD = new Date(begin).format('yyyyMMdd')
        def prefix = date.format('yyyyMMdd_')
        coll.aggregate([
                $$('$match', [timestamp: begin, type: 'day']),
                $$('$project', [count: '$count', users: '$users', bingo_count: '$bingo_count', reg_count: '$reg_count', regs: '$regs']),
                $$('$group', [_id: null, count: [$sum: '$count'], bingo_count: [$sum: '$bingo_count'], reg_count: [$sum: '$reg_count'],
                    user_set: ['$addToSet': '$users'], reg_set: ['$addToSet': '$regs']
                ])
        ]).results().each {BasicDBObject obj ->
            //当前抓取总数据
            def sets = obj['user_set'] as Set
            def regsets = obj['reg_set'] as Set
            def users = new HashSet()
            def regs = new HashSet()
            sets.each {List item->
                item.each {users.add(it as Integer)}

            }
            regsets.each {List item->
                item.each {regs.add(it as Integer)}
            }
            //每日新增中抓中数
            def query = [user_id: [$in: regs], status: true, timestamp: [$gte: begin, $lt: end], is_delete: [$ne: true]]
            def reg_bingo_count = catch_record.count($$(query))
            def set = [type: 'total_all', count: obj['count'], bingo_count: obj['bingo_count'], reg_count: obj['reg_count'], users: users, user_count: users.size(), reg_user_count: regs.size(), reg_bingo_count: reg_bingo_count, regs: regs]
            coll.update($$(_id: YMD + '_total_doll'), $$($set: set), true, false)
            // 更新数据总表 5抓取次数 6 抓中 7 人数 13新抓
            def stat_report = mongo.getDB('xy_admin').getCollection('stat_report')
            def report = [doll_count: obj['count'], bingo_count: obj['bingo_count'], user_count: users.size(), reg_count: obj['reg_count'], reg_user_count: regs.size()]
            stat_report.update(new BasicDBObject(_id: "${prefix}allreport".toString()), $$($set: report), true, false)
        }
    }

    // 历史总抓取人数,总抓取次数,总抓中次数
    static dollTotalStatics(int i){
        def begin = yesTday - i * DAY_MILLON
        def YMD = new Date(begin).format('yyyyMMdd')
        def ids = coll.distinct('toy_id', $$([type: 'day', timestamp: [$gte: begin, $lt: begin + DAY_MILLON]]))
        def q = [timestamp: [$lt: begin + DAY_MILLON], type: 'day', toy_id: [$in: ids]]
        coll.aggregate([
                new BasicDBObject('$match', q),
                new BasicDBObject('$project', [toyId: '$toy_id', users: '$users', count:'$count', bingo_count:'$bingo_count']),
                new BasicDBObject('$group', [_id: '$toyId', count: [$sum: '$count'], bingo_count: [$sum: '$bingo_count'],
                   user_set: ['$addToSet': '$users']
                ])]
        ).results().each {
            def obj = it as Map
            //println it
            def sets = obj.remove('user_set') as Set
            def users = new HashSet()
            sets.each {List item->
                item.each {users.add(it as Integer)}

            }
            def toyId = obj.remove('_id')
            def log = $$(type:'total',toy_id:toyId, user_count: users.size(), timestamp:begin)
            log.putAll(obj)
            coll.update($$(_id: "${YMD}_${toyId}".toString() + '_total_doll'), new BasicDBObject('$set': log), true, false)
        }
    }

    //商品库存、消耗计算
    static stockStatics(int i) {
        def begin = yesTday - i * DAY_MILLON
        def end = begin + DAY_MILLON
        def YMD = new Date(begin).format('yyyyMMdd')

        def query = $$(post_time: [$gte: begin, $lt: end], is_delete: false)
        println catch_success_log.aggregate([
                $$('$match', query),
                $$('$project', [_id: '$toy._id'])
        ]).results()
        //查询有多少商品需要统计
        catch_success_log.aggregate([
                $$('$match', query),
                $$('$project', [_id: '$toy._id']),
                $$('$group', [_id: '$_id', count: [$sum: 1]])
        ]).results().each {BasicDBObject obj ->
            def toyId = obj['_id'] as Integer
            def post_count = obj['count'] as Integer //当日邮寄数量
            def post_total = apply_post_logs.count($$('toy._id': toyId, post_time: [$lt: end])) //总共邮寄数量，消耗
            def toy = catch_toy.findOne($$(_id: toyId))
            def log = $$(type:'day',toy_id:toyId, winrate: toy['winrate'], rate: toy['rate'], price: toy['price'], cost: toy['cost'],
                    stock: toy['stock']['total'] ?: 0, post_count: post_count, post_total: post_total, timestamp: begin)
            coll.update($$(_id: "${YMD}_${toyId}_doll".toString()), new BasicDBObject('$set': log), true, false)
        }

        def q = $$(post_type: [$ne: 2], timestamp: [$lt: end], is_delete: false, is_award: false)
        catch_success_log.aggregate([
                $$('$match', q),
                $$('$project', [_id: '$toy._id']),
                $$('$group', [_id: '$_id', count: [$sum: 1]])
        ]).results().each {BasicDBObject obj->
            def toyId = obj['_id'] as Integer
            def remaining = obj['count'] as Integer //剩余娃娃
            def toy = catch_toy.findOne($$(_id: toyId))
            def log = $$(type:'day',toy_id:toyId, winrate: toy['winrate'], rate: toy['rate'], price: toy['price'], cost: toy['cost'],
                    stock: toy['stock']['total'] ?: 0, remaining: remaining, timestamp: begin)
            coll.update($$(_id: "${YMD}_${toyId}_doll".toString()), new BasicDBObject('$set': log), true, false)
        }

        //兑换成积分的娃娃的数量
        def match = $$(type: 'expire_points', timestamp: [$lt: end], is_delete: [$ne: true])
        def logids = user_award_logs.distinct('success_log_id', $$(match))

        user_award_logs.aggregate([
                $$('$match', [_id: [$in: logids]]),
                $$('$project', [_id: '$toy._id']),
                $$('$group', [_id: '$_id', count: [$sum: 1]])
        ]).results().each {BasicDBObject obj->
            def toyId = obj['_id'] as Integer
            def exchange_count = obj['count'] as Integer //兑换娃娃的个数
            def toy = catch_toy.findOne($$(_id: toyId))
            def log = $$(type:'day',toy_id:toyId, winrate: toy['winrate'], rate: toy['rate'], price: toy['price'], cost: toy['cost'],
                    stock: toy['stock']['total'] ?: 0, exchange_count: exchange_count, timestamp: begin)
            coll.update($$(_id: "${YMD}_${toyId}_doll".toString()), new BasicDBObject('$set': log), true, false)
        }
    }

    public static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    public static BasicDBObject $$(Map map) {
        return new BasicDBObject(map)
    }

    static Integer DAY = 0

    static void main(String[] args) {
        try{
            long l = System.currentTimeMillis()
            //统计每个娃娃每日抓取人数,抓取次数, 抓中次数,
            dollStatics(DAY)
            println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   dollStatics, cost  ${System.currentTimeMillis() - l} ms"

            l = System.currentTimeMillis()
            // 总抓取人数,总抓取次数,总抓中次数
            dollTotalStatics(DAY)
            println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   dollTotalStatics, cost  ${System.currentTimeMillis() - l} ms"

            l = System.currentTimeMillis()
            // 单日 总抓取人数,总抓取次数,总抓中次数
            dollTotalDay(DAY)
            println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   dollTotalDay, cost  ${System.currentTimeMillis() - l} ms"

            l = System.currentTimeMillis()
            // 库存统计
            stockStatics(DAY)
            println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   stockStatics, cost  ${System.currentTimeMillis() - l} ms"

        }catch (Exception e){
            println "Exception : " + e
        }

    }

}

