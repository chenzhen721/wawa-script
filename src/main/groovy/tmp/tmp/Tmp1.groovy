#!/usr/bin/env groovy
package tmp.tmp

import com.mongodb.BasicDBObject
import com.mongodb.DBCollection
import com.mongodb.DBObject
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
        @Grab('commons-codec:commons-codec:1.6')
])
import com.mongodb.Mongo
import com.mongodb.MongoURI
import redis.clients.jedis.Jedis

import java.text.SimpleDateFormat

/**
 * 房间发言数统计
 */
class Tmp1 {
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

    static final String jedis_host = getProperties("main_jedis_host", "192.168.31.246")
    static final Integer main_jedis_port = getProperties("main_jedis_port",6379) as Integer
    static mainRedis = new Jedis(jedis_host, main_jedis_port, 50000)

    static mongo  = new Mongo(new MongoURI(getProperties('mongo.uri','mongodb://192.168.2.27:10000/?w=1') as String))
    /*static final String jedis_host = getProperties("main_jedis_host", "192.168.31.236")
    static final Integer main_jedis_port = getProperties("main_jedis_port", 6379) as Integer*/
//    static redis = new Jedis(jedis_host, main_jedis_port)

    static DAY_MILLON = 24 * 3600 * 1000L
    static long zeroMill = new Date().clearTime().getTime()
    static Long yesTday = zeroMill - DAY_MILLON

    static DBCollection stat_channels = mongo.getDB('xy_admin').getCollection('stat_channels')

    static statics(int i) {

        def catch_record = mongo.getDB('xy_catch').getCollection('catch_record')

        def gteMill = yesTday - i * DAY_MILLON
        def ltMill = gteMill + DAY_MILLON
        def YMD = new Date(gteMill).format('yyyyMMdd')
        //设置超时、概率
        def catch_room = mongo.getDB('xy_catch').getCollection('catch_room')
        def catch_toy = mongo.getDB('xy_catch').getCollection('catch_toy')
        //补充线上中奖id
        def catch_user = mongo.getDB('xy_catch').getCollection('catch_user')
        def users = mongo.getDB('xy').getCollection('users')
        def finance_log_DB = mongo.getDB('xy_admin').getCollection('finance_log')
        def apply_post_logs = mongo.getDB('xylog').getCollection('apply_post_logs')
        def catch_success_logs = mongo.getDB('xylog').getCollection('catch_success_logs')
        def diamond_cost_logs = mongo.getDB("xylog").getCollection("diamond_cost_logs")
        def diamond_add_logs = mongo.getDB("xylog").getCollection("diamond_add_logs")
        def stat_daily = mongo.getDB('xy_admin').getCollection('stat_daily')
        def goods = mongo.getDB('xy_admin').getCollection('goods')
        def stat_regpay = mongo.getDB('xy_admin').getCollection('stat_regpay')
        def category = mongo.getDB('xy_admin').getCollection('category')
        def sdf = new SimpleDateFormat('yyyyMMdd')

        /*def total = catch_record.count($$(device_type: 3))
        def count = 0, fail_count = 0, succ_count = 0, fail_succ_count = 0
        catch_record.find($$(device_type: 3, first_doll: [$exists: true])).toArray().each {BasicDBObject obj->
            def score = obj.get('score') as Integer
            def config = obj.get('config_num') as Integer
            def status = obj['status'] as Boolean ?: false
            if (score <= config) {
                count = count + 1
                if (status) {
                    succ_count = succ_count + 1
                }
            }else if (score > config) {
                fail_count = fail_count + 1
                if (status) {
                    fail_succ_count = fail_succ_count + 1
                }
            }
        }
        println "total: ${total}, count: ${count}, succ_count: ${succ_count}, fail_count: ${fail_count}, fail_succ_count: ${fail_succ_count}".toString()*/

        /*[20171201:20171215, 20171216:20171231, 20180101:20180115].each {Integer key, Integer value ->
            //12月1号 - 12月15号
            def start = sdf.parse('' + key).getTime()
            def end = sdf.parse('' + value).getTime()
            def uids = users.distinct('_id', $$(timestamp: [$gte: start, $lt: end]))
            def a = [] as Set, a_pay = [] as Set, b = [] as Set, b_pay = [] as Set
            def a_cny = 0d, b_cny = 0d
            uids.each {Integer id ->
                def records = catch_record.find($$(user_id: id, is_delete: false, type: 2)).sort($$(timestamp: 1)).limit(3)
                if (records.size() >= 2) { //抓取次数大于等于两次
                    boolean status = false
                    for(DBObject obj : records) {
                        if (obj['status'] != null && obj['status'] == true) {
                            status = true
                            break
                        }
                    }
                    if (status) {
                        a.add(id)
                    } else {
                        b.add(id)
                    }
                }
            }
            finance_log_DB.find($$(user_id: [$in: a], via: [$ne: 'Admin'])).toArray().each {BasicDBObject obj ->
                a_pay.add(obj['user_id'])
                a_cny = a_cny + (obj['cny'] as Double)
            }
            finance_log_DB.find($$(user_id: [$in: b], via: [$ne: 'Admin'])).toArray().each {BasicDBObject obj ->
                b_pay.add(obj['user_id'])
                b_cny = b_cny + (obj['cny'] as Double)
            }
            println "start: ${key}  end: ${value}".toString()
            println "A 总数-${a.size()} 付费-${a_pay.size()} LTV-${a_cny/a_pay.size()}".toString()
            println "B 总数-${b.size()} 付费-${b_pay.size()} LTV-${b_cny/b_pay.size()}".toString()
        }


        def list = finance_log_DB.find($$(via: [$ne: 'Admin'], timestamp: [$gte: 1516809600000, $lt: 1516896000000])).toArray()*.cny
        println list
        println list.sum{ it as Double}

        println finance_log_DB.find($$(via: [$ne: 'Admin'], timestamp: [$gte: 1516809600000, $lt: 1516896000000])).toArray()*.qd*/

        /*[20180121, 20180122, 20180123, 20180124, 20180125].each {Integer ymd ->
            def begin = sdf.parse('' + ymd).getTime()
            def end = begin + 24 * 3600 * 1000L
            def timebtn = [$gte: begin, $lt: end]
            println ymd
            def logs = finance_log_DB.distinct('user_id', $$(timestamp: timebtn))
            println '充值人数：' + logs.size()

            def regpay = stat_regpay.findOne($$(_id: ymd + '_meme_union_regpay'))
            def regs = regpay['regs'] as Set
            println '注册人数：' + regs.size()
            def regspay = []
            logs.each {Integer id ->
                if (regs.contains(id)) {
                    regspay.add(id)
                }
            }
            println '新增充值：' + regspay
            println '新增抓取：' + catch_record.distinct('user_id', $$([user_id: [$in: regs], timestamp: timebtn, is_delete: [$ne: true]])).size()
        }*/

        [20180208:20180218].each {Integer start, Integer end ->
            def s = sdf.parse('' + start).getTime()
            def e = sdf.parse('' + end).getTime()
            def payed = [:], nopay = [:]
            users.find($$(timestamp: [$gte: s, $lt: e])).toArray().each {BasicDBObject obj ->
                def finance = finance_log_DB.find($$(user_id: obj['_id'], diamond: [$ne: 0], via: [$ne: 'Admin'])).sort($$(timestamp: 1)).limit(1).toArray()
                if (finance.size() <= 0) {
                    nopay.put(obj['_id'], null) //未付费
                } else {
                    payed.put(obj['_id'], finance[0]['timestamp']) //付费
                }
            }
            //前三次抓到娃娃的用户
            //付费用户
            int j = 1
            [payed, nopay].each { Map pay ->
                def succ = [], fail = []
                pay.each { Integer user_id, Long timestamp ->
                    //付费前
                    def query = $$(user_id: user_id)
                    if (j == 1) query.put('timestamp', [$lt: timestamp])

                    def precatch = catch_record.find(query).sort($$(timestmap: 1)).limit(3).toArray()
                    def status = false
                    for (DBObject item : precatch) {
                        if (item['status'] == Boolean.TRUE) {
                            status = true
                        }
                        if (status) {
                            //前三次抓中, 至今抓到娃娃的个数
                            succ.add(user_id)
                        } else {
                            //前三次未中
                            fail.add(user_id)
                        }
                    }
                }
                def s1 = [], s2 = [], s3 = [], s4 = [], s5 = []
                def f1 = [], f2 = [], f3 = [], f4 = [], f5 = []
                int sf = 0
                [succ, fail].each { List uids ->
                    def total_count3 = 0, total_count4 = 0
                    catch_record.aggregate([
                            $$($match: [user_id: [$in: uids], status: true]),
                            $$($group: [_id: '$user_id', count: [$sum: 1]])
                    ]).results().each { BasicDBObject group ->

                        def count = group['count'] as Integer ?: 0
                        if (count == 1) {
                            sf == 0 ? s1.add(group['_id'] as Integer) : f1.add(group['_id'] as Integer)
                        } else if (count == 2) {
                            sf == 0 ? s2.add(group['_id'] as Integer) : f2.add(group['_id'] as Integer)
                        } else if (count == 3) {
                            sf == 0 ? s3.add(group['_id'] as Integer) : f3.add(group['_id'] as Integer)
                            total_count3 = total_count3 + count
                        } else {
                            sf == 0 ? s4.add(group['_id'] as Integer) : f4.add(group['_id'] as Integer)
                            total_count4 = total_count4 + count
                        }
                    }
                    sf = sf + 1

                    println "抓中总数3个：" + total_count3 + ",抓中总数3+：" + total_count4
                }

                def f = []
                fail.each {Integer id ->
                    if (catch_record.count($$(user_id: id, status: true, is_delete: false)) <= 0) {
                        f.add(id)
                    }
                }
                def fi = finance_log_DB.find($$(user_id: [$in: f], via: [$ne: 'Admin'])).toArray().sum { it.cny ?: 0d }

                println "增加项${j}：" + f.size() + ':' + fi + ':' + ':' + getInviteDiamond(f, s, e)



                int index = 1
                [s1, s2, s3, s4, f1, f2, f3, f4].each { List c ->
                    //付费寄送的
                    def ids = []
                    if (index == 1 || index == 2 || index == 5 || index == 6) {
                        ids = finance_log_DB.distinct('user_id', $$(diamond: 0, user_id: [$in: c]))
                        def total = 0d
                        finance_log_DB.find($$(user_id: [$in: ids], diamond: [$gt: 0])).toArray().each { BasicDBObject obj ->
                            def cny = obj['cny'] as Double ?: 0d
                            total = total + cny
                        }
                        println "付费寄送 ${j} ${index}：   ${ids.size()},  ${total},  ${getInviteDiamond(ids, s, e)}".toString()
                    }
                    c.removeAll(ids)
                    def total = 0d
                    finance_log_DB.find($$(user_id: [$in: c])).toArray().each { BasicDBObject obj ->
                        def cny = obj['cny'] as Double ?: 0d
                        total = total + cny
                    }
                    println "${j} ${index}:   ${c.size()},  ${total}, ${getInviteDiamond(c, s, e)}".toString()
                    index = index + 1
                }
                j = j + 1
            }
        }

        //println mainRedis.get("user:1208411:first:doll")
        /*def begin = sdf.parse('20180201').clearTime().getTime()
        def toy_map = MapWithDefault.<Integer, Integer>newInstance(new HashMap<Integer, Integer>()) { 0 }
        finance_log_DB.distinct('user_id', $$(via: [$ne: 'Admin'])).each {Integer user_id ->
            def first_pay_list = finance_log_DB.find($$(user_id: user_id, via: [$ne: 'Admin'])).sort($$(timestamp: 1)).limit(1).toArray() ?: []
            if (first_pay_list.size() > 0) {
                def start = first_pay_list[0]['timestamp'] as Long
                if (start > begin) {
                    catch_record.aggregate([
                            $$('$match', [user_id: user_id, timestamp: [$gte: start], is_delete: false, type: 2]),
                            $$('$group', [_id: '$toy._id', count: [$sum: 1]])
                    ]).results().each { BasicDBObject obj ->
                        def toyId = obj['_id'] as Integer
                        def count = obj['count'] as Integer
                        toy_map[toyId] = toy_map[toyId] + count
                    }
                }
            }
        }
        toy_map.each {Integer toyId, Integer count->
            def toy = catch_toy.findOne($$(_id: toyId))
            def g = goods.findOne($$(toy_id: toyId))
            def cateName = ''
            if (g != null && g['cate_id'] != null) {
                def cate = category.findOne($$(_id: g['cate_id']))
                cateName = cate?.get('name') ?: ''
            }
            println "${toy['_id']},${toy['name']},${cateName},${toy['price']},${count}".toString()
        }*/

        /*[20180201,20180202,20180203,20180204,20180205].each {Integer ymd ->
            def start = sdf.parse('' + ymd).getTime()
            def end = start + DAY_MILLON
            def count = catch_record.count($$(first_doll: true, is_delete: false, timestamp: [$gte: start, $lt: end]))
            def bingo = catch_record.count($$(first_doll: true, is_delete: false, status: true, timestamp: [$gte: start, $lt: end]))
            println "${ymd}, ${count}, ${bingo}".toString()
        }*/

        /*def total = 0
        apply_post_logs.find($$(timestamp: [$gte: 1517932800000, $lt: 1518019200000])).toArray().each {BasicDBObject obj ->
            total = total + (obj['record_ids'] as List).size()
        }
        println total*/

    }

    static getInviteDiamond(def ids, long s, long e) {
        def diamond_logs = mongo.getDB('xylog').getCollection('diamond_add_logs')
        Integer diamond = 0 as Integer
        diamond_logs.find($$(type: 'invite_diamond', user_id: [$in: ids], timestamp: [$gte: s, $lt: e])).toArray().each {BasicDBObject obj ->
            diamond = diamond + (obj['award']['diamond'] as Integer ?: 0)
        }
        diamond
    }

    public static final String APP_ID = "984069e5f8edd8ca4411e81863371f16"

    static Integer day = 0

    static void main(String[] args) {
        long l = System.currentTimeMillis()
        statics(0 )
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${this.getSimpleName()},statics cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

    }


    static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    static BasicDBObject $$(Map map) {
        return new BasicDBObject(map)
    }

}