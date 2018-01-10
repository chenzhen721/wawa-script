#!/usr/bin/env groovy
package tmp.tmp

import com.mongodb.BasicDBObject
import com.mongodb.DBCollection
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
        @Grab('commons-codec:commons-codec:1.6')
])
import com.mongodb.Mongo
import com.mongodb.MongoURI
import groovy.json.JsonSlurper
import org.apache.commons.codec.digest.DigestUtils

/**
 * 房间发言数统计
 */
class Tmp {
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

    static mongo  = new Mongo(new MongoURI(getProperties('mongo.uri','mongodb://192.168.2.27:10000/?w=1') as String))
    /*static final String jedis_host = getProperties("main_jedis_host", "192.168.31.236")
    static final Integer main_jedis_port = getProperties("main_jedis_port", 6379) as Integer*/
//    static redis = new Jedis(jedis_host, main_jedis_port)

    static DAY_MILLON = 24 * 3600 * 1000L
    static long zeroMill = new Date().clearTime().getTime()
    static Long yesTday = zeroMill - DAY_MILLON

    static statics(int i) {

        def catch_record = mongo.getDB('xy_catch').getCollection('catch_record')

        //获取meme发货订单
        /*def query = $$(pack_id: [$exists: true], post_type: 1)
        def packIds = catch_record.find(query).toArray()*.pack_id
        println packIds
        def sb = new StringBuffer()
        if (1 <= catch_record.update($$(pack_id: [$in: packIds], post_type: 1), $$($set: [post_type: 2]), false, true).getN()) {
            def list = catch_record.find($$(pack_id: [$in: packIds]), $$(toy: 1, address: 1)).sort($$(pack_id: 1, timestamp: -1)).toArray()
            list.each {BasicDBObject obj ->
                def address = obj['address']
                sb.append(obj['_id']).append(',').append(obj['toy']['name']).append(',')
                        .append(address['province']).append(',').append(address['city']).append(',')
                        .append(address['region']).append(',').append(address['address']).append(',')
                        .append(address['name']).append(',').append(address['tel']).append(',').append(System.lineSeparator())
            }
        }
        println sb.toString()

        File file = new File('E:/a.txt')

        file.write(sb.toString())*/


        def gteMill = yesTday - i * DAY_MILLON
        def ltMill = gteMill + DAY_MILLON
        def YMD = new Date(gteMill).format('yyyyMMdd')



        /*def  count = catch_record.count($$(timestamp: [$get: gteMill, $lt: ltMill]))
        def bingo = catch_record.count($$(type: 2, timestamp: [$get: gteMill, $lt: ltMill]))

        println new Date(gteMill).format('yyyy-MM-dd') + ' ' + count + ' ' + bingo


        def users = mongo.getDB('xy').getCollection('users')
        def reg = new HashSet()
        //当日注册用户
        users.find(new BasicDBObject(timestamp: [$gte: gteMill, $lt: gteMill + DAY_MILLON]),
                new BasicDBObject(_id: 1, timestamp: 1, qd: 1)).toArray().each { BasicDBObject obj ->
            def uid = obj.get('_id') as Integer
            reg.add(uid)
        }
        //次日登录
        def day_login = mongo.getDB("xylog").getCollection("day_login")
        def login = day_login.count($$(timestamp: [$gte: ltMill, $lt: ltMill + DAY_MILLON]))

        println new Date(gteMill).format('yyyy-MM-dd') + ' ' + login*/


        /*def users = mongo.getDB('xy').getCollection('users')
        users.find($$(timestamp: [$gte: zeroMill])).each {BasicDBObject obj ->
            def nick_name = obj['nick_name'] as String
            def decode = URLDecoder.decode(obj['nick_name'] as String, 'UTF-8')

            if (nick_name != decode) {
                users.update($$(_id: obj['_id']), $$($set: [nick_name: decode]), false, false)
                println obj['_id'] + ":" + nick_name + " decode to: " + decode
            }

            //println obj['_id'] + ":" + obj['nick_name'] + "decode to:" +
        }*/

        //设置超时、概率
        def catch_room = mongo.getDB('xy_catch').getCollection('catch_room')
        /*catch_room.find($$(online: true)).toArray().each {BasicDBObject obj ->
            if (!setProbAndtime(obj['fid'] as String, 25, 40)) {
                println obj['_id']
            } else {
                //catch_room.update($$(_id: obj['_id']), $$($set: [winrate: 25]))
                println "success ${obj['_id']}".toString()
            }
        }*/

        //批量上下线
        /*def catch_room = mongo.getDB('xy_catch').getCollection('catch_room')
        println catch_room.update(new BasicDBObject(), $$($set: [online: false]), false, true)*/


        def catch_toy = mongo.getDB('xy_catch').getCollection('catch_toy')
        /*def sb = new StringBuffer()
        catch_room.find().toArray().each {BasicDBObject obj->
            String toy = catch_toy.findOne(obj['toy_id'])
            sb.append("${obj['name']}").append(',').append("${obj['fid']}").append(',')
                    .append("${toy['_id']}").append(',').append("${obj['name']}").append(',').append(System.lineSeparator())
        }
        println sb.toString()*/

        //补充线上中奖id
        def catch_success_log = mongo.getDB('xylog').getCollection('catch_success_logs')
        //获取goods_id
        /*def file = new File('/empty/crontab/metadata/goodsid.txt')
        def ids = new HashMap()
        file.readLines().each {String line ->
            def a = line.split(',')
            ids.put(Integer.parseInt(a[2]), a[4])
        }
        println ids*/

        //设置goods_id
        /*catch_record.find($$(type: 2, goods_id: [$exists: false])).toArray().each {BasicDBObject obj ->
            def gid = ids.get(obj['toy']['_id']) as Integer
            catch_record.update($$(_id: obj['_id']), $$($set: [goods_id: gid]), false, false)
        }*/
        //设置成功记录
        /*def logs = catch_success_log.find($$(goods_id: [$exists: false]))
        //println logs
        logs.each {BasicDBObject obj ->
            def gid = ids.get(obj['toy']['_id']) as Integer
            catch_success_log.update($$(_id: obj['_id']), $$($set: [goods_id: gid]), false, false)
            println obj['_id']
        }*/

        def apply_post_log = mongo.getDB('xylog').getCollection('apply_post_logs')
        def catch_user = mongo.getDB('xy_catch').getCollection('catch_user')
        //异常订单拆单
        /*def file = new File('/empty/crontab/metadata/BUG12.txt')
        def ids = []
        file.readLines().each {String line ->
            if (line != null && line != '') {
                def a = line.split(',')
                if (a.length > 1) {
                    ids.add(a[1] as String)
                }
            }
        }
        println ids.size()

        //先查询是否有已申请的异常订单
        apply_post_log.find($$('toys.record_id': [$in: ids], is_delete: [$ne: true])).toArray().each {BasicDBObject post_log->
            if (post_log != null) {
                apply_post_log.update($$(_id: post_log['_id']), $$($set: [is_delete: true, status: 2]))
                def toys = post_log['toys'] as List
                if (toys != null && toys.size() > 0) {
                    toys.each { BasicDBObject toy ->
                        def _id = toy['record_id'] as String
                        //正常抓取的记录还原
                        if (!ids.contains(_id)) {
                            catch_success_log.update($$(_id: _id), $$($set: [post_type: 0], $unset: [pack_id: 1, apply_time: 1]))
                            println 'normal:' + _id
                        }
                    }
                }
            }
        }

        println catch_success_log.update($$(_id: [$in: ids]), $$($set: [is_delete: true]), false, true)*/

        //重复提交订单BUG
        /*def logs = apply_post_log.find(new BasicDBObject())
                .sort($$(timestmap: -1)).toArray()
        println logs.size()
        logs.each{BasicDBObject post_log->
            def toys = post_log['toys'] as List
            if (toys != null && toys.size() > 0) {
                def records = toys*.record_id
                println records
                if (catch_success_log.count($$(_id: [$in: records], post_type: 0)) > 0) {
                    println catch_success_log.update($$(_id: [$in: records]), $$($set: [post_type: 0], $unset: [pack_id: 1, apply_time: 1]), false, true)
                    apply_post_log.update($$(_id: post_log['_id']), $$($set: [is_delete: true]))
                    println post_log
                }
            }
        }*/

        def channels = mongo.getDB('xy_admin').getCollection('channels')

        //统计渠道抓取人数和次数
        /*def users = mongo.getDB('xy').getCollection('users')
        def uids = users.find($$(qd: 'tj')).toArray()*._id
        def time = [$gte: gteMill, $lt: ltMill]
        def u = [] as Set
        def record = catch_record.find($$(user_id: [$in: uids], timestamp: time)).toArray()
        def num = record.size()
        record.each {BasicDBObject obj->
            u.add(obj['user_id'] as Integer)
        }
        println 'count: ' + num + ' users:' + u.size()*/

        //已申请邮寄订单补充 goods_id
        /*apply_post_log.find($$(is_delete: [$ne: true], status: [$ne: 2])).toArray().each {BasicDBObject obj->
            def toys = obj['toys'] as List
            if (toys != null && toys.size() > 0) {
                def update = new BasicDBObject()
                int index = 0;
                toys.each {BasicDBObject record->
                    if (record['goods_id'] == null && record['_id'] != null) {
                        def goods_id = ids.get(record['_id'] as Integer)
                        if (goods_id != null) {
                            update.put("toys.${index}.goods_id".toString(), goods_id as Integer)
                        }
                    }
                    index = index + 1
                }
                if (update.size() > 0) {
                    println update
                    apply_post_log.update($$(_id: obj['_id']), $$($set: update), false, false)
                    println obj['_id']
                }
            }
        }*/

        //批量添加status
        //println apply_post_log.update($$(status: [$exists: false]), $$($set: [status: 0]), false, true)


        //添加发货地址
        /*apply_post_log.find($$(address_list: [$exists: false])).toArray().each {BasicDBObject obj ->
            if (obj['address'] != null) {
                def address = obj['address']
                def addressstr = "${address['province'] ?: ''}${address['city'] ?: ''}${address['region'] ?: ''}${address['address']}".toString()
                println apply_post_log.update($$(_id: obj['_id']), $$($set: [address_list: addressstr]), false, false)
            }
        }*/
        //下单脚本

        //恢复订单
        /*apply_post_log.find(new BasicDBObject()).toArray().each {BasicDBObject obj->
            def _id = obj['_id'] as String
            def success_logs = catch_success_log.find($$(pack_id: _id))
            def logWithId = [is_delete: false] as Map
            def toys = []
            def userId = null
            def ids = []
            success_logs.each {BasicDBObject succ ->
                ids.add(succ['_id'])
                //包裹中有一条删除记录则删除全部
                if (succ['is_delete'] != null && succ['is_delete'] == true) {
                    logWithId.put('is_delete', true)
                }
                //
                userId = succ['user_id'] as Integer
                def toy = succ['toy'] as Map
                Integer goods_id = succ['goods_id'] as Integer ?: null
                if (goods_id == null) {
                    //goods.clear()
                    return
                }
                toy.put('goods_id', goods_id)
                //goods.put(goods_id, goods.get(goods_id) + 1)
                toy.put('room_id', succ['room_id'])
                toy.put('record_id', succ['_id'])
                toys.add(succ['toy'])
            }

            if (userId != null) {
                def def_addr = catch_user.findOne(userId)
                if (def_addr == null) {
                    return [code: 0]
                }
                def list = def_addr['address_list'] as List
                if (list != null || list.size() > 0) {
                    def address = null
                    for (int ind = 0; ind < list.size(); ind++) {
                        def add = list.get(ind)
                        if (add['is_default'] == Boolean.TRUE) {
                            address = add
                            break
                        }
                    }
                    if (address != null) {
                        logWithId.putAll([user_id: userId, record_ids: ids, toys: toys, timestamp: System.currentTimeMillis(), post_type: 1, status: 0, address: address])
                        println apply_post_log.update($$(_id: _id), $$($set: logWithId), false, false)
                    }
                }
            }
        }*/

        //apply_post_log.update($$(order_id: [$exists: true]), $$($unset: [order_id: 1, push_time: 1]), false, true)

        //同步订单
        /*def file = new File('/empty/crontab/metadata/order-shipping-1127.txt')
        def ids = new HashMap()
        file.readLines().each {String line ->
            if (line != null && !line.isEmpty()) {
                def a = line.split(',')
                ids.put(a[0], a[1])
            }
        }
        println ids
        def missing = []
        def missmatch = []
        def missorder = []
        ids.each {String order_id, String shipping->
            def post_log = apply_post_log.findOne($$('post_info.order_id': order_id))
            if (post_log == null) {
                missing.add(order_id + ',' + shipping)
            } else {
                def post_info = post_log['post_info'] as Map
                def no = post_info['shipping_no']
                def set = $$('post_info.shipping_no': shipping)
                if (no != shipping) {
                    missmatch.add(post_info['_id'] + ',' + order_id + ',' + shipping)
                }
                if (order_id != post_log['order_id']) {
                    set.put('order_id', order_id)
                    missorder.add(post_log['_id'] + ',' + order_id + ',' + shipping)
                }
                apply_post_log.update($$(_id: post_log['_id']), $$($set: set))
            }
        }
        println missing
        println missmatch
        println missorder*/
        //apply_post_log.find($$())

        //查询已发货的订单
        /*def count = 0
        apply_post_log.find($$(post_type: 3)).toArray().each {BasicDBObject obj->
            def ids = obj.get('record_ids') as List
            count = count + ids.size()
        }
        println count*/


        /*catch_success_log.find($$(coin: 0)).toArray().each {BasicDBObject obj->
            def _id = obj['_id'] as String //
            //补单数据
            if (_id.endsWith('_supplement')) {
                _id = _id.replace('_supplement', '')
                def record = catch_record.findOne($$(_id: _id))
                catch_success_log.update($$(_id: obj['_id']), $$($set: [coin: record['coin'], relative_record: _id]))
            }
            println obj['_id']
        }*/

        //查询无跟踪渠道
        //def channels = mongo.getDB('xy_admin').getCollection('channels')
        /*def ids = channels.find(new BasicDBObject()).toArray()*._id
        println mongo.getDB('xy').getCollection('users').distinct('qd', $$(qd: [$nin: ids]))*/

        def users = mongo.getDB('xy').getCollection('users')
        def finance_log_DB = mongo.getDB('xy_admin').getCollection('finance_log')
        /*13.times {
            int day = 12 - it
            def begin = yesTday - day * DAY_MILLON
            def end = begin + DAY_MILLON
            //充值用户
            def regs = users.find(new BasicDBObject(timestamp: [$gte: begin, $lt: end]))*._id
            Double total_cny = 0, day1cny = 0, day2cny = 0, day3cny = 0, day4cny = 0
            def total_uids = new HashSet(), day1_uids = new HashSet(),day2_uids = new HashSet(),day3_uids = new HashSet(),day4_uids = new HashSet()
            finance_log_DB.find($$(user_id: [$in: regs], via: [$ne: 'Admin'], timestamp: [$lt: gteMill])).toArray().each {BasicDBObject obj->
                def timestamp = obj['timestamp'] as Long
                def cny = obj.cny as Double
                if (timestamp > end && timestamp < end + DAY_MILLON) {
                    day1cny = day1cny + cny
                    day1_uids.add(obj.user_id)
                }
                if (timestamp > end + DAY_MILLON && timestamp < end + 2 * DAY_MILLON) {
                    day2cny = day2cny + cny
                    day2_uids.add(obj.user_id)
                }
                if (timestamp > end + 2 * DAY_MILLON && timestamp < end + 3 * DAY_MILLON) {
                    day3cny = day3cny + cny
                    day3_uids.add(obj.user_id)
                }
                if (timestamp > end + 3 * DAY_MILLON && timestamp < end + 4 * DAY_MILLON) {
                    day4cny = day4cny + cny
                    day4_uids.add(obj.user_id)
                }
                total_cny = total_cny + cny
                total_uids.add(obj.user_id)
            }
            def day_login = mongo.getDB("xylog").getCollection("day_login")
            def start = end
            def util = zeroMill

            def days = 0 as Integer

            total_uids.each {Integer id ->
                def obj = day_login.find($$(user_id: id, timestamp: [$gte: begin, $lt: util])).sort($$(timestamp: -1)).limit(1)
                def time = obj[0]['timestamp'] as Long
                days = days + (((time - start) / DAY_MILLON) as Double).toInteger() + 1
            }
            println "${new Date(begin).format('yyyy-MM-dd')}  ${days} / ${total_uids.size()}".toString()

            *//*day_login.find($$(user_id: [$in: total_uids], timestamp: [$gte: begin, $lt: util])).toArray().each {BasicDBObject obj ->
                def time = obj['timestamp'] as Long
                days = days + (((time - start) / DAY_MILLON) as Double).toInteger() + 1
                println days
            }*//*
            //def avg = (days / total_uids.size()) as Double
            println "${new Date(begin).format('yyyy-MM-dd')}  ${days} / ${total_uids.size()}".toString()


            println "${new Date(begin).format('yyyy-MM-dd')}    ${total_cny}:${total_uids.size()}".toString() +
                    "    ${day1cny}:${day1_uids.size()}    ${day2cny}:${day2_uids.size()}    ${day3cny}:${day3_uids.size()}    ${day4cny}:${day4_uids.size()}"
        }*/

        def award_log = mongo.getDB('xylog').getCollection('user_award_logs')

        /*catch_success_log.find($$(post_type: 0, is_award: true, timestamp: [$gte: 1513612800000])).each {BasicDBObject obj->
            //
            def user_id = obj['user_id']
            def award = award_log.findOne($$(success_log_id: obj['_id']))
            if (award == null) {
                println 'none:' + user_id
            } else {
                def points = award['award']['points'] as Integer
                def user = users.findOne(user_id)
                def count = user['bag']['points']['count']
                if (count < points) {
                    users.update($$(_id: user_id), $$($set: ['bag.points.count': 0]), false, false)
                    catch_success_log.update($$(_id: obj['_id']), $$($set: [exception: true, need: points - count]))
                    println 'not enough:' + count + ':' + points
                } else {
                    users.update($$(_id: user_id), $$($inc: ['bag.points.count': - points]), false, false)
                    catch_success_log.update($$(_id: obj['_id']), $$($unset: [is_award: true]), false, false)
                }
                award_log.update($$(_id: award['_id']), $$($set: [is_delete: true]))
            }
        }*/

        //catch_success_log.distinct('_id', $$(post_type: 0, is_award: true, timestamp: [$gte: 1513612800000]))

        /*award_log.find($$(is_delete: true, user_id: 1203097)).toArray().each {BasicDBObject obj->
            def points = obj['award']['points'] as Integer
            def user_id = obj['user_id']
            def user = users.findOne(user_id)
            def count = user['bag']['points']['count']
            if (count < points) {
                users.update($$(_id: user_id), $$($set: ['bag.points.count': -count]), false, false)
                catch_success_log.update($$(_id: obj['_id']), $$($set: [exception: true, need: points - count]))
                println 'not enough:' + user_id + ':' + count + ':' + points
            } else {
                users.update($$(_id: user_id), $$($inc: ['bag.points.count': -points]), false, false)
                catch_success_log.update($$(_id: obj['_id']), $$($unset: [is_award: true]), false, false)
            }
        }*/
        /*def apply = apply_post_log.find($$(post_type: 3, timestamp: [$gte: 1512057600000,$lt: 1512403200000]))
        println apply.size()
        def n = 0
        apply.each {BasicDBObject obj ->
            n = n + (obj['record_ids'] as List).size()
        }
        println n*/


        //日期/当日新增人数/截止到目前的付费/截止到目前的付费人数/截止到目前这些付费人数的经历总天数/
        /*31.times {
            int day = 30 - it
            def begin = yesTday - day * DAY_MILLON
            def end = begin + DAY_MILLON
            def s = "${new Date(begin).format('yyyy-MM-dd')}".toString()
            //充值用户
            def regs = users.find(new BasicDBObject(timestamp: [$gte: begin, $lt: end], qd: 'wawa_kuailai_gzh'))*._id
            s = s + '/' + regs.size()
            Double total_cny = 0, day1cny = 0, day2cny = 0, day3cny = 0, day4cny = 0
            def total_uids = new HashSet(), day1_uids = new HashSet(),day2_uids = new HashSet(),day3_uids = new HashSet(),day4_uids = new HashSet()
            finance_log_DB.find($$(user_id: [$in: regs], via: [$ne: 'Admin'], timestamp: [$lt: gteMill])).toArray().each {BasicDBObject obj->
                def cny = obj.cny as Double
                total_cny = total_cny + cny
                total_uids.add(obj.user_id)
            }
            def day_login = mongo.getDB("xylog").getCollection("day_login")
            def start = end
            def util = zeroMill

            def days = 0 as Integer

            total_uids.each {Integer id ->
                def obj = day_login.find($$(user_id: id, timestamp: [$gte: begin, $lt: util])).sort($$(timestamp: -1)).limit(1)
                def time = obj[0]['timestamp'] as Long
                days = days + (((time - start) / DAY_MILLON) as Double).toInteger() + 1
            }
            //println "${new Date(begin).format('yyyy-MM-dd')}  ${days} / ${total_uids.size()}".toString()

//            day_login.find($$(user_id: [$in: total_uids], timestamp: [$gte: begin, $lt: util])).toArray().each {BasicDBObject obj ->
//                def time = obj['timestamp'] as Long
//                days = days + (((time - start) / DAY_MILLON) as Double).toInteger() + 1
//                println days
//            }
            //def avg = (days / total_uids.size()) as Double
            s = s + '/' + total_cny + '/' + total_uids.size() + '/' + days
            println s
        }*/

        //查询所有充值用户的次数
        /*SortedMap map = new TreeMap() //充值次数， 对应的人数
        def uids = finance_log_DB.distinct('user_id', $$(via: [$ne: 'Admin']))
        uids.each {Integer uid ->
            def count = finance_log_DB.count($$(user_id: uid, via: [$ne: 'Admin']))
            def c = map.get(count) ?: 0
            c = c + 1
            map.put(count, c)
        }
        map.each {Long count, Integer c ->
            println count + ',' + c
        }*/

    }

    public static final String APP_ID = "984069e5f8edd8ca4411e81863371f16"

    static Boolean setProbAndtime(String device_id, Integer prob, Integer time) {
        String host = "http://doll.artqiyi.com/api/index.php"
        String prob_controller = "?app=doll&act=set_winning_probability&"
        //String time_controller = "?app=doll&act=set_playtime&"
        //参与验签字符串
        Long ts = System.currentTimeMillis()
        String prob_param = "device_id=${device_id}&platform=meme&ts=${ts}&winning_probability=${prob}".toString()
        def sign = DigestUtils.md5Hex(DigestUtils.md5Hex(prob_param
                .replaceAll('=', '').replaceAll('&', '')) + APP_ID)
        prob_param = prob_param + "&sign=" + sign
        def prob_url = host + prob_controller + prob_param
        def content = new URL(prob_url).getText("UTF-8")
        def obj = new JsonSlurper().parseText(content) as Map
        if (obj == null || Boolean.TRUE != (Boolean.parseBoolean(obj['done'] as String))) {
            println prob_url + ' error.' + content
            return false
        }

        /*String time_param = "device_id=${device_id}&platform=meme&playtime=${time}&ts=${ts}".toString()
        def time_sign = DigestUtils.md5Hex(DigestUtils.md5Hex(time_param.replaceAll('=', '')
                .replaceAll('&', '')) + APP_ID)
        time_param = time_param + "&sign=" + time_sign
        def time_url = host + time_controller + time_param
        content = new URL(time_url).getText("UTF-8")
        obj = new JsonSlurper().parseText(content) as Map
        if (obj == null || Boolean.TRUE != (Boolean.parseBoolean(obj['done'] as String))) {
            println time_url + '  error.' + content
            return false
        }*/
        return true
    }

    static Integer day = 0

    static void main(String[] args) {
        long l = System.currentTimeMillis()
//        1.times {
            statics(0 )
//        }
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${this.getSimpleName()},statics cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

    }


    static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    static BasicDBObject $$(Map map) {
        return new BasicDBObject(map)
    }

    //http://testdoll.artqiyi.com/api/index.php?app=doll&act=assign&&user_id=1201085&platform=meme&sign=10399c9d1c6f05bb5d8f24e4786f6730
    static void test() {
        /*
        import com.weixin.util.MD5Util
        def utf8 = 'UTF-8'
        def app_id= '984069e5f8edd8ca4411e81863371f16'
        def device_id = 'dd9vQpyMxkQ6ZSGHy49sSE'
        def user_id = 1201313
        def platform = 'meme'
        def ts = 1509947232220
        def str = "device_id${device_id}platform${platform}ts${ts}user_id${user_id}".toString()


        def sign = MD5Util.MD5Encode(MD5Util.MD5Encode(str, utf8) + app_id, utf8)
        println 'assign:' + sign

        println "http://testdoll.artqiyi.com/api/index.php?app=doll&act=assign&device_id=${device_id}&user_id=${user_id}&platform=${platform}&sign=${sign}&ts=${ts}".toString()

        //action 1

        def action = 7
        def str1 = "action${action}".toString() + str
        sign = MD5Util.MD5Encode(MD5Util.MD5Encode(str1, utf8) + app_id, utf8)
        println "http://testdoll.artqiyi.com/api/index.php?app=doll&act=operate&device_id=${device_id}&action=${action}&platform=${platform}&user_id=${user_id}&sign=${sign}&ts=${ts}".toString()

        action = 8
        str1 = "action${action}".toString() + str
        sign = MD5Util.MD5Encode(MD5Util.MD5Encode(str1, utf8) + app_id, utf8)
        println "http://testdoll.artqiyi.com/api/index.php?app=doll&act=operate&device_id=${device_id}&action=${action}&platform=${platform}&user_id=${user_id}&sign=${sign}&ts=${ts}".toString()


        action = 9
        str1 = "action${action}".toString() + str
        sign = MD5Util.MD5Encode(MD5Util.MD5Encode(str1, utf8) + app_id, utf8)
        println "http://testdoll.artqiyi.com/api/index.php?app=doll&act=operate&device_id=${device_id}&action=${action}&platform=${platform}&user_id=${user_id}&sign=${sign}&ts=${ts}".toString()





        def playtime = '40'
        str = "device_id${device_id}platform${platform}playtime${playtime}ts${ts}".toString()
        sign = MD5Util.MD5Encode(MD5Util.MD5Encode(str, utf8) + app_id, utf8)
        println 'set_playtime:' + sign
        println "http://testdoll.artqiyi.com/api/index.php?app=doll&act=set_playtime&device_id=${device_id}&playtime=${playtime}&platform=${platform}&sign=${sign}&ts=${ts}".toString()


        */










    }





}