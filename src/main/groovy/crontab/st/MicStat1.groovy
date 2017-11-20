#!/usr/bin/env groovy
package crontab.st

import com.mongodb.BasicDBObject
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
class MicStat1 {
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

        def catch_record = mongo.getDB('xy_lab').getCollection('catch_record')

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
        def catch_room = mongo.getDB('xy_lab').getCollection('catch_room')
        catch_room.find($$(playtime: [$ne: 40], winrate: [$ne: 25])).toArray().each {BasicDBObject obj ->
            if (!setProbAndtime(obj['fid'] as String, 25, 40)) {
                println obj['_id']
            }
            catch_room.update($$(_id: obj['_id']), $$($set: [playtime: 40, winrate: 25]))
        }

    }

    public static final String APP_ID = "984069e5f8edd8ca4411e81863371f16"

    static Boolean setProbAndtime(String device_id, Integer prob, Integer time) {
        String host = "http://doll.artqiyi.com/api/index.php"
        String prob_controller = "?app=doll&act=set_winning_probability&"
        String time_controller = "?app=doll&act=set_playtime&"
        //参与验签字符串
        Long ts = System.currentTimeMillis()
        String prob_param = "device_id=${device_id}&platfort=meme&ts=${ts}&winning_probability=${prob}".toString()
        def sign = DigestUtils.md5Hex(DigestUtils.md5Hex(prob_param.replaceAll('=', '')) + APP_ID)
        prob_param = prob_param + "&sign=" + sign
        def prob_url = host + prob_controller + prob_param
        def content = new URL(prob_url).getText("UTF-8")
        def obj = new JsonSlurper().parseText(content) as Map
        if (obj == null || Boolean.TRUE != (Boolean.parseBoolean(obj['done'] as String))) {
            println prob_url + ' error.' + content
            return false
        }

        String time_param = "device_id=${device_id}&platfort=meme&playtime=${time}&ts=${ts}".toString()
        def time_sign = DigestUtils.md5Hex(DigestUtils.md5Hex(time_param.replaceAll('=', '')) + APP_ID)
        time_param = time_param + "&sign=" + time_sign
        def time_url = host + time_controller + time_param
        content = new URL(time_url).getText("UTF-8")
        obj = new JsonSlurper().parseText(content) as Map
        if (obj == null || Boolean.TRUE != (Boolean.parseBoolean(obj['done'] as String))) {
            println time_url + '  error.' + content
            return false
        }
        return true
    }

    static Integer day = 0

    static void main(String[] args) {
        long l = System.currentTimeMillis()
        1.times {
            statics(it)
        }
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${MicStat1.class.getSimpleName()},statics cost  ${System.currentTimeMillis() - l} ms"
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