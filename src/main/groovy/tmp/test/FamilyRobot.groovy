#!/usr/bin/env groovy

@GrabResolver(name = 'restlet', root = 'http://192.168.31.253:8081/nexus/content/groups/public')
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
])
import com.mongodb.Mongo
import com.mongodb.MongoURI
import com.mongodb.BasicDBObject
import java.security.MessageDigest
import groovy.json.JsonSlurper
import org.apache.commons.lang.math.RandomUtils
import redis.clients.jedis.Jedis


/**
 * 家族机器人
 */

class FamilyRobot{
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

    static mongo  = new Mongo(new MongoURI(getProperties('mongo.uri','mongodb://192.168.31.246:27017/?w=1') as String))
    static api_url  = getProperties('api.domain','http://localhost:8080/')

    static final String jedis_host = getProperties("main_jedis_host", "192.168.31.246")
    static final String chat_jedis_host = getProperties("chat_jedis_host", "192.168.31.246")
    static final String live_jedis_host = getProperties("live_jedis_host", "192.168.31.246")
    static final String user_jedis_host = getProperties("user_jedis_host", "192.168.31.246")

    static final Integer main_jedis_port = getProperties("main_jedis_port",6379) as Integer
    static final Integer chat_jedis_port = getProperties("chat_jedis_port",6379) as Integer
    static final Integer live_jedis_port = getProperties("live_jedis_port",6379) as Integer
    static final Integer user_jedis_port = getProperties("user_jedis_port",6379) as Integer

    static mainRedis = new Jedis(jedis_host, main_jedis_port, 50000)
    static liveRedis = new Jedis(live_jedis_host, live_jedis_port, 50000)
    static userRedis = new Jedis(user_jedis_host, user_jedis_port, 50000)

    final static String KEY = "wJfSNrsVM1HoU5zx8PY2OzFzr3N8dIyt"
    static users = mongo.getDB("xy").getCollection("users");
    static rooms = mongo.getDB("xy").getCollection("rooms");
    static xy_users = mongo.getDB("xy_user").getCollection("users");
    static final Integer familyId = 1203503
    static Map<Integer,String> tokens = new HashMap<>();

    static List<Integer> robots = [1202354];

    static void main(String[] args) {
        goRobot()
    }

    static goRobot(){
        getToken(robots)
        tokens.each {Integer uid, String token ->
            //翻卡
            openCard(uid, token);
            //升级
            levelup(token);
            //捐献金币
            coin(token);
        }
    }

    static getToken(List robots){
        robots.each {Integer id ->
            def user = xy_users.findOne($$("mm_no":id.toString()), $$(token:1));
            tokens.put(id, user.get('token') as String);
        }
    }
    static openCard(Integer userId, String token){
        String responseContent = new URL("${api_url}card/list/${token}").getText("utf-8")
        def jsonSlurper = new JsonSlurper()
        Map result = jsonSlurper.parseText(responseContent) as Map
        Map data = result.get('data') as Map
        List cards =  data.get('cards') as List
        cards.each {Map card ->
            String sign = MD5("${userId}${card['_id']}${card['timestamp']}${KEY}".toString())
            String api_req = "${api_url}card/open/${token}/${card['_id']}?s=${sign}".toString()
            println new URL(api_req).getText("utf-8")
        }

    }

    static levelup(String token){
        println new URL("${api_url}user/level_up/${token}").getText("utf-8")
    }

    static coin(String token){
        Long coin = RandomUtils.nextInt(10000)
        println new URL("${api_url}familygame/donate_coin/${token}/${familyId}?coin=${coin}").getText("utf-8")
    }

    public static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    public static BasicDBObject $$(Map map) {
        return new BasicDBObject(map);
    }

    public static String MD5(String s) {
        def hexDigits =  ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'] as char[];

        try {
            byte[] btInput = s.getBytes();
            // 获得MD5摘要算法的 MessageDigest 对象
            MessageDigest mdInst = MessageDigest.getInstance("MD5");
            // 使用指定的字节更新摘要
            mdInst.update(btInput);
            // 获得密文
            byte[] md = mdInst.digest();
            // 把密文转换成十六进制的字符串形式
            int j = md.length;
            def str = new char[j * 2];
            int k = 0;
            for (int i = 0; i < j; i++) {
                byte byte0 = md[i];
                str[k++] = hexDigits[byte0 >>> 4 & 0xf];
                str[k++] = hexDigits[byte0 & 0xf];
            }
            return new String(str);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}



