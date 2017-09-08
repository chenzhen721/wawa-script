#!/usr/bin/env groovy
import com.mongodb.DBObject
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
    static familys = mongo.getDB("xy_family").getCollection("familys");
    static xy_users = mongo.getDB("xy_user").getCollection("users");
    static final Integer familyId = 1203503
    static String leaderToken;
    static final Integer leaderId = 1202354
    static Map<Integer,String> tokens = new HashMap<>();
    static Integer currHour = Calendar.getInstance().get(Calendar.HOUR_OF_DAY)
    static List<Integer> robots = [1202354,1202470,1202486,1202464,1202441,1202431,1202428,
                                   1202348,1201075,1201081,1201136,1201139,1201127,1201164,1201154
                                  ];

    static void main(String[] args) {
        goRobot()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')} ".toString()
    }

    static goRobot(){
        getToken(robots)
        leaderToken = tokens.get(leaderId)
        tokens.each {Integer uid, String token ->
            //申请家族
            //applyFamily(token);
            //翻卡
            openCard(uid, token);
            //升级
            levelup(token);
            //偷窃
            steal(token);
            //捐献金币
            coin(token);
        }
        initRank();
        build();
        ack();

    }

    static applyFamily(String token){
        new URL("${api_url}/member/apply/${token}/${familyId}").getText("utf-8")

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
            new URL(api_req).getText("utf-8")
        }

    }

    static levelup(String token){
        new URL("${api_url}user/level_up/${token}").getText("utf-8")
    }

    static steal(String token){
        def ids =  users.find($$("finance.coin_count":[$gt:50000]), $$(_id:1)).sort($$("finance.coin_count":-1)).limit(50)*._id
        Collections.shuffle(ids);
        ids.subList(0, 5).each {Integer id ->
            println "steal: "+new URL("${api_url}useritem/steal/${token}/3/${id}").getText("utf-8")
        }
    }

    static coin(String token){
        if(currHour >= 9 && currHour < 23) {
            Long coin = RandomUtils.nextInt(1200)
            if (coin > 300) {
                coin *= 10
                println "donate: " + new URL("${api_url}familygame/donate_coin/${token}/${familyId}?coin=${coin}").getText("utf-8")
            }
        }
    }

    static build(){
        (108..101).each { Integer ackId ->
            println new URL("${api_url}familygame/build_equipment/${leaderToken}/${ackId}").getText("utf-8")
        }
        (208..201).each{Integer defId ->
           println new URL("${api_url}familygame/build_equipment/${leaderToken}/${defId}").getText("utf-8")
        }
    }

    static ack(){
        if(currHour >= 10 && currHour < 22) {
            Integer count = familys.findOne(familyId).get("weapon_count") as Integer
            if(count > 8){
                (108..101).each{Integer ackId ->
                    Integer topFamilyId = top1Family()
                    if(topFamilyId != familyId ){
                        new URL("${api_url}familygame/ack/${leaderToken}/${ackId}/${topFamilyId}").getText("utf-8")
                        Thread.sleep(3*1000l);
                    }
                }
            }
        }
    }

    static String rediskey = "family:prestige:rank:list"
    static Integer top1Family(){
        Set<String> tops = mainRedis.zrevrange(rediskey, 0 ,0) as Set
        return tops[0] as Integer
    }

    static initRank(){
        familys.find($$(_id:[$gt:0]), $$(prestige:1)).toArray().each {DBObject data ->
            mainRedis.zadd(rediskey, data['prestige'] as Double, data["_id"] as String)
        }
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



