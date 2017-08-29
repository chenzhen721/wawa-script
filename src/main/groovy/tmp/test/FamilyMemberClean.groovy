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
 * 家族人数清理
 */

class FamilyMemberClean{
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

    static users = mongo.getDB("xy").getCollection("users");
    static familys = mongo.getDB("xy_family").getCollection("familys");
    static rooms = mongo.getDB("xy").getCollection("rooms");
    static xy_users = mongo.getDB("xy_user").getCollection("users");
    static Map<Integer,String> tokens = new HashMap<>();
    static DAY_MILLON = 24 * 3600 * 1000L
    static long zeroMill = new Date().clearTime().getTime()
    static Long yesTday = zeroMill - 2*DAY_MILLON
    static Map<Integer,Integer> familyList = [1202676:1202413,1202435:1201194,1201115:1201085,1202678:1202416,1202680:1202425];
    //static Map<Integer,Integer> familyList = [1202435:1201194];

    static void main(String[] args) {
        goClean()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')} ".toString()
    }

    static goClean(){
        getToken(familyList.values().toList())
        familyList.each {Integer fid, Integer uid ->
            println "fid:${fid}  uid: ${uid}  token: ${tokens.get(uid)}"
            List<Integer> userList = getUsers(fid);
            println "${userList}"
            userList.each {Integer kickUid ->
                kickFamily(tokens.get(uid), kickUid);
            }
        };
    }

    static List<Integer> getUsers(Integer fid){
        Integer memberCount = familys.findOne(fid).get('member_count') as Integer
        Integer count = memberCount-200;
        println "clean count : ${count}"
        List<Integer> userList = null;
        if(count > 0){
            def usersNolongs = users.find($$("family.family_id":fid, "family.family_priv": 4, last_login:[$lt:yesTday])).limit(count).sort($$(last_login:1)).toArray()
            userList = usersNolongs.collect{return it['_id'] as Integer}
        }
        return userList;
    }

    static kickFamily(String token, Integer userId){
        new URL("${api_url}/member/kick/${token}/${userId}").getText("utf-8")

    }
    static getToken(List robots){
        robots.each {Integer id ->
            def user = xy_users.findOne($$("mm_no":id.toString()), $$(token:1));
            tokens.put(id, user.get('token') as String);
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



