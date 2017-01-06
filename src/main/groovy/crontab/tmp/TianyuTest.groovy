#!/usr/bin/env groovy
package crontab.tmp

import com.mongodb.BasicDBObject
import com.mongodb.DBCollection
import com.mongodb.DBCursor
import com.mongodb.DBObject
@Grapes([
@Grab('org.mongodb:mongo-java-driver:2.14.2'),
@Grab('commons-lang:commons-lang:2.6'),
@Grab('redis.clients:jedis:2.1.0'),
@Grab(group = 'net.sf.json-lib', module = 'json-lib', version = '2.3', classifier = 'jdk15'),
]) import com.mongodb.Mongo
import com.mongodb.MongoURI
import groovy.json.JsonSlurper
import net.sf.json.JSONObject
import org.apache.commons.lang.StringUtils
import redis.clients.jedis.Jedis
import sun.misc.BASE64Encoder

import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import java.security.InvalidKeyException
import java.security.NoSuchAlgorithmException
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/**
 * 腾讯天御测试
 */
class TianyuTest {
    static Properties props = null;
    static String profilepath="/empty/crontab/db.properties";

    static getProperties(String key, Object defaultValue){
        try {
            if(props == null){
                props = new Properties();
                props.load(new FileInputStream(profilepath));
            }
        } catch (Exception e) {
            println e;
        }
        return props.get(key, defaultValue)
    }

    static final String jedis_host = getProperties("main_jedis_host", "192.168.31.249")
    static final String chat_jedis_host = getProperties("chat_jedis_host", "192.168.31.249")
    static final String live_jedis_host = getProperties("live_jedis_host", "192.168.31.249")
    static final String api_domain = getProperties("api.domain", "http://localhost:8080/")

    static final Integer main_jedis_port = getProperties("main_jedis_port",6379) as Integer
    static final Integer chat_jedis_port = getProperties("chat_jedis_port",6379) as Integer
    static final Integer live_jedis_port = getProperties("live_jedis_port",6379) as Integer

    static final String user_jedis_host = getProperties("user_jedis_host", "192.168.31.246")
    static final Integer user_jedis_port = getProperties("user_jedis_port",6379) as Integer
    static userRedis = new Jedis(user_jedis_host,user_jedis_port)


    static mongo  = new Mongo(new MongoURI(getProperties('mongo.uri','mongodb://192.168.31.249:27017/?w=1') as String))
    static users = mongo.getDB('xy').getCollection('users')
    static user_accounts = mongo.getDB('xy_user').getCollection('users')
    static day_login = mongo.getDB('xylog').getCollection('day_login')
    static lottery_logs = mongo.getDB('xylog').getCollection('lottery_logs')

    static mainRedis = new Jedis(jedis_host,main_jedis_port)


    static DAY_MILLON = 24 * 3600 * 1000L
    static WEEK_MILLON = 7 * DAY_MILLON
    private static final Integer TIME_OUT = 5 * 1000;

    static Map<Integer,Integer> result = new HashMap<>();
    /**
     *  accountType
     *   0：其他账号
         1：QQ开放帐号
         2：微信开放帐号
         4：手机账号
         6：手机动态码
         7：邮箱账号

     appId	可选	string	accountType是QQ或微信开放账号时，该参数必填，表示QQ或微信分配给给网站或应用的appId，用来唯一标识网站或应用
     WEIXIN_APP_ID = "wxc3074c6fb652a29a"
     QQ_APP_ID = "101118713"

     uid 必须	string	用户ID，accountType不同对应不同的用户ID。如果是QQ或微信用户则填入对应的openId
     * @param args
     */
    static void main(String[] args) { //待优化，可以到历史表查询记录
        long l = System.currentTimeMillis()
        test_users();
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${TianyuTest.class.getSimpleName()}, cost  ${System.currentTimeMillis() - l} ms"
    }

    static test_users(){
        Set<Integer> users = new HashSet();
        DBCursor cursor = lottery_logs.find($$(active_name : 'app_meme_luck_gift', award_name:'118'), $$(user_id:1, timestamp:1)).batchSize(10000)
        while (cursor.hasNext()) {
            def lottery = cursor.next()
            Integer uid = lottery['user_id'] as Integer
            if(users.add(uid)){
                Map<String,String> params = new HashMap<>();
                Long timestamp = lottery['timestamp'] as Long
                params['postTime'] = String.valueOf((timestamp / 1000) as Integer)
                wrapUserInfo(params, uid);
                test(uid, params);
            }

        }
        println result;
    }

    static void wrapUserInfo(Map<String,String> params, Integer uid){
        def login =  day_login.find($$(user_id : uid), $$(timestamp:1, ip : 1)).sort($$(timestamp : -1)).limit(1).toArray()
        params['ip'] = StringUtils.split(login[0]['ip'].toString(), ",")[0]
        params['loginTime'] = String.valueOf(((login[0]['timestamp'] as Long) / 1000) as Integer)
        def user = users.findOne($$(_id : uid), $$(mm_no:1))
        def account = user_accounts.findOne($$(mm_no:user['mm_no'].toString()))
        if(account){
            def mobile = account['mobile']
            def via = account['via']
            params['accountType'] = '0';
            params['uid'] = uid.toString();
            if(mobile){
                params['accountType'] = '4';
                params['uid'] = mobile;
            }
            if(via.equals("weixin")){
                params['accountType'] = '2';
                params['uid'] = account['weixin_openid'];
                params['appId'] = "wxc3074c6fb652a29a";

            }else if(via.equals("qq")){
                params['accountType'] = '1';
                params['uid'] = account['qq_openid'];
                params['appId'] = "101118713";
            }
            params['registerTime'] = String.valueOf(((account['regTime'] as Long) / 1000) as Integer)
        }
    }


    static test(Integer uid, Map<String,String> params){
        push_api(uid, params, GenURL_ActivityAntiRush.generateUrl(1, params['accountType'], params['uid'], params['ip'], params['postTime'], params['appId']))
        push_api(uid, params, GenURL_ActivityAntiRush.generateUrl(2, params['accountType'], params['uid'], params['ip'], params['registerTime'], params['appId']))
        push_api(uid, params, GenURL_ActivityAntiRush.generateUrl(3, params['accountType'], params['uid'], params['ip'],params['loginTime'], params['appId']))
        println 'sleep 10 sec ...'
        Thread.sleep(10000l)

    }

    static push_api(Integer uid, Map<String,String> params, String push_url){
        HttpURLConnection conn = null;
        try{
            conn = (HttpURLConnection)new URL(push_url).openConnection()
            conn.setRequestMethod("GET")
            conn.setDoOutput(true)
            conn.setConnectTimeout(TIME_OUT);
            conn.setReadTimeout(TIME_OUT);
            conn.connect()
            def jsonText = conn.getInputStream().getText("UTF-8")
            if(StringUtils.isEmpty(jsonText)) return
            def json = new JsonSlurper().parseText(jsonText)
            if((json['code'] as Integer) != 0){
                println 'user : ' + uid + ' ' + jsonText
                println "params: ${params}"
                return
            }
            if((json['level'] as Integer) > 0){
                println 'user : ' + uid + '  warning >>>>>>>>>>>>>>>>>>>>>>' + " result: "+ json
                println "params: ${params}"
                result.put(uid, json['level'] as Integer)
            }
        }catch (Exception e){
            println "staticWeek Exception : " + e;
        }finally{
            if (conn != null) {
                conn.disconnect();
                conn = null;
            }
        }
    }


    public static BasicDBObject $$(String key,Object value){
        return new BasicDBObject(key,value);
    }

    public static BasicDBObject $$(Map map){
        return new BasicDBObject(map);
    }

    static class GenURL_ActivityAntiRush{
        /* Basic request URL */
        private static final String URL = "csec.api.qcloud.com/v2/index.php";
        private static final String SECRET_ID = "AKIDp3roycBk2MXzLTFE23exe8A1yU1mPdG6";
        private static final String SECRET_KEY = "PcqXptzLuYFLbKudBNb6s16LZpbYdDOi";
        /**
         * 编码
         * @param bstr
         * @return String
         */
        private static String encode(byte[] bstr){
            return new BASE64Encoder().encode(bstr);
        }

        /* Signature algorithm using HMAC-SHA1 */
        public static String hmacSHA1(String key, String text) throws InvalidKeyException, NoSuchAlgorithmException
        {
            Mac mac = Mac.getInstance("HmacSHA1");
            mac.init(new SecretKeySpec(key.getBytes(), "HmacSHA1"));
            return encode(mac.doFinal(text.getBytes()));
        }

        /* Assemble query string */
        private static String makeQueryString(Map<String, String> args, String charset) throws UnsupportedEncodingException
        {
            String url = "";

            for (Map.Entry<String, String> entry : args.entrySet())
                url += entry.getKey() + "=" + (charset == null ? entry.getValue() : URLEncoder.encode(entry.getValue(), charset)) + "&";

            return url.substring(0, url.length()-1);
        }

        /* Generates an available URL */
        public static String makeURL(
                String method,
                String action,
                String region,
                String secretId,
                String secretKey,
                Map<String, String> args,
                String charset)
                throws InvalidKeyException, NoSuchAlgorithmException, UnsupportedEncodingException
        {
            SortedMap<String, String> arguments = new TreeMap<String, String>();

            /* Sort all parameters, then calculate signature */
            arguments.putAll(args);
            arguments.put("Nonce", String.valueOf((int)(Math.random() * 0x7fffffff)));
            arguments.put("Action", action);
            arguments.put("Region", region);
            arguments.put("SecretId", secretId);
            arguments.put("Timestamp", String.valueOf((System.currentTimeMillis() / 1000) as Integer));
            arguments.put("Signature", hmacSHA1(secretKey, String.format("%s%s?%s", method, URL, makeQueryString(arguments, null))));

            /* Assemble final request URL */
            return String.format("https://%s?%s", URL, makeQueryString(arguments, charset));
        }

        public static String generateUrl(Integer type, String accountType, String uid, String ip, String sec_time, String appId){


            Map<String, String> args = new TreeMap<String, String>();

            /* 帐号信息参数 */
            args.put("accountType", accountType);
            if(StringUtils.isNotEmpty(appId))
                args.put("appId", appId);
            args.put("uid", uid);
            String action = "";
            switch (type){
                case 1:
                    args.put("userIp", ip);
                    args.put("postTime", sec_time);
                    action = "ActivityAntiRush";
                    break;
                case 2:
                    args.put("registerIp", ip);
                    args.put("registerTime", sec_time);
                    action = "RegisterProtection";
                    break;
                case 3:
                    args.put("loginIp", ip);
                    args.put("loginTime", sec_time);
                    action = "LoginProtection";
                    break;
            }
            return makeURL("GET", action, "gz", SECRET_ID, SECRET_KEY, args, "utf-8");
        }
    }

}

