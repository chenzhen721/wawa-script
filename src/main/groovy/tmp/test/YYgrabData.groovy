#!/usr/bin/env groovy
import com.mongodb.DBObject
@GrabResolver(name = 'restlet', root = 'http://192.168.31.253:8081/nexus/content/groups/public')
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
        @Grab('org.apache.httpcomponents:httpclient:4.2.5'),
])
import com.mongodb.Mongo
import com.mongodb.MongoURI
import com.mongodb.BasicDBObject
import groovy.json.JsonOutput
import org.apache.http.HttpEntity
import org.apache.http.HttpResponse
import org.apache.http.HttpStatus
import org.apache.http.NameValuePair
import org.apache.http.StatusLine
import org.apache.http.client.HttpClient
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.message.BasicNameValuePair
import org.apache.http.params.HttpConnectionParams
import org.apache.http.params.HttpParams
import org.apache.http.util.EntityUtils

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

    static void main(String[] args) {
        Long cur = System.currentTimeMillis()
/*        println "yy  static >>>>>>>>>>>>>"
        59.times {
            staticYY();
            Thread.sleep(1000l);
        }*/
        5000.times {
            staticTiantian();
            Thread.sleep(100l);
        }
        println "room counts : ${roomCounts.size()}";
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')} ".toString()
    }

    static String redisRoomKey = "yy:room:live"
    static String redisKey = "${new Date().format('yyyy-MM-dd')}:yy:total".toString()
    static Set<String> roomCounts = new HashSet<>(1000);
    static void staticYY(){
        String responseContent = new URL("http://data.3g.yy.com/play/assemble/crane").getText("utf-8")
        def jsonSlurper = new JsonSlurper()
        Map result = jsonSlurper.parseText(responseContent) as Map
        def modules = (result.get("data") as Map).get("modules") as List
        def data =  (modules[0] as Map).get('data') as List
        Map<String, String> counts = mainRedis.hgetAll(redisRoomKey)
        //println counts
        println "mechine size : ${counts.size()}";
        data.each {
            Integer linkMic = it["linkMic"] as Integer
            String uid = it["uid"] as String
            if(linkMic.equals(1)){
                Integer count = (counts.get(uid) ?: 0)as Integer
                if(count == 0){//如果1秒钟之前为0则为新上用户
                    mainRedis.incr(redisKey)
                }
            }
            mainRedis.hset(redisRoomKey, uid.toString(), linkMic.toString())
        }

        println "${new Date().format('yyyy-MM-dd HH:mm')} total : " + mainRedis.get(redisKey)
        println "${new Date().format('yyyy-MM-dd HH:mm')} total : " + mainRedis.get(redisKey)
    }

    static void staticTiantian(){
        postData("http://wwj-new.same.com/api/v1/zone/change", null)
        Map result = postData("http://wwj-new.same.com/api/v1/room/list", null) as Map
        def rooms = (result.get("data") as Map).get("rooms") as List
        rooms.each {
            roomCounts.add(it["machine_token"] as String)
        }
    }

    public static Object postData(String POST_URL, Map params) {
        String responseContent = "";
        HttpPost httpPost = new HttpPost(POST_URL);
        if (params != null) {
            List<NameValuePair> nvps = new ArrayList<NameValuePair>();
            Set<Map.Entry<String, String>> paramEntrys = params.entrySet();
            for (Map.Entry<String, String> entry : paramEntrys) {
                nvps.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
            }
            httpPost.setEntity(new UrlEncodedFormEntity(nvps, "utf-8"));
        }

        httpPost.setHeader("Authorization", "7f6e9242f5cd40756e155943e4f785fb4fba2b09");

        HttpClient httpClient = getHttpClient()

        HttpResponse response = httpClient.execute(httpPost);
        StatusLine status = response.getStatusLine();
        if (status.getStatusCode() >= HttpStatus.SC_MULTIPLE_CHOICES) {
            System.out.printf("Did not receive successful HTTP response: status code = {}, status message = {}", status.getStatusCode(), status.getReasonPhrase());
            httpPost.abort();
        }

        HttpEntity entity = response.getEntity();
        if (entity != null) {
            responseContent = EntityUtils.toString(entity, "utf-8");
            def jsonSlurper = new JsonSlurper()
            def result = jsonSlurper.parseText(responseContent)
            EntityUtils.consume(entity);
            return result
        } else {
            System.out.printf("Http entity is null! request url is {},response status is {}", POST_URL, response.getStatusLine());
        }
        httpClient.getConnectionManager().shutdown()
    }

    /**
     * 获取httpClient
     * @return
     */
    def static HttpClient getHttpClient() {
        HttpClient httpClient = new DefaultHttpClient();
        HttpParams httpParams = httpClient.getParams();
        HttpConnectionParams.setSoTimeout(httpParams, 10 * 1000);
        HttpConnectionParams.setConnectionTimeout(httpParams, 10 * 1000);
        return httpClient
    }

    public static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    public static BasicDBObject $$(Map map) {
        return new BasicDBObject(map);
    }
}



