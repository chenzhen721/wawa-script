#!/usr/bin/env groovy
package crontab.st

//@GrabResolver(name = 'restlet', root = 'http://192.168.31.253:8081/nexus/content/groups/public')
@GrabResolver(name = 'restlet', root = 'http://210.22.151.242:8081/nexus/content/groups/public')
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
        @Grab('com.ttpod:https-util:1.0'),
        @Grab('org.apache.httpcomponents:httpclient:4.2.5')

])
import com.mongodb.BasicDBObject
import com.mongodb.DBCollection
import com.mongodb.DBCursor
import com.mongodb.DBObject
import com.mongodb.Mongo
import com.mongodb.MongoURI
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.json.StringEscapeUtils
import org.apache.commons.lang.StringUtils
import org.apache.http.HttpEntity
import org.apache.http.HttpResponse
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.params.HttpConnectionParams
import org.apache.http.params.HttpParams
import org.apache.http.util.EntityUtils
import redis.clients.jedis.Jedis

import java.text.SimpleDateFormat
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

class WeixinMessage {

    static Properties props = null;
    static String profilepath = "/empty/crontab/db.properties";
    static mongo = new Mongo(new MongoURI(getProperties('mongo.uri', 'mongodb://192.168.31.249:10000/?w=1') as String))

    static DBCollection weixin_msg = mongo.getDB('xy_union').getCollection('weixin_msgs')
    static DBCollection users = mongo.getDB('xy_user').getCollection('users')

    static String jedis_host = getProperties("main_jedis_host", "192.168.31.249")
    static Integer main_jedis_port = getProperties("main_jedis_port", 6379) as Integer
    static mainRedis = new Jedis(jedis_host, main_jedis_port)

    public static Map<String,String> APP_ID_SECRETS = ['wx87f81569b7e4b5f6':'8421fd4781b1c29077c2e82e71ce3d2a', 'wxf64f0972d4922815':'fbf4fd32c00a82d5cbe5161c5e699a0e']
    public static Map<String,String> APP_ID_TOKENS = new HashMap<>();

    static String WEIXIN_URL = 'https://api.weixin.qq.com/cgi-bin/'
    public static Integer requestCount = 0
    public static Integer successCount = 0
    // 过期时间
    static final Long MIS_FIRE = 60 * 60 * 1000
    static final Long DAY_MILLION = 24 * 60 * 60 * 1000
    static Map errorCode = [:]

    //test
    static List<String> openIds = ["ok2Ip1hYtZOjKzacYzr06gskuNcs","ok2Ip1rk-Fx4PdJSdH25zKObb_oU"];
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

    static String getAccessRedisKey(String appId){
        return  "weixin:${appId}:token".toString()
    }
    static String redis_lock_key = 'WeixinMessage:redis:lock'
    public static CountDownLatch downLatch = new CountDownLatch(0)

    static final ThreadPoolExecutor threadPool =
                        new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors() * 2, 200,
                                    60L, TimeUnit.SECONDS,
                                        new LinkedBlockingQueue<Runnable>(1000)) ;

    static void main(String[] args) {
        Long begin = System.currentTimeMillis()
        if(mainRedis.setnx(redis_lock_key, begin.toString()) == 1){
            try{
                mainRedis.expire(redis_lock_key, 60 * 60 * 1000)
                sendMessage()
                downLatch.await();
                println "${WeixinMessage.class.getSimpleName()}:${new Date().format('yyyy-MM-dd HH:mm:ss')}: total ${requestCount} , success ${successCount} " +
                        "fail ${requestCount - successCount} finish cost ${System.currentTimeMillis() - begin} ms"
            }catch (Exception e){
                println e
            }finally{
                mainRedis.del(redis_lock_key)
            }
        }else{
            println "${WeixinMessage.class.getSimpleName()}:${new Date().format('yyyy-MM-dd HH:mm:ss')}: already running..........."
        }
        //testTemplateMsg();
    }

    static void sendMessage(){
        Long now = System.currentTimeMillis()
        def msgs = weixin_msg.find($$(is_send:0,'next_fire': [$lte: now])).sort($$(next_fire:-1)).limit(1000).toArray()
        println "msgs size : ${msgs.size()}".toString()
        if(msgs.size() == 0) return;
        //初始化微信token
        initAccessToken();
        downLatch = new CountDownLatch(msgs.size())
        msgs.each {row ->
            final String appId = row['app_id'] as String
            final String openId = row['open_id'] as String
            final String _id = row['_id'] as String
            threadPool.execute(new Runnable() {
                @Override
                void run() {
                    try{
                        if(!APP_ID_SECRETS.containsKey(appId)) return

                        Integer error = -1
                        def template = row['template'] as DBObject
                        if(template != null){
                            error = sendTemplateMsg(template, openId, appId)
                        }
                        WeixinMessage.requestCount++
                        if (error == 0) {
                            WeixinMessage.successCount++
                            row['success_send'] = 1;
                        }
                        row['send_time'] = now;
                        row['is_send'] = 1;
                        weixin_msg.save(row)
                    }catch (Exception e){
                        println e;
                    }finally{
                        downLatch.countDown();
                    }
                }
            })
        }
        threadPool.shutdown();
    }

    /**
     * 发送图文信息
     * https://mp.weixin.qq.com/wiki?t=resource/res_main&id=mp1421140547&token=&lang=zh_CN
     * @param data
     */
    private static Integer sendCustomImageText(DBObject template, String openId, String appId) {
        String requestUrl = WEIXIN_URL + 'message/custom/send?access_token='.concat(getAccessToken(appId))
        List articleList = new ArrayList()
        articleList.add([title: template['title'], description: template['description'], picurl: template['pic_url'], url: template['url']])
        Map map = new HashMap()
        map.put('articles', articleList)
        Map params = new HashMap()
        params.put('sendObj', [msgtype: 'news', touser: openId, news: map])
        Map respMap = this.postWX('POST', requestUrl, params, appId)
        println "sendCustomImageText:" + respMap
        Integer error = respMap['errcode'] as Integer
        return error
    }

    /**
     * 发送文本信息
     * @param data
     */
    private static Integer sendCustomText(DBObject template, String openId, String appId) {
        String requestUrl = WEIXIN_URL + 'message/custom/send?access_token='.concat(getAccessToken(appId))
        Map map = new HashMap()
        map.put('touser', openId)
        map.put('msgtype', 'text')
        map.put('text', ['content': template['content']])
        Map params = new HashMap()
        params.put('sendObj', map)
        Map respMap = this.postWX('POST', requestUrl, params, appId)
        Integer error = respMap['errcode'] as Integer
        println "sendCustomText:" + respMap
        return error
    }

    static testTemplateMsg(){
        def template = $$('template_id':"pedgM13fkPvhs6E0LV6ew-8E9ociJuRpnrTso-TlZH4")
        template['url'] = "www.17laihou.com"
        def data = new HashMap();
        data['first'] = ['value':'哇!好友泽新抓中了娃娃，特来给您发钻石拉，赶紧抢了钻石去抓娃娃。','color':'#173177']
        data['keyword1'] = ['value':'100钻石','color':'#173177']
        data['keyword2'] = ['value':'2018年01月05日','color':'#173177']
        data['remark'] = ['value':'钻石数量有限，先到先得，速速去抢!','color':'#FF0000']
        template['data'] = data;
        openIds.each {String openid ->
            sendTemplateMsg(template, openid,'wx45d43a50adf5a470')
        }

    }
    /**
     * 发送模板消息
     * @param data
     */
    private static Integer sendTemplateMsg(DBObject template, String openId, String appId) {
        String requestUrl = WEIXIN_URL + 'message/template/send?access_token='.concat(getAccessToken(appId))
        Map map = new HashMap()
        map.put('touser', openId)
        map.put('template_id', template['id'])
        map.put('url', template['url'])
        map.put('data', template['data'] as Map)
        Map params = new HashMap()
        params.put('sendObj', map)
        Map respMap = postWX('POST', requestUrl, params, appId)
        Integer error = respMap['errcode'] as Integer
        println "sendTemplateMsg:" + respMap
        return error
    }

    /**
     * 获取token
     * 微信每日调用accessToken次数有限(2000次/日,有效7200秒)
     * @param req
     */
    static void initAccessToken() {
        APP_ID_SECRETS.keySet().each {String appId ->
            String access_token = mainRedis.get(getAccessRedisKey(appId))
            //println "${appId} : access_token:${access_token} ttl : ${mainRedis.ttl(getAccessRedisKey(appId))}".toString()
            if (access_token == null) {
                access_token = getTokenFromWeixin(appId)
            }
            if(access_token != null){
                APP_ID_TOKENS[appId] = access_token
            }
        }
    }

    static String getTokenFromWeixin(String appId){
        String requestUrl = WEIXIN_URL + 'token?grant_type=client_credential&appid=' + appId + '&secret=' + APP_ID_SECRETS[appId]
        println requestUrl
        Map respMap = postWX('GET', requestUrl, new HashMap(), appId)
        String errcode = respMap['errcode']
        String access_token = respMap['access_token']
        Integer expires = respMap['expires_in'] as Integer
        if(access_token != null){
            mainRedis.setex(getAccessRedisKey(appId), expires, access_token)
        }
        return access_token;
    }

    static String getAccessToken(String appId){
        String access_token =  APP_ID_TOKENS[appId]
        //println "getAccessToken : ${access_token}".toString()
        if(access_token == null){
            initAccessToken();
            return getAccessToken(appId);
        }
        return access_token;
    }

    /**
     * 请求微信服务端
     * @param postMethod
     * @param params
     * @param requestUrl
     * @param msg
     * @return
     */
    private static Map postWX(String postMethod, String requestUrl, Map params, String appId) {
        HttpResponse response
        Map map = new HashMap()
        if (postMethod == 'POST') {
            Object sendObj = params['sendObj']
            String sendContent = StringEscapeUtils.unescapeJavaScript(JsonOutput.toJson(sendObj))
            HttpPost httpPost = new HttpPost(requestUrl);
            StringEntity entity = new StringEntity(sendContent, ContentType.APPLICATION_JSON);
            entity.setContentEncoding("UTF-8");
            entity.setContentType("application/json");
            httpPost.setEntity(entity)
            HttpClient httpClient = getHttpClient()
            response = httpClient.execute(httpPost);

        } else {
            HttpGet httpGet = new HttpGet(requestUrl);
            HttpClient httpClient = getHttpClient()
            response = httpClient.execute(httpGet);
        }
        HttpEntity entity = response.getEntity();
        if (entity != null) {
            String responseContent = EntityUtils.toString(entity, "utf-8");
            def jsonSlurper = new JsonSlurper()
            map = jsonSlurper.parseText(responseContent) as Map
            println "post response  : ${map}".toString()
            // 如果是40001 token问题 则需要再次生成token,防止token正好过期
            Integer errcode = map.get('errcode') as Integer
            if (errcode == 40001) {
                getTokenFromWeixin(appId)
            }
            EntityUtils.consume(entity);
        } else {
            System.out.printf("Http entity is null! request url is {},response status is {}", requestUrl, response.getStatusLine());
        }
        httpClient.getConnectionManager().shutdown()
        return map
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
        return new BasicDBObject(map)
    }

}
