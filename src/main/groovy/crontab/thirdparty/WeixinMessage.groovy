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

    static Map<String,String> APP_ID_SECRETS = ['wx45d43a50adf5a470':'40e8dc2daac9f04bfbac32a64eb6dfff', 'wxf64f0972d4922815':'fbf4fd32c00a82d5cbe5161c5e699a0e']
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
            new ThreadPoolExecutor(1, 10,
                    60L, TimeUnit.SECONDS,
                    new LinkedBlockingQueue<Runnable>()) ;

    static void main(String[] args) {
        Long begin = System.currentTimeMillis()
        if(mainRedis.setnx(redis_lock_key, begin.toString()) == 1){
            mainRedis.expire(redis_lock_key, 60 * 60 * 1000)
            sendMessage()
            downLatch.await();
            mainRedis.del(redis_lock_key)
            println "${WeixinMessage.class.getSimpleName()}:${new Date().format('yyyy-MM-dd HH:mm:ss')}: total ${requestCount} , success ${successCount} " +
                    "fail ${requestCount - successCount} finish cost ${System.currentTimeMillis() - begin} ms"
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
        initErrorCode();
        downLatch = new CountDownLatch(msgs.size())
        msgs.each {row ->
            final String appId = row['app_id'] as String
            final String openId = row['open_id'] as String
            final String _id = row['_id'] as String
            threadPool.execute(new Runnable() {
                @Override
                void run() {
                    try{
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
                        println "Exception :"+e;
                    }finally{
                        downLatch.countDown();
                    }
                }
            })
        }
        threadPool.shutdown();
/*
        def cur = weixin_msg.find($$(is_send : 0, app_id:[$ne:null], openId:[$ne:null], 'next_fire': [$lte: now])).batchSize(1000)
        while (cur.hasNext()) {
            final row = cur.next()
            final String appId = row['app_id'] as String
            final String openId = row['open_id'] as String
            if(openId == null || appId == null) return
            threadPool.execute(new Runnable() {
                @Override
                void run() {
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
                }
            })

        }
        threadPool.shutdown();
        */
    }

    static String getOpenIdByUid(String appId, Integer uid){
        String openId = null;
        def user = users.findOne($$(mm_no:uid.toString()), $$(weixin:1))
        if(user != null){
            def weixin = user['weixin'] as Map
            if(weixin != null && weixin.size() > 0){
                openId = weixin[appId]
            }
        }
        return openId
    }

    public static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    public static BasicDBObject $$(Map map) {
        return new BasicDBObject(map)
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
    static String getAccessToken(String appId) {
        String access_token = mainRedis.get(getAccessRedisKey(appId))
        if (access_token == null) {
            String requestUrl = WEIXIN_URL + 'token?grant_type=client_credential&appid=' + appId + '&secret=' + APP_ID_SECRETS[appId]
            println requestUrl
            Map respMap = postWX('GET', requestUrl, new HashMap(), appId)
            println respMap
            String errcode = respMap['errcode']
            access_token = respMap['access_token']
            Integer expires = respMap['expires_in'] as Integer
            if(access_token != null){
                mainRedis.setex(getAccessRedisKey(appId), expires, access_token)
            }
        }
        return access_token
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
            // 如果是40001 token问题 则需要再次生成token,防止token正好过期
            Integer errcode = map.get('errcode') as Integer
            if (errcode == 40001) {
                mainRedis.del(getAccessRedisKey(appId))
                requestUrl = requestUrl.substring(0, requestUrl.indexOf('access_token'))
                requestUrl = requestUrl.concat('access_token=' + getAccessToken(appId))
                return postWX(postMethod, requestUrl, params,appId)
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


    /**
     * @return
     */
    static initErrorCode() {
        errorCode.put(-1, '系统繁忙，此时请开发者稍候再试')
        errorCode.put(0, '请求成功')
        errorCode.put(40001, '获取access_token时AppSecret错误，或者access_token无效。请开发者认真比对 AppSecret的正确性，或查看是否正在为恰当的公众号调用接口')
        errorCode.put(40002, '不合法的凭证类型')
        errorCode.put(40003, '不合法的OpenID，请开发者确认OpenID(该用户)是否已关注公众号，或是否是其他公众 号的OpenID')
        errorCode.put(40004, '不合法的媒体文件类型')
        errorCode.put(40005, '不合法的文件类型')
        errorCode.put(40006, '不合法的文件大小')
        errorCode.put(40007, '不合法的媒体文件id')
        errorCode.put(40008, '不合法的消息类型')
        errorCode.put(40009, '不合法的图片文件大小')
        errorCode.put(40010, '不合法的语音文件大小')
        errorCode.put(40011, '不合法的视频文件大小')
        errorCode.put(40012, '不合法的缩略图文件大小')
        errorCode.put(40013, '不合法的AppID，请开发者检查AppID的正确性，避免异常字符，注意大小写')
        errorCode.put(40014, '不合法的access_token，请开发者认真比对access_token的有效性(如是否过期)，或查 看是否正在为恰当的公众号调用接口')
        errorCode.put(40015, '不合法的菜单类型')
        errorCode.put(40016, '不合法的按钮个数')
        errorCode.put(40017, '不合法的按钮个数')
        errorCode.put(40018, '不合法的按钮名字长度')
        errorCode.put(40019, '不合法的按钮KEY长度')
        errorCode.put(40020, '不合法的按钮URL长度')
        errorCode.put(40021, '不合法的菜单版本号')
        errorCode.put(40022, '不合法的子菜单级数')
        errorCode.put(40023, '不合法的子菜单按钮个数')
        errorCode.put(40024, '不合法的子菜单按钮类型')
        errorCode.put(40025, '不合法的子菜单按钮名字长度')
        errorCode.put(40026, '不合法的子菜单按钮KEY长度')
        errorCode.put(40027, '不合法的子菜单按钮URL长度')
        errorCode.put(40028, '不合法的自定义菜单使用用户')
        errorCode.put(40029, '不合法的oauth_code')
        errorCode.put(40030, '不合法的refresh_token')
        errorCode.put(40031, '不合法的openid列表')
        errorCode.put(40032, '不合法的openid列表长度')
        errorCode.put(40033, '不合法的请求字符，不能包含\\uxxxx格式的字符')
        errorCode.put(40035, '不合法的参数')
        errorCode.put(40038, '不合法的请求格式')
        errorCode.put(40039, '不合法的URL长度')
        errorCode.put(40050, '不合法的分组id')
        errorCode.put(40051, '分组名字不合法')
        errorCode.put(41001, '缺少access_token参数')
        errorCode.put(41002, '缺少appid参数')
        errorCode.put(41003, '缺少refresh_token参数')
        errorCode.put(41004, '缺少secret参数')
        errorCode.put(41005, '缺少多媒体文件数据')
        errorCode.put(41006, '缺少media_id参数')
        errorCode.put(41007, '缺少子菜单数据')
        errorCode.put(41008, '缺少oauth code')
        errorCode.put(41009, '缺少openid')
        errorCode.put(42001, 'access_token超时，请检查access_token的有效期，请参考基础支持-获取access_token 中，对access_token的详细机制说明')
        errorCode.put(42002, 'refresh_token超时')
        errorCode.put(42003, 'oauth_code超时')
        errorCode.put(43001, '需要GET请求')
        errorCode.put(43002, '需要POST请求')
        errorCode.put(43003, '需要HTTPS请求')
        errorCode.put(43004, '需要接收者关注')
        errorCode.put(43005, '需要好友关系')
        errorCode.put(44001, '多媒体文件为空')
        errorCode.put(44002, 'POST的数据包为空')
        errorCode.put(44003, '图文消息内容为空')
        errorCode.put(44004, '文本消息内容为空')
        errorCode.put(45001, '多媒体文件大小超过限制')
        errorCode.put(45002, '消息内容超过限制')
        errorCode.put(45003, '标题字段超过限制')

        errorCode.put(45004, '描述字段超过限制')
        errorCode.put(45005, '链接字段超过限制')
        errorCode.put(45006, '图片链接字段超过限制')

        errorCode.put(45007, '语音播放时间超过限制')
        errorCode.put(45008, '图文消息超过限制')
        errorCode.put(45009, '接口调用超过限制')
        errorCode.put(45010, '创建菜单个数超过限制')
        errorCode.put(45015, '回复时间超过限制')
        errorCode.put(45016, '系统分组，不允许修改')
        errorCode.put(45017, '分组名字过长')
        errorCode.put(45018, '分组数量超过上限')
        errorCode.put(46001, '不存在媒体数据')
        errorCode.put(46002, '不存在的菜单版本')
        errorCode.put(46003, '不存在的菜单数据')
        errorCode.put(46004, '不存在的用户')
        errorCode.put(47001, '解析JSON/XML内容错误')
        errorCode.put(48001, 'api功能未授权，请确认公众号已获得该接口，可以在公众平台官网-开发者中心页中查看 接口权限')
        errorCode.put(50001, '用户未授权该api')
        errorCode.put(61451, '参数错误(invalid parameter)')
        errorCode.put(61452, '无效客服账号(invalid kf_account)')
        errorCode.put(61453, '客服帐号已存在(kf_account exsited)')
        errorCode.put(61454, '客服帐号名长度超过限制(仅允许10个英文字符，不包括@及@后的公众号的微信 号)(invalid kf_acount length)')
        errorCode.put(61455, '客服帐号名包含非法字符(仅允许英文+数字)(illegal character in kf_account)')
        errorCode.put(61456, '客服帐号个数超过限制(10个客服账号)(kf_account count exceeded)')
        errorCode.put(61457, '无效头像文件类型(invalid file type)')
        errorCode.put(61450, '系统错误(system error)')
    }

}
