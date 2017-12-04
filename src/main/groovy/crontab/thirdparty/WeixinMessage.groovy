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

class WeixinMessage {

    static Properties props = null;
    static String profilepath = "/empty/crontab/db.properties";
//    static mongo = new Mongo(new MongoURI(getProperties('mongo.uri', 'mongodb://192.168.31.246:27017/?w=1') as String))
    static mongo = new Mongo(new MongoURI(getProperties('mongo.uri', 'mongodb://192.168.31.249:10000/?w=1') as String))
    static DBCollection union = mongo.getDB('xy_union').getCollection('weixin_template')
    static DBCollection customer = mongo.getDB('xy_union').getCollection('weixin_customer')

    static String jedis_host = getProperties("main_jedis_host", "192.168.31.249")
    static Integer main_jedis_port = getProperties("main_jedis_port", 6379) as Integer
    static mainRedis = new Jedis(jedis_host, main_jedis_port)

    static String APP_ID = 'wx45d43a50adf5a470'
    static String APP_SECRET = '40e8dc2daac9f04bfbac32a64eb6dfff'
    static String WEIXIN_ACCESS_TOKEN = 'weixin:' + APP_ID + ':token'
    static String WEIXIN_URL = 'https://api.weixin.qq.com/cgi-bin/'
    static Integer requestCount = 0
    static Integer successCount = 0
    // 过期时间
    static final Long MIS_FIRE = 60 * 60 * 1000
    static final Long DAY_MILLION = 60 * 60 * 24 * 1000
    static Map errorCode = [:]

    static List<String> openIds = ["ok2Ip1rVmmkypvPCHwGS3p6FDPUA","ok2Ip1hYtZOjKzacYzr06gskuNcs","ok2Ip1ke1-QxuicU9gpRqZ1tA10A","ok2Ip1pFWWphfjGld52014oXmEbc"];
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

    static void main(String[] args) {
        initErrorCode()
        Long begin = System.currentTimeMillis()
        sendMessage()
        println "${WeixinMessage.class.getSimpleName()}:${new Date().format('yyyy-MM-dd HH:mm:ss')}: finish cost ${System.currentTimeMillis() - begin} ms"
        /*
        后台配置模板批量发送消息
        sendCustomerMessage()
        Long end = System.currentTimeMillis()
        Long cost = end - begin
        println "${WeixinMessage.class.getSimpleName()}:${new Date().format('yyyy-MM-dd HH:mm:ss')}: total ${requestCount} , success ${successCount} " +
                "fail ${requestCount - successCount} finish cost ${cost} ms"
        */
    }

    static void sendMessage(){
        openIds.each {String openId->
            DBObject template = new BasicDBObject();
            template['title']="妈呀！ 你今天有一堆钻石马上到期咯"
            template['description']= '送你的一堆钻石即将过期了，速速领了去抓娃娃吧。 （测试，，，这仅仅是个测试！ 别当真）'
            template['pic_url']= 'https://mmbiz.qpic.cn/mmbiz_jpg/kGE3RectqDza7OuqZicNV2vrGr3dibBHIsUJUAG7kj0aZJpBgqm2sfWoTYEH9Azg97XnITNn0qFRtvubISdaYqCg/0?wx_fmt=jpeg'
            template['url']= '17laihou.com'
            template['content']= "哇！ 你有一堆钻石马上到期咯，速速领取了去抓娃娃吧。"
            //sendCustomText(template, openId)
            sendCustomImageText(template, openId)
        }

    }

    /**
     *
     * 发送微信客服消息
     * 图文消息
     * @param openIds
     * @param text
     */
    static void sendCustomerMessage() {
        Long now = System.currentTimeMillis()
        BasicDBObject searchExpression = new BasicDBObject('begin': [$lte: now], 'end': [$gte: now], msg_type: [$ne: null], 'next_fire': [$lte: now])
        DBCursor cursor = union.find(searchExpression)
        while (cursor.hasNext()) {
            def template = cursor.next()
            String fire = template['fire']
            Long nextFire = template['next_fire'] as Long
            String msgtype = template['msg_type']
            BasicDBObject customerExpression = new BasicDBObject('expires': [$gte: now])
            List<DBObject> customerList = customer.find(customerExpression).toArray()
            Integer error = 0
            // 判断是否过期
            Long misFire = nextFire + MIS_FIRE
            // 计算过期后 下一次触发时间
            if (now > misFire) {
                String time = new Date().format('yyyy-MM-dd') + ' ' + fire
                SimpleDateFormat sdf = new SimpleDateFormat('yyyy-MM-dd HH:mm:ss')
                Long tmp = sdf.parse(time).getTime()
                if (tmp < now) {
                    tmp += DAY_MILLION
                }
                // 设置下一次触发时间
                nextFire = tmp
                String id = template['_id']
                BasicDBObject modifyExpression = new BasicDBObject(next_fire: nextFire)
                union.findAndModify(new BasicDBObject('_id', id), null, null, false, new BasicDBObject('$set', modifyExpression), true, true)
            } else {
                for (DBObject customer : customerList) {
                    Long sendBegin = System.currentTimeMillis()
                    requestCount++
                    String openId = customer['_id']
                    if (msgtype == 'news') {
                        error = sendCustomImageText(template, openId)
                    } else {
                        error = sendCustomText(template, openId)
                    }
                    if (error == 0) {
                        successCount++
                        String id = template['_id']
                        BasicDBObject modifyExpression = new BasicDBObject(last_fire: now, next_fire: nextFire + DAY_MILLION)
                        union.findAndModify(new BasicDBObject('_id', id), null, null, false, new BasicDBObject('$set', modifyExpression), true, true)
                    } else {
                        Long sendFinish = System.currentTimeMillis()
                        Long sendCost = sendBegin - sendFinish
                        String msg = errorCode.get(error)
                        println "${WeixinMessage.class.getSimpleName()}:${new Date().format('yyyy-MM-dd HH:mm:ss')} msg : ${msg}, error : ${error}, cost : ${sendCost} ms"
                    }
                }
            }
        }
    }

    private static getNextFire() {

    }

    /**
     * 发送图文信息
     * https://mp.weixin.qq.com/wiki?t=resource/res_main&id=mp1421140547&token=&lang=zh_CN
     * @param data
     */
    private static Integer sendCustomImageText(DBObject template, String openId) {
        String requestUrl = WEIXIN_URL + 'message/custom/send?access_token='.concat(getAccessToken())
        List articleList = new ArrayList()
        articleList.add([title: template['title'], description: template['description'], picurl: template['pic_url'], url: template['url']])
        Map map = new HashMap()
        map.put('articles', articleList)
        Map params = new HashMap()
        params.put('sendObj', [msgtype: 'news', touser: openId, news: map])
        Map respMap = this.postWX('POST', requestUrl, params)
        println "sendCustomImageText:" + respMap
        Integer error = respMap['errcode'] as Integer
        return error
    }

    /**
     * 发送文本信息
     * @param data
     */
    private static Integer sendCustomText(DBObject template, String openId) {
        String requestUrl = WEIXIN_URL + 'message/custom/send?access_token='.concat(getAccessToken())
        Map map = new HashMap()
        map.put('touser', openId)
        map.put('msgtype', 'text')
        map.put('text', ['content': template['content']])
        Map params = new HashMap()
        params.put('sendObj', map)
        Map respMap = this.postWX('POST', requestUrl, params)
        Integer error = respMap['errcode'] as Integer
        println "sendCustomText:" + respMap
        return error
    }

    /**
     * 获取token
     * 微信每日调用accessToken次数有限(2000次/日,有效7200秒)
     * @param req
     */
    private static String getAccessToken() {
        String access_token = mainRedis.get(WEIXIN_ACCESS_TOKEN)
        String requestUrl = WEIXIN_URL + 'token'
        if (access_token == null) {
            requestUrl += '?grant_type=client_credential&appid=' + APP_ID + '&secret=' + APP_SECRET
            Map respMap = this.postWX('GET', requestUrl, new HashMap())
            println respMap
            String errcode = respMap['errcode']
            access_token = respMap['access_token']
            Integer expires = respMap['expires_in'] as Integer
            mainRedis.set(WEIXIN_ACCESS_TOKEN, access_token)
            mainRedis.expire(WEIXIN_ACCESS_TOKEN, expires)
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
    private static Map postWX(String postMethod, String requestUrl, Map params) {
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
                mainRedis.del(WEIXIN_ACCESS_TOKEN)
                requestUrl = requestUrl.substring(0, requestUrl.indexOf('access_token'))
                requestUrl = requestUrl.concat('access_token=' + getAccessToken())
                return postWX(postMethod, requestUrl, params)
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
     * 我写这段代码的时候心里想疯
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
