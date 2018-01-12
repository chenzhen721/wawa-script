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
import org.apache.commons.codec.binary.Base64
import org.apache.commons.lang.StringUtils
import org.apache.commons.lang.math.RandomUtils
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

/**
 * 娃娃上新微信模板消息生成
 */
class ToyRenewMsgGenerator {

    static Properties props = null;
    static String profilepath = "/empty/crontab/db.properties";
    static mongo = new Mongo(new MongoURI(getProperties('mongo.uri', 'mongodb://192.168.31.249:10000/?w=1') as String))
    static DBCollection invitor_logs = mongo.getDB('xylog').getCollection('invitor_logs')
    static DBCollection catch_success_logs = mongo.getDB('xylog').getCollection('catch_success_logs')
    static DBCollection apply_post_logs = mongo.getDB('xylog').getCollection('apply_post_logs')
    static DBCollection catch_record = mongo.getDB('xy_catch').getCollection('catch_record')
    static DBCollection weixin_msg = mongo.getDB('xy_union').getCollection('weixin_msgs')
    static DBCollection users = mongo.getDB('xy').getCollection("users")
    static DBCollection xy_users = mongo.getDB('xy_user').getCollection('users')
    static String jedis_host = getProperties("main_jedis_host", "192.168.31.249")
    static Integer main_jedis_port = getProperties("main_jedis_port", 6379) as Integer
    static mainRedis = new Jedis(jedis_host, main_jedis_port)


    static long zeroMill = new Date().clearTime().getTime()
    static final Long DAY_MILLION = 24 * 60 * 60 * 1000

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

    static List<Integer> test_ids = [1352463, 1357719, 1202904, 1351843]
    static Boolean test = Boolean.FALSE

    static void main(String[] args) {
        Long begin = System.currentTimeMillis()
        testSend('王者荣耀梦奇布偶、王者荣耀288皮肤','点击详情查看，抓王者荣耀劲爆新品~');
        //scanUserRenewToys('王者荣耀梦奇布偶、王者荣耀288皮肤','点击详情查看，抓王者荣耀劲爆新品~')
        //removeDuplicate();
        println "${ToyRenewMsgGenerator.class.getSimpleName()}:${new Date().format('yyyy-MM-dd HH:mm:ss')}: finish cost ${System.currentTimeMillis() - begin} ms" +
                "\n=========================================================================="
    }

    static void removeDuplicate(){
        weixin_msg.find($$('template.data.remark.value':'点击详情查看，抓王者荣耀劲爆新品~')).toArray().each {data ->
            Integer userId = data['to_id'] as Integer
            String app_id = data['app_id'] as String
            def msg = weixin_msg.find($$('to_id':userId,app_id:app_id,'template.data.remark.value':'点击详情查看，抓王者荣耀劲爆新品~')).sort($$(is_send:1)).toArray()
            if(msg.size() > 1){
                weixin_msg.remove(msg[0])
            }
        }
    }

    static testSend(String toyName, String remark){
        test_ids.each {Integer userId ->
            pushMsg2Queue(userId, new ToyRenewTemplate(userId,'测试消息', toyName, remark),System.currentTimeMillis())
        }
    }

    //用户推送娃娃上新提醒
    static void scanUserRenewToys(String toyNames, String remark){
        def query = $$("last_login":[$gt: zeroMill - 15 * DAY_MILLION]);
        def cur = users.find(query, $$(nick_name:1)).batchSize(1000)
        Long total = users.count(query)
        Long total_send = 0;
        //Integer limit = ((total/1000)as Integer)+1;
        while (cur.hasNext()){
            def user = cur.next()
            Integer userId = user['_id'] as Integer
            String nick_name = user['nick_name'] as String
            if(isTest(userId)) {
                //println "${userId} ${nick_name}: ${points}"
                Long time = System.currentTimeMillis()
                Integer limit = ((total/1000)as Integer)+1; //每1000条15分钟一组
                Long next_fire = time + limit * 15 * 60 * 1000
                println "next_fire:${new Date(next_fire).format('yyyy-MM-dd HH:mm:ss')}"
                //pushMsg2Queue(userId, new ToyRenewTemplate(userId, nick_name, toyNames, remark), next_fire)
            }
        }
        println "total : ${total}".toString()
    }

    static String getNickName(Integer userId){
        return users.findOne($$(_id:userId), $$(nick_name:1))?.get('nick_name')
    }

    static Boolean isTest(Integer userId){
        if(test){
            return test_ids.contains(userId)
        }
        return Boolean.TRUE
    }
    //推送消息到消息队列待发送
    static pushMsg2Queue(Integer to_uid, WxTemplate wxTemplate, Long next_fire){
        //获取用户appid和openid
        def user = xy_users.findOne($$(mm_no:to_uid.toString()), $$(weixin:1))
        if(user == null) {
            return
        }
        def weixin = user['weixin'] as Map
        if(weixin == null || weixin.size() == 0){
            return
        }
        //TODO 多个公众号并行,临时处理
        weixin.each {String app_id, String open_id ->
            Long time = System.currentTimeMillis()
            def template = wxTemplate.generate(app_id)
            def msg = $$(_id: to_uid+'_'+app_id+'_'+time, to_id:to_uid,
                            app_id:app_id,open_id:open_id, timestamp:time,
                                template:template, is_send:0, next_fire:next_fire)
            weixin_msg.insert(msg)
        }

    }

    public static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    public static BasicDBObject $$(Map map) {
        return new BasicDBObject(map)
    }
}

/**
 * 娃娃上新提醒
 {{first.DATA}}
 订单号：{{keyword1.DATA}}
 商品：{{keyword2.DATA}}
 {{remark.DATA}}
 */
class ToyRenewTemplate extends WxTemplate{
    static Map<String,String> template_ids = ['wx45d43a50adf5a470':'ktMO_XUO3BeWrPeXiVTTx_gmTGOqTdelt4YpZA-gqRI', 'wxf64f0972d4922815':'eAZMbdfp072nYir240cyU1Pr4p1w3d6B5VtpIs71B2s']
    public ToyRenewTemplate(Integer uid, String nickName, String toyName, String remark){
        this.path = '';
        this.uid = uid;
        this.event_id = 'ToyRenew';
        this.data["first"] = ['value':"${nickName}，有新商品上线啦!".toString(),'color':'#173177']
        this.data["keyword1"] = ['value':"暂无",'color':'#173177']
        this.data["keyword2"] = ['value':"${toyName}".toString(),'color':'#173177']
        this.data["remark"] = ['value':remark,'color':'#173177']
    }
    public String getTemplateId(String appId){
        return template_ids[appId]
    }
}

abstract class WxTemplate{
    protected String id;
    protected String url;
    protected String path;
    protected Map data = new HashMap();
    protected String event_id;
    protected Integer uid;
    protected static final String STATIC_API_URL = "http://aochu-api.17laihou.com/statistic/weixin_template";
    protected static Map<String,String> DOMAIN_IDS = ['wx45d43a50adf5a470':'http://www.17laihou.com/', 'wxf64f0972d4922815':'http://aochu.17laihou.com/']

    public String getId(){return this.id}
    public String getUrl(){return this.url}
    public Map getData(){return this.data}

    public Map generate(String appId){
        String redirect = DOMAIN_IDS[appId] + path;
        String trace_id = "${event_id}_${uid}_${System.currentTimeMillis()}".toString()
        String url = STATIC_API_URL+"?event=${event_id}&uid=${uid}&trace_id=${trace_id}&redirect_url=${URLEncoder.encode(redirect, "UTF-8")}".toString()
        return ['id': this.getTemplateId(appId), url:url, data:this.getData()];
    }
    protected abstract String getTemplateId(String appId);
}