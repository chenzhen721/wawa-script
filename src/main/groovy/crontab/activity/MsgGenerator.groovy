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
 * 用户抓中娃娃推送好友红包
 */
class MsgGenerator {

    static Properties props = null;
    static String profilepath = "/empty/crontab/db.properties";
    static mongo = new Mongo(new MongoURI(getProperties('mongo.uri', 'mongodb://192.168.31.249:10000/?w=1') as String))
    static DBCollection invitor_logs = mongo.getDB('xylog').getCollection('invitor_logs')
    static DBCollection catch_record = mongo.getDB('xy_catch').getCollection('catch_record')
    static DBCollection weixin_msg = mongo.getDB('xy_union').getCollection('weixin_msgs')
    static DBCollection red_packets = mongo.getDB('xy_activity').getCollection('red_packets')
    static DBCollection users = mongo.getDB('xy').getCollection("users")
    static DBCollection xy_users = mongo.getDB('xy_user').getCollection('users')
    static String jedis_host = getProperties("main_jedis_host", "192.168.31.249")
    static Integer main_jedis_port = getProperties("main_jedis_port", 6379) as Integer
    static mainRedis = new Jedis(jedis_host, main_jedis_port)
    static String LAST_SCAN_REDIS_KEY = "last:scan:catch:record:time"
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

    static void main(String[] args) {
        Long begin = System.currentTimeMillis()
        //genenrate()
        test()
        println "${MsgGenerator.class.getSimpleName()}:${new Date().format('yyyy-MM-dd HH:mm:ss')}: finish cost ${System.currentTimeMillis() - begin} ms"
    }
    static List<Integer> test_ids = [1202923,1352463]
    static Long per_end =  System.currentTimeMillis();
    static Long per_begin =  System.currentTimeMillis();
    static void genenrate(){
        per_end = System.currentTimeMillis();
        per_begin = ((mainRedis.getSet(LAST_SCAN_REDIS_KEY, per_end.toString()) ?: per_end) as Long)
        println "from :${new Date(per_begin).format('yyyy-MM-dd HH:mm:ss')}, to : ${new Date(per_end).format('yyyy-MM-dd HH:mm:ss')} ms"
    }

    static test(){
        test_ids.each {Integer userId ->
            pushMsg2Queue(userId, new ToyExpireTemplate())
            pushMsg2Queue(userId, new PointsExpireTemplate())
            pushMsg2Queue(userId, new InviterTemplate())
            pushMsg2Queue(userId, new DeliverTemplate())
            pushMsg2Queue(userId, new ToyRenewTemplate())
        }
    }

    //扫描最近抓中娃娃的用户
    static List<Integer> scanUserFromCatchRecord(){
        def List = catch_record.distinct("user_id", $$(status:true, timestamp:[$gt:per_begin, $lt:per_end]))
        return List;
    }

    //推送消息到消息队列待发送
    static pushMsg2Queue(Integer to_uid, WxTemplate wxTemplate){
        //获取用户appid和openid
        def user = xy_users.findOne($$(mm_no:to_uid.toString()), $$(weixin:1))
        println user
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
                    template:template, is_send:0, next_fire:time)
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
    购物车商品过期提醒
     {{first.DATA}}
     过期商品：{{keyword1.DATA}}
     剩余时间：{{keyword2.DATA}}
     {{remark.DATA}}
 */
class ToyExpireTemplate extends WxTemplate{
    static Map<String,String> template_ids = ['wx45d43a50adf5a470':'64GFNFZVbdvpCT0G5BOBIMPOYkypMSisKkuujc9Cacs', 'wxf64f0972d4922815':'P1nqV2mcWKlNLpCa-IS3A6hpGTiOq_N6cCLC5eyRxE0']

    public ToyExpireTemplate(){
        this.path = '11';
        this.data["first"] = ['value':"亲爱的XXX，您抓到的娃娃要过期啦",'color':'#173177']
        this.data["keyword1"] = ['value':"娃娃XX",'color':'#173177']
        this.data["keyword2"] = ['value':"X天",'color':'#173177']
        this.data["remark"] = ['value':"过期后娃娃将从背包中消失，点击详情立即申请邮寄或者兑换成积分~~",'color':'#173177']
    }

    public String getTemplateId(String appId){
        return template_ids[appId]
    }
}
/**
 *积分过期提醒
 {{first.DATA}}
 {{FieldName.DATA}}:{{Account.DATA}}
 {{change.DATA}}积分:{{CreditChange.DATA}}
 积分余额:{{CreditTotal.DATA}}
 {{Remark.DATA}}
 */
class PointsExpireTemplate extends WxTemplate{
    static Map<String,String> template_ids = ['wx45d43a50adf5a470':'FKcFyAqMOloOZgxkVleI6GI5eWBLKX0ujqcfLT0uLt0', 'wxf64f0972d4922815':'N73mxREncvMZJfao16nfMtS0nPwZJ0l7MhTPwoGq2fY']

    public PointsExpireTemplate(){
        this.path = '22';
        this.data["first"] = ['value':"您的积分余额已满足兑换条件咯~",'color':'#173177']
        this.data["FieldName"] = ['value':"",'color':'#173177']
        this.data["Account"] = ['value':"",'color':'#173177']
        this.data["change"] = ['value':"可兑换",'color':'#173177']
        this.data["CreditChange"] = ['value':"1000积分兑换30钻石，2000积分兑换60钻石，4000积分兑换120钻石，8000积分兑换240钻石",'color':'#173177']
        this.data["CreditTotal"] = ['value':"199积分",'color':'#173177']
        this.data["Remark"] = ['value':"点击详情，立即兑换积分！！",'color':'#173177']
    }

    public String getTemplateId(String appId){
        return template_ids[appId]
    }
}

/**
 *  邀请的好友注册时获得钻石
 *
 *   {{first.DATA}}
     注册用户：{{keyword1.DATA}}
     注册时间：{{keyword2.DATA}}
     注册来源：{{keyword3.DATA}}
     {{remark.DATA}}
 */
class InviterTemplate extends WxTemplate{
    static Map<String,String> template_ids = ['wx45d43a50adf5a470':'9Szzu0vCp1XDz8mS51BM8BClv4XeQuBUXJ-CBvGbgCM', 'wxf64f0972d4922815':'YQa06vsqhXGTio3jNpSKdPdWTInMJH1Aizpe7HjeUh0']

    public InviterTemplate(){
        this.path = '33';
        this.data["first"] = ['value':"XXX，您的好友通过您的邀请，加入阿喵抓娃娃，您又获得20钻石！",'color':'#173177']
        this.data["keyword1"] = ['value':"xxx（被邀请的用户昵称）",'color':'#173177']
        this.data["keyword2"] = ['value':"xxxx (注册时间)",'color':'#173177']
        this.data["keyword3"] = ['value':"您分享的二维码",'color':'#173177']
        this.data["remark"] = ['value':"您在阿喵抓娃娃的好友队伍越来越强大了哦！点击详情 邀请更多好友一起抓娃娃吧~",'color':'#173177']
    }

    public String getTemplateId(String appId){
        return template_ids[appId]
    }
}

/**
 * 快递已发出
 * {{first.DATA}}
 产品名称：{{keyword1.DATA}}
 快递公司：{{keyword2.DATA}}
 运单号码：{{keyword3.DATA}}
 收货地址：{{keyword4.DATA}}
 {{remark.DATA}}
 */
class DeliverTemplate extends WxTemplate{
    static Map<String,String> template_ids = ['wx45d43a50adf5a470':'JoEnK4uR1tGlKpFNwWaqHDw4hVSMu5qDwxuHNf93gwk', 'wxf64f0972d4922815':'DKsQMNCC8CkwDcaCoZaQU2wyMTpEUiuYfe_5LZ3URpE']
    public DeliverTemplate(){
        this.path = '44';
        this.data["first"] = ['value':"亲爱的xxx，您申请的商品已经寄出，请注意查收！",'color':'#173177']
        this.data["keyword1"] = ['value':"产品名称",'color':'#173177']
        this.data["keyword2"] = ['value':"快递公司",'color':'#173177']
        this.data["keyword3"] = ['value':"运单号码",'color':'#173177']
        this.data["keyword3"] = ['value':"收货地址",'color':'#173177']
        this.data["remark"] = ['value':"阿喵抓娃娃，更多新品已上线，点击“详情” 快来抓我吧~",'color':'#173177']
    }
    public String getTemplateId(String appId){
        return template_ids[appId]
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
    public ToyRenewTemplate(){
        this.data["first"] = ['value':"XXX，新娃娃已经到货了。",'color':'#173177']
        this.data["keyword1"] = ['value':"暂无",'color':'#173177']
        this.data["keyword2"] = ['value':"XXX 娃娃名字",'color':'#173177']
        this.data["remark"] = ['value':"点击详情，立即查看新娃娃",'color':'#173177']
        this.path = '55';
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
    protected static Map<String,String> domain_ids = ['wx45d43a50adf5a470':'http://www.17laihou.com/', 'wxf64f0972d4922815':'http://aochu.17laihou.com/']

    public String getId(){return this.id}
    public String getUrl(){return this.url}
    public Map getData(){return this.data}
    public Map generate(String appId){
        return ['id': this.getTemplateId(appId), url:domain_ids[appId] + path, data:this.getData()];
    }
    protected abstract String getTemplateId(String appId);
}