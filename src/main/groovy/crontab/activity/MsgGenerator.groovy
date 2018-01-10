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
    static DBCollection catch_success_logs = mongo.getDB('xylog').getCollection('catch_success_logs')
    static DBCollection apply_post_logs = mongo.getDB('xylog').getCollection('apply_post_logs')
    static DBCollection catch_record = mongo.getDB('xy_catch').getCollection('catch_record')
    static DBCollection weixin_msg = mongo.getDB('xy_union').getCollection('weixin_msgs')
    static DBCollection users = mongo.getDB('xy').getCollection("users")
    static DBCollection xy_users = mongo.getDB('xy_user').getCollection('users')
    static String jedis_host = getProperties("main_jedis_host", "192.168.31.249")
    static Integer main_jedis_port = getProperties("main_jedis_port", 6379) as Integer
    static mainRedis = new Jedis(jedis_host, main_jedis_port)

    static String LAST_SCAN_REDIS_KEY = "last:scan:catch:record:time"

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
    static Boolean test = Boolean.TRUE

    static void main(String[] args) {
        Long begin = System.currentTimeMillis()
        genenrate()
        //test()
        println "${MsgGenerator.class.getSimpleName()}:${new Date().format('yyyy-MM-dd HH:mm:ss')}: finish cost ${System.currentTimeMillis() - begin} ms" +
                "\n=========================================================================="
    }

    static Long per_end =  System.currentTimeMillis();
    static Long per_begin =  System.currentTimeMillis();
    static String redis_key = "weixin:msg:push:clock:"

    static void genenrate(){
        per_end = System.currentTimeMillis();
        per_begin = ((mainRedis.getSet(LAST_SCAN_REDIS_KEY, per_end.toString()) ?: per_end) as Long)
        println "from :${new Date(per_begin).format('yyyy-MM-dd HH:mm:ss')}, to : ${new Date(per_end).format('yyyy-MM-dd HH:mm:ss')} ms"

        Long now = System.currentTimeMillis()
        //过期娃娃提醒 剩三天 每天晚上6点
        if(getHourOfDay(now).equals(18)
                && mainRedis.sadd(redis_key+'toyexpire',new Date(now).format('yyyy-MM-dd')) == 1){
            Long begin = System.currentTimeMillis()
            scanToyExpire(3)
            scanToyExpire(1)
            println "每天晚上6点 scanToyExpire :${new Date().format('yyyy-MM-dd HH:mm:ss')}: finish cost ${System.currentTimeMillis() - begin} ms"
        }

        //用户剩余积分提醒 每周5晚上9点
        if(getDayOfWeek(now).equals(5) && getHourOfDay(now).equals(21)
                && mainRedis.sadd(redis_key+'userpoints',new Date(now).format('yyyy-MM-dd')) == 1){
            Long begin = System.currentTimeMillis()
            scanUserPoints()
            println "每周5晚上9点 scanUserPoints :${new Date().format('yyyy-MM-dd HH:mm:ss')}: finish cost ${System.currentTimeMillis() - begin} ms"
        }

        //邀请用户获得钻石
        Long begin = System.currentTimeMillis()
        scanUserInviterAward()
        println "scanUserInviterAward :${new Date().format('yyyy-MM-dd HH:mm:ss')}: finish cost ${System.currentTimeMillis() - begin} ms"

        //快递发货
        begin = System.currentTimeMillis()
        scanUserDeliverInfo()
        println "scanUserDeliverInfo :${new Date().format('yyyy-MM-dd HH:mm:ss')}: finish cost ${System.currentTimeMillis() - begin} ms"
    }

    static test(){
        test_ids.each {Integer userId ->
            pushMsg2Queue(userId, new ToyExpireTemplate('宁姐','海绵宝宝', 3))
            pushMsg2Queue(userId, new PointsExpireTemplate('宁姐', 200))
            pushMsg2Queue(userId, new InviterTemplate('宁姐','张三', 10, 1515554460000))
            pushMsg2Queue(userId, new DeliverTemplate('宁姐','海绵宝宝','申通快递','5210213132','上海闵行区合川大厦6m'))
            pushMsg2Queue(userId, new ToyRenewTemplate('宁姐','海绵宝宝'))
        }
    }


    //过期时间10天
    static Integer EXPIRE_DAYS = 10
    //扫描快过期娃娃
    static scanToyExpire(Integer expireDays){
        Long begin = zeroMill - ((EXPIRE_DAYS-expireDays) * DAY_MILLION)
        Long end = begin + DAY_MILLION
        List dolls = catch_success_logs.find($$(is_award: false, is_delete:false, post_type: 0, timestamp:[$gt:begin, $lt:end])).sort($$(user_id:-1)).toArray()
        Map<Integer,Set<String>> userOfDolls = new HashMap<>();
        dolls.each {DBObject doll ->
            Integer userId = doll['user_id'] as Integer
            Long timestamp = doll['timestamp'] as Long
            String toy = (doll['toy'] as Map)['name']
            Set<String> toys = userOfDolls.get(userId)
            if(toys == null){
                toys = new HashSet<>()
                userOfDolls.put(userId, toys)
            }
            toys.add(toy)
            println "userId : ${new Date(timestamp).format('yyyy-MM-dd HH:mm:ss')}".toString()
        }
        userOfDolls.each {Integer userId, Set<String> toys ->
            if(isTest(userId)){
                //println "${userId} ${getNickName(userId)}: ${toys.join(',')}, ${expireDays}天过期"
                pushMsg2Queue(userId, new ToyExpireTemplate(getNickName(userId),toys.join(','), expireDays))
            }
        }
    }

    //扫描用户剩余积分
    static void scanUserPoints(){
        def cur = users.find($$("bag.points.count":[$gte:1000]), $$(bag:1,nick_name:1)).batchSize(100)
        while (cur.hasNext()){
            def user = cur.next()
            Integer userId = user['_id'] as Integer
            String nick_name = user['nick_name'] as String
            Integer points = (user['bag'] as Map)['points']['count'] as Integer

            if(isTest(userId)) {
                //println "${userId} ${nick_name}: ${points}"
                pushMsg2Queue(userId, new PointsExpireTemplate(nick_name, points))
            }
        }
    }

    //扫描用户邀请好友加入获得钻石
    static void scanUserInviterAward(){
        def cur = invitor_logs.find($$(diamond_count :[$gt:0], "is_used":[$ne:true], timestamp:[$gt:per_begin, $lt:per_end]),
                                                        $$(diamond_count:1,user_id:1,invitor:1,timestamp:1)).batchSize(100)
        while (cur.hasNext()){
            def user = cur.next()
            Integer userId = user['invitor'] as Integer //邀请人
            Integer invitedUId = user['user_id'] as Integer //被邀请人
            Integer diamond_count = user['diamond_count'] as Integer
            Long timestamp = user['timestamp'] as Long
            if(isTest(userId)){
                //println " ${getNickName(userId)} 邀请了: ${getNickName(invitedUId)} 获得:${diamond_count}"
                pushMsg2Queue(userId, new InviterTemplate(getNickName(userId), getNickName(invitedUId), diamond_count, timestamp))
            }
        }
    }
    //扫描用户邮寄信息
    static void scanUserDeliverInfo(){
        def cur = apply_post_logs.find($$(post_type:3, "post_info":[$ne:null], push_time:[$gt:per_begin, $lt:per_end]),
                            $$(user_id:1,toys:1, post_info:1, timestamp:1,address_list:1)).batchSize(100)
        while (cur.hasNext()){
            def user = cur.next()
            Integer userId = user['user_id'] as Integer
            List toys = user['toys'] as List //被邀请人
            Map post_info = user['post_info'] as Map
            String shipping_name = post_info['shipping_name'] as String
            String shipping_no = post_info['shipping_no'] as String
            String address = user['address_list'] as String
            Long timestamp = user['push_time'] as Long
            Set<String> toySets = new HashSet<>()
            toySets.addAll(toys*.name)
            if(isTest(userId)){
                //println " ${getNickName(userId)} : ${address} 快递信息:${shipping_name} ${shipping_no}, 娃娃: ${toySets.join(',')}"
                pushMsg2Queue(userId, new DeliverTemplate(getNickName(userId),toySets.join(','),shipping_name,shipping_no,address))
            }
        }
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

    /**
     * 获得本周第几天
     * @param timestamp 传入时间戳
     * @return
     */
    public static Integer getDayOfWeek(Long timestamp){
        Calendar cal = Calendar.getInstance();
        if(timestamp != null){
            cal.setTimeInMillis(timestamp);
        }
        int dayOfWeek = cal.get(Calendar.DAY_OF_WEEK) - 1;
        if (dayOfWeek == 0)
            dayOfWeek = 7;

        return dayOfWeek;
    }

    /**
     * 获得当天的几点钟
     * @param timestamp 传入时间戳
     * @return
     */
    public static Integer getHourOfDay(Long timestamp){
        Calendar cal = Calendar.getInstance();
        if(timestamp != null){
            cal.setTimeInMillis(timestamp);
        }
        int hourOfDay = cal.get(Calendar.HOUR_OF_DAY);
     /*   if (hourOfDay == 0)
            hourOfDay = 24;*/

        return hourOfDay;
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

    public ToyExpireTemplate(String nickName, String toyName, Integer day){
        this.path = '/user/center';
        this.data["first"] = ['value':"亲爱的${nickName}，您抓到的娃娃要过期啦".toString(),'color':'#173177']
        this.data["keyword1"] = ['value':"${toyName}".toString(),'color':'#173177']
        this.data["keyword2"] = ['value':"${day}天".toString(),'color':'#173177']
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

    public PointsExpireTemplate(String nickName, Integer points){
        this.path = '/user/center';
        this.data["first"] = ['value':"",'color':'#173177']
        this.data["FieldName"] = ['value':nickName,'color':'#173177']
        this.data["Account"] = ['value':"您的积分余额已满足兑换条件咯",'color':'#173177']
        this.data["change"] = ['value':"可兑换",'color':'#173177']
        this.data["CreditChange"] = ['value':"1000积分兑换30钻石，2000积分兑换60钻石，4000积分兑换120钻石，8000积分兑换240钻石",'color':'#173177']
        this.data["CreditTotal"] = ['value':"${points}积分".toString(),'color':'#173177']
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

    public InviterTemplate(String nickName, String invitorName, Integer diamond, Long registerTime){
        this.path = '/user/center';
        this.data["first"] = ['value':"${nickName}，您的好友通过您的邀请，加入阿喵抓娃娃，您获得${diamond}钻石！".toString(),'color':'#173177']
        this.data["keyword1"] = ['value':"${invitorName}".toString(),'color':'#173177']
        this.data["keyword2"] = ['value':"${new Date(registerTime).format('yyyy-MM-dd')}".toString(),'color':'#173177']
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
    public DeliverTemplate(String nickName, String toyName, String comName, String no, String address){
        this.path = '';
        this.data["first"] = ['value':"亲爱的${nickName}，您申请的商品已经寄出，请注意查收！".toString(),'color':'#173177']
        this.data["keyword1"] = ['value':"${toyName}".toString(),'color':'#173177']
        this.data["keyword2"] = ['value':"${comName}".toString(),'color':'#173177']
        this.data["keyword3"] = ['value':"${no}".toString(),'color':'#173177']
        this.data["keyword4"] = ['value':"${address}".toString(),'color':'#173177']
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
    public ToyRenewTemplate(String nickName, String toyName){
        this.path = '';
        this.data["first"] = ['value':"${nickName}，新娃娃已经到货了。".toString(),'color':'#173177']
        this.data["keyword1"] = ['value':"暂无",'color':'#173177']
        this.data["keyword2"] = ['value':"${toyName}".toString(),'color':'#173177']
        this.data["remark"] = ['value':"点击详情，立即查看新娃娃",'color':'#173177']
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