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
 * 微信模板消息生成
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
    static DBCollection red_packets = mongo.getDB('xy_activity').getCollection('red_packets')
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
    static Boolean test = Boolean.FALSE

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

    //开始扫描匹配的用户，推送微信模板消息
    static void genenrate(){
        //获取上次截止时间，作为本次扫描的时间段
        per_end = System.currentTimeMillis();
        per_begin = ((mainRedis.getSet(LAST_SCAN_REDIS_KEY, per_end.toString()) ?: per_end) as Long)
        println "from :${new Date(per_begin).format('yyyy-MM-dd HH:mm:ss')}, to : ${new Date(per_end).format('yyyy-MM-dd HH:mm:ss')} ms"

        Long now = System.currentTimeMillis()
        //过期娃娃提醒 剩三天和剩余一天 每天13点
        if(getHourOfDay(now).equals(13)
                && mainRedis.sadd(redis_key+'toyexpire',new Date(now).format('yyyy-MM-dd')) == 1){
            Long begin = System.currentTimeMillis()
            scanToyExpire(1)
            scanToyExpire(3)
            println "每天晚上6点 scanToyExpire :${new Date().format('yyyy-MM-dd HH:mm:ss')}: finish cost ${System.currentTimeMillis() - begin} ms"
        }

        //用户剩余积分提醒 每周日晚上9点
        if(getDayOfWeek(now).equals(7) && getHourOfDay(now).equals(21)
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
        println "scanUserDeliverInfo :${new Date().format('yyyy-MM-dd HH:mm:ss')}: finish cost ${System.currentTimeMillis() - begin} ms"//快递发货

        //用户抓中娃娃发送红包
        begin = System.currentTimeMillis()
        scanUserRedpacket()
        println "scanUserRedpacket :${new Date().format('yyyy-MM-dd HH:mm:ss')}: finish cost ${System.currentTimeMillis() - begin} ms"
    }

    static test(){
        test_ids.each {Integer userId ->
            //pushMsg2Queue(userId, new ToyExpireTemplate(userId, '宁姐','海绵宝宝', 3))
            //pushMsg2Queue(userId, new PointsExpireTemplate(userId, '宁姐', 200))
            //pushMsg2Queue(userId, new InviterTemplate(userId, '宁姐','张三', 10, 1515554460000))
            //pushMsg2Queue(userId, new DeliverTemplate(userId, '宁姐','海绵宝宝','申通快递','5210213132','上海闵行区合川大厦6m'))
            pushMsg2Queue(userId, new RedpacketTemplate(userId, '兄弟','王者荣耀梦奇布偶、王者荣耀288皮肤','点击详情查看，抓王者荣耀劲爆新品~'))
        }
    }


    //过期时间10天
    static Integer EXPIRE_DAYS = 10
    static Set<Integer> alreadyPushUsers = new HashSet<>()
    //扫描快过期娃娃
    static scanToyExpire(Integer expireDays){
        Long begin = zeroMill - ((EXPIRE_DAYS-expireDays) * DAY_MILLION)
        Long end = begin + DAY_MILLION
        List dolls = catch_success_logs.find($$(is_award: false, is_delete:false, post_type: 0, timestamp:[$gte:begin, $lt:end])).sort($$(user_id:-1)).toArray()
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
        }
        userOfDolls.each {Integer userId, Set<String> toys ->
            if(isTest(userId) && alreadyPushUsers.add(userId)){
                //println "${userId} ${getNickName(userId)}: ${toys.join(',')}, ${expireDays}天过期"
                pushMsg2Queue(userId, new ToyExpireTemplate(userId, getNickName(userId),toys.join(','), expireDays))
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
                pushMsg2Queue(userId, new PointsExpireTemplate(userId, nick_name, points))
            }
        }
    }

    //扫描用户邀请好友加入获得钻石
    static void scanUserInviterAward(){
        def cur = invitor_logs.find($$(diamond_count :[$gt:0], "is_used":[$ne:true], timestamp:[$gte:per_begin, $lt:per_end]),
                                                        $$(diamond_count:1,user_id:1,invitor:1,timestamp:1)).batchSize(100)
        while (cur.hasNext()){
            def user = cur.next()
            Integer userId = user['invitor'] as Integer //邀请人
            Integer invitedUId = user['user_id'] as Integer //被邀请人
            Integer diamond_count = user['diamond_count'] as Integer
            Long timestamp = user['timestamp'] as Long
            if(isTest(userId)){
                //println " ${getNickName(userId)} 邀请了: ${getNickName(invitedUId)} 获得:${diamond_count}"
                pushMsg2Queue(userId, new InviterTemplate(userId, getNickName(userId), getNickName(invitedUId), diamond_count, timestamp))
            }
        }
    }
    //扫描用户邮寄信息
    static void scanUserDeliverInfo(){
        def cur = apply_post_logs.find($$(post_type:3, "post_info":[$ne:null], 'post_info.next_time':[$gte:per_begin, $lt:per_end]),
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
            println "DeliverInfo : ${userId} : ${toySets.join(',')}"
            if(isTest(userId)){
                //println " ${getNickName(userId)} : ${address} 快递信息:${shipping_name} ${shipping_no}, 娃娃: ${toySets.join(',')}"
                pushMsg2Queue(userId, new DeliverTemplate(userId, getNickName(userId),toySets.join(','),shipping_name,shipping_no,address))
            }
        }
    }

    //扫描用户是否抓中娃娃
    static friends_limit = 1 //好友必须超过2名才能发红包
    static void scanUserRedpacket(){
        //抓中娃娃的用户
        def userIds = catch_record.distinct("user_id", $$(status:true, timestamp:[$gt:per_begin, $lt:per_end]))
        userIds.each {Integer userId ->
            //邀请的好友
            def friends = invitor_logs.find($$($or: [[user_id : userId], [invitor : userId]]),$$(user_id:1)).toArray()*.user_id;
            if(friends.size() <= friends_limit) return;
            //生成红包
            def redpacket_id = generateRedpacket(userId, friends)
            //生成红包成功,则生成微信客服消息模板
            if(StringUtils.isNotBlank(redpacket_id)){
                friends.each {Integer tid ->
                    //不能给自己发红包
                    if (userId.equals(tid)) return
                    pushMsg2Queue(tid, new RedpacketTemplate(tid, getNickName(userId), "activity/packet?packet_id=${redpacket_id}".toString()))
                }
            }
        }
    }

    //生成红包
    static String generateRedpacket(Integer userId, List<Integer> friends){
        try {
            Long time = System.currentTimeMillis();
            def _id = "${new Date().format('yyyyMMdd')}_${userId}".toString()
            def redpacket_id = "${userId}_${time}".toString()
            //一个用户一天只能发一次红包
            if(red_packets.count($$(_id :_id)) >= 1) return;
            //红包数量 = 好友人数/2
            Integer count = (friends.size() / 2) as Integer
            //红包奖励总钻石 = 红包数量 * 20
            def award_diamond = count * 20
            List<Integer> packets = distribute(award_diamond, count);
            def toy = getToyInfo(userId)
            red_packets.insert($$(_id:_id, redpacket_id:redpacket_id,user_id:userId, friends:friends, packets:packets, draw_uids: [],toy:toy,
                    count:count, award_diamond:award_diamond, status:1, timestamp:time, expires:time+3*DAY_MILLION))
            return redpacket_id;
        }catch (Exception e){
            println e
        }
        return null
    }

    static getToyInfo(Integer userId){
        def record = catch_record.findOne($$(user_id:userId,status:true, timestamp:[$gt:per_begin, $lt:per_end]),$$(room_id:1,toy:1))
        def toy = $$(room_id:record['room_id'],name:record['toy']['name'],pic:record['toy']['head_pic'])
        return toy
    }

    static min = 10d
    public static List<Integer> distribute(Integer total, int num){
        List<Integer> ar = new LinkedList<>();
        for (int i = 1; i < num; i++) {
            int remian = num - i;
            Double safe_total =  Math.ceil((total - remian * min) / remian) ;//随机安全上限
            Double money = min + RandomUtils.nextInt(safe_total.intValue());
            total = total - money.intValue();
            ar.add(money.intValue());
        }
        ar.add(total);
        Collections.shuffle(ar);
        return ar;
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
            if(template){
                def msg = $$(_id: to_uid+'_'+app_id+'_'+time, to_id:to_uid,app_id:app_id,open_id:open_id,
                        event:wxTemplate.getEvent_id(),timestamp:time,template:template, is_send:0, next_fire:time)
                weixin_msg.insert(msg)
            }
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
        return hourOfDay;
    }
}
class ToyExpireTemplate extends WxTemplate{
    static Map<String,String> template_ids = ['wx45d43a50adf5a470':'64GFNFZVbdvpCT0G5BOBIMPOYkypMSisKkuujc9Cacs', 'wxf64f0972d4922815':'P1nqV2mcWKlNLpCa-IS3A6hpGTiOq_N6cCLC5eyRxE0']

    public ToyExpireTemplate(Integer uid, String nickName, String toyName, Integer day){
        this.path = 'user/center';
        this.event_id = 'ToyExpire';
        this.uid = uid;
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

    public PointsExpireTemplate(Integer uid, String nickName, Integer points){
        this.path = 'user/center';
        this.event_id = 'PointsExpire';
        this.uid = uid;
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

    public InviterTemplate(Integer uid, String nickName, String invitorName, Integer diamond, Long registerTime){
        this.path = 'user/center';
        this.event_id = 'Inviter';
        this.uid = uid;
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
    public DeliverTemplate(Integer uid, String nickName, String toyName, String comName, String no, String address){
        this.path = '';
        this.event_id = 'DeliverInfo';
        this.uid = uid;
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
 * 抓中娃娃发钻石红包
 */
class RedpacketTemplate extends WxTemplate{
    static Map<String,String> TEMPLATE_IDS = ['wx45d43a50adf5a470':'pedgM13fkPvhs6E0LV6ew-8E9ociJuRpnrTso-TlZH4', 'wxf64f0972d4922815':'Ie5KJJ7UhNAowE6MHqY_8S3GTNLt85BSr6NgOlwa2Uw']

    public RedpacketTemplate(Integer uid, String nickName, String path){
        this.path = path;
        this.uid = uid;
        this.event_id = 'redpacket';
        data['first'] = ['value':"哇!好友${nickName}抓中了娃娃，特来给您发钻石拉，赶紧抢了钻石去抓娃娃。".toString(),'color':'#173177']
        data['keyword1'] = ['value':'100钻石','color':'#173177']
        data['keyword2'] = ['value':"${new Date().format('yyyy-MM-dd')}".toString(),'color':'#173177']
        data['remark'] = ['value':'钻石数量有限，先到先得，速速去抢!','color':'#FF0000']
    }

    public String getTemplateId(String appId){
        return TEMPLATE_IDS[appId]
    }
}

abstract class WxTemplate{
    protected String id;
    protected String url;
    protected String path;
    protected Map data = new HashMap();
    protected String event_id;
    protected Integer uid;
    protected static final String STATIC_API_URL = "http://api.lezhuale.com/statistic/weixin_template";
    protected static Map<String,String> DOMAIN_IDS = ['wx87f81569b7e4b5f6':'http://test.lezhuale.com/', 'wxf64f0972d4922815':'http://www.lezhuale.com/']


    public String getId(){return this.id}
    public String getUrl(){return this.url}
    public Map getData(){return this.data}
    public String getEvent_id(){return this.event_id}

    public Map generate(String appId){
        if(!DOMAIN_IDS.containsKey(appId)) return null
        String redirect = DOMAIN_IDS[appId] + path;
        String trace_id = "${event_id}_${uid}_${System.currentTimeMillis()}".toString()
        String url = STATIC_API_URL+"?event=${getEvent_id()}&uid=${uid}&trace_id=${trace_id}&redirect_url=${URLEncoder.encode(redirect, "UTF-8")}".toString()
        return ['id': getTemplateId(appId), url:url, data:this.getData()];
    }

    protected abstract String getTemplateId(String appId);
}