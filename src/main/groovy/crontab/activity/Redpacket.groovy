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
class Redpacket {

    static Properties props = null;
    static String profilepath = "/empty/crontab/db.properties";
    static mongo = new Mongo(new MongoURI(getProperties('mongo.uri', 'mongodb://192.168.31.249:10000/?w=1') as String))
    static DBCollection invitor_logs = mongo.getDB('xylog').getCollection('invitor_logs')
    static DBCollection catch_record = mongo.getDB('xy_catch').getCollection('catch_record')
    static DBCollection weixin_msg = mongo.getDB('xy_union').getCollection('weixin_msgs')
    static DBCollection red_packets = mongo.getDB('xy_activity').getCollection('red_packets')
    static DBCollection users = mongo.getDB('xy').getCollection("users")
    static String jedis_host = getProperties("main_jedis_host", "192.168.31.249")
    static String site_domain = getProperties("site.domain", "http://test.17laihou.com/")
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
        /*red_packets.remove($$(_id:[$ne:null]))
        weixin_msg.remove($$(_id:[$ne:null]))*/
        genenrateRedPacket()
        println "${Redpacket.class.getSimpleName()}:${new Date().format('yyyy-MM-dd HH:mm:ss')}: finish cost ${System.currentTimeMillis() - begin} ms"
    }

    static Long end =  System.currentTimeMillis();
    static Long begin =  System.currentTimeMillis();
    static void genenrateRedPacket(){
        end = System.currentTimeMillis();
        begin = ((mainRedis.getSet(LAST_SCAN_REDIS_KEY, end.toString()) ?: end) as Long)
        List<Integer> userIds = scanUserFromCatchRecord()
        userIds.each {Integer userId ->
            sendRedPacket(userId, getRelations(userId))
        }

    }

    //扫描最近抓中娃娃的用户
    static List<Integer> scanUserFromCatchRecord(){
        println "begin:${new Date(begin).format('yyyy-MM-dd HH:mm:ss')}, end ${new Date(end).format('yyyy-MM-dd HH:mm:ss')} ms"
        def List = catch_record.distinct("user_id", $$(status:true, timestamp:[$gt:begin, $lt:end]))
        return List;
    }

    //获取邀请的用户(好友)
    static List<Integer> getRelations(Integer userId){
        List list = invitor_logs.find($$($or: [[user_id : userId], [invitor : userId]]),$$(user_id:1)).toArray()*.user_id
        return list;
    }

    static friends_limit = 1 //好友必须超过2名才能发红包
    //记录待发送红包
    static sendRedPacket(Integer userId, List<Integer> friends){
        if(friends.size() <= friends_limit) return;
        //生成红包
        def redpacket_id = generateRedpacket(userId, friends)
        //生成红包成功,则生成微信客服消息模板添加到微信消息队列
        if(StringUtils.isNotBlank(redpacket_id)){
            push2Msg(userId, friends,redpacket_id)
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
            return null
        }
    }

    static final String path = 'activity/packet'
    static final String wx_pic_url = 'https://mmbiz.qpic.cn/mmbiz_png/kGE3RectqDwSvWBwjb6OJeSoCcjN7IhuJqsBd50UFqjulmmYdVADeMVAibtUhhmAkQFElCRiclxL8RpynXUSyXoA/0?wx_fmt=png'
    //添加到微信待发送消息队列
    static push2Msg(Integer userId, List<Integer> friends, String redpacket_id){
        try{
            Long time = System.currentTimeMillis();
            //生成消息模板
            List<BasicDBObject> msgs = new ArrayList<>(friends.size());
            String nickname = users.findOne($$(_id:userId),$$(nick_name:1))?.get('nick_name')
/*
            客服消息，必须用户48小时内在公众号内有活跃
            BasicDBObject customMsg = $$(_id:redpacket_id)
            customMsg['title']="${nickname}给你发来一个钻石红包，点击领取".toString()
            customMsg['description']= "哇！${nickname}又抓中娃娃了，并且给大家发了一大波钻石红包！赶紧去抢钻石抓娃娃".toString()
            customMsg['pic_url']= wx_pic_url
            customMsg['url']= site_domain + path + "?packet_id=${redpacket_id}".toString()
            customMsg['content']= ""
*/
            //模板消息
            def template = $$(_id:redpacket_id)
            template['path'] = path + "?packet_id=${redpacket_id}".toString()
            def data = new HashMap();
            data['first'] = ['value':"哇!好友${nickname}抓中了娃娃，特来给您发钻石拉，赶紧抢了钻石去抓娃娃。".toString(),'color':'#173177']
            data['keyword1'] = ['value':'100钻石','color':'#173177']
            data['keyword2'] = ['value':"${new Date().format('yyyy-MM-dd')}".toString(),'color':'#173177']
            data['remark'] = ['value':'钻石数量有限，先到先得，速速去抢!','color':'#FF0000']
            template['data'] = data;
            friends.each {Integer tid ->
                if(!userId.equals(tid)){
                    def msg = $$(_id: redpacket_id+'_'+tid,from_id:userId,to_id:tid,timestamp:time,template:template, is_send:0, next_fire:time)
                    //def msg = $$(_id: redpacket_id+'_'+tid,from_id:userId,to_id:tid,timestamp:time,custom_text:customMsg,template:template, is_send:0, next_fire:time)
                    msgs.add(msg)
                }
            }
            //push入待发送队列
            weixin_msg.insert(msgs)
        }catch (Exception e){
            println "push2Msg exception :" + e;
        }
    }

    static getToyInfo(Integer userId){
        def record = catch_record.findOne($$(user_id:userId,status:true, timestamp:[$gt:begin, $lt:end]),$$(room_id:1,toy:1))
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

    public static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    public static BasicDBObject $$(Map map) {
        return new BasicDBObject(map)
    }


}
