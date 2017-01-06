#!/usr/bin/env groovy
package tmp

@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
        @Grab(group = 'net.sf.json-lib', module = 'json-lib', version = '2.3', classifier = 'jdk15'),
])
import com.mongodb.MongoURI
import groovy.json.JsonSlurper
import com.mongodb.BasicDBObject
import com.mongodb.Mongo
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPubSub
import net.sf.json.JSONObject
import java.security.MessageDigest

class auto_red {


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

    static final String jedis_host = getProperties("main_jedis_host", "192.168.31.246")
    static final String chat_jedis_host = getProperties("chat_jedis_host", "192.168.31.246")
    static final String live_jedis_host = getProperties("live_jedis_host", "192.168.31.246")
    static final String user_jedis_host = getProperties("user_jedis_host", "192.168.31.246")

    static final Integer main_jedis_port = getProperties("main_jedis_port",6379) as Integer
    static final Integer chat_jedis_port = getProperties("chat_jedis_port",6379) as Integer
    static final Integer live_jedis_port = getProperties("live_jedis_port",6379) as Integer
    static final Integer user_jedis_port = getProperties("user_jedis_port",6379) as Integer

    static mongo = new Mongo(new MongoURI(getProperties('mongo.uri', 'mongodb://192.168.31.249:27017/?w=1') as String))
    static chatRedis = new Jedis(jedis_host, chat_jedis_port)
    static mainRedis = new Jedis(jedis_host, main_jedis_port)
    static userRedis = new Jedis(user_jedis_host,user_jedis_port)
    private final static String DRAW_KEY = "OSSDPGLK234MV/ITFOS23SD@#XA1CQQ&6UQ6";

    static url = "http://api.memeyule.com"


    def final static users = [//1201053 : "54127e4f6be1becc54d92981a89f2317",
                              20199309 : "8c76b50ee4791e7d0a89bd430536b248",
                        ] as Map

    static change_nickname(){
        users.each {k, v ->
            def nick_name = nick_names.get(new Random().nextInt(nick_names.size()))
            new URL(url+"/user/edit?access_token=${v}&nick_name=${nick_name}").getText("utf-8")
        }
    }
    static Integer next_hour = 0
    static auto_fetch(String message){
        JSONObject obj = JSONObject.fromObject(message);
        def data = obj['data_d'] as Map
        def final room =  data['room_id'] as String
        def final time = data['timestamp'] as String
        def final cost = data['cost'] as Long
        def redisKey = "room:" + room + ":users"

        long size = userRedis.scard(redisKey) ?: 0
        println "${new Date().format("yyyy-MM-dd HH:mm:ss")} roomid: ${room}  members: ${size}   cost : ${cost.toString()}".toString()
        if(size <= 60 && cost < 3000){
            return
        }

        if( next_hour != Calendar.getInstance().get(Calendar.HOUR_OF_DAY) ){
            next_hour = Calendar.getInstance().get(Calendar.HOUR_OF_DAY)
            change_nickname();
        }

        Thread.sleep(200)
        new Thread(new Runnable() {
            @Override
            public void run() {
                try{
                    draw(room, time)
                }catch(Exception e){
                    e.printStackTrace();
                }

            }

            def draw(def room_id, def timestamp){
                int index = 0
                while(index++ < 5000){
                    if(!invokeApi(room_id, timestamp)){
                        break;
                    }
                    Thread.sleep(200)
                }
                println new Date().format("yyyy-MM-dd HH:mm:ss") + " draw over>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
            }

            def Boolean invokeApi(def room_id, def timestamp){
                def code = "0"
                users.each {k,v ->
                    def sign = MD5("${room_id}${timestamp}${k}${DRAW_KEY}").toLowerCase()
                    def returnStr =  new URL(url+"/redpacket/drawPrize?access_token=${v}&room_id=${room_id}&s=${sign}").getText("utf-8")
                    def returnJson = JSONObject.fromObject(returnStr)
                    code = returnJson["code"] as String
                }

                if(code.equals("30513") || code.equals("30511")){
                    return Boolean.FALSE;
                }
                return Boolean.TRUE;
            }
        }).start();

    }

    static List<String> nick_names = new ArrayList<>(10000)
    static void main(String[] args)
    {
        new File("/empty/crontab/fansi.cvs").splitEachLine(','){row->
            nick_names .add(row[0])
        }

        println "auto_red listener ...."
        chatRedis.subscribe(new JedisPubSub() {
            @Override
            void onMessage(String channel, String message) {
                auto_fetch(message)
            }
            @Override
            void onPMessage(String s, String s2, String s3) {}
            @Override
            void onSubscribe(String s, int i) {}
            @Override
            void onUnsubscribe(String s, int i) {}
            @Override
            void onPUnsubscribe(String s, int i) {}
            @Override
            void onPSubscribe(String s, int i) {}
        } , "auto_red")
    }

    public static String MD5(String s) {
        def hexDigits =  ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'] as char[];

        try {
            byte[] btInput = s.getBytes();
            // 获得MD5摘要算法的 MessageDigest 对象
            MessageDigest mdInst = MessageDigest.getInstance("MD5");
            // 使用指定的字节更新摘要
            mdInst.update(btInput);
            // 获得密文
            byte[] md = mdInst.digest();
            // 把密文转换成十六进制的字符串形式
            int j = md.length;
            def str = new char[j * 2];
            int k = 0;
            for (int i = 0; i < j; i++) {
                byte byte0 = md[i];
                str[k++] = hexDigits[byte0 >>> 4 & 0xf];
                str[k++] = hexDigits[byte0 & 0xf];
            }
            return new String(str);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

}