#!/usr/bin/env groovy
package crontab

import com.mongodb.BasicDBObject
import com.mongodb.DBCollection
import com.mongodb.DBObject
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
        @Grab('org.apache.httpcomponents:httpclient:4.2.5')
])
import com.mongodb.Mongo
import com.mongodb.MongoURI
import com.mongodb.util.Hash
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import org.apache.commons.lang.StringUtils
import org.apache.http.client.HttpClient
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.params.HttpConnectionParams
import org.apache.http.params.HttpParams
import redis.clients.jedis.Jedis
import redis.clients.jedis.Pipeline
import redis.clients.jedis.Response

/**
 * 定时更新房间的在线人数
 */
class UpdateUserAndLive {

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


    static final String jedis_host = getProperties("main_jedis_host", "192.168.31.236")
    static final String chat_jedis_host = getProperties("chat_jedis_host", "192.168.31.236")
    static final String live_jedis_host = getProperties("live_jedis_host", "192.168.31.236")
    static final String user_jedis_host = getProperties("user_jedis_host", "192.168.31.236")

    static final Integer main_jedis_port = getProperties("main_jedis_port", 6379) as Integer
    static final Integer chat_jedis_port = getProperties("chat_jedis_port", 6379) as Integer
    static final Integer live_jedis_port = getProperties("live_jedis_port", 6379) as Integer
    static final Integer user_jedis_port = getProperties("user_jedis_port", 6380) as Integer

    static redis = new Jedis(jedis_host, main_jedis_port)
    static chatRedis = new Jedis(chat_jedis_host, chat_jedis_port)
    static userRedis = new Jedis(user_jedis_host, user_jedis_port)
    static liveRedis = new Jedis(live_jedis_host, live_jedis_port)

    static M = new Mongo(new MongoURI(getProperties('mongo.uri', 'mongodb://192.168.31.231:20000,192.168.31.236:20000,192.168.31.231:20001/?w=1&slaveok=true') as String))

    static mongo = M.getDB("xy")
    static rooms = mongo.getCollection("rooms")
    static users = mongo.getCollection("users")
    static orders = M.getDB("shop").getCollection('orders')
    static room_cost_coll = M.getDB('xylog').getCollection('room_cost')
    static DBCollection familyDB = M.getDB('xyactive').getCollection('familys')

    static final String api_domain = getProperties("api.domain", "http://test-aiapi.memeyule.com/")

    private static final Integer TIME_OUT = 10 * 60 * 1000;
    private static final Integer THREE_MINUTE_SECONDS = 3 * 60;

    //static final long delay = 45 * 1000L
    static long zeroMill = new Date().clearTime().getTime()
    static Integer DAY_SEC = 24 * 3600
    static DAY_MILLON = DAY_SEC * 1000L
    static MIN_MILLON = 60 * 1000L
    static WEEK_MILLON = 7 * DAY_MILLON

    static final String WS_DOMAIN = getProperties('ws.domain', 'http://test-aiws.memeyule.com:6010')

    static main(arg) {
        final UpdateUserAndLive task = new UpdateUserAndLive()
        long l = System.currentTimeMillis()
        //房间在线人数统计
        long begin = l
        Integer i = task.roomUserCount()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  roomUserCount---> update ${i} rows , cost  ${System.currentTimeMillis() - l} ms"

        //房间用户上麦状态检测
        l = System.currentTimeMillis()
        task.roomMicLiveCheck()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  roomMicLiveCheck---> cost: ${System.currentTimeMillis() - l} ms"

        //异常交易检测
        l = System.currentTimeMillis()
        def trans = [
                room_cost      : room_cost_coll,
                finance_log    : M.getDB('xy_admin').getCollection('finance_log'),
                exchange_log   : M.getDB('xylog').getCollection('exchange_log'),
                diamond_logs   : M.getDB('xy_admin').getCollection('diamond_logs'),
                diamond_cost_logs   : M.getDB('xy_admin').getCollection('diamond_cost_logs')
        ]
        trans.each { k, v ->
            long lc = System.currentTimeMillis()
            task.userTranCheck(k, v)
            println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  users.${k} , cost  ${System.currentTimeMillis() - lc} ms"
        } println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  userTransCheck---->cost:  ${System.currentTimeMillis() - l} ms"

        //自动解封用户
        l = System.currentTimeMillis()
        task.auto_unfreeze()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  auto_unfreeze---->cost:  ${System.currentTimeMillis() - l} ms"

        //延时支付订单检查
        l = System.currentTimeMillis()
        task.delayOrderCheck()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  delayOrderCheck---->cost:  ${System.currentTimeMillis() - l} ms"

        //家族活动
        l = System.currentTimeMillis()
        task.familyEventAward()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  familyEventAward---->cost:  ${System.currentTimeMillis() - l} ms"

        Long totalCost = System.currentTimeMillis() - begin
        //落地定时执行的日志
        l = System.currentTimeMillis()
        def timerName = 'UpdateUserAndLive'
        saveTimerLogs(timerName, totalCost)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  save timer_logs , cost  ${System.currentTimeMillis() - l} ms"

        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  ${UpdateUserAndLive.class.getSimpleName()}:costTotal:  ${System.currentTimeMillis() - begin} ms"

    }

    //落地定时执行的日志
    private static saveTimerLogs(String timerName, Long totalCost) {
        def timerLogsDB = M.getDB("xyrank").getCollection("timer_logs")
        def tmp = System.currentTimeMillis()
        def id = timerName + "_" + new Date().format("yyyyMMdd")
        def update = new BasicDBObject(timer_name: timerName, cost_total: totalCost, cat: 'minute', unit: 'ms', timestamp: tmp)
        timerLogsDB.findAndModify(new BasicDBObject('_id', id), null, null, false, new BasicDBObject('$set', update), true, true)
    }

    static final String vistor_key = "web:ttxiuvistor:counts"
    static final Integer VISITOR_RATIO = 1

    Integer roomUserCount() {

        long now = System.currentTimeMillis()
        int i = 0
        Long vCount = 0
        def roomWithCount = new HashMap<Integer, Long>()
        def liveRooms = rooms.find(new BasicDBObject(test: [$ne: true]), new BasicDBObject(_id: 1, live: 1)).toArray()
        //println "roomUserCount query total time cost : ${System.currentTimeMillis() - now}"
        long n = System.currentTimeMillis()
        liveRooms.each { dbo -> //,status:Boolean.TRUE
            i++
            Integer room_id = dbo.get("_id") as Integer
            Boolean live = dbo.get("live") as Boolean
            Long room_count = (userRedis.scard("room:${room_id}:users".toString()) ?: 0)
            //Long robots_count = (userRedis.scard("room:${room_id}:robots".toString()) ?: 0)
            Long robots_count =0
            Long visiter_count = (robots_count + room_count) * VISITOR_RATIO
            rooms.update(new BasicDBObject("_id", room_id),
                    new BasicDBObject('$set', new BasicDBObject("visiter_count", visiter_count))
            )
            vCount += room_count
        }
        redis.set(vistor_key, vCount.toString())
        return i
    }

    //记录直播间人气
    private void recordRoomCount(HashMap<Integer, Long> roomWithCount) {
        try {
            if (roomWithCount.size() == 0) return;

            String date = new Date().format("yyyyMMdd")
            String room_user_count_key = "live:room:user:count:${date}".toString();
            String room_user_time_key = "live:room:user:count:times:${date}".toString();
            if (liveRedis.hsetnx(room_user_count_key, date, '1') == 1) {
                liveRedis.expire(room_user_count_key, 2 * DAY_SEC)
            }
            if (liveRedis.hsetnx(room_user_time_key, date, '1') == 1) {
                liveRedis.expire(room_user_time_key, 2 * DAY_SEC)
            }
            roomWithCount.each { Integer room_id, Long room_count ->
                liveRedis.hincrBy(room_user_count_key, room_id.toString(), room_count)
                liveRedis.hincrBy(room_user_time_key, room_id.toString(), 1)
            }
        } catch (Exception e) {
            println "recordRoomCount Exception : ${e.getMessage()}";
        }

    }

    /**
     * 直播间上麦用户心跳检测
     * @return
     */
    def roomMicLiveCheck() {
        //获取小于当前时间2分钟前得开播的房间列表
        def roomList = rooms.find(new BasicDBObject( $or: [['mic_first': [$ne: null]], ['mic_sec': [$ne: null]]]),
                new BasicDBObject(mic_first: 1, mic_sec: 1)).toArray()

        roomList.each { room ->
            def roomId = room.get("_id")
            Integer mic_first = (room?.get("mic_first") ?: 0) as Integer
            Integer mic_sec = (room?.get("mic_sec") ?: 0) as Integer
            println "room : ${roomId}, mic_first : ${mic_first}, mic_sec: ${mic_sec}".toString()
            Boolean isOffLive = Boolean.FALSE
            //检查是否心跳存在和推流状态
            def set = new BasicDBObject()
            def query = new BasicDBObject(_id:roomId as Integer)
            if (mic_first > 0 && !isLive(roomId.toString(), mic_first)) {
                query.append('mic_first', mic_first)
                set.append('mic_first', null)
                isOffLive = Boolean.TRUE
            }
            if (mic_sec > 0 && !isLive(roomId.toString(), mic_sec)) {
                query.append('mic_sec', mic_sec)
                set.append('mic_sec', null)
                isOffLive = Boolean.TRUE
            }
            if(isOffLive){
                rooms.update(query, new BasicDBObject('$set', set))

            }

        }
    }

    boolean isLive(def roomId, def userId){
        String key = "room:mic:live:"+roomId+':'+userId
        return liveRedis.ttl(key) > 0
    }

    long WAIT = 30 * 1000L
    //失败事务处理 30 秒
    def userTranCheck(String field, DBCollection collection) {
        users.find(new BasicDBObject(field + '.timestamp', [$lt: System.currentTimeMillis() - WAIT]), new BasicDBObject(field, 1))
                .limit(50).toArray().each { user ->
            List logs = user.get(field) as List
            def uid = new BasicDBObject('_id', user.get('_id'))
            if (logs) {
                logs.each { DBObject log ->
                    def _id = log.get('_id')
                    collection.save(log)
                    users.update(uid, new BasicDBObject('$pull', new BasicDBObject(field, [_id: _id])))
                    println "clean ${field} : ${log}"
                }
            }
        }
    }

    //失败事务处理 30 秒
    def orderTranCheck(String field, DBCollection collection) {
        orders.find(new BasicDBObject(field + '.timestamp', [$lt: System.currentTimeMillis() - WAIT]), new BasicDBObject(field, 1))
                .limit(50).toArray().each { order ->
            List logs = order.get(field) as List
            def oid = new BasicDBObject('_id', order.get('_id'))
            if (logs) {
                logs.each { DBObject log ->
                    def _id = log.get('_id')
                    collection.save(log)
                    orders.update(oid, new BasicDBObject('$pull', new BasicDBObject(field, [_id: _id])))
                    println "clean ${field} : ${log}"
                }
            }
        }
    }

    def familyEventAward() {
        def api_url = api_domain + "job/family_event_award".toString()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')} result : ${request(api_url)}"
    }

    //检测间隔速率  0 挡为慢速， 1 挡为快速检测挡
    static final Integer speed = 0;

    def delayOrderCheck() {
        def api_url = api_domain + "pay/delay_order_fix?speed=${speed}".toString()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')} result : ${request(api_url)}"
    }

    static String request(String url) {
        HttpURLConnection conn = null;
        def jsonText = "";
        try {
            conn = (HttpURLConnection) new URL(url).openConnection()
            conn.setRequestMethod("GET")
            conn.setDoOutput(true)
            conn.setConnectTimeout(3000);
            conn.setReadTimeout(3000);
            conn.connect()
            jsonText = conn.getInputStream().getText("UTF-8")

        } catch (Exception e) {
            println "request Exception : " + e;
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
        return jsonText;
    }

    static String request_post(String url, String params) {
        HttpURLConnection conn = null;
        PrintWriter pw = null;
        BufferedReader br = null;
        def jsonText = "";
        try {
            conn = (HttpURLConnection) new URL(url).openConnection()
            conn.setRequestMethod("POST")
            conn.setDoOutput(true)
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(5000);
            conn.setDoOutput(true);
            conn.setDoInput(true);
            pw = new PrintWriter(conn.getOutputStream());
            pw.print(params);
            pw.flush();
            br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String line;
            while ((line = br.readLine()) != null) {
                jsonText += line;
            }
        } catch (Exception e) {
            println("发送 POST 请求出现异常！" + e);
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
            if (pw != null) {
                pw.close()
            }
            if (br != null) {
                br.close()
            }
        }
        return jsonText;
    }

    /**
     * 用户封禁到期自动解封
     */
    def auto_unfreeze() {
        users.update($$("status": false, unfreeze_time: [$lte: System.currentTimeMillis()])
                , $$('$set': $$(status: true)).append('$unset', $$("unfreeze_time", 1)), false, true);
    }

    public static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    public static BasicDBObject $$(Map map) {
        return new BasicDBObject(map)
    }

    private static void publish(Map params, String roomId) {
        String url = getRoomPublishUrl(roomId);
        String content = JsonOutput.toJson(params)
        request_post(url, content)
    }


    private static String getRoomPublishUrl(String roomId) {
        return String.format("%s%s", WS_DOMAIN, "/api/publish/room/${roomId}");
    }
}