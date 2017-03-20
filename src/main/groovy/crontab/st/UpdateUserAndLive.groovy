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
    static logRoomEdit = M.getDB("xylog").getCollection("room_edit")
    static rooms = mongo.getCollection("rooms")
    static users = mongo.getCollection("users")
    static orders = M.getDB("shop").getCollection('orders')
    static room_cost_coll = M.getDB('xylog').getCollection('room_cost')
    static star_recommends = M.getDB('xy_admin').getCollection('star_recommends')

    static final String api_domain = getProperties("api.domain", "http://test-aiapi.memeyule.com/")

    private static final Integer TIME_OUT = 10 * 60 * 1000;
    private static final Integer THREE_MINUTE_SECONDS = 3 * 60;

    //static final long delay = 45 * 1000L
    static long zeroMill = new Date().clearTime().getTime()
    static Integer DAY_SEC = 24 * 3600
    static DAY_MILLON = DAY_SEC * 1000L
    static MIN_MILLON = 60 * 1000L
    static WEEK_MILLON = 7 * DAY_MILLON

    static
    final String CLOSE_GAME_SERVER_URL = getProperties('aigd.domain', 'http://test-aigd.memeyule.com:6050/api/room/close?room_id=ROOM_ID&game_id=GAME_ID&live_id=LIVE_ID')
    static final String WS_DOMAIN = getProperties('ws.domain', 'http://test-aiws.memeyule.com:6010')

    static main(arg) {
        final UpdateUserAndLive task = new UpdateUserAndLive()

        long l = System.currentTimeMillis()

        //直播间人数统计
        long begin = l
        Integer i = task.roomUserCount()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  roomUserCount---> update ${i} rows , cost  ${System.currentTimeMillis() - l} ms"

        //直播间直播状态检测
        l = System.currentTimeMillis()
        task.liveCheck()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  liveCheck---> cost: ${System.currentTimeMillis() - l} ms"

        //异常交易检测
        l = System.currentTimeMillis()
        def trans = [
                room_cost      : room_cost_coll,
                finance_log    : M.getDB('xy_admin').getCollection('finance_log'),
                exchange_log   : M.getDB('xylog').getCollection('exchange_log'),
                mission_logs   : M.getDB('xylog').getCollection('mission_logs'),
                withdrawl_log  : M.getDB('xy_admin').getCollection('withdrawl_log'),
                star_award_logs: M.getDB('game_log').getCollection('star_award_logs'),
                diamond_logs   : M.getDB('shop').getCollection('diamond_logs'),
                orders         : M.getDB('shop').getCollection('orders'),
        ]
        trans.each { k, v ->
            long lc = System.currentTimeMillis()
            task.userTranCheck(k, v)
            println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  users.${k} , cost  ${System.currentTimeMillis() - lc} ms"
        } println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  userTransCheck---->cost:  ${System.currentTimeMillis() - l} ms"

        // 订单交易与钻石流水的异常检测
        l = System.currentTimeMillis()
        def order_trans = [
                diamond_logs: M.getDB('shop').getCollection('diamond_logs')
        ]
        order_trans.each { k, v ->
            long lc = System.currentTimeMillis()
            task.orderTranCheck(k, v)
            println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  order.${k} , cost  ${System.currentTimeMillis() - lc} ms"
        } println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  orderTranCheck---->cost:  ${System.currentTimeMillis() - l} ms"

        //自动解封用户
        l = System.currentTimeMillis()
        task.auto_unfreeze()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  auto_unfreeze---->cost:  ${System.currentTimeMillis() - l} ms"

        //主播推荐有效期检测
        l = System.currentTimeMillis()
        task.recommendCheck()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  recommendCheck---->cost:  ${System.currentTimeMillis() - l} ms"

        //延时支付订单检查
        l = System.currentTimeMillis()
        task.delayOrderCheck()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  delayOrderCheck---->cost:  ${System.currentTimeMillis() - l} ms"

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
        def liveRooms = rooms.find(new BasicDBObject(timestamp: [$gte: now - 2 * DAY_MILLON], test: [$ne: true]), new BasicDBObject(_id: 1, live: 1)).toArray()
        //println "roomUserCount query total time cost : ${System.currentTimeMillis() - now}"
        //long n = System.currentTimeMillis()
        liveRooms.each { dbo -> //,status:Boolean.TRUE
            i++
            Integer room_id = dbo.get("_id") as Integer
            Boolean live = dbo.get("live") as Boolean
            Long room_count = (userRedis.scard("room:${room_id}:users".toString()) ?: 0)
            Long robots_count = (userRedis.scard("room:${room_id}:robots".toString()) ?: 0)
            Long visiter_count = (robots_count + room_count) * VISITOR_RATIO
            rooms.update(new BasicDBObject("_id", room_id),
                    new BasicDBObject('$set', new BasicDBObject("visiter_count", visiter_count))
            )
            vCount += room_count
            if (live) {
                roomWithCount.put(room_id, room_count)
            }
        }
        //println "roomUserCount update time cost : ${System.currentTimeMillis() - n}"
        redis.set(vistor_key, vCount.toString())
        recordRoomCount(roomWithCount)
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

    def liveCheck() {
        //获取小于当前时间2分钟前得开播的房间列表
        def roomList = rooms.find(new BasicDBObject('live': true, timestamp: [$lte: System.currentTimeMillis() - (2 * MIN_MILLON)]),
                new BasicDBObject(live_id: 1, timestamp: 1, type: 1, game_id: 1, live_type: 1)).toArray()

        //获取七牛推流直播间列表
        List<String> liveStreamList = getLiveStreamIds()

        roomList.each { room ->
            def roomId = room.get("_id")
            String live_id = room.get("live_id")
            String game_id = room.get("game_id")
            Integer live_type = room?.get("live_type") as Integer
            Integer type = (room.get("type") ?: 0) as Integer
            //检查是否心跳存在和推流状态
            if (!isLive(roomId.toString(), liveStreamList)) {
                Long l = Math.max(System.currentTimeMillis() - 60000, (room['timestamp'] ?: 0) as Long) + 1
                if (logRoomEdit.update(
                        new BasicDBObject(type: "live_on", data: live_id, room: roomId),
                        new BasicDBObject('$set': [etime: l, status: 'LiveStatusCheck'])).getN() == 1) {
                    try {
                        delLiveRedis(roomId)
                        // 关闭时通知游戏服务端关闭
                        notifyGameServerClose(game_id, roomId.toString(), live_id)
                        logRoomEdit.save(new BasicDBObject(type: 'live_off', room: roomId, data: live_id, live_type: live_type, status: 'LiveStatusCheck', timestamp: l))
                    }
                    finally {
                        def set = new BasicDBObject(live: false, live_id: '', timestamp: l, live_end_time: l, game_id: '', position: null, pull_urls: null, push_urls: null)
                        rooms.update(new BasicDBObject(_id: roomId, live: Boolean.TRUE), new BasicDBObject('$set', set))
                        def params = new HashMap()
                        def body = ['live': false, room_id: roomId, 'template_id': 'live_on']
                        params.put('action', 'room.live.rc')
                        params.put('data', body)
                        publish(params, roomId.toString())
                    }
                }
            }

        }
    }

    /**
     * 获取七牛推流直播间列表
     * @return
     */
    private static List<String> getLiveStreamIds() {
        JsonSlurper jsonSlurper = new JsonSlurper()
        // 查询当前七牛开播的流列表
        String url = "${api_domain}/monitor/live_list"
        String result = request(url)
        Map map = jsonSlurper.parseText(result) as Map
        List<String> liveStreamList = (map['data'] ?: Collections.emptyList()) as List
        return liveStreamList
    }
    /**
     * 检查是否流断了
     * @return
     */
    private static Boolean isLive(String roomId, List<String> liveStreamList) {
        if (!userRedis.exists("room:${roomId}:live")) {
            println("${new Date().format('yyyy-MM-dd HH:mm:ss')}  ${roomId}:hearth was broken ,it will be close ..")
            return Boolean.FALSE
        }
        if (!liveStreamList.contains(roomId)) {//如果流不存在列表中  记录当前失败次数
            String key = "live:${roomId}:bad:stream"
            Long errorTimes = liveRedis.incr(key)
            liveRedis.expire(key, 90)
            println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  stream check ${roomId}: recordTimes : ${errorTimes}"
            return errorTimes < 3 //三次检测流未正常推送则关闭直播间
        }
        return Boolean.TRUE
    }


    private void delLiveRedis(def roomId) {
        try {
            liveRedis.keys("live:${roomId}:*".toString()).each { liveRedis.del(it) };
            println "${new Date().format('yyyy-MM-dd HH:mm:ss')} liveRedis DEL live:${roomId}:*"
        }
        catch (e) {
            e.printStackTrace()
        }
    }

    /**
     * 通知游戏服务器关闭直播间
     * @param game_id
     * @param roomId
     * @param live_id
     */
    private void notifyGameServerClose(String game_id, String roomId, String live_id) {
        String url = CLOSE_GAME_SERVER_URL.replace('ROOM_ID', roomId.toString()).replace('GAME_ID', game_id).replace('LIVE_ID', live_id)
        println request(url)
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


    def recommendCheck() {
        def expire_query = new BasicDBObject('is_recomm': Boolean.TRUE, 'expires': [$lt: System.currentTimeMillis()])
        star_recommends.updateMulti(expire_query, new BasicDBObject('$set', [expires: null, is_recomm: Boolean.FALSE]))

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