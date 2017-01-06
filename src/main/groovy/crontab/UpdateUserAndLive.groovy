#!/usr/bin/env groovy
package crontab

import com.mongodb.BasicDBObject
import com.mongodb.DBCollection
import com.mongodb.DBObject
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
])
import com.mongodb.Mongo
import redis.clients.jedis.Jedis


/**
 * 定时更新房间的在线人数
 *
 * date: 13-2-28 下午2:46
 * @author: yangyang.cong@ttpod.com
 */
class UpdateUserAndLive {

    static Properties props = null;
    static String profilepath="/empty/crontab/db.properties";

    static getProperties(String key, Object defaultValue){
        try {
            if(props == null){
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

    static redis = new Jedis(jedis_host, main_jedis_port)
    static chatRedis = new Jedis(chat_jedis_host,chat_jedis_port)
    static userRedis = new Jedis(user_jedis_host,user_jedis_port)
    static liveRedis = new Jedis(live_jedis_host,live_jedis_port)

    static M  = new Mongo(new com.mongodb.MongoURI(getProperties('mongo.uri','mongodb://192.168.31.249:27017/?w=1') as String))

    static mongo = M.getDB("xy")
    static logRoomEdit =M.getDB("xylog").getCollection("room_edit")
    static rooms = mongo.getCollection("rooms")
    static users = mongo.getCollection("users")
    static boxes = mongo.getCollection("boxes")
    static room_candidates = mongo.getCollection("room_candidates")
    static nest = M.getDB("xy_nest").getCollection("nests")
    static songs = mongo.getCollection("songs")
    static labels =  M.getDB('xylog').getCollection('labels')
    static guard_users =  M.getDB('xylog').getCollection('guard_users')
    static pk_logs =  M.getDB('xylog').getCollection('pk_logs')

    static room_cost_coll = M.getDB('xylog').getCollection('room_cost')
    static room_mic_log = M.getDB('xylog').getCollection('room_mic_log')
    static star_recommends = M.getDB('xy_admin').getCollection('star_recommends')
    static rewards = M.getDB('xyactive').getCollection('rewards')
    static config = M.getDB('xy_admin').getCollection('config')

    static final String api_domain = getProperties("api.domain", "http://localhost:8080/")


    private static final Integer TIME_OUT = 10 * 60 * 1000;

    //static final long delay = 45 * 1000L
    static long zeroMill = new Date().clearTime().getTime()
    static Integer DAY_SEC = 24 * 3600
    static DAY_MILLON = DAY_SEC * 1000L
    static WEEK_MILLON = 7 * DAY_MILLON

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

        //vip有效期检测
        l = System.currentTimeMillis()
        task.vipCheck()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  vipCheck---->cost:  ${System.currentTimeMillis() - l} ms"

        //异常交易检测
        l = System.currentTimeMillis()
        def trans =[
                room_cost:room_cost_coll,
                finance_log:M.getDB('xy_admin').getCollection('finance_log'),
                exchange_log: M.getDB('xylog').getCollection('exchange_log'),
                withdrawl_log:M.getDB('xy_admin').getCollection('withdrawl_log')
        ]
        trans.each {k,v->
            long  lc = System.currentTimeMillis()
            task.userTranCheck(k,v)
            println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  users.${k} , cost  ${System.currentTimeMillis() - lc} ms"
        }
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  userTransCheck---->cost:  ${System.currentTimeMillis() - l} ms"
        /*
        l = System.currentTimeMillis()
        task.songTranCheck()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  songTranCheck , cost  ${System.currentTimeMillis() - l} ms"

        */

/*

        //05. 小窝有效期
        l = System.currentTimeMillis()
        task.boxCheck()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  boxCheck---->cost:  ${System.currentTimeMillis() - l} ms"

        //06.包厢用户数量
        l = System.currentTimeMillis()
        task.boxUserCount()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  boxUserCount---->cost:  ${System.currentTimeMillis() - l} ms"

        //07.包厢直播状态监测
        l = System.currentTimeMillis()
        task.boxliveCheck()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  boxliveCheck---->cost:  ${System.currentTimeMillis() - l} ms"


        //小窝用户数量
        l = System.currentTimeMillis()
        task.nestUserCount()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  nestUserCount---->cost:  ${System.currentTimeMillis() - l} ms"

        //小窝麦上状态检测
        l = System.currentTimeMillis()
        task.nestliveCheck()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  nestliveCheck---->cost:  ${System.currentTimeMillis() - l} ms"
*/

        //守护有效期检测
        l = System.currentTimeMillis()
        task.guardCheck()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  guardCheck---->cost:  ${System.currentTimeMillis() - l} ms"

        //自动解封用户
        l = System.currentTimeMillis()
        task.auto_unfreeze()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  auto_unfreeze---->cost:  ${System.currentTimeMillis() - l} ms"

        //PK游戏清理
        l = System.currentTimeMillis()
        task.pkGameClean()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  pkGameClean---->cost:  ${System.currentTimeMillis() - l} ms"

        //直播间连麦用户心跳检测
        l = System.currentTimeMillis()
        task.micUserliveCheck()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  micUserliveCheck---->cost:  ${System.currentTimeMillis() - l} ms"

        //主播推荐有效期检测
        l = System.currentTimeMillis()
        task.recommendCheck()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  recommendCheck---->cost:  ${System.currentTimeMillis() - l} ms"

        //悬赏有效期检测
        /*l = System.currentTimeMillis()
        task.rewardCheck()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  rewardCheck---->cost:  ${System.currentTimeMillis() - l} ms"*/

        //临时直播推荐有效期检测
        l = System.currentTimeMillis()
        task.tmpRoomCheck()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  tmpRoomCheck---->cost:  ${System.currentTimeMillis() - l} ms"

        //延时支付订单检查
        l = System.currentTimeMillis()
        task.delayOrderCheck()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  delayOrderCheck---->cost:  ${System.currentTimeMillis() - l} ms"

        Long totalCost = System.currentTimeMillis() - begin
        //落地定时执行的日志
        l = System.currentTimeMillis()
        def timerName = 'UpdateUserAndLive'
        saveTimerLogs(timerName,totalCost)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  save timer_logs , cost  ${System.currentTimeMillis() - l} ms"

        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  ${UpdateUserAndLive.class.getSimpleName()}:costTotal:  ${System.currentTimeMillis() - begin} ms"

    }

    //落地定时执行的日志
    private static saveTimerLogs(String timerName,Long totalCost)
    {
        def timerLogsDB = M.getDB("xyrank").getCollection("timer_logs")
        def tmp = System.currentTimeMillis()
        def id = timerName + "_"  + new Date().format("yyyyMMdd")
        def update = new BasicDBObject(timer_name:timerName,cost_total:totalCost,cat:'minute',unit:'ms',timestamp:tmp)
        timerLogsDB.findAndModify(new BasicDBObject('_id',id), null, null, false,new BasicDBObject('$set',update),true, true)


    }

    static final String vistor_key = "web:ttxiuvistor:counts"
    static final Integer VISITOR_RATIO = 2
    Integer roomUserCount() {

        long now = System.currentTimeMillis()
        int i = 0
        Long vCount = 0
        def roomWithCount = new HashMap<Integer, Long>()
        def liveRooms = rooms.find(new BasicDBObject(timestamp:[$gte:now - 2 * DAY_MILLON], test: [$ne: true] ), new BasicDBObject(_id:1, live:1)).toArray()
        //println "roomUserCount query total time cost : ${System.currentTimeMillis() - now}"
        //long n = System.currentTimeMillis()
        liveRooms.each { dbo -> //,status:Boolean.TRUE
            i++
            Integer room_id = dbo.get("_id") as Integer
            Boolean live = dbo.get("live") as Boolean
            Long room_count =  (userRedis.scard("room:${room_id}:users".toString())?:0)
            Long visiter_count = room_count * VISITOR_RATIO
            rooms.update(new BasicDBObject("_id", room_id),
                    new BasicDBObject('$set', new BasicDBObject("visiter_count",visiter_count))
            )
            vCount += room_count
            if(live){
                roomWithCount.put(room_id,room_count)
            }
        }
        //println "roomUserCount update time cost : ${System.currentTimeMillis() - n}"
        redis.set(vistor_key, vCount.toString())
        recordRoomCount(roomWithCount)
        return i
    }

    //记录直播间人气
    private void recordRoomCount(HashMap<Integer, Long> roomWithCount){
        try{
            if(roomWithCount.size() == 0) return ;

            String date = new Date().format("yyyyMMdd")
            String room_user_count_key = "live:room:user:count:${date}".toString();
            String room_user_time_key = "live:room:user:count:times:${date}".toString();
            if(liveRedis.hsetnx(room_user_count_key, date, '1') == 1){
                liveRedis.expire(room_user_count_key, 2 * DAY_SEC)
            }
            if(liveRedis.hsetnx(room_user_time_key, date, '1') == 1){
                liveRedis.expire(room_user_time_key, 2 * DAY_SEC)
            }
            roomWithCount.each {Integer room_id, Long room_count ->
                liveRedis.hincrBy(room_user_count_key, room_id.toString(), room_count)
                liveRedis.hincrBy(room_user_time_key, room_id.toString(), 1)
            }
        }catch (Exception e){
            println "recordRoomCount Exception : ${e.getMessage()}";
        }

    }

    def liveCheck()
    {
        rooms.find(new BasicDBObject('live',true),new BasicDBObject(live_id:1,timestamp:1,type:1,mic_user:1,mic_live_id:1, live_type:1)).toArray().each {room->
            def roomId = room.get("_id")
            String live_id = room.get("live_id")
            Integer mic_user = room.get("mic_user") as Integer
            String mic_live_id = room.get("mic_live_id") as String
            Integer live_type = room?.get("live_type") as Integer
            Integer type = (room.get("type") ?: 0) as Integer
            if (!userRedis.exists("room:${roomId}:live") ){ //断线啦 减一分钟
                Long l  = Math.max(System.currentTimeMillis() - 60000 , (room['timestamp'] ?: 0) as Long) +1
                if(logRoomEdit.update(
                        new BasicDBObject(type:"live_on",data:live_id,room:roomId),
                        new BasicDBObject('$set':[etime:l,status: 'LiveStatusCheck'])).getN() == 1)
                {
                    try
                    {
                        delLiveRedis(roomId)
                        clearSong(roomId, live_id)
                        clearLabel(roomId,live_id)
                        resetMicUserStatus(roomId, mic_user, mic_live_id)
                        logRoomEdit.save(new BasicDBObject(type:'live_off',room:roomId,data:live_id,live_type:live_type,status: 'LiveStatusCheck',timestamp:l))
                    }
                    finally
                    {
                        def set =new BasicDBObject(live:false,live_id:'',timestamp:l,live_end_time:l, mic_user:0, mic_live_id:'', position: null, pull_urls:null)
                        //家族房间
                        if(type.equals(2)){
                            set.append('xy_star_id', null)
                        }
                        rooms.update(new BasicDBObject(_id:roomId,live:Boolean.TRUE),
                                new BasicDBObject('$set',set))
                        chatRedis.publish("ROOMchannel:${roomId}",'{"action": "room.live","data_d":{"live":false}}')
                    }
                }
            }
            //用户连麦心跳
            if(mic_user != null && mic_user > 0){
                String mic_status_key = "live:user:mic:status:"+mic_user;
                if (!liveRedis.exists(mic_status_key) || liveRedis.ttl(mic_status_key) <= 0 ){
                    resetMicUserStatus(roomId, mic_user, mic_live_id)
                }
            }

        }
    }

    def boxliveCheck()
    {
        try{
            boxes.find(new BasicDBObject(["status":2, $or:[[mic_first:[$gt:0]],[mic_sec:[$gt:0]]]]),new BasicDBObject(mic_sec:1,mic_first:1)).toArray().each {box->
                def boxId = box.get("_id") as Integer
                Integer mic_first_id = box.get("mic_first") as Integer
                Integer mic_sec_id = box.get("mic_sec") as Integer
                cleanMicUser('mic_first',mic_first_id, boxId);
                cleanMicUser('mic_sec',mic_sec_id, boxId);
            }
        }catch(Exception e){
          println e
        }
    }


    private static void cleanMicUser(String mic, Integer uid, Integer boxId){
        if(uid > 0){
            if (!redis.exists("box:${boxId}:live:${uid}")){
                def nick_name = users.findOne(new BasicDBObject(_id:uid), new BasicDBObject(nick_name:1))?.get('nick_name')
                boxes.update(new BasicDBObject(_id:boxId).append(mic,uid), new BasicDBObject($set :new BasicDBObject(mic, 0)))
                //清除连麦模式状态
                liveRedis.del("box:${boxId}:micFirst")
                //推送
                chatRedis.publish("BOXchannel:${boxId}",'{"action": "mic.off","data_d":{"mic":"'+mic+'","box_id":'+boxId+',"user_id":'+uid+', "nick_name":"'+nick_name+'"}}')
            }
        }
    }

    private  void  delLiveRedis(def roomId)
    {
        try
        {
            liveRedis.keys("live:${roomId}:*".toString()).each{liveRedis.del(it)};
            println "${new Date().format('yyyy-MM-dd HH:mm:ss')} liveRedis DEL live:${roomId}:*"
        }
        catch(e)
        {
            e.printStackTrace()
        }
    }

    private void clearSong(def roomId, String live_id)
    {
        songs.find(//清算未处理歌曲
                new BasicDBObject(room_id:roomId,live_id:live_id,status:3),
                new BasicDBObject(xy_user_id:1,cost:1)
        ).toArray().each{song->
            def song_id = song.get("_id")
            if(1 ==songs.update(new BasicDBObject("_id":song_id,status:3, room_id: roomId),
                    new BasicDBObject('$set', new BasicDBObject(status:4, lastModif: new Date(),last_update:System.currentTimeMillis()))).getN())
            {
                def xy_user_id = song.get("xy_user_id")
                users.update(new BasicDBObject('_id',xy_user_id),
                        new BasicDBObject('$inc',new BasicDBObject("finance.coin_count",song.get("cost"))))
            }
        }
    }

    private void clearLabel(def roomId, String live_id)
    {
        labels.find(//清算未处理求爱签
                new BasicDBObject(room_id:roomId,live_id:live_id,status:['$in':[0,3]]),
                new BasicDBObject(xy_user_id:1,cost:1)
        ).toArray().each{label->
            def label_id = label.get("_id")
            if(1 ==labels.update(new BasicDBObject("_id":label_id, room_id: roomId),
                    new BasicDBObject('$set', new BasicDBObject(status:4, lastModif: new Date(),last_update:System.currentTimeMillis()))).getN())
            {
                def xy_user_id = label.get("xy_user_id")
                users.update(new BasicDBObject('_id',xy_user_id),
                        new BasicDBObject('$inc',new BasicDBObject("finance.coin_count",label.get("cost"))))
            }
        }
    }

    //连麦用户心跳检测
    def micUserliveCheck()
    {
        rooms.find(new BasicDBObject(mic_user : [$gt: 0]),new BasicDBObject(mic_user:1,mic_live_id:1)).toArray().each {room->
            def roomId = room.get("_id")
            Integer mic_user = room.get("mic_user") as Integer
            String mic_live_id = room.get("mic_live_id") as String
            //用户连麦心跳
            if(mic_user != null && mic_user > 0){
                String mic_status_key = "live:user:mic:status:"+mic_user;
                if (!liveRedis.exists(mic_status_key) || liveRedis.ttl(mic_status_key) <= 0 ){
                    resetMicUserStatus(roomId, mic_user, mic_live_id)
                }
            }
        }
    }

    //清理连麦用户
    private void resetMicUserStatus(def roomId, Integer mic_user, String live_id){
        if(mic_user != null && mic_user > 0){
            def set = new BasicDBObject(mic_user:0, mic_live_id:'')
            rooms.update(new BasicDBObject(_id:roomId, mic_user:mic_user), new BasicDBObject('$set',set))
            liveRedis.del("live:user:mic:status:"+mic_user)
            if(live_id != null && live_id != '')
                room_mic_log.update($$(live_id : live_id), $$($set:[live_end_time: System.currentTimeMillis()]))

            chatRedis.publish("ROOMchannel:${roomId}",'{"action": "mic.user_off","data_d":{"user_id":'+mic_user+',"room_id":'+roomId+'}}')
        }
    }

    long WAIT = 30*1000L
    //失败事务处理 30 秒
    def userTranCheck(String field,DBCollection collection){
        users.find(new BasicDBObject(field+'.timestamp',[$lt:System.currentTimeMillis() - WAIT]),new BasicDBObject(field,1))
                .limit(50).toArray().each {user->
            List logs = user.get(field) as List
            def uid = new BasicDBObject('_id',user.get('_id'))
            if (logs){
                logs.each {DBObject log->
                    def _id = log.get('_id')
                    collection.save(log)
                    users.update(uid,new BasicDBObject('$pull',new BasicDBObject(field,[_id:_id])))
                    println "clean ${field} : ${log}"
                }
            }
        }
    }


    def songTranCheck(){
        users.find(new BasicDBObject('order_songs.timestamp',[$lt:System.currentTimeMillis() - WAIT]),new BasicDBObject('order_songs',1))
                .limit(50).toArray().each {user->
            List  order_songs = user.get('order_songs') as List
            def uid = new BasicDBObject('_id',user.get('_id'))
            if (order_songs){
                order_songs.each {DBObject song->
                    def _id = song.get('_id')

                    def live_id= song.get('live_id')
                    def room_id= song.get('room_id')

                    //检查是否还在直播
                    if (rooms.count(new BasicDBObject('_id',room_id).append('live_id',live_id)) == 1){
                        songs.save(song)
                        users.update(uid,new BasicDBObject('$pull',[order_songs:[_id:_id]]))
                        println "clean order_songs : ${song}"
                    }else{// 退钱
                        Integer cost = song.get('cost')
                        users.update(uid,new BasicDBObject('$pull',[order_songs:[_id:_id]])
                                .append('$inc',["finance.coin_count":cost]))
                        println "rollback order_songs : ${song}"
                    }

                }
            }
        }
    }


    def vipCheck(){
        users.updateMulti(new BasicDBObject('vip_expires',[$lt:System.currentTimeMillis()]),
                new BasicDBObject('$unset',[vip_expires:1,vip:1,vip_hiding:1]))
    }


    def recommendCheck(){
        def expire_query = new BasicDBObject('is_recomm' : Boolean.TRUE, 'expires' : [$lt:System.currentTimeMillis()])
        star_recommends.updateMulti(expire_query, new BasicDBObject('$set',[expires:null,is_recomm:Boolean.FALSE]))

    }

    static final Double rate = 0.99d
    def rewardCheck(){
        //已接单更新为完成状态
        def expire_query = new BasicDBObject(status : 2, 'expires' : [$lt:System.currentTimeMillis()])
        rewards.updateMulti(expire_query, new BasicDBObject('$set',[status:3]))
        //15分钟内未接单订单 退还90%金额 更新为取消状态
        def query = new BasicDBObject(status : 1, 'expires' : [$lt:System.currentTimeMillis()])
        rewards.find(query,new BasicDBObject('user_id':1, cost:1)).limit(50).toArray().each {reward ->
            String _id = reward['_id']
            Integer cost = reward['cost'] as Integer
            Integer user_id = reward['user_id'] as Integer
            Integer return_coin = (cost * rate) as Integer
            //println  " rewards find : ${user_id}  cost: ${cost} "
            if(rewards.update(new BasicDBObject(_id : _id, status : 1), new BasicDBObject('$set',[status:4, return_coin:return_coin])).getN() == 1){
                //println  " rewards set status: ${user_id}  return_coin: ${return_coin} "
                users.update(new BasicDBObject('_id',user_id), new BasicDBObject('$inc',new BasicDBObject("finance.coin_count",return_coin)))
            }

        }
    }

    def tmpRoomCheck(){
        def expire_query = $$('love_expires' : [$lt:System.currentTimeMillis()])
        def roomIds = room_candidates.find(expire_query, $$(_id : 1)).limit(10).toArray()*._id
        if(roomIds != null && roomIds.size() > 0){
            def updateQuery = $$(_id: [$in:roomIds])
            rooms.updateMulti(updateQuery, new BasicDBObject('$set',[temp : Boolean.TRUE]))
            room_candidates.remove(updateQuery)
        }
    }

    //检测间隔速率  0 挡为慢速， 1 挡为快速检测挡
    static final Integer speed = 0;
    def delayOrderCheck(){
        HttpURLConnection conn = null;
        def jsonText = "";
        try{
            def api_url = new URL(api_domain+"pay/delay_order_fix?speed=${speed}".toString())
            conn = (HttpURLConnection)api_url.openConnection()
            conn.setRequestMethod("GET")
            conn.setDoOutput(true)
            conn.setConnectTimeout(TIME_OUT);
            conn.setReadTimeout(TIME_OUT);
            conn.connect()
            jsonText = conn.getInputStream().getText("UTF-8")
        }catch (Exception e){
            println "staticWeek Exception : " + e;
        }finally{
            if (conn != null) {
                conn.disconnect();
                conn = null;
            }
        }
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')} result : ${jsonText}"
    }
    /**
     * 包厢人数同步
     * @return
     */
    Integer boxUserCount() {
        int i = 0
        boxes.find(new BasicDBObject('status',2)).toArray().each { dbo -> //,status:Boolean.TRUE
            i++
            Integer box_id = dbo.get("_id") as Integer
            Long box_count =  (redis.scard("box:${box_id}:users".toString())?:0)
            boxes.update(new BasicDBObject("_id", box_id),
                    new BasicDBObject('$set', new BasicDBObject("visitor_count",box_count))
            )
        }
        return i
    }

    /**
     * 包厢过期续费
     * @return
     */
    def boxCheck(){
        boxes.updateMulti(new BasicDBObject('expires',[$lt:System.currentTimeMillis()]).append('status',2),
                new BasicDBObject('$set',[status:3]))
    }

    /**
     * 小窝直播心跳检测
     * @return
     */
    def nestliveCheck()
    {
        try{
            nest.find(new BasicDBObject(["status":2, $or:[[mic_first:[$gt:0]],[mic_sec:[$gt:0]]]]),new BasicDBObject(mic_sec:1,mic_first:1)).toArray().each {nest->
                def nestId = nest.get("_id") as Integer
                Integer mic_first_id = nest.get("mic_first") as Integer
                Integer mic_sec_id = nest.get("mic_sec") as Integer
                if(mic_first_id > 0){
                    cleanNestMicUser(1, 'mic_first', mic_first_id, nestId);
                }
                if(mic_sec_id > 0) {
                    cleanNestMicUser(2, 'mic_sec', mic_sec_id, nestId);
                }
            }
        }catch(Exception e){
            println e
        }
    }


    private static void cleanNestMicUser(Integer mic, String mic_field,Integer uid, Integer nestId){
        if(uid > 0){
            if (!redis.exists("nest:${nestId}:live:${uid}")){
                def user = users.findOne(new BasicDBObject(_id:uid), new BasicDBObject(nick_name:1,pic:1,"finance.coin_spend_total": 1))
                nest.update(new BasicDBObject(_id:nestId).append(mic_field,uid), new BasicDBObject($set :new BasicDBObject(mic_field, null)))
                //推送
                chatRedis.publish("WO:CHANNEL::${nestId}",'{"action": "mic.off","data_d":{"mic":"'+mic+'","nest_id":'+nestId+',"timestamp":'+System.currentTimeMillis() +', "user":"'+user+'"}}')
            }
        }
    }

    /**
     * 小窝人数同步
     * @return
     */
    def Integer nestUserCount() {
        int i = 0
        nest.find(new BasicDBObject('status',2)).toArray().each { dbo -> //,status:Boolean.TRUE
            i++
            Integer nest_id = dbo.get("_id") as Integer
            Long box_count =  (redis.scard("nest:${nest_id}:users".toString())?:0)
            nest.update(new BasicDBObject("_id", nest_id),
                    new BasicDBObject('$set', new BasicDBObject("visitor_count",box_count))
            )
        }
        return i
    }



    /**
     * 守护失效检测
     * @return
     */
    def guardCheck()
    {
        try{
            def expire_users = guard_users.find(new BasicDBObject('expire':[$lt:System.currentTimeMillis()], status:Boolean.TRUE),
                                    new BasicDBObject(room:1,user_id:1)).toArray()
            if(expire_users != null && expire_users.size() > 0){
                expire_users.each {guard->
                    String roomId= guard.get("room") as String
                    String user_id = guard.get("user_id") as String
                    def redisKey =  "room:guarder:${roomId}".toString()
                    userRedis.srem(redisKey, user_id)
                    userRedis.del("guard:car:${roomId}:${user_id}".toString())
                }
                guard_users.updateMulti(new BasicDBObject('expire':[$lt:System.currentTimeMillis()], status:Boolean.TRUE),
                        new BasicDBObject('$set',[status:Boolean.FALSE,last_rank:9999]))
            }

        }catch(Exception e){
            println e
        }
    }

    /**
     * PK游戏清理
     * @param field
     * @param collection
     * @return
     */
    def pkGameClean(){
        pk_logs.find(new BasicDBObject(status : 3,'expire':[$lte:System.currentTimeMillis()]),new BasicDBObject(f_id:1,t_id:1))
                .limit(50).toArray().each {pklog->
            String _id = pklog.get('_id') as String
            String f_id = pklog.get('f_id') as String
            String t_id = pklog.get('t_id') as String
            redis.srem("pk:stars", f_id)
            redis.srem("pk:stars", t_id)
            pk_logs.update(new BasicDBObject(_id:_id), new BasicDBObject('$set',[status:5,timeout:System.currentTimeMillis()]))
            //推送
            chatRedis.publish("ROOMchannel:${f_id}",'{"action": "pk.timeout","data_d":{"room_id":"'+f_id+'"}}')
            chatRedis.publish("ROOMchannel:${t_id}",'{"action": "pk.timeout","data_d":{"room_id":"'+t_id+'"}}')
        }
    }

    /**
     * 用户封禁到期自动解封
     */
    def auto_unfreeze(){
        users.update($$("status":false, unfreeze_time:[$lte:System.currentTimeMillis()])
                ,$$('$set':$$(status:true)).append('$unset', $$("unfreeze_time",1)), false, true);
    }

    public static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    public static BasicDBObject $$(Map map) {
        return new BasicDBObject(map)
    }


}
