#!/usr/bin/env groovy
package tmp

@GrabResolver(name = 'restlet', root = 'http://210.22.151.242:8081/nexus/content/groups/public')
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
        @Grab('com.ttpod:https-util:1.0'),
])
import com.mongodb.*
import groovy.json.JsonBuilder
import groovy.json.JsonSlurper
import org.apache.commons.lang.StringUtils
import redis.clients.jedis.Jedis
import com.https.HttpsUtil
import java.text.SimpleDateFormat


import groovy.json.JsonSlurper
import org.apache.commons.lang.StringUtils
import java.security.MessageDigest
import com.https.HttpsUtil
import com.mongodb.BasicDBObject
import com.mongodb.DBObject
import com.mongodb.Mongo
import com.mongodb.MongoURI
import groovy.json.JsonBuilder
class tmp {

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

    static final String jedis_host = getProperties("main_jedis_host", "192.168.31.249")
    static final String chat_jedis_host = getProperties("chat_jedis_host", "192.168.31.249")
    static final String live_jedis_host = getProperties("live_jedis_host", "192.168.31.249")

    static final Integer main_jedis_port = getProperties("main_jedis_port",6379) as Integer
    static final Integer chat_jedis_port = getProperties("chat_jedis_port",6379) as Integer
    static final Integer live_jedis_port = getProperties("live_jedis_port",6379) as Integer

    static mongo  = new Mongo(new MongoURI(getProperties('mongo.uri','mongodb://192.168.31.249:27017/?w=1') as String))
    static historyMongo = new Mongo(new MongoURI(getProperties('mongo_history.uri', 'mongodb://192.168.31.246:27017/?w=1') as String))
    static historyDB = historyMongo.getDB('xylog_history')
    static mainRedis = new Jedis(jedis_host,main_jedis_port)



    def final Long DAY_MILL = 24*3600*1000L
    static DAY_MILLON = 24 * 3600 * 1000L
    static finance_log = mongo.getDB('xy_admin').getCollection('finance_log')
    static config = mongo.getDB('xy_admin').getCollection('config')
    static ops = mongo.getDB('xy_admin').getCollection('ops')
    static medals = mongo.getDB('xy_admin').getCollection('medals')
    static applys = mongo.getDB('xy_admin').getCollection('applys')
    static exchange_log = mongo.getDB('xylog').getCollection('exchange_log')
    static star_recommends = mongo.getDB('xy_admin').getCollection('star_recommends')
    static channel_pay = mongo.getDB('xy_admin').getCollection('channel_pay')
    static users = mongo.getDB('xy').getCollection('users')
    static rooms = mongo.getDB('xy').getCollection('rooms')
    static day_login = mongo.getDB('xylog').getCollection('day_login')
    static lottery_logs = mongo.getDB('xylog').getCollection('lottery_logs')
    static weekstar_bonus_logs  = mongo.getDB('xylog').getCollection('weekstar_bonus_logs')
    static app_info = mongo.getDB('xylog').getCollection('app_info')
    static forbidden_logs = mongo.getDB('xylog').getCollection('forbidden_logs')
    static xy_user = mongo.getDB('xy_user').getCollection('users')
    static guard_users = mongo.getDB('xylog').getCollection('guard_users')
    static day_login_history = historyDB.getCollection('day_login_history')
    static lottery_logs_history  = historyDB.getCollection('lottery_logs_history')
    static room_cost_2014  = historyDB.getCollection('room_cost_2014')
    static room_cost_2015  = historyDB.getCollection('room_cost_2015')
    static room_cost_2016  = historyDB.getCollection('room_cost_2016')
    static DBCollection room_cost_DB =  mongo.getDB("xylog").getCollection("room_cost")
    static DBCollection order_logs =  mongo.getDB("xylog").getCollection("order_logs")
    static DBCollection room_feather =  mongo.getDB("xylog").getCollection("room_feather")
    static DBCollection week_stars =  mongo.getDB("xyrank").getCollection("week_stars")
    static channels = mongo.getDB('xy_admin').getCollection('channels')
    static stat_daily = mongo.getDB('xy_admin').getCollection('stat_daily')
    static room_cdn = mongo.getDB("xy_union").getCollection('room_cdn')
    static long zeroMill = new Date().clearTime().getTime()
    static Long yesTday = zeroMill - DAY_MILLON


    static String replace_from = "img.99yjn.";
    static String replace_to = "img.17xunzhao.";

    static replaceImg(String db, String colName){
        try{
            def col = mongo.getDB(db).getCollection(colName)
            col.find().toArray().each {DBObject obj ->
                Boolean flag = Boolean.FALSE
                def update = new BasicDBObject();
                obj.keySet().each {String key ->
                    def val = obj.get(key)
                    if(val.toString().contains(replace_from)){
                        flag = Boolean.TRUE
                        println val
                        String newval = val.toString().replace(replace_from, replace_to)
                        newval = newval.toString().replace(replace_from, replace_to)
                        update.append(key, newval)
                    }
                }
                if(flag){
                    col.update(obj, new BasicDBObject('$set',update))
                }

            }
        }catch (Exception e){
            println "replaceImg Exception :" + e
        }

    }
    static userReplace(){
        def pageSize = 500
        def page = 1
        def countRows = users.count(new BasicDBObject(via:[$ne:"robot"]))
        def allPage = (int) ((countRows + pageSize - 1) / pageSize);
        println allPage
        while (page <= allPage && page < pageSize) {
            def user_list = users.find(new BasicDBObject(via:[$ne:"robot"]), new BasicDBObject('_id':1,'pic':1)).skip((page - 1) * pageSize).limit(pageSize).toArray()
            user_list.each {DBObject obj ->
                String pic = (obj?.get('pic') ?: "") as String
                Integer _id = obj.get('_id') as Integer
                if(pic != null && pic.contains(replace_from)){
                    println pic
                    String newval = pic.toString().replace(replace_from, replace_to)
                    newval = newval.toString().replace(replace_from, replace_to)
                    users.update(new BasicDBObject('_id',_id), new BasicDBObject('$set',new BasicDBObject('pic',newval)))
                }
            }
            user_list = null;
            Thread.sleep(1000)
            ++page;
        }
    }

    static userBatchReplace(){
        DBCursor cur = users.find(new BasicDBObject(), new BasicDBObject('_id':1,'pic':1)).batchSize(100000);
        while (cur.hasNext()){
            DBObject obj = cur.next()
            String pic = obj?.get('pic') as String
            Integer _id = obj.get('_id') as Integer
            if(pic.toString().contains("img.2339.") || pic.toString().contains(".2339.") ){
                String newval = pic.toString().replace("img.2339.", 'img.sumeme.')
                users.update(new BasicDBObject('_id',_id), new BasicDBObject('$set',new BasicDBObject('pic',newval)))
            }
        }
        cur.close();
    }

    static userUnionBatchReplace(){
        DBCursor cur = users.find(new BasicDBObject(priv:2), new BasicDBObject('_id':1,'union_pic':1)).batchSize(100000);
        while (cur.hasNext()){
            DBObject obj = cur.next()
            Integer _id = obj.get('_id') as Integer
            Map union_pic = obj?.get('union_pic') as Map
            if(union_pic){
                def pic  = union_pic.get('bd')
                List bds  = union_pic.get('bds') as List
                List newbds  = new ArrayList()
                bds.each {String db_pic ->
                    if(db_pic.toString().contains("img.2339.") || db_pic.toString().contains(".2339.") ){
                        String newval = db_pic.toString().replace("img.2339.", 'img.sumeme.')
                        newbds.add(newval)
                    }
                }
                if(newbds.size() > 0)
                    union_pic.put('bds',newbds)
                if(pic.toString().contains("img.2339.") || pic.toString().contains(".2339.") ){
                    String newval = pic.toString().replace("img.2339.", 'img.sumeme.')
                    union_pic.put('bd',newval)
                    println union_pic
                    users.update(new BasicDBObject('_id',_id), new BasicDBObject('$set',new BasicDBObject('union_pic':union_pic)))
                }
            }

        }
        cur.close();
    }

    static roomTypeUpdate(){


        rooms.updateMulti(new BasicDBObject('type':null), new BasicDBObject('$set',new BasicDBObject('type',1)));
    }


    static removeRoomPic(){
        mongo.getDB('xylog_history').getCollection('room_20150813').insert(rooms.find().toArray())
        rooms.updateMulti(new BasicDBObject('live',new BasicDBObject('$ne',true)),new BasicDBObject('$set',new BasicDBObject('pic_url',"")))
    }

    static recover(){
        /*mongo.getDB('xylog_history').getCollection('room_20150813').find(new BasicDBObject('_id',new BasicDBObject('$gt',1))).toArray().each {DBObject room ->
            def id = room['_id'] as Integer
            def pic_url = room['pic_url'] as String
            rooms.update(new BasicDBObject(_id:id), new BasicDBObject('$set', new BasicDBObject('pic_url',pic_url)))
        }*/
        rooms.insert(mongo.getDB('xylog_history').getCollection('room_20150813').find().toArray())
    }

    static testRoomcostAggregate(){
        def xylog = mongo.getDB("xylog")
        def room_cost_DB =  xylog.getCollection("room_cost")
        room_cost_DB.setReadPreference(ReadPreference.secondaryPreferred())
        def res = room_cost_DB.aggregate(
                new BasicDBObject('$match', new BasicDBObject(timestamp:[$gte: System.currentTimeMillis() - 30 * DAY_MILLON])),
                new BasicDBObject('$project', [_id: '$session._id',earned:'$session.data.earned',cost:'$star_cost']),
                new BasicDBObject('$group', [_id: '$_id', num: [$sum: '$earned'], cost: [$sum: '$cost']]),
                new BasicDBObject('$sort', [num:-1]),
                new BasicDBObject('$limit',20000) //top N 算法
        )
        Iterator objs = res.results().iterator()
        println objs.size()
    }

    static testRoomcostArray(){
        def xylog = mongo.getDB("xylog")
        def room_cost_DB =  xylog.getCollection("room_cost")
        room_cost_DB.setReadPreference(ReadPreference.secondaryPreferred())
        def result = room_cost_DB.find(new BasicDBObject(timestamp:[$lt:  System.currentTimeMillis() - 30 * DAY_MILLON])).toArray()
        println result.size()
    }

    static testRoomCostBatch(){
        room_cost_DB.setReadPreference(ReadPreference.secondary())
        def cursor = room_cost_DB.find(new BasicDBObject(timestamp:[$lt: System.currentTimeMillis() - 30 * DAY_MILLON])).batchSize(20000)
        println cursor.toArray().size();

        /*while (cursor.hasNext()) {
           def obj = cursor.next()
            //println obj
           //println cursor.toArray().size();
        }*/
    }

    static statisticCharge() {
        def userIds = finance_log.distinct("user_id", $$(timestamp: [$gte: 1453953600000, $lt:1454428740000], coin :[$gte:1000], via: [$ne: 'Admin'])).collect{it as Integer}
        println userIds.size()
        def old_count = finance_log.distinct("user_id", $$(timestamp: [$lt: 1453953600000], via: [$ne: 'Admin'], user_id: [$in: userIds]))
        println old_count.size()
        println userIds.size() - old_count.size()
    }
    public static BasicDBObject $$(String key,Object value){
        return new BasicDBObject(key,value);
    }

    public static BasicDBObject $$(Map map){
        return new BasicDBObject(map);
    }

    public static statisticFirstPayUser(Long gteMill){
        println " "
        def channel_pc = [] as List
        //查询pc渠道id
        channels.find(new BasicDBObject(), new BasicDBObject(_id: 1, client: 1)).toArray().each { BasicDBObject obj ->
            def id = obj.get('_id') as String
            def client = obj.get('client') as String
            if ('1'.equals(client)) {
                channel_pc.add(id)
            }
        }
        println "----------${new Date(gteMill).format("yyyy-MM-dd HH:mm:ss")}----------"
        channel_pc.each {String channel ->
            List userIds  = finance_log.distinct('to_id', $$(via: [$ne: 'Admin'], qd: channel, timestamp: [$gte: gteMill, $lt: gteMill + DAY_MILLON]))

            def firstPayUser = new HashSet();
            def alreadyPayUser = new HashSet();
            finance_log.find(new BasicDBObject('via': [$ne: 'Admin'],
                    $or: [[user_id: [$in: userIds]], [to_id: [$in: userIds]]],
                    timestamp: [$lt: gteMill])).toArray().each { BasicDBObject obj ->
                def fid = obj.get('user_id') as Integer
                def tid = obj.get('to_id') as Integer
                if (fid != null) alreadyPayUser.add(fid)
                if (tid != null && tid != fid) alreadyPayUser.add(tid)
            }
            userIds.each {Integer uid ->
                if(!alreadyPayUser.contains(uid)) firstPayUser.add(uid)
            }
            if(firstPayUser.size() > 0)
                println "${channel} --> PCUserids : ${userIds.size()}  PcFirstPayUser : ${firstPayUser.size() }"
        }


       /* Long begin = yesTday
        def payed_user = new HashSet(5000)
        Integer firstTotal = 0
        stat_daily.find(new BasicDBObject(type: "allpay", timestamp: [$gte: begin - 29 * DAY_MILLON, $lt: begin + DAY_MILLON]), new BasicDBObject(timestamp:1,first_pay: 1))
                .toArray().each { BasicDBObject obj ->
            def uids = (obj.get('first_pay') as List) ?: []
            def timestamp = obj.get('timestamp') as Long
            //println "${new Date(timestamp).format("yyyy-MM-dd HH:mm:ss:SSS")} uids : ${uids.size()}"
            payed_user.addAll(uids)

            def firstPays = finance_log.count(new BasicDBObject('via': [$ne: 'Admin'],
                    $or: [[user_id: [$in: uids]], [to_id: [$in: uids]]],
                    qd: [$in : channel_pc], timestamp: [$lt: timestamp + DAY_MILLON]))
            firstTotal += firstPays
            //println "firstPays : ${firstPays}"
        }
        //println "payed_user : ${payed_user.size()}"
        //println "firstTotal : ${firstTotal}"
        PCfirstPayUser = finance_log.find(new BasicDBObject(via: [$ne: 'Admin'], to_id: [$in: payed_user], qd: [$in : channel_pc], timestamp: [$gte: begin - 29 * DAY_MILLON, $lt: begin + DAY_MILLON])).toArray()

        //println "PCfirstPayUser : ${PCfirstPayUser.size() }"*/
    }

    static void recommendRecover(){
        def config = config.findOne('index_star_recommend_list')
        List<Integer> star_ids = config?.get('stars') as List //新人推荐
        List<Integer> top_stars = config?.get('top_stars') as List //top推荐
        List<Integer> wait_stars = config?.get('wait_stars') as List //等待主播

        wait_stars.each {Integer star_id ->
            def info  = $$(_id : star_id, type: 1, is_recomm:Boolean.FALSE, timestamp : System.currentTimeMillis())
            star_recommends.update($$(_id : star_id), $$($set : info), true ,false)
        }
        def weeks =  '1,2,3,4,5,6,7'
        def day_begin = '00:00:00'  //每日开始时间段
        def begin = 000000  //每日开始时间段
        def day_end = '23:59:59'  //每日结束时间段
        def end = 235959  //每日结束时间段
        def dayOfweeks =  [1,2,3,4,5,6,7]
        Map weekFileds = new HashMap();
        dayOfweeks.each {Integer dayOfweek ->
            weekFileds.put(dayOfweek, $$(begin:begin as Integer, end:end as Integer))
        }

        star_ids.each {Integer star_id ->
            def info  = $$(_id : star_id, type: 2, is_recomm:Boolean.TRUE, timestamp : System.currentTimeMillis(),
                    weeks:weeks,day_begin:day_begin, day_end:day_end ,week:weekFileds)
            star_recommends.update($$(_id : star_id), $$($set : info), true ,false)
        }
        top_stars.each {Integer star_id ->
            def info  = $$(_id : star_id, type: 1, is_recomm:Boolean.TRUE, timestamp : System.currentTimeMillis(),
                    weeks:weeks,day_begin:day_begin, day_end:day_end ,week:weekFileds)
            star_recommends.update($$(_id : star_id), $$($set : info), true ,false)
        }
    }

    /**
     * 抽奖历史记录查询
     * @param userId
     */
    static void lottery_logs_history(Integer userId){
        def startStr = "2015-12-01 000000"
        def endStr = "2016-04-01 000000"
        def stime = 0L
        def etime = 0L
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HHmmss")
        try {
            stime = sdf.parse(startStr).getTime()
            etime = sdf.parse(endStr).getTime()
        } catch (Exception e) {
            println e
        }
        DBCursor cursor = lottery_logs_history.find($$(timestamp: ['$gte': stime, '$lt': etime])).batchSize(10000)
        while (cursor.hasNext()) {
            def lottery = cursor.next()
            Integer user_id = lottery['user_id'] as Integer
            if(userId.equals(user_id)){
                println lottery
            }
        }
    }

    static void howManyUserUnactive(){
        println "howManyUserUnactive 1000 : " + users.count($$('finance.coin_spend_total': [$lte:1000], 'last_login':[$lte:1435507200000]))
        println "howManyUserUnactive 15000: " + users.count($$('finance.coin_spend_total': [$lte:15000], 'last_login':[$lte:1435507200000]))
    }


    static void historyDataQuery(){
        def query = $$(timestamp:[$gte:1401552000000,$lt:1404144000000])
        println "send_gift 14 count : " + room_cost_2014.count(query)
        println "send_gift 15 count : " + room_cost_2015.count(query)
        println "send_gift 16 count : " + room_cost_2016.count(query)
        println "send_gift now count : " + room_cost_DB.count(query)
    }

    static void countBagGifts(){
        List<Integer> userIds = new ArrayList<>()
        DBCursor cursor = lottery_logs.find($$(active_name : 'app_meme_luck_gift', award_name:'118'), $$(user_id:1)).batchSize(10000)
        while (cursor.hasNext()) {
            def lottery = cursor.next()
            userIds.add(lottery['user_id'] as Integer)
        }

        DBCursor usersCursor = users.find($$(_id: [$in : userIds], 'bag.118':[$gt:0]), $$('bag.118':1, nick_name:1)).batchSize(5000)
        while (usersCursor.hasNext()) {
            def user = usersCursor.next()
            Map bag = user["bag"] as Map
            println "user : ${user['_id']}  huoju : ${bag['118']}"
        }
    }

    static void weekstar_bonus_logs_modify(){
        weekstar_bonus_logs.find().toArray().each {DBObject log ->
            Long timstamp = log.removeField('timstamp') as Long
            log.put('timestamp', timstamp)
            weekstar_bonus_logs.save(log)
        }

    }

    static Map<Integer, List> diff_users = new HashMap();
    static findRoomcostTopUserAggregate(Integer star_id){
        println "<<----------------------------- ${star_id} -------------------------->>>"
        def xylog = mongo.getDB("xylog")
        def room_cost_DB =  xylog.getCollection("room_cost")
        room_cost_DB.setReadPreference(ReadPreference.secondaryPreferred())
        def res = room_cost_DB.aggregate(
                new BasicDBObject('$match', new BasicDBObject(type:'send_gift', 'session.data._id' : 118, 'session.data.xy_star_id' : star_id,
                                                                    timestamp:[$gte: System.currentTimeMillis() - 15 * DAY_MILLON])),
                new BasicDBObject('$project', [_id: '$session._id',count:'$session.data.count']),
                new BasicDBObject('$group', [_id: '$_id', count: [$sum: '$count']]),
                new BasicDBObject('$sort', [count:-1]),
                new BasicDBObject('$limit',500) //top N 算法
        )
        Iterator objs = res.results().iterator()
        Map<String, List> result = new HashMap();
        while (objs.hasNext()){
            def user = objs.next()
            //println user['_id'] +" : "+ user['count'] + " ip : "+ getDayLoginIp(user['_id'] as Integer)
            Integer userId = user['_id'] as Integer
            Integer count = user['count'] as Integer
            def ip = getDayLoginIp(userId)
            List users = result.get(ip) ?: new ArrayList<>();
            String u = userId +" : "+ count
            users.add(u)
            result.put(ip, users)

            List rooms = diff_users.get(userId) ?: new ArrayList<>();
            rooms.add(star_id +" : "+ count)
            diff_users.put(userId, rooms)
            println userId +" : "+ count
        }
        println "<<-----------------------------ip and user-------------------------->>>"
        result = result.sort {a,   b ->
            def counta = (a.value as List).size(), countb = (b.value as List).size()
            -(counta <=> countb)
        }
        result.each {key, List val ->
            println key + " : size : " + val.size() + " " + val;
        }
    }

    static String getDayLoginIp(Integer uid){
        def login =  day_login.find($$(user_id : uid), $$(ip : 1)).sort($$(timestamp : -1)).limit(1).toArray()
        if(login.size() == 0 || login == null) return "";
        return StringUtils.split(login[0]['ip'].toString(), ",")[0]
    }

    static String ACTIVE_NAME = "Warcraft2016";
    static awardExp(Integer uid, Long exp){
        def user_info = users.findAndModify(new BasicDBObject('_id',uid),
                new BasicDBObject('$inc',new BasicDBObject('finance.coin_spend_total',exp)))

        Integer userId = user_info.get('_id') as Integer
        def nickName = user_info.get('nick_name') as String
        def lotteryId = userId + "_" + ACTIVE_NAME + "_" + System.currentTimeMillis()
        saveLotteryLog(lotteryId, ACTIVE_NAME, userId, null, nickName, "exp_"+exp.toString(), 0, 0, 0)
    }

    public static Boolean saveLotteryLog(String lotteryId, String active_name, Integer userId, Integer star_id, String nickName,
                                         String award_name, int award_count,  long award_coin, int cost_coin){
        Long time = System.currentTimeMillis();
        Map<Object,Object> obj = new HashMap<Object,Object>();
        obj.put('_id', lotteryId);
        obj.put("user_id", userId);
        obj.put("star_id", star_id);
        obj.put("nick_name", nickName);
        obj.put("award_name", award_name);
        obj.put("award_count", award_count);
        obj.put("timestamp", time);
        obj.put("active_name", active_name);
        obj.put("cost_coin", cost_coin);
        obj.put("award_coin", award_coin);
        lottery_logs.save(new BasicDBObject(obj))
    }


    // 渠道激活信息
    static fetchTalkingData(int i) {
        try {
            def gteMill = yesTday - i * DAY_MILLON
            def date = new Date(gteMill)
            def dateStr = date.format('yyyy-MM-dd')
            def coll = mongo.getDB('xy_admin').getCollection('stat_channels')
            //设置请求json
            def builder = new JsonBuilder()
            //查询所有渠道信息
            def channellist = 'https://api.talkingdata.com/metrics/app/v1/channellist'
            def queryContent = [
                    accesskey: '94c8954c3168b6d7adc9bcdcbf3fdf74',
                    filter   : [// *数据筛选条件
                                platformids: [1, 2]
                    ]
            ]
            builder.setContent(queryContent)
            def response = HttpsUtil.postMethod(channellist, builder.toString())
            def channel = [:]
            println "channelid >>>>>>>>>>"
            println response
            if (StringUtils.isNotBlank(response)) {
                def respMap = new JsonSlurper().parseText(response) as Map
                if (respMap && 200 == respMap.get('status')) {
                    def channels = respMap.get('result') as List
                    channels.each { Map obj ->
                        channel.put(obj.get('channelid') as String, obj.get('channelname') as String)
                    }
                }
            }

            if (channel.size() > 0) {
                def queryUrl = "https://api.talkingdata.com/metrics/app/v1"
                //查询所有渠道激活信息
                queryContent.put("metrics", ['newuser'])
                queryContent.put("groupby", 'channelid')
                queryContent.put("filter", [
                        start: "${dateStr}".toString(),// *查询时期的起始日
                        end  : "${dateStr}".toString(),// *查询时期的截止日
                ])
                builder.setContent(queryContent)
                response = HttpsUtil.postMethod(queryUrl, builder.toString())
                println "newuser response>>>>>>>>>>"
                println response
                if (StringUtils.isNotBlank(response)) {
                    def respMap = new JsonSlurper().parseText(response) as Map
                    if (respMap && 200 == respMap.get('status')) {
                        def channels = respMap.get('result') as List
                        channels.each { Map obj ->
                            def channelid = obj.get('channelid') as String
                            def newuser = (obj.get('newuser') ?: 0) as Integer
                            def channelName = channel.get(channelid) as String
                            println "${channelName} :  ${newuser}"
                            coll.update(new BasicDBObject('_id', "${date.format('yyyyMMdd')}_${channelName}".toString()), new BasicDBObject('$set', [active: newuser]))
                        }
                    }
                }
            }
        } catch (Exception e) {
            println "fetchTalkingData Exception:" + e
        }

    }

    static room_feather(Integer room, Integer userId){
        def query = $$( "type": "send_meme","session._id": userId.toString());
        if(room != null){
            query.append('room',room)
        }
        room_feather.find(query, $$(timestamp:1, room:1))
                .sort($$(timestamp:-1)).toArray().each {DBObject meme ->
            println new Date(meme['timestamp'] as Long).format('yyyy-MM-dd HH:mm:ss') + ", " + userId + ", " + meme['room']
        }
    }


    static Map<Integer, List> diff_user_ips = new HashMap();
    static checkUserIp(List<Integer> userIds){
        Map<String, List> result = new HashMap();
        userIds.each {Integer userId ->
            def ip = getDayLoginIp(userId)
            List users = result.get(ip) ?: new ArrayList<>();
            users.add(userId)
            result.put(ip, users)
        }
        println "<<-----------------------------ip and user-------------------------->>>"
        result = result.sort {a,   b ->
            def counta = (a.value as List).size(), countb = (b.value as List).size()
            -(counta <=> countb)
        }
        result.each {key, List val ->
            if(val.size() > 2){
                println key + " : size : " + val.size() + " " + val;
            }

        }
    }

    static stayStatics(int i) {
        def gteMill = yesTday - i * DAY_MILLON
        println "yesTday : ${new Date(yesTday).format("yyyy-MM-dd HH:mm:ss")} date: ${ new Date(gteMill).format("yyyy-MM-dd HH:mm:ss")} ----------------------->>>>>"
        [1, 3, 7, 30].each { Integer d ->
            Long gt = gteMill + d * DAY_MILLON
            println "gte: ${new Date(gt).format("yyyy-MM-dd HH:mm:ss")}, lt: ${new Date(gt + DAY_MILLON).format("yyyy-MM-dd HH:mm:ss")}, isTongji : ${gt <= yesTday}";

        }
    }

    /**
     * 统计支付时间
     */
    static staticChargeCostTime(){
        def orderList = order_logs.find($$(status:2)).toArray()
        Long maxCost = 0;
        Long minCost = 99999999999;
        Long totalCost = 0;
        Integer totalCount = 0;
        orderList.each {DBObject order ->
            Long timestamp = order['timestamp'] as Long
            Long modify_time = order['modify_time'] as Long
            Long costTime = modify_time - timestamp;
            if(costTime > maxCost) maxCost = costTime;
            if(costTime < minCost) minCost = costTime;
            totalCost += costTime;
            totalCount += 1;
        }
        Long avgCost = (totalCost/totalCount) as Long
        println  "totalCount : ${totalCount}, totalCost : ${totalCost}, maxCost : ${maxCost}  minCost : ${minCost}  avgCost : ${avgCost}"
    }

    static delRoomCdn(){
        try{
            room_cdn.remove(new BasicDBObject(timestamp:[$lt: zeroMill - 7 * DAY_MILLON], type:[$ne:'replay']))
        }catch (Exception e){
            println "delRoomCdn  Exception " + e
        }

    }

    static findUserNameOfRegisterUsers(){
        def query = $$(timestamp:[$gte: Date.parse("yyyy-MM-dd HH:mm:ss" ,"2016-08-01 00:00:00").getTime(),
                                  $lt: Date.parse("yyyy-MM-dd HH:mm:ss" ,"2016-09-08 00:00:00").getTime()])
        List<DBObject> userList = users.find(query, $$(_id:1, nick_name:1)).toArray()
        println "userList size ${userList.size()}"
        /*userList.each { DBObject user ->
            Integer uid = user['_id'] as Integer
        }*/

        //checkUserIp(userList.collect{it['_id'] as Integer})
        //检查nickname
        checkNickName(userList)
    }

    static void checkNickName(List<DBObject> userList ){
        Map<Integer, List<String>> scoreMaps = new HashMap<>();
        Integer max = 0;
        userList.each {DBObject user ->
            String nickname = user['nick_name']
            if(!StringUtils.contains(nickname, '萌新')){
                String url = "http://mon.memeyule.com/monapi/shumei/check?nickname=${URLEncoder.encode(nickname, 'utf-8')}&userId=${user['_id']}".toString()
                String jsonText = request(url)
                try{
                    if(StringUtils.isNotEmpty(jsonText)){
                        def json = new JsonSlurper().parse(new StringReader(jsonText))
                        Integer code = json['code'] as Integer
                        Map data = json['data'] as Map
                        if(code.equals(1)){
                            Integer score = data['score'] as Integer
                            List names = scoreMaps.get(score) ?: new ArrayList<>();
                            names.add(nickname)
                            scoreMaps.put(score, names)
                        }
                    }
                    if(max++ == 3000){
                        Thread.sleep(100)
                        println "sleep..."
                        max=0;
                    }
                }catch (Exception e){
                    println  "Exception : ${e}"
                    println  "url : ${url}"
                    println  "res jsonText : ${jsonText}"
                }
            }
        }
        println scoreMaps
        scoreMaps.each {Integer score, List val ->
            if(score > 500){
                println score + " : size : " + val.size() + " " + val;
            }
        }
    }

    //修复兑换VC用户身份问题
    static recoverExchange_log(){
        //获取兑换流水
        DBCursor cursor = exchange_log.find($$(session:null)).batchSize(10000)
        while (cursor.hasNext()){
            DBObject exchangeLog = cursor.next();
            Integer userId = exchangeLog.get('user_id') as Integer
            Long timestamp = exchangeLog.get('timestamp') as Long
            String priv = (users.findOne(userId,$$(priv:1))?.get('priv') ?:"3") as String
            //判断用户是否之前有签约过主播 applys.find($$(status:[$in:[2,4]]), $$(xy_user_id:1))
            if(applys.count($$(xy_user_id:userId,status:[$in:[2,4]])) > 0){
                //有签约过主播判断此条兑换记录是否在主播签约时间内
                if(isAppleStar(userId, timestamp)){
                    priv = "2"
                }
            }
            //设置用户权限
            setExchange(exchangeLog, priv, userId)
        }
    }

    //判断此条兑换记录是否在主播签约时间内
    static Boolean isAppleStar(Integer userId, Long timestamp){

        applys.find($$(xy_user_id:userId,status:[$in:[2,4]])).sort($$(timestamp:1)).toArray().each {DBObject apply ->
            Long begin = System.currentTimeMillis();
            Long end = begin
            Integer status = apply['status'] as Integer
            Long lastmodif = apply['lastmodif'] as Long
            //当前还属于签约状态 获取时间
            if(status.equals(2)){
                begin = lastmodif
            }else{//如果已经解约 通过操作日志获取 获取签约时间
                begin = (ops.findOne($$("type": "apply_handle", "data.status":2, "data.user_id":userId,
                                timestamp : [$lt:lastmodif]), $$(timestamp : 1),$$(timestamp : -1))?.get('timestamp') ?: 0) as Long
                end = lastmodif
            }
            if(begin > 0 && end > 0 && begin < end ){
                if(timestamp >= begin && timestamp < end){
                    println "apply >>>>>> ${userId} : ${status} exchange : ${new Date(timestamp).format("yyyy-MM-dd HH:mm:ss")} begin : ${new Date(begin).format("yyyy-MM-dd HH:mm:ss")}  end: ${new Date(end).format("yyyy-MM-dd HH:mm:ss")}"
                    return Boolean.TRUE;
                }
            }

        }
        return Boolean.FALSE
    }

    static setExchange(DBObject exchangeLog, String priv, Integer userId){
        def session = $$("priv": priv, "_id": userId.toString(),)
        exchangeLog.put('session', session)
        //println exchangeLog;
        exchange_log.save(exchangeLog)
    }

    static recoverExchange(){
        Long begin = Date.parse("yyyy-MM-dd HH:mm:ss" ,"2016-05-01 00:00:00").getTime()
        Long end = Date.parse("yyyy-MM-dd HH:mm:ss" ,"2016-11-10 00:00:00").getTime()
        exchange_log.update($$(timestamp:[$gte:begin,$lt:end], 'session.priv' : '2'), $$($set:['session.priv':'3']), false, true)
    }

    static String request(String url){
        HttpURLConnection conn = null;
        def jsonText = "";
        try{
            conn = (HttpURLConnection)new URL(url).openConnection()
            conn.setRequestMethod("GET")
            conn.setDoOutput(true)
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(5000);
            conn.connect()
            jsonText = conn.getInputStream().getText("UTF-8")

        }catch (Exception e){
            println "request Exception : " + e;
        }finally{
            if (conn != null) {
                conn.disconnect();
                conn = null;
            }
        }
        return jsonText;
    }

    //检查每日兑换比例
    static void checkChargeRate(){
        DBCursor curosr = finance_log.find($$(via:'ali_m', timestamp:[$gte:1475251200000,$lt:1476892800000])).batchSize(5000)
        while (curosr.hasNext()){
            def finance = curosr.next()
            Double cny = finance['cny'] as Double
            Long coin = finance['coin'] as Long
            def c = cny*100
            def cl = (cny*100) as Long
            Long dcoin = (new BigDecimal(cny.toString()).multiply(new BigDecimal('100')))
            if(c!= coin){
                println "cny=${cny}, coin=${coin}, c=${c}, cl=${cl}, dcoin=${dcoin}"
            }
        }
    }

    //升级所有VIP 到新vip
    static void uplevelAllVip(){
        println "vip 1 " + users.count($$(vip:1))
        println "vip -1 " + users.count($$(vip:-1))
        //users.updateMulti($$(vip:1), $$('$set':[vip:2]))
        //users.updateMulti($$(vip:-1), $$('$set':[vip:2]))
    }

    static void initMvipMedal(){
        medals.updateMulti($$(medal_type:1), $$('$set':[is_mvip:Boolean.FALSE]))
    }

    //批量取消掉所以主播
    static void batchCancelAllStarRecomm(){
        rooms.updateMulti($$(broker_recomm:1), $$('$unset':[broker_recomm:1]))
    }

    static void main(String[] args){
        def l = System.currentTimeMillis()
 /*      List dbs = ['xy_family','xyrank']
        dbs.each {String db ->
           Set<String> collections = mongo.getDB(db).getCollectionNames()
           collections.each {String col ->
               replaceImg(db,col)
           }
        }


        replaceImg('xy','rooms')
        replaceImg('xy_admin','gifts')
        replaceImg('xy_admin','medals')
        replaceImg('xy_admin','cars')
        replaceImg('xy_admin','posters')

        userReplace();
*/

        //roomTypeUpdate();
        /*
       println "userReplace begin----------"
       //userReplace();
       userBatchReplace();
       println "userReplace end cost : ${System.currentTimeMillis()-l} ----------"
        */
        //removeRoomPic()
        //recover()
        /*long l = System.currentTimeMillis()
        testRoomCostBatch();
        println "testRoomCostBatch cost  ${System.currentTimeMillis() -l} ms".toString()*/
/*
        l = System.currentTimeMillis()
        */
        //println "week star remove :" + week_stars.remove($$(week:47,timestamp:null,most_user_count:null)).getN()
        //rooms.updateMulti($$(_id:[$ne:null]), $$('$set', [rank_value:0]))
        /*findRoomcostTopUserAggregate(18189255)
        findRoomcostTopUserAggregate(26415443)
        println "diff user >>>>>>>>>>>>>>>>>>>>>>>>>>>"
        diff_users.each {key, List val ->
            if(val.size() >= 2)
                println key + "  : " +  val;
        }*/
        //awardExp(24364018,753000l)
        //users.updateMulti($$('bag.id':2), $$('$unset', ['bag.id':1]))
        //fetchTalkingData(0)
        //rooms.updateMulti($$(_id:[$ne:null], "live_type":2, "live_cate": null), $$('$set', ["live_cate": "非户外"]))
        //room_feather(29831061, 29847424);
        /*def userIds = [30011283,
                       30010620,
                       29989603]
        checkUserIp(userIds)*/
        /*31.times {
            stayStatics(it)
        }*/
        /*room_feather(null, 28806883)
        room_feather(null, 29680594)
        room_feather(null, 15215864)
        room_feather(null, 25298955)
        room_feather(null, 29008249)*/
        //staticChargeCostTime();
        //delRoomCdn();
        //findUserNameOfRegisterUsers();
        //checkChargeRate();
        //recoverExchange_log()
        //recoverExchange()
        uplevelAllVip()
        //initMvipMedal();
        //batchCancelAllStarRecomm();
        println " cost  ${System.currentTimeMillis() -l} ms".toString()
    }

}
