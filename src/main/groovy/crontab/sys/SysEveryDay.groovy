#!/usr/bin/env groovy
package crontab.sys

import com.mongodb.BasicDBObject
import com.mongodb.DBCollection
@Grapes([
@Grab('org.mongodb:mongo-java-driver:2.14.2'),
@Grab('commons-lang:commons-lang:2.6'),
@Grab('redis.clients:jedis:2.1.0'),
]) import com.mongodb.Mongo
import com.mongodb.DBObject
import com.mongodb.MongoURI
import com.mongodb.ReadPreference

/**
 *
 *
 * date: 14-02-08 下午2:46
 * @author: haigen.xiong@ttpod.com
 */
class SysEveryDay
{
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

    static mongo  = new Mongo(new MongoURI(getProperties('mongo.uri','mongodb://192.168.31.246:27017/?w=1') as String))
    static historyMongo = new Mongo(new MongoURI(getProperties('mongo_history.uri', 'mongodb://192.168.31.246:27017/?w=1') as String))
    //static mongo_history = new Mongo("10.0.5.46", 27017)  //历史归档库

    static DAY_MILLON = 24 * 3600 * 1000L
    static HOUR_MILLON = 1 * 3600 * 1000L
    static long zeroMill = new Date().clearTime().getTime()
    static Long yesTday = zeroMill - DAY_MILLON
    static String YMD = new Date(yesTday).format("yyyyMMdd")
    static DBCollection room_cost_DB =  mongo.getDB("xylog").getCollection("room_cost")
    static lottery_logs = mongo.getDB('xylog').getCollection('lottery_logs')
    static order_logs = mongo.getDB("xylog").getCollection("order_logs");
    static familyRoomStarDB = mongo.getDB("xylog").getCollection("family_room_cost_star")
    static room_cdn = mongo.getDB("xy_union").getCollection('room_cdn')
    //TODO 历史同步库
    //static  historyDB = mongo_history.getDB('xylog_history')
    static  historyDB = historyMongo.getDB('xylog_history')

    //day_login 同步到day_login_history
    static sysDayLoginToHistory()
    {
        try{
            def day_login = mongo.getDB('xylog').getCollection('day_login')
            day_login.setReadPreference(ReadPreference.secondary())
            def result = day_login.find(new BasicDBObject(timestamp:[$gte: yesTday, $lt:zeroMill])).toArray()

            def  day_login_history = historyDB.getCollection('day_login_history')
            if(result.size() > 0)
                day_login_history.insert(result)
            /*result.each {DBObject obj ->
                day_login_history.save(obj)

            }*/
        }catch (Exception e){
            println "sysDayLoginToHistory:"+e
        }

    }


    //room_cost 同步到room_cost2014
    private final static Long SYS_TO_ROOMCOST_MILLIS = 15*1000L
    static sysToRoomCostHistory()
    {
        try{
            def  history_DB = historyDB.getCollection('room_cost_'+new Date(yesTday).format("yyyy"))
            def room_cost = mongo.getDB("xylog").getCollection("room_cost")
            room_cost.setReadPreference(ReadPreference.secondary())
            long begin = yesTday
            //按小时批量同步
            while(begin < zeroMill){
                long end = begin + HOUR_MILLON
                def result = room_cost.find(new BasicDBObject(timestamp: [$gte: begin, $lt: end])).toArray()
                if(result.size() > 0)
                    history_DB.insert(result)
                Thread.sleep(SYS_TO_ROOMCOST_MILLIS)
                begin = end
            }
            /*
            def result = room_cost_DB.find(new BasicDBObject(timestamp:[$lt: zeroMill - 31 * DAY_MILLON])).toArray()
            def  history_DB = historyDB.getCollection('room_cost_'+new Date(yesTday).format("yyyy"))
            result.each {DBObject obj ->
                history_DB.save(obj)

            }*/
        }catch (Exception e){
            println "sysToRoomCostHistory:"+e
        }


    }


    //删除 follower_logs 数据
    static delRoomFollower()
    {
        def follower_logs= mongo.getDB('xylog').getCollection('follower_logs')
        follower_logs.remove(new BasicDBObject(timestamp:[$lt: zeroMill - 30 * DAY_MILLON]))
    }


    //删除 day_login 数据 保留最近32天
    static delDayLogin()
    {
        def day_login = mongo.getDB('xylog').getCollection('day_login')
        day_login.remove(new BasicDBObject(timestamp:[$lt: zeroMill - 32 * DAY_MILLON]))
    }

    // 删除 room_cost 数据
    static delRoomCost()
    {
        room_cost_DB.remove(new BasicDBObject(timestamp:[$lt: zeroMill - 31 * DAY_MILLON]))
    }

    //删除 room_cost_star 数据
    static delRoomCostStar()
    {
        def starDayDB = mongo.getDB('xylog').getCollection('room_cost_star')
        starDayDB.remove(new BasicDBObject(timestamp:[$lt: zeroMill - 30 * DAY_MILLON]))
    }

    //删除 room_cost_usr 数据
    static delRoomCostUsr()
    {
        def usrDayDB = mongo.getDB('xylog').getCollection('room_cost_usr')
        usrDayDB.remove(new BasicDBObject(timestamp:[$lt: zeroMill - 30 * DAY_MILLON]))
    }

   //删除 family_member_cost 数据
    static delFamilyMemberCost()
    {
        def familyMemberCostDB = mongo.getDB('xylog').getCollection('family_member_cost')
        familyMemberCostDB.remove(new BasicDBObject(timestamp:[$lt: zeroMill - 30 * DAY_MILLON]))
    }

    //删除 room_cost_family 数据
    static delRoomCostFamily()
    {
        def familyMemberCostDB = mongo.getDB('xylog').getCollection('room_cost_family')
        familyMemberCostDB.remove(new BasicDBObject(timestamp:[$lt: zeroMill - 30 * DAY_MILLON]))
    }

    //删除历史订单数据
    static delOrder(){
        try{
            order_logs.remove(new BasicDBObject(timestamp:[$lt: zeroMill - 3 * DAY_MILLON]))
        }catch (Exception e){
            println "delOrder  Exception " + e
        }

    }
    //删除历史cdn相关数据
    static delRoomCdn(){
        try{
            room_cdn.remove(new BasicDBObject(timestamp:[$lt: zeroMill - 7 * DAY_MILLON], type:[$ne:'replay']))
        }catch (Exception e){
            println "delRoomCdn  Exception " + e
        }

    }

    //每日用户各种消费类型的数据压缩
    static usr_Types = [ 'send_gift','song','label','grab_sofa','level_up',"open_egg","broadcast","send_bell","buy_vip","send_fortune", "football_shoot","open_card","buy_guard", "car_race", "buy_watch"]
    static void staticRoomCostDayUsr()
    {
        try{
            def room_cost = mongo.getDB("xylog").getCollection("room_cost")
            room_cost.setReadPreference(ReadPreference.secondary())
            usr_Types.each{type->
                def res = room_cost.aggregate(
                        new BasicDBObject('$match', [type:type,timestamp: [$gte: yesTday, $lt: zeroMill]]),
                        new BasicDBObject('$project', [_id: '$session._id',cost:'$cost']),
                        new BasicDBObject('$group', [_id: '$_id', num: [$sum: '$cost']])
                )
                Iterable objs = res.results()

                def list = new ArrayList(100000)
                def day = new Date(yesTday).format("yyyyMMdd")
                objs.each {row ->
                    def cost = row.num as Long
                    if(cost>0)
                    {
                        list.add(new BasicDBObject(_id:"${row._id}_${day}_${type}".toString(),type:type,user_id:row._id as Integer,cost:cost,timestamp:yesTday))
                    }
                }
                def coll = mongo.getDB("xylog").getCollection("room_cost_day_usr")
                if(list.size() > 0)
                    coll.insert(list)
            }
        }catch (Exception e){
            println "staticRoomCostDayUsr:"+e
        }

    }

    //每日主播收到各种消费类型的数据压缩
    static star_types = [ 'send_gift','label','grab_sofa','level_up']
    static void staticRoomCostDayStar()
    {
        try{
            //点歌
            long l = System.currentTimeMillis()
            staticRoomSongDayStar()
            star_types.each{type->
                def res = room_cost_DB.aggregate(
                        new BasicDBObject('$match', [timestamp: [$gte: yesTday, $lt: zeroMill], type: type]),
                        new BasicDBObject('$project', [star_id: '$session.data.xy_star_id', earned: '$session.data.earned']),
                        new BasicDBObject('$group', [_id: '$star_id', num: [$sum: '$earned']]))

                Iterable objs =  res.results()

                def list = new ArrayList(100000)
                def day = new Date(yesTday).format("yyyyMMdd")
                objs.each {row ->
                    def earned = row.num as Long
                    def star_id = row._id as Integer
                    if(null!=star_id && earned>0)
                    {
                        list.add(new BasicDBObject(_id:"$star_id}_${day}_${type}".toString(),type:type,star_id:star_id as Integer,earned:earned,timestamp:yesTday))
                    }
                }
                def coll = mongo.getDB("xylog").getCollection("room_cost_day_star")
                if(list.size() > 0)
                    coll.insert(list)
            }
        }catch (Exception e){
            println "staticRoomCostDayStar:"+e
        }
    }

   //每日主播收到点歌类型的数据压缩
    static void staticRoomSongDayStar()
    {
        def type = 'song'
        def res = room_cost_DB.aggregate( //
                new BasicDBObject('$match', [timestamp: [$gte: yesTday, $lt: zeroMill], type: type]),
                new BasicDBObject('$project', [star_id: '$session.data.xy_star_id', earned: '$session.data.earned']),
                new BasicDBObject('$group', [_id: '$star_id', num: [$sum: '$earned'], count: [$sum: 1]]))

        Iterable objs =  res.results()

        def day = new Date(yesTday).format("yyyyMMdd")
        def coll = mongo.getDB("xylog").getCollection("room_cost_day_star")
        objs.each {row ->
            def earned = row.num as Long
            def star_id = row._id as Integer
            def count = row.count as Integer

            def update = new BasicDBObject(type:type,star_id:star_id as Integer,earned:earned,count:count,timestamp:yesTday)
            def id =star_id+"_" + day + "_" + type
            coll.findAndModify(new BasicDBObject('_id',id), null, null, false,
                    new BasicDBObject('$set',update),true, true)
        }
    }


    //房间粉丝消费日压缩
    static staticRoomFenSiDay(){
        def l = System.currentTimeMillis()
        def timeLimit = new BasicDBObject(timestamp:[$gte:l - 90*DAY_MILLON]) // 最近90天开播过的
        def room_cost =  mongo.getDB("xylog").getCollection("room_cost")
        mongo.getDB("xy").getCollection("rooms").find(timeLimit,new BasicDBObject("live_id":1, "family_id":1)).toArray().each{
                    Integer roomId = it._id as Integer
                    def query = ['session.data.xy_star_id': roomId,timestamp:[$gte: yesTday, $lt: zeroMill]]
                    //家族房间粉丝消费
                    Integer room_family_id = it?.family_id as Integer
                    if(room_family_id != null) {
                        query = ['room_family_id': room_family_id,timestamp:[$gte: yesTday, $lt: zeroMill]]
                    }
                    if(room_cost.count(new BasicDBObject(query)) > 0){
                        def res = room_cost.aggregate(
                                new BasicDBObject('$match', query),
                                new BasicDBObject('$project', [_id: '$session._id',cost:'$star_cost']),
                                new BasicDBObject('$group', [_id: '$_id', num: [$sum: '$cost']]),
                                new BasicDBObject('$sort', [num:-1]),
                                new BasicDBObject('$limit',500) //top N 算法
                        )

                        Iterator objs =   res.results().iterator()
                        def fenSiDB = mongo.getDB("xylog").getCollection("room_fensi_cost")
                        objs.each {row ->
                            def user_id = row._id
                            if(user_id)
                            {
                                def id = user_id + "_" + roomId + "_" + YMD
                                def update = new BasicDBObject(user_id:user_id as Integer,num:row.num,room:roomId,timestamp:yesTday)
                                fenSiDB.findAndModify(new BasicDBObject('_id',id), null, null, false,
                                        new BasicDBObject('$set',update),true, true)
                            }
                        }
                    }
            }
    }

    //富豪消费日压缩
    static staticRoomCostUsr()
    {
        def room_cost = mongo.getDB("xylog").getCollection("room_cost")
        room_cost.setReadPreference(ReadPreference.secondary())
        def res = room_cost.aggregate(
                new BasicDBObject('$match', [timestamp: [$gte: yesTday, $lt: zeroMill]]),
                new BasicDBObject('$project', [_id: '$session._id',cost:'$star_cost']),
                new BasicDBObject('$group', [_id: '$_id', num: [$sum: '$cost']]),
                new BasicDBObject('$sort', [num:-1]),
                new BasicDBObject('$limit',100000))//top N 算法

        Iterator objs = res.results().iterator()
        def roomUserDB = mongo.getDB("xylog").getCollection("room_cost_usr")
        objs.each {row ->
            def user_id = row._id
            if(user_id)
            {
                def id = row._id + "_"  + YMD
                def update = new BasicDBObject(user_id:row._id as Integer,num:row.num,timestamp:yesTday)
                roomUserDB.findAndModify(new BasicDBObject('_id',id), null, null, false,
                        new BasicDBObject('$set',update),true, true)
            }
        }

    }

    //明星获豆日压缩
    static staticRoomCostStar()
    {
        try{
            def res = room_cost_DB.aggregate(
                    new BasicDBObject('$match', [timestamp: [$gte:yesTday, $lt: zeroMill]]),
                    new BasicDBObject('$project', [star_id: '$session.data.xy_star_id', earned: '$session.data.earned']),
                    new BasicDBObject('$group', [_id: '$star_id', num: [$sum: '$earned']]),
                    new BasicDBObject('$sort', [num:-1]),
                    new BasicDBObject('$limit',500))//top N 算法

            Iterator objs = res.results().iterator()
            def roomStarDB = mongo.getDB("xylog").getCollection("room_cost_star")
            objs.each {row ->
                def star_id = row._id
                if(star_id)
                {
                    def id = star_id + "_"  + YMD
                    def update = new BasicDBObject(star_id:star_id as Integer,num:row.num,timestamp:yesTday)
                    roomStarDB.findAndModify(new BasicDBObject('_id',id), null, null, false,
                            new BasicDBObject('$set',update),true, true)
                }

            }
        }catch (Exception e){
            println "staticRoomCostStar:"+e
        }

    }

    //家族直播间主播贡献总榜
    static staticStarFamilyCostDay(){
        try{
            def l = System.currentTimeMillis()
            def timeLimit = new BasicDBObject(timestamp:[$gte:l - 90*DAY_MILLON], type:2) // 最近90天开播过的家族房间
            def room_cost =  mongo.getDB("xylog").getCollection("room_cost")
            mongo.getDB("xy").getCollection("rooms").find(timeLimit,new BasicDBObject("live_id":1, "family_id":1)).toArray().each{
                Integer roomId = it._id as Integer
                Integer family_id = it?.family_id as Integer
                def query = ['room': roomId, timestamp:[$gte: yesTday, $lt: zeroMill]]
                if(room_cost.count(new BasicDBObject(query)) > 0){
                    def res = mongo.getDB("xylog").getCollection("room_cost").aggregate(
                            new BasicDBObject('$match', query),
                            new BasicDBObject('$project', [star_id: '$session.data.xy_star_id', earned: '$session.data.earned',cost:'$star_cost']),
                            new BasicDBObject('$group', [_id: '$star_id', num: [$sum: '$earned'], cost: [$sum: '$cost']]),
                            new BasicDBObject('$sort', [num:-1]),
                            new BasicDBObject('$limit',100) //top N 算法
                    )
                    Iterable objs = res.results()
                    objs.each {row ->
                        def star_id = row._id
                        if (star_id)
                        {
                            def id = star_id + "_"  + family_id +"_"+YMD
                            def update = new BasicDBObject(family_id:family_id,star_id:star_id as Integer, room:roomId,cost:row.cost,num:row.num,timestamp:l)
                            familyRoomStarDB.findAndModify(new BasicDBObject('_id',id), null, null, false,
                                    new BasicDBObject('$set',update),true, true)
                        }
                    }
                }
            }
        }catch(Exception e){
            println "staticStarFamilyCostDay Exception :" + e
        }



    }

    //家族成员消费日压缩 resume
    static staticFamilyMemberDay(List<DBObject> familyLst)
    {
        familyLst.each
        {
            Integer familyId = it._id as Integer
            def query = [family_id:familyId,timestamp:[$gte: yesTday, $lt: zeroMill]]
            def res = mongo.getDB("xylog").getCollection("room_cost").aggregate(
                    new BasicDBObject('$match', query),
                    new BasicDBObject('$project', [_id: '$session._id',cost:'$cost']),
                    new BasicDBObject('$group', [_id: '$_id', num: [$sum: '$cost']]),
                    new BasicDBObject('$sort', [num:-1]),
                    new BasicDBObject('$limit',500) //top N 算法
            )

            Iterator objs = res.results().iterator()
            def memberDB = mongo.getDB("xylog").getCollection("family_member_cost")
            def userDB =  mongo.getDB("xy").getCollection("users")
            objs.each{ row ->
                def user_id = row._id
                def user = userDB.findOne(new BasicDBObject(_id:user_id),new BasicDBObject(priv:1))
                Integer priv = user?.get("priv") as Integer
                def id = user_id + "_" + familyId + "_" + YMD
                def num = row.num as Integer
                if(num>0 && (2!=priv))
                {
                    def update = new BasicDBObject(user_id:user_id as Integer,family_id:familyId,timestamp:yesTday,num:num)
                    memberDB.findAndModify(new BasicDBObject('_id',id), null, null, false,
                            new BasicDBObject('$set':update),true, true)
                }
            }
        }
    }

    //家族的消费日压缩 resume
    static staticFamilyCostDay(List<DBObject> familyLst)
    {
        familyLst.each
        {
            Integer familyId = it._id as Integer

            def query = new BasicDBObject(family_id: familyId,timestamp:[$gte: yesTday, $lt: zeroMill])
            def costList = mongo.getDB("xylog").getCollection("room_cost").
                    find(query,new BasicDBObject(cost:1))
                    .toArray()

            def cost = costList.sum {it.cost?:0} as Long
            def id = familyId + "_" + YMD
            if(cost>0)
            {
                def update = new BasicDBObject(num:cost,family_id:familyId,timestamp:yesTday)
                def memberDB = mongo.getDB("xylog").getCollection("room_cost_family")
                memberDB.findAndModify(new BasicDBObject('_id',id), null, null, false,
                        new BasicDBObject('$set':update),true, true)
            }
        }

    }


    private static final Long SLEEP_MILLIS = 2* 60 * 1000L
    static void main(String[] args)
    {
        long l = System.currentTimeMillis()
        long begin  = l

        List sb = new ArrayList()
        //01.room_cost 同步到room_cost_history
        sysToRoomCostHistory()
        sb << "${new Date().format('yyyy-MM-dd HH:mm:ss')}   sysToRoomCostHistory, cost  ${System.currentTimeMillis() -l} ms".toString()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   sysToRoomCostHistory, cost  ${System.currentTimeMillis() -l} ms".toString()
        Thread.sleep(SLEEP_MILLIS)

        //02.保留 room_cost 31天的数据
        l = System.currentTimeMillis()
        delRoomCost()
        sb << "${new Date().format('yyyy-MM-dd HH:mm:ss')}   delRoomCost, cost  ${System.currentTimeMillis() -l} ms".toString()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   delRoomCost, cost  ${System.currentTimeMillis() -l} ms".toString()
        Thread.sleep(SLEEP_MILLIS)

        //03.保留最近7天的悄悄
        l = System.currentTimeMillis()
        delWhisperStatics()
        sb << "${new Date().format('yyyy-MM-dd HH:mm:ss')}   delWhisperStatics, cost  ${System.currentTimeMillis() -l} ms".toString()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   delWhisperStatics, cost  ${System.currentTimeMillis() -l} ms".toString()
        Thread.sleep(SLEEP_MILLIS)

        //04.day_login 同步到day_login_history
        l = System.currentTimeMillis()
        sysDayLoginToHistory()
        sb << "${new Date().format('yyyy-MM-dd HH:mm:ss')}   sysDayLoginToHistory, cost  ${System.currentTimeMillis() -l} ms".toString()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   sysDayLoginToHistory, cost  ${System.currentTimeMillis() -l} ms".toString()
        Thread.sleep(SLEEP_MILLIS)

        //05.day_login保留最近30天的登录日志
        l = System.currentTimeMillis()
        delDayLogin()
        sb << "${new Date().format('yyyy-MM-dd HH:mm:ss')}   delDayLogin, cost  ${System.currentTimeMillis() -l} ms".toString()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   delDayLogin, cost  ${System.currentTimeMillis() -l} ms".toString()
        Thread.sleep(SLEEP_MILLIS)

        //06.room_feather 同步到room_feather_history
        l = System.currentTimeMillis()
        sysToFeatherHistory()
        sb << "${new Date().format('yyyy-MM-dd HH:mm:ss')}   sysToFeatherHistory, cost  ${System.currentTimeMillis() -l} ms".toString()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   sysToFeatherHistory, cost  ${System.currentTimeMillis() -l} ms".toString()
        Thread.sleep(SLEEP_MILLIS)

        //07.删除 room_feather 数据 保留2天
        l = System.currentTimeMillis()
        delRoomFeather()
        sb << "${new Date().format('yyyy-MM-dd HH:mm:ss')}   delRoomFeather, cost  ${System.currentTimeMillis() -l} ms".toString()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   delRoomFeather, cost  ${System.currentTimeMillis() -l} ms".toString()
        Thread.sleep(SLEEP_MILLIS)

        //08.删除 room_feather_day 数据 保留29天
        l = System.currentTimeMillis()
        delRoomFeatherDay()
        sb << "${new Date().format('yyyy-MM-dd HH:mm:ss')}   delRoomFeatherDay, cost  ${System.currentTimeMillis() -l} ms".toString()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   delRoomFeatherDay, cost  ${System.currentTimeMillis() -l} ms".toString()
        Thread.sleep(SLEEP_MILLIS)

        //08.删除 delRoomFollower 数据 保留29天
        l = System.currentTimeMillis()
        delRoomFollower()
        sb << "${new Date().format('yyyy-MM-dd HH:mm:ss')}   delRoomFollower, cost  ${System.currentTimeMillis() -l} ms".toString()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   delRoomFollower, cost  ${System.currentTimeMillis() -l} ms".toString()
        Thread.sleep(SLEEP_MILLIS)

        //09.删除 room_cost_star 数据
        l = System.currentTimeMillis()
        delRoomCostStar()
        sb << "${new Date().format('yyyy-MM-dd HH:mm:ss')}   delRoomCostStar, cost  ${System.currentTimeMillis() -l} ms".toString()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   delRoomCostStar, cost  ${System.currentTimeMillis() -l} ms".toString()
        Thread.sleep(SLEEP_MILLIS)

        //10.删除 room_cost_usr 数据
        l = System.currentTimeMillis()
        delRoomCostUsr()
        sb << "${new Date().format('yyyy-MM-dd HH:mm:ss')}   delRoomCostUsr, cost  ${System.currentTimeMillis() -l} ms".toString()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   delRoomCostUsr, cost  ${System.currentTimeMillis() -l} ms".toString()
        Thread.sleep(SLEEP_MILLIS)

        //11.删除 family_member_cost 数据
        /*l = System.currentTimeMillis()
        delFamilyMemberCost()
        sb << "${new Date().format('yyyy-MM-dd HH:mm:ss')}   delFamilyMemberCost, cost  ${System.currentTimeMillis() -l} ms".toString()
        Thread.sleep(SLEEP_MILLIS)*/

        //12.删除 room_cost_family 数据
        /*l = System.currentTimeMillis()
        delRoomCostFamily()
        sb << "${new Date().format('yyyy-MM-dd HH:mm:ss')}   delRoomCostFamily, cost  ${System.currentTimeMillis() -l} ms".toString()
        Thread.sleep(SLEEP_MILLIS)*/

        //13.每日用户在直播间各种消费类型的数据压缩
        l = System.currentTimeMillis()
        staticRoomCostDayUsr()
        sb << "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticRoomCostDayUsr, cost  ${System.currentTimeMillis() -l} ms".toString()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticRoomCostDayUsr, cost  ${System.currentTimeMillis() -l} ms".toString()
        Thread.sleep(SLEEP_MILLIS)

        //14.每日主播收到各种消费类型的数据压缩
        l = System.currentTimeMillis()
        staticRoomCostDayStar()
        sb << "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticRoomCostDayStar, cost  ${System.currentTimeMillis() -l} ms".toString()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticRoomCostDayStar, cost  ${System.currentTimeMillis() -l} ms".toString()
        Thread.sleep(SLEEP_MILLIS)

        //15.房间粉丝消费日压缩
        l = System.currentTimeMillis()
        staticRoomFenSiDay()
        sb << "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticRoomFenSiDay, cost  ${System.currentTimeMillis() -l} ms".toString()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticRoomFenSiDay, cost  ${System.currentTimeMillis() -l} ms".toString()
        Thread.sleep(SLEEP_MILLIS)

        //16.富豪消费日压缩
        l = System.currentTimeMillis()
        staticRoomCostUsr()
        sb << "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticRoomCostUsr, cost  ${System.currentTimeMillis() -l} ms".toString()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticRoomCostUsr, cost  ${System.currentTimeMillis() -l} ms".toString()
        Thread.sleep(SLEEP_MILLIS)

        //17.明星获豆日压缩
        l = System.currentTimeMillis()
        staticRoomCostStar()
        sb << "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticRoomCostStar, cost  ${System.currentTimeMillis() -l} ms".toString()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticRoomCostStar, cost  ${System.currentTimeMillis() -l} ms".toString()
        Thread.sleep(SLEEP_MILLIS)

        //18.家族成员消费日压缩
        /*l = System.currentTimeMillis()
        List<DBObject> familyLst = mongo.getDB("xy_family").getCollection("familys").find(new BasicDBObject("status",2),new BasicDBObject("_id",1)).toArray()
        staticFamilyMemberDay(familyLst)
        sb << "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticFamilyMemberDay, cost  ${System.currentTimeMillis() -l} ms".toString()
        Thread.sleep(SLEEP_MILLIS)*/

        //19.家族消费日压缩
        /*l = System.currentTimeMillis()
        staticFamilyCostDay(familyLst)
        sb << "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticFamilyCostDay, cost  ${System.currentTimeMillis() -l} ms".toString()
        Thread.sleep(SLEEP_MILLIS)*/

        //同步中奖记录
        l = System.currentTimeMillis()
        sysToLotteryHistory();
        sb << "${new Date().format('yyyy-MM-dd HH:mm:ss')}   sysToLotteryHistory, cost  ${System.currentTimeMillis() -l} ms".toString()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   sysToLotteryHistory, cost  ${System.currentTimeMillis() -l} ms".toString()
        Thread.sleep(SLEEP_MILLIS)

        //删除30天前中奖记录
        l = System.currentTimeMillis()
        delLottery();
        sb << "${new Date().format('yyyy-MM-dd HH:mm:ss')}   delLottery, cost  ${System.currentTimeMillis() -l} ms".toString()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   delLottery, cost  ${System.currentTimeMillis() -l} ms".toString()
        Thread.sleep(SLEEP_MILLIS)

        //删除3天前历史支付订单记录
        l = System.currentTimeMillis()
        delOrder();
        sb << "${new Date().format('yyyy-MM-dd HH:mm:ss')}   delOrder, cost  ${System.currentTimeMillis() -l} ms".toString()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   delOrder, cost  ${System.currentTimeMillis() -l} ms".toString()
        Thread.sleep(SLEEP_MILLIS)

        //删除3天前历史cdn相关信息
        l = System.currentTimeMillis()
        delRoomCdn();
        sb << "${new Date().format('yyyy-MM-dd HH:mm:ss')}   delRoomCdn, cost  ${System.currentTimeMillis() -l} ms".toString()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   delRoomCdn, cost  ${System.currentTimeMillis() -l} ms".toString()
        Thread.sleep(SLEEP_MILLIS)

        l = System.currentTimeMillis()
        staticStarFamilyCostDay()
        sb << "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticStarFamilyCostDay, cost  ${System.currentTimeMillis() -l} ms".toString()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticStarFamilyCostDay, cost  ${System.currentTimeMillis() -l} ms".toString()
        Thread.sleep(SLEEP_MILLIS)

        //落地定时执行的日志
        jobFinish(begin,sb)

    }


    /**
     * 标记任务完成  用于运维监控
     * @return
     */
    private static jobFinish(Long begin, List sb){
        def timerName = 'SysEveryDay'
        Long totalCost = System.currentTimeMillis() - begin
        saveTimerLogs(timerName, totalCost, sb)
        println "${new Date().format('yyyy-MM-dd')}:${SysEveryDay.class.getSimpleName()}:finish  cost  ${System.currentTimeMillis() - begin} ms"
    }

    //落地定时执行的日志
    private static saveTimerLogs(String timerName,Long totalCost, List costDetail)
    {
        def timerLogsDB =  mongo.getDB("xyrank").getCollection("timer_logs")
        def tmp = System.currentTimeMillis()
        def id = timerName + "_"  + new Date().format("yyyyMMdd")
        def update = new BasicDBObject(timer_name:timerName,cost_total:totalCost,cat:'day',unit:'ms',timestamp:tmp, costDetail:costDetail)
        timerLogsDB.findAndModify(new BasicDBObject('_id',id), null, null, false,new BasicDBObject('$set',update),true, true)
    }

}

