#!/usr/bin/env groovy
package st
/**
 * Author: monkey 
 * Date: 2017/3/23
 */
import com.mongodb.BasicDBObject
import com.mongodb.DBCollection
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
//        @Grab('org.codehaus.groovy:groovy-all:2.1.3'),
])

import com.mongodb.Mongo
import com.mongodb.MongoURI
import org.apache.commons.lang.StringUtils
import redis.clients.jedis.Jedis

import java.text.SimpleDateFormat
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/**
 * 每天统计一份数据
 */
class Recovery {

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

    static mongo = new Mongo(new MongoURI(getProperties('mongo.uri', 'mongodb://192.168.31.231:20000,192.168.31.236:20000,192.168.31.231:20001/?w=1&slaveok=true') as String))


    static DAY_MILLON = 24 * 3600 * 1000L

    static long zeroMill = new Date().clearTime().getTime()
    static Long yesTday = zeroMill - DAY_MILLON
    static final String live_jedis_host = getProperties("live_jedis_host", "192.168.31.246")
    static final Integer live_jedis_port = getProperties("live_jedis_port", 6379) as Integer
    static liveRedis = new Jedis(live_jedis_host, live_jedis_port)
    static DBCollection coll = mongo.getDB('xy_admin').getCollection('stat_daily')
    static DBCollection users = mongo.getDB('xy').getCollection('users')
    static DBCollection channels = mongo.getDB('xy_admin').getCollection('channels')
    static DBCollection diamond_logs = mongo.getDB('xy_admin').getCollection('diamond_logs')
    static DBCollection diamond_cost_logs = mongo.getDB('xy_admin').getCollection('diamond_cost_logs')
    static DBCollection diamond_dailyReport_stat = mongo.getDB('xy_admin').getCollection('diamond_dailyReport_stat')
    static DBCollection channel_pay_DB = mongo.getDB('xy_admin').getCollection('channel_pay')
    static DBCollection finance_log_DB = mongo.getDB('xy_admin').getCollection('finance_log')
    static DBCollection star_award_logs = mongo.getDB('game_log').getCollection('star_award_logs')
    static DBCollection room_cost = mongo.getDB('xylog').getCollection('room_cost')
    static DBCollection stat_lives = mongo.getDB('xy_admin').getCollection('stat_lives')
    static DBCollection rooms = mongo.getDB('xy').getCollection('rooms')
    static DBCollection lives = mongo.getDB('xylog').getCollection('room_edit')
    static Long VALID_DAY = 7200
    static DBCollection room_follower_day = mongo.getDB('xylog').getCollection('room_follower_day')
    static DBCollection share_logs = mongo.getDB('xylog').getCollection('share_logs')
    static DBCollection room_meme_day = mongo.getDB('xylog').getCollection('room_meme_day')

    /**
     * 红包统计
     */
    static static_red_packet(int day){
        for (int i = 0; i < day; i++) {
            red_packet_static(i)
        }
    }

    /**
     * 充值统计recovery
     */
    static pay_statics_recovery(int day) {
        for (int i = 0; i < day; i++) {
            financeStatics(i)
        }
    }

    /**
     * 钻石统计recovery
     */
    static diamond_statics_recovery(int day) {
        for (int i = 0; i < day; i++) {
            statics_diamond(i)
        }
    }

    /**
     * 按支付渠道统计
     */
    static payStatics_recovery(int day) {
        for (int i = 0; i < day; i++) {
            payStatics(i)
        }
    }
    /**
     * 运营总表recovery
     */
    static staticTotalReport_recovery(int day) {
        for (int i = 0; i < day; i++) {
            staticTotalReport(i)
        }
    }

    /**
     * 经纪人收益统计
     * @param day
     */
    static staticBroker_recovery(int day) {
        for (int i = 0; i < day; i++) {
            staticBroker(i)
        }
    }

    /**
     * 直播统计
     * @param day
     */
    static staticLive_recovery(int day) {
//        for (int i = 0; i < day; i++) {
//            staticsEarned(i)
//        }
//        for (int i = 0; i < day; i++) {
//            staticsAwardEarned(i)
//        }
        for (int i = 0; i < day; i++) {
            staticsLiveTime(i)
        }
//        for (int i = 0; i < day; i++) {
//            staticsTotalLiveTime(i)
//        }
//        for (int i = 0; i < day; i++) {
//            staticsOthers(i)
//        }

    }

    static static_cash(int day){
        statics_red_packet(day)
    }

//    2017-03-23	1205491	2017-03-23 18:05:35		NaN
//    2017-03-23	1205491	2017-03-23 17:33:24	2017-03-23 18:05:17	32
//    2017-03-22	1205491	2017-03-22 22:06:48	2017-03-22 22:43:31	37
//    2017-03-22	1205491	2017-03-22 20:40:33	2017-03-22 22:06:38	86
//    2017-03-22	1205491	2017-03-22 18:10:15	2017-03-22 18:12:04	2
//    2017-03-22	1205491	2017-03-22 15:29:46	2017-03-22 18:09:04	159
//    2017-03-22	1205491	2017-03-22 04:03:52	2017-03-22 06:14:25	131
//    2017-03-21	1205491	2017-03-21 04:33:47	2017-03-21 06:02:08	88
//    2017-03-21	1205491	2017-03-21 03:59:13	2017-03-21 04:32:03	33
//    2017-03-20	1205491	2017-03-20 03:33:58	2017-03-20 06:01:12	147
    //获取直播时长
    private static AtomicLong getLiveTime(BasicDBObject query, Long yesterday, HashSet liveSet) {
        def fmt = new SimpleDateFormat('yyyyMMddHHmmss')
        def millLong = new AtomicLong()
        Long lastOffTime = yesterday
        lives.find(query).toArray().each { BasicDBObject live_off ->
            Long endTime = live_off['timestamp'] as Long
            if (endTime > lastOffTime) {
                lastOffTime = endTime
            }
            String live_id = live_off.data
            liveSet.add(live_id)
            Long starTime = fmt.parse(live_id.substring(live_id.indexOf('_') + 1)).getTime()
            if (starTime < yesterday) {
                starTime = yesterday
            }
            def live_time = endTime - starTime
            if (live_time > 0) {
                millLong.addAndGet(live_time)
            }
        }
        //检查正在直播,大于最后一次关播时间
        query.type = 'live_on'
        query.timestamp = new HashMap(query.timestamp)
        query.timestamp['$gte'] = lastOffTime
        def live_on = lives.findOne(query, null, new BasicDBObject('timestamp', -1))
        if (live_on) {
            Long zeroTime = yesterday + DAY_MILLON
            Long mayOffTime = live_on.get('etime') ?: zeroTime
            Long live_time = Math.min(zeroTime, mayOffTime) - live_on.get('timestamp')
            liveSet.add(live_on.get('data'))
            if (live_time > 0) {
                millLong.addAndGet(live_time)
            }
        }
        return millLong
    }

    //统计主播赚取的柠檬
    static staticsEarned(int i) {
        Long tmp = zeroMill - i * DAY_MILLON
        def day = new Date(tmp).format('yyyyMMdd')

        def timeBetween = Collections.unmodifiableMap([$gte: tmp, $lt: tmp + DAY_MILLON])
        //1392144
        //截止：20151026,包括（send_gift, grab_sofa, send_bell, song, buy_guard, send_treasure, level_up等）
        def earnIter = room_cost.aggregate(
                //new BasicDBObject('$match', ['session.data.xy_star_id':1384037,timestamp:timeBetween]),
                new BasicDBObject('$match', ['session.data.xy_star_id': [$ne: null], timestamp: timeBetween]),
                new BasicDBObject('$project', [_id: '$session.data.xy_star_id', user_id: '$session._id', earned: '$session.data.earned', live: '$live']),
                new BasicDBObject('$group', [_id: '$_id', earned: [$sum: '$earned'], pc_earned: [$sum: '$earned'], lives: [$addToSet: '$live'], users: [$addToSet: '$user_id']])
        ).results()
        def list = []


        def result = new HashMap()
        earnIter.each { BasicDBObject obj ->
            def user_id = obj.get('_id') as Integer
            result.put(user_id, obj)
        }

        //统计主播在其它房间收到的礼物 开始
        room_cost.aggregate(
                new BasicDBObject($match: ['session.data.xy_user_id': [$in: result.keySet()], timestamp: timeBetween]),
                new BasicDBObject($project: [_id: '$session.data.xy_user_id', earned: '$session.data.earned']),
                new BasicDBObject($group: [_id: '$_id', earned: [$sum: '$earned']])
        ).results().each { BasicDBObject obj ->
            def user_id = obj.get('_id') as Integer
            def earned = obj.get('earned') as Long
            def liveObj = result.get(user_id) as BasicDBObject
            liveObj.put('earned', earned + ((liveObj?.get('earned') ?: 0) as Long))
        }
        //统计主播在其它房间收到的礼物 结束

        room_cost.aggregate(
                new BasicDBObject($match: ['session.data.xy_star_id': [$in: result.keySet()], timestamp: timeBetween, live_type: 2]),
                new BasicDBObject($project: [_id: '$session.data.xy_star_id', earned: '$session.data.earned']),
                new BasicDBObject($group: [_id: '$_id', earned: [$sum: '$earned']])
        ).results().each { BasicDBObject obj ->
            def user_id = obj.get('_id') as Integer
            def earned = obj.get('earned') as Long
            def liveObj = result.get(user_id) as BasicDBObject
            liveObj.put('app_earned', earned)
            liveObj.put('pc_earned', ((liveObj?.get('pc_earned') ?: 0) as Long) - earned)
        }
        //统计主播直播收益 结束
        list.addAll(result.values())
        list.each { BasicDBObject earndObj ->
            def user_id = earndObj.get('_id') as Integer
            if (user_id != null) {
                def liveSet = new HashSet(earndObj.lives)
                def userSet = new HashSet(earndObj.users)
                earndObj._id = "${day}_${user_id}".toString()
                earndObj.timestamp = tmp
                earndObj.user_id = user_id
                earndObj.lives = liveSet
                earndObj.users = userSet.size()
                earndObj.second = 0

                stat_lives.update(new BasicDBObject("_id", earndObj._id), earndObj, true, false)
            }
        }
    }
    //统计直播时长
    static staticsLiveTime(int i) {
        Long yesterday = zeroMill - i * DAY_MILLON
        def timeLimit = new BasicDBObject(timestamp: [$gte: yesterday - 30 * DAY_MILLON]) // 最近30天开播过的
        // todo 解约的话rooms就没了，就统计不到了
//        def starIds = rooms.find(timeLimit, new BasicDBObject("xy_star_id": 1)).toArray()*.xy_star_id
        def starIds = lives.distinct('session.room_id',$$(timeLimit))
        def day = new Date(yesterday).format('yyyyMMdd')
        def timeBetween = Collections.unmodifiableMap([$gte: yesterday, $lt: yesterday + DAY_MILLON])

        starIds.each { it ->
            Integer star_id = Integer.valueOf(it.toString())
            if (star_id != null) {
                String live_log_id = "${day}_${star_id}".toString()
                BasicDBObject liveObj = stat_lives.findOne(new BasicDBObject("_id", live_log_id)) as BasicDBObject
                if (liveObj == null) {
                    liveObj = new BasicDBObject(user_id: star_id, earned: 0, app_earned: 0, pc_earned: 0, lives: new HashSet<>())
                }
                def liveSet = new HashSet(liveObj?.lives ?: new HashSet<>())
                //总直播时间
                def mills = getLiveTime(new BasicDBObject(type: "live_off", 'room': star_id, timestamp: timeBetween), yesterday, liveSet).intValue()
                //手机直播时间
                def app_mills = getLiveTime(new BasicDBObject(type: "live_off", 'room': star_id, timestamp: timeBetween, live_type: 2), yesterday, liveSet).intValue()
                def pc_mills = mills - app_mills //PC 直播时间

                if (mills > 0) {
                    liveObj.second = mills.intdiv(1000)
                    liveObj.pc_second = pc_mills.intdiv(1000)
                    liveObj.app_second = app_mills.intdiv(1000)
                    liveObj.timestamp = yesterday
                    liveObj.user_id = star_id
                    liveObj.lives = liveSet
                    liveObj.value = mills >= VALID_DAY * 1000L ? 1 : 0
                    stat_lives.update(new BasicDBObject("_id", live_log_id), liveObj, true, false)
                }

            }
        }
    }
    /**
     * 统计主播获得的分成能量
     * 可能没有送礼 只有游戏分成的情况
     * @param i
     * @return
     */
    static staticsAwardEarned(int i) {
        Long yesterday = zeroMill - i * DAY_MILLON
        def timeLimit = new BasicDBObject(timestamp: [$gte: yesterday - 30 * DAY_MILLON], test: [$ne: true]) // 最近30天开播过的
        def starIds = rooms.find(timeLimit, new BasicDBObject("xy_star_id": 1)).toArray()*.xy_star_id
        def day = new Date(yesterday).format('yyyyMMdd')
        def timeBetween = Collections.unmodifiableMap([$gte: yesterday, $lt: yesterday + DAY_MILLON])
        starIds.each {
            Integer star_id ->
                String live_log_id = "${day}_${star_id}".toString()
                BasicDBObject liveObj = stat_lives.findOne(new BasicDBObject("_id", live_log_id)) as BasicDBObject
                if (liveObj == null) {
                    liveObj = new BasicDBObject(user_id: star_id, award_count: 0, earned: 0, app_earned: 0, pc_earned: 0, lives: new HashSet<>())
                }
                def res = star_award_logs.aggregate(
                        new BasicDBObject($match: [timestamp: timeBetween, 'room_id': star_id]),
                        new BasicDBObject($project: [_id: '$room_id', earned: '$award_earned']),
                        new BasicDBObject($group: [_id: null, earned: [$sum: '$earned']])
                ).results().iterator()

                def award_count = 0
                if (res.hasNext()) {
                    award_count = res.next().earned
                }
                liveObj.put('award_count', award_count)
                liveObj.put('earned', award_count + ((liveObj?.get('earned') ?: 0) as Long))
                liveObj.put('app_earned', award_count + ((liveObj?.get('app_earned') ?: 0) as Long))
                liveObj.put('timestamp', yesterday)

                stat_lives.update(new BasicDBObject("_id", live_log_id), new BasicDBObject('$set', liveObj), true, false)
        }
    }

    //统计主播总直播时长
    static staticsTotalLiveTime(int i) {
        Long yesterday = zeroMill - i * DAY_MILLON
        def timeLimit = new BasicDBObject(timestamp: [$gte: yesterday - 2 * DAY_MILLON], test: [$ne: true]) // 最近2天开播过的
        def starIds = rooms.find(timeLimit, new BasicDBObject("xy_star_id": 1)).toArray()*.xy_star_id

        starIds.each { Integer star_id ->
            if (star_id != null) {
                stat_lives.aggregate(
                        new BasicDBObject($match: ['user_id': star_id]),
                        new BasicDBObject($project: [second: '$second']),
                        new BasicDBObject($group: [_id: null, second: [$sum: '$second']])
                ).results().each { BasicDBObject obj ->
                    def second = obj.get('second') as Long
                    rooms.update(new BasicDBObject("_id", star_id), new BasicDBObject('$set', [total_live_sec: second]))
                }

            }
        }
    }

    /**
     * 每日关注数量/分享数量/平均人气/么么哒数量
     */
    static staticsOthers(int i) {
        Long yesterday = zeroMill - i * DAY_MILLON
        String day = new Date(yesterday).format('yyyyMMdd')

        def timeLimit = new BasicDBObject(timestamp: [$gte: yesterday - 7 * DAY_MILLON], test: [$ne: true]) // 最近7天开播过的
        def starIds = rooms.find(timeLimit, new BasicDBObject("xy_star_id": 1)).toArray()*.xy_star_id

        def timeBetween = Collections.unmodifiableMap([$gte: yesterday, $lt: yesterday + DAY_MILLON])
        initAvgRoomCountFromRedis(day)
        starIds.each { Integer star_id ->
            if (star_id != null) {
                BasicDBObject liveObj = initLiveObj(i, star_id)
                //关注数量
                def sId = star_id + "_" + day
                Integer followers = (room_follower_day.findOne($$(_id: sId), $$(num: 1))?.num ?: 0) as Integer
                liveObj.followers = followers >= 0 ? followers : 0

                //分享数量
                liveObj.share_count = shareCounts(star_id, timeBetween)

                //平均人气
                liveObj.avg_room_count = getAvgRoomCount(star_id.toString())

                //么么哒数量
                Integer meme = (room_meme_day.findOne($$(_id: sId), $$(num: 1))?.get('num') ?: 0) as Integer
                liveObj.meme = meme >= 0 ? meme : 0
                stat_lives.update(new BasicDBObject("_id", liveObj.remove("_id") as String), new BasicDBObject('$set', liveObj), true, false)
            }
        }
    }

    static Map<String, String> userCounts = Collections.emptyMap();
    static Map<String, String> userTimes = Collections.emptyMap();
    //获得平均观众人数
    private static void initAvgRoomCountFromRedis(String day) {
        try {
            if (userCounts.size() == 0 && userTimes.size() == 0) {
                String room_user_count_key = "live:room:user:count:${day}".toString();
                String room_user_time_key = "live:room:user:count:times:${day}".toString();
                userCounts = liveRedis.hgetAll(room_user_count_key)
                userTimes = liveRedis.hgetAll(room_user_time_key)
                //println "getAll rooms and vistorCount ..."
                liveRedis.del(room_user_count_key)
                liveRedis.del(room_user_time_key)
            }

        } catch (Exception e) {
            println "initAvgRoomCountFromRedis Exception : ${e.getMessage()}"
        }
    }

    private static BasicDBObject initLiveObj(Integer curr, Integer star_id) {
        Long yesterday = zeroMill - curr * DAY_MILLON
        def day = new Date(yesterday).format('yyyyMMdd')
        String live_log_id = "${day}_${star_id}".toString()
        BasicDBObject liveObj = stat_lives.findOne(new BasicDBObject("_id", live_log_id)) as BasicDBObject
        if (liveObj == null) {
            liveObj = new BasicDBObject(_id: live_log_id, user_id: star_id, earned: 0, app_earned: 0, pc_earned: 0, lives: new HashSet<>())
        }
        return liveObj
    }

//分享次数
    private static Integer shareCounts(Integer star_id, Map timeBetween) {
        return share_logs.count($$(star_id: star_id, timestamp: timeBetween))
    }

    //平均观众人数
    private static Integer getAvgRoomCount(String star_id) {
        try {
            if (userCounts.size() > 0 && userTimes.size() > 0) {
                Integer userCount = (userCounts[star_id] ?: 0) as Integer
                Integer userTime = (userTimes[star_id] ?: 0) as Integer
                //println "${star_id} : ${userCount}, ${userTime}"
                if (userTime > 0 && userCount > 0)
                    return (userCount / userTime) as Integer
            }

        } catch (Exception e) {
            println "getAvgRoomCount Exception : ${e.getMessage()}"
        }
        return 0;
    }

    static staticBroker(i) {
        def coll = mongo.getDB('xy_admin').getCollection('stat_brokers')
        def star_award_logs = mongo.getDB('game_log').getCollection('star_award_logs')
        def users = mongo.getDB('xy').getCollection('users')
        def room_cost = mongo.getDB("xylog").getCollection("room_cost")
        def flog = mongo.getDB('xy_admin').getCollection('finance_log')
        def total = 0

        Long begin = zeroMill - i * DAY_MILLON
        def timeBetween = [$gte: begin, $lt: begin + DAY_MILLON]

        users.find(new BasicDBObject('priv', 5), new BasicDBObject('status', 1)).toArray().each { BasicDBObject broker ->

            Integer bid = broker.get('_id') as Integer

            def uids = new HashSet(
                    users.find(new BasicDBObject('star.broker', bid), new BasicDBObject('star', 1)).toArray().collect {
                        it.get('_id')
                    })
            def star = [count: uids.size()]
            def res = room_cost.aggregate(
                    new BasicDBObject('$match', ['session.data.xy_star_id': [$in: uids], timestamp: timeBetween]),
                    new BasicDBObject('$project', [earned: '$session.data.earned']),
                    new BasicDBObject('$group', [_id: null, earned: [$sum: '$earned']])
            )
            Iterator records = res.results().iterator();
            def live_earned = 0
            if (records.hasNext()) {
                live_earned = records.next().earned
            }

            def game_award_res = star_award_logs.aggregate(
                    new BasicDBObject('$match', [timestamp: timeBetween, 'room_id': [$in: uids]]),
                    new BasicDBObject('$project', [earned: '$award_earned']),
                    new BasicDBObject('$group', [_id: null, earned: [$sum: '$earned']])
            ).results().iterator()
            def game_earned = 0
            if (game_award_res.hasNext()) {
                game_earned = game_award_res.next().earned
            }


            star.bean_count = live_earned + game_earned
            def sale = [:]
            res = flog.aggregate(
                    new BasicDBObject('$match', [ext: bid.toString(), timestamp: timeBetween]),
                    new BasicDBObject('$project', [cny: '$cny']),
                    new BasicDBObject('$group', [_id: null, cny: [$sum: '$cny'], count: [$sum: 1]])
            )
            records = res.results().iterator();
            if (records.hasNext()) {
                def rec = records.next()
                sale.cny = rec.cny
                sale.count = rec.count
            }

            def YMD = new Date(begin).format("yyyyMMdd")
            coll.save(new BasicDBObject(
                    _id: "${bid}_${YMD}".toString(),
                    star: star,
                    sale: sale,
                    user_id: bid,
                    timestamp: begin,
            ))

            res = flog.aggregate(
                    new BasicDBObject('$match', [ext: bid.toString()]),
                    new BasicDBObject('$project', [cny: '$cny']),
                    new BasicDBObject('$group', [_id: null, cny: [$sum: '$cny'], count: [$sum: 1]])
            )
            records = res.results().iterator();
            sale = [:]
            if (records.hasNext()) {//字符多位 double 精度
                def rec = records.next()
                sale.cny = rec.cny
                sale.count = rec.count
            }

            users.update(broker.append('broker.flag', [$ne: YMD]), new BasicDBObject(
                    // TODO day add
                    $inc: ['broker.bean_total': star.bean_count ?: 0],
                    $set: [
                            'broker.cny_total' : sale.cny ?: 0,
                            'broker.sale_count': sale.count ?: 0,
                            //'broker.star_total':star.count,
                            'broker.flag'      : YMD
                    ]
            ))
        }
        println("total is ${total}")
    }

    /**
     * 充值统计
     * @return
     */
    static financeStatics(int i) {
        def gteMill = yesTday - i * DAY_MILLON
        def YMD = new Date(gteMill).format("yyyyMMdd")
        def time = [$gte: gteMill, $lt: gteMill + DAY_MILLON]

        def list = mongo.getDB('xy_admin').getCollection('finance_log').find(new BasicDBObject(timestamp: time))
                .toArray()

        def total = new BigDecimal(0)
        def totalCoin = new AtomicLong()

        def pays = MapWithDefault.<String, PayType> newInstance(new HashMap()) { new PayType() }
        Double android_recharge = 0d
        Double ios_recharge = 0d
        Double other_recharge = 0d
        def android_recharge_set = new HashSet()
        def ios_recharge_set = new HashSet()
        def other_recharge_set = new HashSet()
        list.each { obj ->
            def cny = obj.containsField('cny') ? obj['cny'] as Double : 0.0d
            def payType = pays[obj.via]
            payType.count.incrementAndGet()
            payType.user.add(obj.user_id)
            def userId = obj['user_id'] as Integer
            def user = users.findOne($$('_id': userId), $$('qd': 1))
            if (user == null) {
                return
            }
            def qd = user.containsField('qd') ? user['qd'] : 'aiwan_default'
            // client = 2 android 4 ios
            def channel = channels.findOne($$('_id': qd), $$('client': 1))
            def client = channel.containsField('client') ? channel['client'] as Integer : 2
            def via = obj.containsField('via') ? obj['via'] : ''
            if (via != 'Admin') {
                // 统计android和ios的充值人数，去重，如果是admin加币，则不用统计
                if (client == 2) {
                    android_recharge_set.add(userId)
                } else if (client == 4) {
                    ios_recharge_set.add(userId)
                } else {
                    other_recharge_set.add(userId)
                }
            }

            if (cny != null) {
                cny = new BigDecimal(cny)
                total = total.add(cny)
                payType.cny = payType.cny.add(cny)
                // 统计android和ios的充值金额
                if (client == 2) {
                    android_recharge += cny
                } else if (client == 4) {
                    ios_recharge += cny
                } else {
                    other_recharge += cny
                }
            }
            def coin = obj.get('coin') as Long
            if (coin) {
                totalCoin.addAndGet(coin)
                payType.coin.addAndGet(coin)
            }
        }

        def obj = new BasicDBObject(
                _id: "${YMD}_finance".toString(),
                total: total.doubleValue(),
                total_coin: totalCoin,
                type: 'finance',
                android_recharge: android_recharge,
                ios_recharge: ios_recharge,
                other_recharge: other_recharge,
                ios_recharge_count: ios_recharge_set.size(),
                android_recharge_count: android_recharge_set.size(),
                other_recharge_count: other_recharge_set.size(),
                timestamp: gteMill
        )
        pays.each { String key, PayType type -> obj.put(StringUtils.isBlank(key) ? '' : key.toLowerCase(), type.toMap()) }

        coll.save(obj)
    }

    /**
     * 统计钻石的入账，出账，总账
     */
    private static void statics_diamond(int i) {
        def gteMill = yesTday - i * DAY_MILLON
        def YMD = new Date(gteMill).format("yyyyMMdd")
        def time = [$gte: gteMill, $lt: gteMill + DAY_MILLON]
        //本日期初结余=昨日期末结余
        def begin_surplus = lastDaySurplus(gteMill)
        def inc_total = 0
        def desc_total = 0
        def inc_detail = new HashMap<String, Long>()
        def desc_detail = new HashMap<String, Long>()
        def query = $$('timestamp': time)
        def diamond_cursors = diamond_logs.find(query).batchSize(5000)
        def diamond_cost_cursors = diamond_cost_logs.find(query).batchSize(5000)

        // 加币统计
        while (diamond_cursors.hasNext()) {
            def obj = diamond_cursors.next()
            def diamond_count = obj['diamond_count'] as Long
            def type = obj['type'] as String
            inc_total += diamond_count
            def current_type_add_diamond = inc_detail.containsKey(type) ? inc_detail[type] : 0L
            current_type_add_diamond += diamond_count
            inc_detail.put(type, current_type_add_diamond)
        }

        // 消费统计
        while (diamond_cost_cursors.hasNext()) {
            def obj = diamond_cost_cursors.next()
            def diamond_count = obj['diamond_count'] as Long
            def type = obj['type'] as String
            desc_total += diamond_count
            def current_type_minus_total = desc_detail.containsKey(type) ? desc_detail[type] : 0L
            current_type_minus_total += diamond_count
            desc_detail.put(type, current_type_minus_total)
        }

        def total = inc_total - desc_total
        def myId = "${YMD}_diamond_dailyReport_stat".toString()

        def row = $$('_id': myId, 'inc_total': inc_total, 'desc_total': desc_total, 'total': total, 'timestamp': gteMill,
                'inc_detail': inc_detail, 'desc_detail': desc_detail, 'begin_surplus': begin_surplus, 'end_surplus': total + begin_surplus)
        diamond_dailyReport_stat.save($$(row))
    }

    /**
     * 日期末结余
     * @param begin
     * @return
     */
    static Long lastDaySurplus(Long begin) {
        long yesterDay = begin - DAY_MILLON
        String ymd = new Date(yesterDay).format("yyyyMMdd")
        def last_day = diamond_dailyReport_stat.findOne($$(_id: "${ymd}_diamond_dailyReport_stat".toString()))
        return (last_day?.get('end_surplus') ?: 0) as Long;
    }

    /**
     * 充值统计(充值方式划分)
     * @param i
     * @return
     */
    static payStatics(int i) {
        def gteMill = yesTday - i * DAY_MILLON
        def YMD = new Date(gteMill).format("yyyyMMdd")
        def time = [$gte: gteMill, $lt: gteMill + DAY_MILLON]

        Map<String, Number> old_ids = new HashMap<String, Number>()
        finance_log_DB.aggregate(
                new BasicDBObject('$match', new BasicDBObject('via', [$ne: 'Admin'])),
                new BasicDBObject('$project', [_id: '$user_id', timestamp: '$timestamp']),
                new BasicDBObject('$group', [_id: '$_id', timestamp: [$min: '$timestamp']])
        ).results().each {
            def obj = $$(it as Map)
            old_ids.put(obj.get('_id') as String, obj.get('timestamp') as Number)
        }
        def typeMap = new HashMap<String, PayStat>()
        PayStat total = new PayStat()
        def pc = channel_pay_DB.find($$([client: "1", _id: [$ne: 'Admin']])).toArray()*._id
        def mobile = channel_pay_DB.find($$([client: ['$ne': "1"], _id: [$ne: 'Admin']])).toArray()*._id
        [pc    : pc,
         mobile: mobile,
        ].each { String k, List<String> v ->
            PayStat all = new PayStat()
            PayStat delta = new PayStat()
            def cursor = finance_log_DB.find($$([timestamp: time, via: [$in: v.toArray()]]),
                    $$(user_id: 1, cny: 1, coin: 1, timestamp: 1)).batchSize(50000)
            while (cursor.hasNext()) {
                def obj = cursor.next()
                def user_id = obj['user_id'] as String
                def cny = new BigDecimal(((Number) obj.get('cny')).doubleValue())
                def coin = obj.get('coin') as Long
                all.add(user_id, cny, coin)
                total.add(user_id, cny, coin)
                //该用户之前无充值记录或首冲记录为当天则算为当天新增用户
                if (old_ids.containsKey(user_id)) {
                    def userTimestamp = old_ids.get(user_id) as Long
                    Long day = gteMill
                    Long userday = new Date(userTimestamp).clearTime().getTime()
                    if (day.equals(userday)) {
                        delta.add(user_id, cny, coin)
                    }
                }
            }
            typeMap.put(k + 'all', all)
            typeMap.put(k + 'delta', delta)
        }
        coll.update(new BasicDBObject(_id: YMD + '_allpay'),
                new BasicDBObject(type: 'allpay',
                        user_pay: total.toMap(),
                        user_pay_pc: typeMap.get('pcall').toMap(),
                        user_pay_pc_delta: typeMap.get('pcdelta').toMap(),
                        user_pay_mobile: typeMap.get('mobileall').toMap(),
                        user_pay_mobile_delta: typeMap.get('mobiledelta').toMap(),
                        timestamp: gteMill
                ), true, false)
    }

    /**
     * 统计运营数据总表
     * @param i
     * @return
     */
    static staticTotalReport(int i) {
        long l = System.currentTimeMillis()
        def gteMill = yesTday - i * DAY_MILLON
        def date = new Date(gteMill)//
        def prefix = date.format('yyyyMMdd_')
        //运营统计报表
        def stat_report = mongo.getDB('xy_admin').getCollection('stat_report')
        // 查询充值信息
        def pay = coll.findOne(new BasicDBObject(_id: "${prefix}allpay".toString()))
        // 查询充值柠檬
        def pay_coin = coll.findOne(new BasicDBObject(_id: "${prefix}finance".toString()))
        // 查询注册人数
        def regs = users.count(new BasicDBObject(timestamp: [$gte: gteMill, $lt: gteMill + DAY_MILLON]))
        // 查询消费信息
        def cost = coll.findOne(new BasicDBObject(_id: "${prefix}allcost".toString()))
        def map = new HashMap()
        map.put('type', 'allreport')
        map.put('timestamp', gteMill)
        map.put('pay_coin', (pay_coin?.get('total_coin') ?: 0) as Integer)
        if (pay != null) {
            def user_pay = pay.get('user_pay') as BasicDBObject
            map.put('pay_cny', (user_pay.get('cny') ?: 0) as Double)
            map.put('pay_user', (user_pay.get('user') ?: 0) as Integer)
        }
        map.put('regs', regs)
        if (cost != null) {
            def user_cost = cost.get('user_cost') as BasicDBObject
            def coin = user_cost.get('cost') as Double
            def cost_cny = new BigDecimal(coin / 100).toDouble()
            map.put('cost_cny', cost_cny)
            map.put('cost_user', (user_cost.get('user') ?: 0) as Integer)
        }
        stat_report.update(new BasicDBObject(_id: "${prefix}allreport".toString()), new BasicDBObject(map), true, false)
    }

    private static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    private static BasicDBObject $$(Map map) {
        return new BasicDBObject(map)
    }

    static class PayType {
        final user = new HashSet(1000)
        final count = new AtomicInteger()
        final coin = new AtomicLong()
        def cny = new BigDecimal(0)

        def toMap() { [user: user.size(), count: count.get(), coin: coin.get(), cny: cny.doubleValue()] }
    }

    static class PayStat {
        final Set user = new HashSet(2000)
        final AtomicInteger count = new AtomicInteger()
        final AtomicLong coin = new AtomicLong()
        def BigDecimal cny = new BigDecimal(0)

        def toMap() { [user: user.size(), count: count.get(), coin: coin.get(), cny: cny.doubleValue()] }

        def add(def user_id, BigDecimal deltaCny, Long deltaCoin) {
            count.incrementAndGet()
            user.add(user_id)
            cny = cny.add(deltaCny)
            coin.addAndGet(deltaCoin)
        }

        def add(def user_id, BigDecimal deltaCny, Long deltaCoin, Integer deltaCount) {
            count.addAndGet(deltaCount)
            user.add(user_id)
            cny = cny.add(deltaCny)
            coin.addAndGet(deltaCoin)
        }
    }

    static red_packet_static(int i){
        def begin = yesTday - i * DAY_MILLON
        def YMD = new Date(begin).format('yyyyMMdd_')
        def timebetween = [$gte: begin, $lt: begin + DAY_MILLON]
        def red_packet_logs = mongo.getDB('game_log').getCollection('red_packet_logs')
        def row = new BasicDBObject('system': ['coin_count': 0, 'cash_count': 0, users: 0], 'newcomer': ['coin_count': 0, 'cash_count': 0, users: 0], 'friend': ['coin_count': 0, 'cash_count': 0, users: 0], 'unlock': ['coin_count': 0, 'cash_count': 0, users: 0], 'exchange': ['coin_count': 0, 'cash_count': 0, users: 0],'apply':['coin_count': 0, 'cash_count': 0, users: 0])
        red_packet_logs.aggregate(
                new BasicDBObject('$match', [timestamp: timebetween]),
                new BasicDBObject('$project', [type: '$type', coin_count: '$coin_count', cash_count: '$cash_count', user_id: '$user_id']),
                new BasicDBObject('$group', [_id: '$type', coin_count: [$sum: '$coin_count'], cash_count: [$sum: '$coin_count'], users: [$addToSet: '$user_id']])
        ).results().each { BasicDBObject obj ->
            println("obj is ${obj}")
            def type = obj.removeField('_id').toString()
            Set<Integer> userList = obj['users'] as Set<Integer>
            obj.put('users', userList.size())
            row.put(type, obj)
        }
        println("row is ${row}")

        // 兑换阳光
        def red_packet_cost_logs = mongo.getDB('game_log').getCollection('red_packet_cost_logs')
        red_packet_cost_logs.aggregate(
                new BasicDBObject('$match', [timestamp: timebetween]),
                new BasicDBObject('$project', [type: '$type', coin_count: '$coin_count', cash_count: '$cash_count', user_id: '$user_id']),
                new BasicDBObject('$group', [_id: '$type', coin_count: [$sum: '$coin_count'], cash_count: [$sum: '$cash_count'], users: [$addToSet: '$user_id']])
        ).results().each { BasicDBObject obj ->
            def type = obj.removeField('_id').toString()
            Set<Integer> userList = obj['users'] as Set<Integer>
            obj.put('users', userList.size())
            row.put(type, obj)
        }

        // 解锁阳光
        def unlock_logs = mongo.getDB('game_log').getCollection('red_packet_unlock_logs')
        unlock_logs.aggregate(
                new BasicDBObject('$match', [timestamp: timebetween]),
                new BasicDBObject('$project', [coin_count: '$coin_count', user_id: '$user_id']),
                new BasicDBObject('$group', [_id: null, coin_count: [$sum: '$coin_count'], users: [$addToSet: '$user_id']])
        ).results().each { BasicDBObject obj ->
            Set<Integer> userList = obj['users'] as Set<Integer>
            obj.put('users', userList.size())
            row.put('unlock', obj)
        }

        // 好友数
        def red_packet_friend_logs = mongo.getDB('game_log').getCollection('red_packet_friend_logs')
        def count = red_packet_friend_logs.distinct('user_id', $$('timestamp': timebetween)).size()
        row.put('friend_count', count)

        // 提现
        def red_packet_apply_logs = mongo.getDB('game_log').getCollection('red_packet_apply_logs')
        red_packet_apply_logs.aggregate(
                new BasicDBObject('$match', ['last_modify': timebetween, 'status': 2]),
                new BasicDBObject('$project', [income: '$income', user_id: '$user_id']),
                new BasicDBObject('$group', [_id: null, cash_count: [$sum: '$income'], users: [$addToSet: '$user_id']])
        ).results().each { BasicDBObject obj ->
            Set<Integer> userList = obj['users'] as Set<Integer>
            obj.put('users', userList.size())
            obj.remove('_id')
            obj.put('coin_count',0)
            row.put('apply', obj)
        }

        row.timestamp = begin
        row.type = "red_packet"
        coll.update(new BasicDBObject("_id", "${YMD}_red_packet".toString()), new BasicDBObject('$set', row), true, false)
    }

    /**
     * 标记任务完成  用于运维监控
     * @return
     */
    private static jobFinish(Long begin) {
        def timerName = 'Recovery'
        Long totalCost = System.currentTimeMillis() - begin
        saveTimerLogs(timerName, totalCost)
        println "${new Date().format('yyyy-MM-dd')}:${Recovery.class.getSimpleName()}:finish  cost  ${System.currentTimeMillis() - begin} ms"
    }

    //落地定时执行的日志
    private static saveTimerLogs(String timerName, Long totalCost) {
        def timerLogsDB = mongo.getDB("xyrank").getCollection("timer_logs")
        def tmp = System.currentTimeMillis()
        def id = timerName + "_" + new Date().format("yyyyMMdd")
        def update = new BasicDBObject(timer_name: timerName, cost_total: totalCost, cat: 'day', unit: 'ms', timestamp: tmp)
        timerLogsDB.findAndModify(new BasicDBObject('_id', id), null, null, false, new BasicDBObject('$set', update), true, true)
    }

    static int day = 2

    static void main(String[] args) {
        long l = System.currentTimeMillis()
        long begin = l

//        // 充值统计recovery
//        pay_statics_recovery(day)
//        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   pay_statics_recovery, cost  ${System.currentTimeMillis() - l} ms"
//        Thread.sleep(1000L)
//
//
//        // 钻石出入统计
//        l = System.currentTimeMillis()
//        diamond_statics_recovery(day)
//        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   diamond_statics_recovery, cost  ${System.currentTimeMillis() - l} ms"
//        Thread.sleep(1000L)
//
//
//        // 支付渠道统计充值
//        l = System.currentTimeMillis()
//        payStatics_recovery(day)
//        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   pay_statics_recovery, cost  ${System.currentTimeMillis() - l} ms"
//        Thread.sleep(1000L)

        // 经纪人旗下主播收益统计
//        l = System.currentTimeMillis()
//        staticBroker_recovery(day)
//        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticBroker_recovery, cost  ${System.currentTimeMillis() - l} ms"
//        Thread.sleep(1000L)

//        // 钻石出入统计
//        l = System.currentTimeMillis()
//        staticTotalReport_recovery(day)
//        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticTotalReport_recovery, cost  ${System.currentTimeMillis() - l} ms"
//        Thread.sleep(1000L)

        // 直播统计
//        l = System.currentTimeMillis()
//        staticLive_recovery(day)
//        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   staticLive_recovery, cost  ${System.currentTimeMillis() - l} ms"
//        Thread.sleep(1000L)

//        static_cash(day)

        static_red_packet(day)
        //落地定时执行的日志
//        jobFinish(begin)
    }

}