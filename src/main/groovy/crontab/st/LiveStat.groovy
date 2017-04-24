#!/usr/bin/env groovy
package crontab.st

import com.mongodb.MongoURI
import redis.clients.jedis.Jedis

@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
])
import java.text.SimpleDateFormat
import java.util.concurrent.atomic.AtomicLong
import com.mongodb.BasicDBObject
import com.mongodb.Mongo

/**
 *  直播统计
 *
 * date: 13-2-28 下午2:46
 * @author: yangyang.cong@ttpod.com
 */
class LiveStat {
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

    static final String live_jedis_host = getProperties("live_jedis_host", "192.168.31.246")
    static final Integer live_jedis_port = getProperties("live_jedis_port", 6379) as Integer
    static liveRedis = new Jedis(live_jedis_host, live_jedis_port)

    static mongo = new Mongo(new MongoURI(getProperties('mongo.uri', 'mongodb://192.168.31.231:20000,192.168.31.236:20000,192.168.31.231:20001/?w=1&slaveok=true') as String))

    static DAY_MILLON = 24 * 3600 * 1000L

    static long zeroMill = new Date().clearTime().getTime()
    static users = mongo.getDB('xy').getCollection('users')
    static lives = mongo.getDB('xylog').getCollection('room_edit')
    static room_cost = mongo.getDB('xylog').getCollection('room_cost')
    static star_award_logs = mongo.getDB('game_log').getCollection('star_award_logs')
    static room_follower_day = mongo.getDB('xylog').getCollection('room_follower_day')
    static share_logs = mongo.getDB('xylog').getCollection('share_logs')
    static room_meme_day = mongo.getDB('xylog').getCollection('room_meme_day')
    static stat_lives = mongo.getDB('xy_admin').getCollection('stat_lives')
    static rooms_rank = mongo.getDB('xyrank').getCollection('rooms')
    static rooms = mongo.getDB('xy').getCollection('rooms')
    static Long VALID_DAY = 7200

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

        //统计主播手机直播收益
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

        // 统计游戏分成得到的能量结束
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
                earndObj.award_count = 0
                earndObj.users = userSet.size()
                earndObj.second = 0
                stat_lives.update(new BasicDBObject("_id", earndObj._id), earndObj, true, false)
            }

        }
    }

    // 统计主播获得的分成能量
    static staticsAwardEarned(int i) {
        Long yesterday = zeroMill - i * DAY_MILLON
        def timeLimit = new BasicDBObject(timestamp: [$gte: zeroMill - 30 * DAY_MILLON], test: [$ne: true]) // 最近30天开播过的
        def starIds = rooms.find(timeLimit, new BasicDBObject("xy_star_id": 1)).toArray()*.xy_star_id
        def day = new Date(yesterday).format('yyyyMMdd')
        def timeBetween = Collections.unmodifiableMap([$gte: yesterday, $lt: yesterday + DAY_MILLON])
        starIds.each {
            Integer star_id ->
                String live_log_id = "${day}_${star_id}".toString()
                BasicDBObject liveObj = stat_lives.findOne(new BasicDBObject("_id", live_log_id)) as BasicDBObject
                if (liveObj == null) {
                    liveObj = new BasicDBObject(user_id: star_id,award_count:0, earned: 0, app_earned: 0, pc_earned: 0, lives: new HashSet<>())
                }
                def res = star_award_logs.aggregate(
                        new BasicDBObject($match: [timestamp: timeBetween,'room_id':star_id]),
                        new BasicDBObject($project: [_id: '$room_id', earned: '$award_earned']),
                        new BasicDBObject($group: [_id: null, earned: [$sum: '$earned']])
                ).results().iterator()

                def award_count = 0
                if(res.hasNext()){
                    award_count = res.next().earned
                }
                liveObj.put('award_count', award_count)
                liveObj.put('earned', award_count + ((liveObj?.get('earned') ?: 0) as Long))
                liveObj.put('app_earned', award_count + ((liveObj?.get('app_earned') ?: 0) as Long))
                liveObj.put('timestamp', yesterday)

                stat_lives.update(new BasicDBObject("_id", live_log_id), new BasicDBObject('$set', liveObj), true, false)
        }

    }

    //统计直播时长
    static staticsLiveTime(int i) {
        Long yesterday = zeroMill - i * DAY_MILLON
        def timeLimit = new BasicDBObject(timestamp: [$gte: zeroMill - 30 * DAY_MILLON], test: [$ne: true]) // 最近30天开播过的
        def starIds = rooms.find(timeLimit, new BasicDBObject("xy_star_id": 1)).toArray()*.xy_star_id

        def day = new Date(yesterday).format('yyyyMMdd')
        def timeBetween = Collections.unmodifiableMap([$gte: yesterday, $lt: yesterday + DAY_MILLON])

        starIds.each { Integer star_id ->
            if (star_id != null) {
                String live_log_id = "${day}_${star_id}".toString()
                BasicDBObject liveObj = stat_lives.findOne(new BasicDBObject("_id", live_log_id)) as BasicDBObject
                if (liveObj == null) {
                    liveObj = new BasicDBObject(user_id: star_id, earned: 0, app_earned: 0, pc_earned: 0, lives: new HashSet<>())
                }
                def liveSet = new HashSet(liveObj?.lives?:new HashSet<>())
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
                    stat_lives.update(new BasicDBObject("_id", live_log_id), new BasicDBObject('$set', liveObj), true, false)
                }

            }
        }

    }

    //统计主播总直播时长
    static staticsTotalLiveTime() {
        def timeLimit = new BasicDBObject(timestamp: [$gte: zeroMill - 2 * DAY_MILLON], test: [$ne: true]) // 最近2天开播过的
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

        def timeLimit = new BasicDBObject(timestamp: [$gte: zeroMill - 7 * DAY_MILLON], test: [$ne: true]) // 最近7天开播过的
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
        //检查正在直播
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

    //分享次数
    private static Integer shareCounts(Integer star_id, Map timeBetween) {
        return share_logs.count($$(star_id: star_id, timestamp: timeBetween))
    }

    //统计家族房间
    static staticsFamilyRoom(int i) {
        Long tmp = zeroMill - i * DAY_MILLON
        def day = new Date(tmp).format('yyyyMMdd')
        def timeBetween = Collections.unmodifiableMap([$gte: tmp, $lt: tmp + DAY_MILLON])
        //查出家族房
        def roomIds = rooms.find(new BasicDBObject(type: 2)).toArray()*._id
        room_cost.aggregate(
                new BasicDBObject('$match', ['session.data.xy_star_id': [$ne: null], timestamp: timeBetween, room: [$in: roomIds]]),
                new BasicDBObject('$project', [_id: '$session.data.xy_star_id', room: '$room', earned: '$session.data.earned', live: '$live']),
                new BasicDBObject('$group', [_id: [id: '$_id', room: '$room'], earned: [$sum: '$earned'], lives: [$addToSet: '$live']])
        ).results().each { BasicDBObject earndObj ->
            def user_id = earndObj.get('_id')?.getAt('id') as Integer
            def room_id = earndObj.get('_id')?.getAt('room') as Integer
            def liveSet = new HashSet(earndObj.lives)
            def earned = earndObj.get('earned') as Long
            def millLong = new AtomicLong()
            //主播在自己直播间直播时长
            def timeline = new HashSet()
            def qtime = [[timestamp: timeBetween], [etime: timeBetween]]
            lives.find(new BasicDBObject(type: "live_on", room: user_id, '$or': qtime))
                    .sort(new BasicDBObject(timestamp: 1)).toArray().each { BasicDBObject obj ->
                def stime = obj.get('timestamp') as Long
                stime = stime < tmp ? tmp : stime
                def etime = obj.get('etime') as Long
                etime = (etime == null || etime > tmp + DAY_MILLON) ? (tmp + DAY_MILLON) : etime
                timeline.add([stime: stime, etime: etime])
            }
            def query = new BasicDBObject(type: "live_on", 'room': room_id, 'session._id': user_id.toString(), '$or': qtime)
            lives.find(query).sort(new BasicDBObject(timestamp: 1)).toArray().each { BasicDBObject obj ->
                def stime = obj.get('timestamp') as Long
                stime = stime < tmp ? tmp : stime
                def etime = obj.get('etime') as Long
                etime = (etime == null || etime > tmp + DAY_MILLON) ? (tmp + DAY_MILLON) : etime
                def live_time = etime - stime
                timeline.each { Map between ->
                    def s = between.get('stime') as Long
                    def e = between.get('etime') as Long
                    if (s <= stime && e >= etime) {
                        live_time = 0
                    } else if (s >= stime && e <= etime) {
                        live_time = live_time - (e - s)
                    } else if (s >= stime && s < etime && e >= etime) {
                        live_time = live_time - (etime - s)
                    } else if (e > stime && e <= etime && s <= stime) {
                        live_time = live_time - (e - stime)
                    }
                }
                if (live_time > 0) {
                    millLong.addAndGet(live_time)
                }
            }
            //需要将原主播统计数据查询出来进行计算
            def _id = "${day}_${user_id}".toString()
            def liveMap = stat_lives.findOne(new BasicDBObject(_id: _id)) as Map
            if (liveMap == null) {
                liveMap = [lives: liveSet, earned: earned, timestamp: tmp, user_id: user_id]
            }
            def mills = millLong.intValue()
            def second = mills.intdiv(1000) + (liveMap.get('second') as Integer ?: 0)
            liveMap.put('second', second)
            liveMap.put('value', second >= VALID_DAY ? 1 : 0)
            stat_lives.update(new BasicDBObject("_id", "${day}_${user_id}".toString()),
                    new BasicDBObject($set: liveMap), true, false)
        }
    }

    /**
     * 首页直播间排序统计
     */
    static staticsRoomRank(int i) {
        Long tmp = zeroMill - i * DAY_MILLON
        def day = new Date(tmp).format('yyyyMMdd')
        //清除昨日统计分值
        rooms_rank.updateMulti($$(_id: [$gte: 0]), $$($unset: [yesterday: 1]))
        //运营加成积分归零
        //rooms_rank.updateMulti($$(op_points: [$gt: 0]), $$($set: [op_points: 0]))

        //昨日直播统计 收益 直播时长 送礼用户数量 昨日关注
        stat_lives.find($$(timestamp: tmp)).toArray().each { BasicDBObject liveStat ->
            def rank = new HashMap();
            Integer starId = liveStat.get('user_id') as Integer
            //收益 每1000vc 记录一分 10分上限
            Long earned = liveStat.get('earned') as Long
            Integer earned_points = cal_earned_points(earned)
            rank.earned = earned
            rank.earned_points = earned_points

            //直播时长 每60分钟 记录一分 10分上限
            Integer second = liveStat.get('second') as Integer
            Integer second_points = cal_second_points(second)
            rank.second = second
            rank.second_points = second_points

            //用户数量 每2个用户 记录一分 10分上限
            Integer users = liveStat.get('users') as Integer
            Integer users_points = cal_users_points(users)
            rank.users = users
            rank.users_points = users_points

            //昨日关注 收到35个关注记录10分
            Integer followers = liveStat.get('followers') as Integer
            Integer followers_points = cal_followers_points(followers)
            rank.followers = followers >= 0 ? followers : 0
            rank.followers_points = followers_points

            rank.total_points = earned_points + second_points + users_points + followers_points
            rank.timestamp = tmp
            rooms_rank.update($$(_id: starId), $$($set: ["yesterday": rank]), true, false)
        }
        //新人主播每日递减
        rooms_rank.updateMulti($$(new_points: [$gt: 0]), $$($inc: [new_points: -1]))
    }

    static Integer cal_earned_points(Long earned) {
        if (earned < 1000) return 0;
        Integer points = (earned / 1000) as Integer
        return points > 10 ? 10 : points
    }

    static Integer cal_second_points(Integer second) {
        if (second < 3600) return 0;
        Integer points = (second / 3600) as Integer
        return points > 10 ? 10 : points
    }

    static Integer cal_users_points(Integer users) {
        if (users < 2) return 0;
        Integer points = (users / 2) as Integer
        return points > 10 ? 10 : points
    }

    static Integer cal_followers_points(Integer users) {
        return users >= 35 ? 10 : 0
    }

    private static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    private static BasicDBObject $$(Map map) {
        return new BasicDBObject(map);
    }

    static Integer day = 1
    static void main(String[] args) {
        long l = System.currentTimeMillis()
        //直播统计
        long begin = l
        //每天赚取柠檬数量
        staticsEarned(day);
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   live staticsEarned cost  ${System.currentTimeMillis() - l} ms"

        //每日直播时长
        l = System.currentTimeMillis()
        staticsLiveTime(day);
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   live staticsLiveTime cost  ${System.currentTimeMillis() - l} ms"
        //每日关注数量/分享数量/平均人气
        staticsOthers(day);
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   live staticsOthers cost  ${System.currentTimeMillis() - l} ms"

        //家族房直播统计
        l = System.currentTimeMillis()
        staticsFamilyRoom(day)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   live staticsFamilyRoom cost  ${System.currentTimeMillis() - l} ms"

        //首页直播间排序统计
        l = System.currentTimeMillis()
        staticsRoomRank(day)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   live staticsRoomRank cost  ${System.currentTimeMillis() - l} ms"

        //主播总直播时长
        l = System.currentTimeMillis()
        staticsTotalLiveTime()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   live staticsTotalLiveTime cost  ${System.currentTimeMillis() - l} ms"

        //每天分成获取的能量
        staticsAwardEarned(day);
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   live staticsAwardEarned cost  ${System.currentTimeMillis() - l} ms"

        jobFinish(begin)
    }

    /**
     * 标记任务完成  用于运维监控
     * @return
     */
    private static jobFinish(Long begin) {
        def timerName = 'LiveStat'
        Long totalCost = System.currentTimeMillis() - begin
        saveTimerLogs(timerName, totalCost)
        println "${new Date().format('yyyy-MM-dd')}:${LiveStat.class.getSimpleName()}:finish  cost  ${System.currentTimeMillis() - begin} ms"
    }

    //落地定时执行的日志
    private static saveTimerLogs(String timerName, Long totalCost) {
        def timerLogsDB = mongo.getDB("xyrank").getCollection("timer_logs")
        def tmp = System.currentTimeMillis()
        def id = timerName + "_" + new Date().format("yyyyMMdd")
        def update = new BasicDBObject(timer_name: timerName, cost_total: totalCost, cat: 'day', unit: 'ms', timestamp: tmp)
        timerLogsDB.findAndModify(new BasicDBObject('_id', id), null, null, false, new BasicDBObject('$set', update), true, true)
    }

}