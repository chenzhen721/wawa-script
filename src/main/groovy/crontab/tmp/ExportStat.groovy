#!/usr/bin/env groovy
package crontab.tmp

import com.mongodb.BasicDBObject
import com.mongodb.DBCollection
import com.mongodb.DBCursor
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
])
import com.mongodb.Mongo
import com.mongodb.DBObject
import com.mongodb.MongoURI
import org.apache.commons.lang.StringUtils
import org.apache.commons.lang.math.NumberUtils

import java.text.SimpleDateFormat
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/**
 * 渠道统计数据恢复
 */
class ExportStat {
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


    static mongo = new Mongo(new MongoURI(getProperties('mongo.uri', 'mongodb://192.168.31.249:10000/?w=1') as String))
    static historyMongo = new Mongo(new MongoURI(getProperties('mongo_history.uri', 'mongodb://192.168.31.246:27017/?w=1') as String))
    static historyDB = historyMongo.getDB('xylog_history')
    static DAY_MILLON = 24 * 3600 * 1000L

    static long zeroMill = new Date().clearTime().getTime()
    static Long yesTday = zeroMill - DAY_MILLON
//    static String YMD = new Date(yesTday).format("yyyyMMdd")
    static room_cost = mongo.getDB('xylog').getCollection('room_cost')
    static room_edit = mongo.getDB('xylog').getCollection('room_edit')
    static trade_logs = mongo.getDB('xylog').getCollection('trade_logs')
    static day_login = mongo.getDB('xylog').getCollection('day_login')
    static debug_logs = mongo.getDB('xylog').getCollection('debug_logs')
    static room_cost_2014 = historyDB.getCollection('room_cost_2014')
    static room_cost_2015 = historyDB.getCollection('room_cost_2015')
    static room_cost_2016 = historyDB.getCollection('room_cost_2016')
    static finance_log = mongo.getDB('xy_admin').getCollection('finance_log')
    static applys = mongo.getDB('xy_admin').getCollection('applys')
    static users = mongo.getDB("xy").getCollection("users")
    static rooms = mongo.getDB("xy").getCollection("rooms")
    static xy_users = mongo.getDB("xy_user").getCollection("users")
    static lottery_logs = mongo.getDB('xylog').getCollection('lottery_logs')
    static channels = mongo.getDB('xy_admin').getCollection('channels')

    public static final String ls = System.lineSeparator();
    public static Map<Integer, Double> preMonthStoreUserMapForPC = new HashMap<Integer, Double>()
    public static Map<Integer, Double> preMonthStoreUserMapForAndroid = new HashMap<Integer, Double>()
    public static List<Integer> storeUserList = new ArrayList<Integer>()

    public static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    public static BasicDBObject $$(Map map) {
        return new BasicDBObject(map);
    }

    /**
     * 个别用户的消费统计
     * @return
     */
    static costStatic() {
        def folder_path = '/empty/static/'
        File folder = new File(folder_path)
        if (!folder.exists()) {
            folder.mkdirs()
        }
        def ids = [17109610]
        def startStr = "2016-03-01 000000"
        def endStr = "2016-04-01 000000"
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HHmmss")
        def stime = 0L
        def etime = 0L
        try {
            stime = sdf.parse(startStr).getTime()
            etime = sdf.parse(endStr).getTime()
        } catch (Exception e) {
            println e
        }
        def query = new BasicDBObject([timestamp: ['$gte': stime, '$lt': etime], type: 'send_gift'])
        println "query: ${query}".toString()
        ids.each { Integer id ->
            println "count= " + room_cost_2016.count(new BasicDBObject('session._id', id as String))
            def buf = new StringBuffer()
            buf.append("时间,用户ID,房间ID,主播ID,礼物名称,数量,花费柠檬").append(ls)
            room_cost_2016.find(query.append('session._id', id as String)).toArray().each { BasicDBObject obj ->
                def timestamp = obj.get('timestamp') as Long
                def _id = obj.get('session')?.getAt('_id')
                def xy_star_id = obj.get('session')?.getAt('data')?.getAt('xy_star_id')
                def xy_user_id = obj.get('session')?.getAt('data')?.getAt('xy_user_id')
                def name = obj.get('session')?.getAt('data')?.getAt('name')
                def count = obj.get('session')?.getAt('data')?.getAt('count')
                buf.append((timestamp != null) ? new Date(timestamp as Long).format('yyyy-MM-dd HH:mm:ss') : '').append(',')
                buf.append(_id).append(',')
                buf.append(obj.get('room')).append(',')
                buf.append(xy_star_id != null ? xy_star_id : xy_user_id).append(',')
                buf.append(name).append(',')
                buf.append(count).append(',')
                buf.append(obj.get('cost')).append(ls)
            }
            //写入文件
            File file = new File(folder_path + "/${id}.csv");
            if (!file.exists()) {
                file.createNewFile();
            }
            file.withWriterAppend { Writer writer ->
                writer.write(buf.toString())
                writer.flush()
                writer.close()
            }
        }
    }

    /**
     * 用户充值流水
     * @return
     */
    static chargeStatic() {
        def folder_path = '/empty/static/'
        File folder = new File(folder_path)
        if (!folder.exists()) {
            folder.mkdirs()
        }
        def ids = [16583767, 16474574, 15593546, 15150349]
        def startStr = "20140101000000"
        def endStr = "20160501000000"

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss")
        def stime = 0L
        def etime = 0L
        try {
            stime = sdf.parse(startStr).getTime()
            etime = sdf.parse(endStr).getTime()
        } catch (Exception e) {
            println e
        }
        def query = new BasicDBObject([timestamp: ['$gte': stime, '$lt': etime]])
        println "query: ${query}".toString()
        ids.each { Integer id ->
            def buf = new StringBuffer()
            buf.append("date,uid,cny,coin,via").append(ls)
            finance_log.find(query.append('user_id', id as Integer)).toArray().each { BasicDBObject obj ->
                def timestamp = obj.get('timestamp') as Long
                def user_id = obj.get('user_id')
                def cny = obj.get('cny')
                def coin = obj.get('coin')
                def via = obj.get('via')

                buf.append((timestamp != null) ? new Date(timestamp as Long).format('yyyy-MM-dd HH:mm:ss') : '').append(',')
                buf.append(user_id).append(',')
                buf.append(cny).append(',')
                buf.append(coin).append(',')
                buf.append(via).append(ls)
            }
            //写入文件
            File file = new File(folder_path + "/${id}_charge.csv");
            if (!file.exists()) {
                file.createNewFile();
            }
            file.withWriterAppend { Writer writer ->
                writer.write(buf.toString())
                writer.flush()
                writer.close()
            }
        }
    }

    /**
     * 每月充值统计
     * @param i
     * @return
     */
    static chargeStatistic(int i) {
        Calendar cal = getCalendar()
        cal.add(Calendar.MONTH, -i)
        long firstDayOfCurrentMonth = cal.getTimeInMillis()  //当月第一天
        cal.add(Calendar.MONTH, -1);
        long firstDayOfLastMonth = cal.getTimeInMillis()  //上月第一天
        String ym_c = new Date(firstDayOfCurrentMonth).format("yyyy-MM-dd")
        String ym_l = new Date(firstDayOfLastMonth).format("yyyy-MM-dd")

        StringBuffer strBuf = new StringBuffer()
        //println "firstDayOfLastMonth: ${ym_l}  firstDayOfCurrentMonth: ${ym_c}".toString()
        finance_log.aggregate(
                new BasicDBObject('$match', [timestamp: [$gte: firstDayOfLastMonth, $lt: firstDayOfCurrentMonth], via: 'itunes']),
                new BasicDBObject('$project', [user_id: '$user_id', cny: '$cny', coin: '$coin', qd: '$qd']),
                new BasicDBObject('$group', [_id: null, cny: [$sum: '$cny'], coin: [$sum: '$coin']]),
                new BasicDBObject('$sort', [cny: -1])
        ).results().each { BasicDBObject obj ->
            strBuf.append(obj['cny'] + "," + obj['coin'])
        }
        println ym_l + "," + strBuf.toString()
    }
    /**
     * 个别主播收礼流水
     * @return
     */
    static earnedStatic() {
        def folder_path = '/empty/static/'
        File folder = new File(folder_path)
        if (!folder.exists()) {
            folder.mkdirs()
        }
        def ids = [16789646]
        def startStr = "20160101000000"
        def endStr = "20160201000000"
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss")
        def stime = 0L
        def etime = 0L
        try {
            stime = sdf.parse(startStr).getTime()
            etime = sdf.parse(endStr).getTime()
        } catch (Exception e) {

        }
        def query = new BasicDBObject([timestamp: ['$gte': stime, '$lt': etime], type: 'send_gift'])
        ids.each { Integer id ->
            def buf = new StringBuffer()
            buf.append("时间,主播ID,用户ID,礼物名称,数量,柠檬,VC").append(ls)
            room_cost_2016.find(query.append('session.data.xy_star_id', id as String)).toArray().each { BasicDBObject obj ->
                def timestamp = obj.get('timestamp') as Long
                def _id = obj.get('session')?.getAt('_id')
                def xy_star_id = obj.get('session')?.getAt('data')?.getAt('xy_star_id')
                def name = obj.get('session')?.getAt('data')?.getAt('name')
                def count = obj.get('session')?.getAt('data')?.getAt('count')
                def earned = obj.get('session')?.getAt('data')?.getAt('earned')

                buf.append((timestamp != null) ? new Date(timestamp as Long).format('yyyyMMddHHmmss') : '').append(',')
                buf.append(xy_star_id).append(',')
                buf.append(_id).append(',')
                buf.append(name).append(',')
                buf.append(count).append(',')
                buf.append(obj.get('cost')).append(',')
                buf.append(earned).append(ls)
            }
            //写入文件
            File file = new File(folder_path + "/${id}.csv");
            if (!file.exists()) {
                file.createNewFile();
            }
            file.withWriterAppend { Writer writer ->
                writer.write(buf.toString())
                writer.flush()
                writer.close()
            }
        }
    }

    /**
     * 富豪等级top100用户信息
     * @return
     */
    static richTopStatic() {
        DBCollection finance_log_DB = mongo.getDB('xy_admin').getCollection('finance_log')
        DBCollection userDB = mongo.getDB("xy").getCollection("users")
        def spendQ = new BasicDBObject('finance.coin_spend_total': [$gt: 0])
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss")
        def buf = new StringBuffer()
        userDB.find(spendQ, new BasicDBObject(["_id": 1, "qd": 1, "nick_name": 1, "finance.coin_spend_total": 1, "finance.coin_count": 1]))
                .sort(new BasicDBObject("finance.coin_spend_total", -1)).limit(100).toArray().each { BasicDBObject obj ->
            def _id = obj.get('_id') as Integer
            def nickName = obj.get('nick_name') as String
            def qd = obj.get('qd')
            def coin = obj.get('finance')?.getAt('coin_count')
            if (StringUtils.isNotBlank(nickName)) {
                nickName = "\"" + nickName.replaceAll("\"", "\"\"") + "\""
            }
            buf.append(_id).append(",").append(nickName).append(",").append(qd).append(",")
            finance_log_DB.aggregate(
                    new BasicDBObject('$match', new BasicDBObject('user_id': _id, 'via': [$ne: 'Admin'])),
                    new BasicDBObject('$project', [_id: '$user_id', timestamp: '$timestamp']),
                    new BasicDBObject('$group', [_id: '$_id', min: [$min: '$timestamp'], max: [$max: '$timestamp']])
            ).results().each {
                def o = new BasicDBObject(it as Map)
                println o.get('_id') + ":" + o.get('min') + ":" + o.get('max')
                def min = o.get('min') as Long
                def max = o.get('max') as Long
                buf.append(min == null ? "" : sdf.format(new Date(min))).append(",")
                buf.append(max == null ? "" : sdf.format(new Date(max))).append(",")
            }
            buf.append(coin).append(System.lineSeparator())
        }
        def folder_path = '/empty/static/'
        File folder = new File(folder_path)
        if (!folder.exists()) {
            folder.mkdirs()
        }
        //写入文件
        File file = new File(folder_path + "/rich_top.csv");
        if (!file.exists()) {
            file.createNewFile();
        }
        file.withWriterAppend { Writer writer ->
            writer.write(buf.toString())
            writer.flush()
            writer.close()
        }
    }

    /**
     * 按月统计充值大于1000元的用户信息
     */
    static mvpStatic(int i) {
        DBCollection userDB = mongo.getDB("xy").getCollection("users")
        def finance_log_DB = mongo.getDB('xy_admin').getCollection('finance_log')
        Calendar cal = getCalendar()
        cal.add(Calendar.MONTH, -i)
        long firstDayOfCurrentMonth = cal.getTimeInMillis()  //当月第一天
        cal.add(Calendar.MONTH, -1);
        long firstDayOfLastMonth = cal.getTimeInMillis()  //上月第一天
        String ym = new Date(firstDayOfLastMonth).format("yyyyMM")

        StringBuffer strBuf = new StringBuffer()
        strBuf.append("日期：" + ym).append(System.lineSeparator())
        finance_log_DB.aggregate(
                new BasicDBObject('$match', [timestamp: [$gte: firstDayOfLastMonth, $lt: firstDayOfCurrentMonth], via: [$ne: 'Admin']]),
                new BasicDBObject('$project', [user_id: '$user_id', cny: '$cny', qd: '$qd']),
                new BasicDBObject('$group', [_id: '$user_id', cny: [$sum: '$cny'], qd: [$addToSet: '$qd']]),
                new BasicDBObject('$match', [cny: [$gte: 1000]]),
                new BasicDBObject('$sort', [cny: -1])
        ).results().each { BasicDBObject obj ->
            def user = userDB.findOne(new BasicDBObject('_id': obj.get('_id') as Integer), new BasicDBObject('nick_name': 1, timestamp: 1)) as BasicDBObject
            def nickName = user?.get('nick_name') as String
            if (StringUtils.isNotBlank(nickName)) {
                nickName = "\"" + nickName.replaceAll("\"", "\"\"") + "\""
            }
            def timestamp = user?.get('timestamp') as Long
            def qd = "\"" + obj.get('qd') + "\""
            strBuf.append(obj.get('_id')).append(',').append(obj.get('cny')).append(',')
                    .append(new Date(timestamp).format('yyyyMMdd')).append(',').append(qd).append(',')
                    .append(nickName).append(System.lineSeparator())
        }
        //写入文件
        def folder_path = '/empty/static/'
        File folder = new File(folder_path)
        if (!folder.exists()) {
            folder.mkdirs()
        }
        File file = new File(folder_path + "/mvp_static.csv");
        if (file.exists()) {
            file.delete();
        }
        file.createNewFile();
        file.withWriterAppend { Writer writer ->
            writer.write(strBuf.toString())
            writer.flush()
            writer.close()
        }
    }

    static payStaticsByUserChannel() {
//        def coll = mongo.getDB('xy_admin').getCollection('stat_daily')
        def users = mongo.getDB('xy').getCollection('users')
        def finance_log = mongo.getDB('xy_admin').getCollection('finance_log')
        def channel = mongo.getDB('xy_admin').getCollection('channels')
        def sdf = new SimpleDateFormat('yyyy-MM-dd')

        def tenDayTime = 10 * 24 * 3600 * 1000L

        def flag = Boolean.TRUE
        StringBuffer strBuf = new StringBuffer()
        def timeList = ['2014-10-01', '2014-10-11', '2014-10-21', '2014-11-01', '2014-11-11', '2014-11-21', '2014-12-01', '2014-12-11', '2014-12-21', '2014-12-28']
        while (timeList.size() > 1) {
            def deBeginStr = timeList.remove(0), deEndStr = timeList.get(0)
            def begin = sdf.parse(deBeginStr).getTime(), end = sdf.parse(deEndStr).getTime()
            strBuf.append("日期：" + new Date(begin).format('yyyy-MM-dd')).append(',')
            def timeBetween = [$gte: begin, $lt: end]
            def logMap = new HashMap()
            //查询当天充值的所有用户及充值金额、获得的柠檬数、充值次数
            finance_log.aggregate(
                    new BasicDBObject('$match', [via: [$ne: 'Admin'], timestamp: timeBetween]),
                    new BasicDBObject('$project', [user_id: '$user_id', coin: '$coin', cny: '$cny', count: '$count']),
                    new BasicDBObject('$group', [_id: '$user_id', coin: [$sum: '$coin'], cny: [$sum: '$cny'], count: [$sum: 1]])
            ).results().each { BasicDBObject logObj ->
                def id = logObj.get('_id') as Integer
                logMap.put(id, logObj)
            }
            if (logMap.size() > 0) {
                def qdMap = new HashMap(), clientMap = new HashMap()
                def YMD = new Date(begin).format("yyyyMMdd")
                //查询用户对应的注册渠道
                users.aggregate(
                        new BasicDBObject('$match', [_id: [$in: logMap.keySet()]]),
                        new BasicDBObject('$project', [qd: '$qd', userId: '$_id']),
                        new BasicDBObject('$group', [_id: '$qd', users: [$addToSet: '$userId']])
                ).results().each { BasicDBObject userObj ->
                    qdMap.put(userObj.get('_id') as String, userObj.get('users'))
                }
                channel.aggregate(
                        new BasicDBObject('$match', [_id: [$in: qdMap.keySet()], client: '2']),
                        new BasicDBObject('$project', [client: '$client', qdId: '$_id']),
                        new BasicDBObject('$group', [_id: '$client', qdIds: [$addToSet: '$qdId']])
                ).results().each { BasicDBObject clientObj ->
                    def client = clientObj.get('_id') as String
                    client = "2".equals(client) ? client : "1"
                    def payStat = clientMap.get("client") as PayStat
                    if (payStat == null) {
                        payStat = new PayStat()
                        clientMap.put(client, payStat)
                    }
                    def qdIds = clientObj.get('qdIds') as List
                    qdIds.each { String qdId ->
                        def uids = qdMap.remove(qdId) as List
                        uids.each { Integer userId ->
                            def finance = logMap.remove(userId) as BasicDBObject
                            def cny = finance.get('cny') ?: 0
                            def coin = finance.get('coin') ?: 0
                            def count = finance.get('count') ?: 0
                            payStat.add(userId, cny as Double, coin as Long, count as Integer)
                        }
                    }
                }
                //处理未添加在基础数据中的渠道数据，默认为pc端
//                logMap.each { Integer k, BasicDBObject finance ->
//                    def payStat = clientMap.get("1") as PayStat
//                    if (payStat == null) {
//                        payStat = new PayStat()
//                        clientMap.put("1", payStat)
//                    }
//                    def cny = finance.get('cny') ?: 0
//                    def coin = finance.get('coin') ?: 0
//                    def count = finance.get('count') ?: 0
//                    payStat.add(k, cny as Double, coin as Long, count as Integer)
//                }
//                def user_pc = clientMap.get('1') as PayStat
                def user_mobile = clientMap.get('2') as PayStat
                if (user_mobile != null) {
                    strBuf.append(user_mobile.user?.size()).append(',').append(user_mobile.cny)
                }
//                def setVal = [type: 'allpay', timestamp: begin] as Map
//                if (user_pc != null) {
//                    setVal.put('user_pc', user_pc.toMap())
//                }
//                if (user_mobile != null) {
//                    setVal.put('user_mobile', user_mobile.toMap())
//                }
//            coll.update(new BasicDBObject(_id: YMD + '_allpay'), new BasicDBObject('$set': setVal), true, false)

            }
            strBuf.append(System.lineSeparator())
        }
        //写入文件
        def folder_path = '/empty/static/'
        File folder = new File(folder_path)
        if (!folder.exists()) {
            folder.mkdirs()
        }
        File file = new File(folder_path + "/paybyuser_static.csv");
        if (!file.exists()) {
            file.createNewFile();
        }
        file.withWriterAppend { Writer writer ->
            writer.write(strBuf.toString())
            writer.flush()
            writer.close()
        }
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

    private static Calendar getCalendar() {
        Calendar cal = Calendar.getInstance()//获取当前日期
        cal.set(Calendar.DAY_OF_MONTH, 1)//设置为1号,当前日期既为本月第一天
        cal.set(Calendar.HOUR_OF_DAY, 0)
        cal.set(Calendar.MINUTE, 0)
        cal.set(Calendar.SECOND, 0)
        cal.set(Calendar.MILLISECOND, 0)
        return cal
    }

    /**
     * 导出用户事件(消费)记录
     */
    static exportUserEvent() {
        println '开始生成用户事件cvs'

        StringBuffer userEventStr = new StringBuffer("user_id,timestamp,event_name,param1,value1,param2,value2")
        userEventStr.append(System.lineSeparator())
        def userSet = [] as HashSet
        def registerTM

        def roomCostCollectionDB = mongo.getDB('xylog').getCollection('room_cost')
        def eventCondin = new BasicDBObject('timestamp': [$gte: 1459864800000, $lt: 1459870200000])
        def eventExists = new BasicDBObject(["session._id": 1, "session.room_id": 1, "type": 1, "timestamp": 1, "star_cost": 1, room: 1])
        roomCostCollectionDB.find(eventCondin, eventExists).toArray().each {
            BasicDBObject obj ->
                userEventStr.append(obj.session._id).append(',')
                if (obj.timestamp != null)
                    registerTM = new Date(obj.timestamp).format('yyyy-MM-dd')
                userEventStr.append(registerTM).append(',')
                userEventStr.append(obj.type).append(',')
                userEventStr.append('消费金额').append(',')
                userEventStr.append(obj.star_cost).append(',')
                userEventStr.append('房间Id').append(',')
                userEventStr.append(obj.room)
                userEventStr.append(System.lineSeparator())
                if (userSet.size() <= 1000)
                    userSet.add(obj.session._id)
        }

        def folder_path = '/empty/static'
        File folder = new File(folder_path)
        if (!folder.exists()) {
            folder.mkdirs()
        }
        File file = new File(folder_path + "/userEvent.csv");
        if (!file.exists()) {
            file.createNewFile();
        }

        file.withWriterAppend { Writer writer ->
            writer.write(userEventStr.toString())
            writer.flush()
            writer.close()
        }

        println '生成用户事件cvs结束'

        println '开始生成用户信息cvs,[userSet = ' + userSet + ']'

        def userCollectionDB = mongo.getDB('xy').getCollection('users')
        def userExists = new BasicDBObject(['_id': 1, 'pic': 1, 'nick_name': 1, 'timestamp': 1])

        StringBuffer userStr = new StringBuffer('user_id,nick_name,pic,registerTM')
        userStr.append(System.lineSeparator())
        userSet.each {
            userId ->
                def obj = userCollectionDB.findOne(new BasicDBObject('_id': userId as Integer), userExists) as BasicDBObject
                userStr.append(userId).append(',')
                if (obj.timestamp != null)
                    registerTM = new Date(obj.timestamp).format('yyyy-MM-dd')
                userStr.append(obj.nick_name).append(',')
                userStr.append(obj.pic).append(',')
                userStr.append(registerTM)
                userStr.append(System.lineSeparator())
        }


        File userFile = new File(folder_path + "/user.csv");
        if (!userFile.exists()) {
            userFile.createNewFile();
        }

        userFile.withWriterAppend { Writer writer ->
            writer.write(userStr.toString())
            writer.flush()
            writer.close()
        }

        println '生成用户信息cvs结束'

    }

    static exportVipAndGuard() {
        List vipList = getCostLogs(new BasicDBObject(type: 'buy_vip'))
        List vipResult = new ArrayList(vipList.size())
        vipList.each { DBObject vip ->
            Integer days = vip['live'] as Integer
            if (days > 30) {
                vipResult.add(vip)
            }
        }
        List guardList = getCostLogs(new BasicDBObject(type: 'buy_guard', 'session.data.days': [$gt: 30]))
        export(vipResult, 'vip')
        export(guardList, 'guard')
    }

    private static void export(List<DBObject> costList, String type) {
        StringBuffer userStr = new StringBuffer('date,userid,cost,days')
        userStr.append(System.lineSeparator())
        costList.each { DBObject costLog ->
            userStr.append(new Date(costLog['timestamp'] as Long).format('yyyy-MM-dd')).append(',')
            userStr.append((costLog['_id'] as String).split("_")[0]).append(',')
            userStr.append(costLog['cost']).append(',')
            userStr.append(getDays(costLog, type))
            userStr.append(System.lineSeparator())
        }

        def folder_path = '/empty/static'
        File file = new File(folder_path + "/cost_${type}.csv");
        if (!file.exists()) {
            file.createNewFile();
        }

        file.withWriterAppend { Writer writer ->
            writer.write(userStr.toString())
            writer.flush()
            writer.close()
        }
    }

    private static Integer getDays(DBObject costLog, String type) {
        if (type.equals('vip')) {
            return costLog['live'] as Integer
        } else if (type.equals('guard')) {
            def data = (costLog['session'] as Map)['data'] as Map
            return data['days'] as Integer
        }
        return 0;
    }


    private static List getCostLogs(BasicDBObject query) {
        List list = new ArrayList(2000)
        list.addAll(room_cost_2014.find(query).toArray())
        list.addAll(room_cost_2015.find(query).toArray())
        list.addAll(room_cost_2016.find(query).toArray())
        list.addAll(room_cost.find(query).toArray())
        return list
    }

    /**
     * 导出手机微信支付流水
     */
    def static exportWeiXin_m() {
        def startStr = "20160420000000"
        def endStr = "20160421000000"
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss")
        def stime = sdf.parse(startStr).getTime()
        def etime = sdf.parse(endStr).getTime()

        def query = new BasicDBObject([timestamp: ['$gte': stime, '$lt': etime], via: 'weixin_m'])
        println "query: ${query}".toString()
        // 定义cvs表头
        def buf = new StringBuffer('交易时间, 用户ID, 到账用户ID,  充值金额, 充值柠檬, 返柠檬, 联运渠道号, 商家订单号, 交易号')
        def column = ['timestamp', 'user_id', 'to_id', 'cny', 'coin', 'returnCoin', 'qd', 'rid', '_id']
        buf.append(System.lineSeparator())
        def value

        finance_log.find(query).toArray().each {
            BasicDBObject obj ->
                column.each {
                    value = obj.get(it)
                    if (it == 'timestamp') {
                        buf.append(new Date(value as Long).format('yyyy-MM-dd HH:mm:ss')).append(',')
                    } else {
                        buf.append(String.valueOf(value)).append(',')
                    }
                }
                buf.replace(buf.length() - 1, buf.length(), '').append(System.lineSeparator())
        }

        // TODO 路径要修改
        def folder_path = '/empty/static'
        File folder = new File(folder_path)
        if (!folder.exists()) {
            folder.mkdirs()
        }

        File file = new File(folder_path + "/weixin_m_20160420.csv");
        if (!file.exists()) {
            file.createNewFile()
        }
        file.withWriterAppend { Writer writer ->
            writer.write(buf.toString())
            writer.flush()
            writer.close()
        }

    }

    /**
     * 导出用户等级大于等于三级的用户
     */
    static exportUsers() {
        def buf = new StringBuffer('user_id, spend_total')
        buf.append(System.lineSeparator())
        def cursor = users.find(new BasicDBObject('finance.coin_spend_total': [$gte: 15000]), new BasicDBObject('finance.coin_spend_total': 1)).batchSize(5000)
        while (cursor.hasNext()) {
            def user = cursor.next()
            String uid = user['_id'] as String
            Map finance = user['finance'] as Map
            Long coin_spend_total = finance['coin_spend_total'] as Long
            buf.append(uid).append(',')
            buf.append(coin_spend_total)
            buf.append(System.lineSeparator())
        }

        def folder_path = '/empty/static'
        File folder = new File(folder_path)
        if (!folder.exists()) {
            folder.mkdirs()
        }

        File file = new File(folder_path + "/user_info.csv");
        if (!file.exists()) {
            file.createNewFile()
        }
        file.withWriterAppend { Writer writer ->
            writer.write(buf.toString())
            writer.flush()
            writer.close()
        }
    }

    static void countBagGifts() {
        List<Integer> userIds = new ArrayList<>()
        DBCursor cursor = lottery_logs.find($$(active_name: 'app_meme_luck_gift', award_name: '118'), $$(user_id: 1)).batchSize(10000)
        while (cursor.hasNext()) {
            def lottery = cursor.next()
            userIds.add(lottery['user_id'] as Integer)
        }

        def buf = new StringBuffer('user_id, count')
        buf.append(System.lineSeparator())

        DBCursor usersCursor = users.find($$(_id: [$in: userIds], 'bag.118': [$gt: 0]), $$('bag.118': 1, nick_name: 1)).batchSize(5000)
        while (usersCursor.hasNext()) {
            def user = usersCursor.next()
            Map bag = user["bag"] as Map
            buf.append(user['_id']).append(',')
            buf.append(bag['118'])
            buf.append(System.lineSeparator())
        }
        def folder_path = '/empty/static'
        File folder = new File(folder_path)
        if (!folder.exists()) {
            folder.mkdirs()
        }

        File file = new File(folder_path + "/user_huoju.csv");
        if (!file.exists()) {
            file.createNewFile()
        }
        file.withWriterAppend { Writer writer ->
            writer.write(buf.toString())
            writer.flush()
            writer.close()
        }
    }

    /**
     * 点乐 百度积分墙
     */
    static void jifen(String via) {
        def buf = new StringBuffer('date, user_id, nick_name, coins')
        buf.append(System.lineSeparator())
        long begin_date = Date.parse("yyyy-MM-dd HH:mm:ss", "2016-03-01 00:00:00").getTime()
        DBCursor usersCursor = trade_logs.find($$(via: via, time: [$gte: begin_date]), $$([resp: 1, uid: 1, time: 1])).batchSize(5000).sort($$('time', -1))
        while (usersCursor.hasNext()) {
            DBObject obj = usersCursor.next()
            if (obj.get('time') != null) {
                buf.append(new Date(obj.get('time') as Long).format('yyyy-MM-dd HH:mm:ss')).append(',')
            }
            def uid = obj.get('uid') as Integer
            buf.append(uid).append(',')
            buf.append(users.findOne($$(_id: uid), $$(nick_name: 1))?.get('nick_name')).append(',')
            def resp = obj.get('resp') as Map
            def coin = resp?.get('coin') as Integer
            if (coin != null) {
                buf.append(Math.abs(coin))
            }
            buf.append(System.lineSeparator())
        }
        def folder_path = '/empty/static'
        File folder = new File(folder_path)
        if (!folder.exists()) {
            folder.mkdirs()
        }

        File file = new File(folder_path + "/user_${via}.csv");
        if (!file.exists()) {
            file.createNewFile()
        }
        file.withWriterAppend { Writer writer ->
            writer.write(buf.toString())
            writer.flush()
            writer.close()
        }
    }

    //ID，昵称，上次开播时间，签约时间，经纪人，联系方式
    static void allStarInfo() {
        def buf = new StringBuffer('id, live_time, apply_time, broker, tel')
        buf.append(System.lineSeparator())
        DBCursor usersCursor = users.find($$(priv: 2))
        while (usersCursor.hasNext()) {
            DBObject obj = usersCursor.next()
            def uid = obj.get('_id') as Integer
            def star = obj.get('star') as Map
            def myroom = rooms.findOne($$('xy_star_id': uid))
            def apply = applys.findOne($$('xy_user_id': uid, status: 2))
            String tel = ' '
            if (apply) {
                tel = apply?.get('tel') ?: ''
            }
            buf.append(uid).append(',')

            if (myroom.get('timestamp')) {
                buf.append(new Date(myroom.get('timestamp') as Long).format('yyyy-MM-dd HH:mm:ss')).append(',')
            } else {
                buf.append(" ").append(',')
            }

            buf.append(new Date(star.get('timestamp') as Long).format('yyyy-MM-dd HH:mm:ss')).append(',')
            buf.append(star.get('broker')).append(',')
            buf.append(tel)
            buf.append(System.lineSeparator())
        }
        def folder_path = '/empty/static'
        File folder = new File(folder_path)
        if (!folder.exists()) {
            folder.mkdirs()
        }

        File file = new File(folder_path + "/star_info.csv");
        if (!file.exists()) {
            file.createNewFile()
        }
        file.withWriterAppend { Writer writer ->
            writer.write(buf.toString())
            writer.flush()
            writer.close()
        }
    }


    static void daylogin() {
        def buf = new StringBuffer('date, total, uname_total, rate')
        buf.append(System.lineSeparator())
        DBCursor usersCursor = day_login.find().batchSize(10000)
        Map<String, Long> result = new TreeMap<>();
        Map<String, Long> usernameResult = new TreeMap<>();
        while (usersCursor.hasNext()) {
            DBObject obj = usersCursor.next()
            def uid = obj.get('user_id') as Integer
            String ymd = new Date(obj.get('timestamp') as Long).format('yyyy-MM-dd')
            Long count = result.get(ymd) ?: 0
            result.put(ymd, ++count);
            def user = users.findOne($$(_id: uid), $$(tuid: 1));
            def tuid = user.get('tuid')
            def userId = NumberUtils.isNumber(tuid.toString()) ? tuid as Integer : tuid as String
            if (xy_users.count($$(_id: userId, pwd: [$ne: null], userName: [$ne: null], "via": "local", mobile: null)) == 1) {
                Long usercount = usernameResult.get(ymd) ?: 0
                usernameResult.put(ymd, ++usercount);
            }
        }
        result.each { String key, Long val ->
            Long total = usernameResult.get(key) ?: 0
            buf.append(key).append(',')
            buf.append(val).append(',')
            buf.append(total).append(',')
            buf.append(String.format("%.2f", total / val))
            buf.append(System.lineSeparator())
        }
        def folder_path = '/empty/static'
        File folder = new File(folder_path)
        if (!folder.exists()) {
            folder.mkdirs()
        }

        File file = new File(folder_path + "/user_logins.csv");
        if (!file.exists()) {
            file.createNewFile()
        }
        file.withWriterAppend { Writer writer ->
            writer.write(buf.toString())
            writer.flush()
            writer.close()
        }
    }

    /**
     *  "errorMessage": "a.a.d.a.a: xhr poll error",
     "os": "API 19",
     "platform": "Android",
     "model": "vivo Y13L/vivo",
     "roomId": NumberInt(28633591),
     "errorCount": NumberInt(234),
     "mobileNet": true,
     "wifiMode": false,
     "appVersion": "4.12.0.20160627495",
     "socketUrl": "http://203.88.174.28:6010",
     "userId": NumberInt(29816169),
     "userLevel": NumberInt(0),
     "userName": "萌新349655",
     "success": false,
     "channel": "baidu"
     */
    static void debug_logs() {
        def info_fields = new ArrayList();
        def buf = new StringBuffer('date, ip')
        def debug = debug_logs.findOne($$('info.success': false));
        Map info = debug.get('info') as Map
        info_fields.addAll(info.keySet())
        info_fields.each { field ->
            buf.append(',').append(field)
        }
        buf.append(System.lineSeparator())

        DBCursor usersCursor = debug_logs.find($$('info.success': false, timestamp: [$gte: 1468310400000]), $$('info': 1, ip: 1, timestamp: 1)).batchSize(10000)
        while (usersCursor.hasNext()) {
            DBObject obj = usersCursor.next()
            buf.append(new Date(obj.get('timestamp') as Long).format('yyyy-MM-dd HH:mm:ss')).append(',')
            buf.append(StringUtils.remove(obj.get('ip').toString(), ','))

            Map debugInfo = obj.get('info') as Map
            info_fields.each { field ->
                String val = (debugInfo[field] ?: ' ') as String;
                buf.append(',').append(StringUtils.remove(val, ','))
            }
            buf.append(System.lineSeparator())
        }
        def folder_path = '/empty/static'
        File folder = new File(folder_path)
        if (!folder.exists()) {
            folder.mkdirs()
        }

        File file = new File(folder_path + "/debug_logs.csv");
        if (!file.exists()) {
            file.createNewFile()
        }
        file.withWriterAppend { Writer writer ->
            writer.write(buf.toString())
            writer.flush()
            writer.close()
        }
    }


    static userChargeStatistic(List<Integer> userIds) {
        def buf = new StringBuffer('uid, cny, coin')
        buf.append(System.lineSeparator())
        //println "firstDayOfLastMonth: ${ym_l}  firstDayOfCurrentMonth: ${ym_c}".toString()
        finance_log.aggregate(
                new BasicDBObject('$match', [user_id: [$in: userIds], via: [$ne: 'Admin']]),
                new BasicDBObject('$project', [user_id: '$user_id', cny: '$cny', coin: '$coin']),
                new BasicDBObject('$group', [_id: '$user_id', cny: [$sum: '$cny'], coin: [$sum: '$coin']]),
                new BasicDBObject('$sort', [cny: -1])
        ).results().each { BasicDBObject obj ->
            buf.append(obj['_id']).append(',')
            buf.append(obj['cny']).append(',')
            buf.append(obj['coin'])
            buf.append(System.lineSeparator())
        }
        def folder_path = '/empty/static'
        File folder = new File(folder_path)
        if (!folder.exists()) {
            folder.mkdirs()
        }

        File file = new File(folder_path + "/user_charge_logs.csv");
        if (!file.exists()) {
            file.createNewFile()
        }
        file.withWriterAppend { Writer writer ->
            writer.write(buf.toString())
            writer.flush()
            writer.close()
        }
    }

    static userMobile(Long coin_spend_total) {
        def buf = new StringBuffer('mobile, uid')
        buf.append(System.lineSeparator())
        //println "firstDayOfLastMonth: ${ym_l}  firstDayOfCurrentMonth: ${ym_c}".toString()
        def cursor = xy_users.find(new BasicDBObject(mobile: [$ne: null]), new BasicDBObject(_id: 1, mobile: 1)).batchSize(1000)
        while (cursor.hasNext()) {
            def obj = cursor.next()
            def tuid = obj['_id']
            String mobile = obj['mobile'] as String
            if (StringUtils.isNotEmpty(mobile)) {
                def user = users.findOne(new BasicDBObject(tuid: tuid, 'finance.coin_spend_total': [$gte: coin_spend_total]), new BasicDBObject(_id: 1))
                if (user) {
                    Integer uid = user['_id'] as Integer
                    buf.append(mobile).append(',')
                    buf.append(uid)
                    buf.append(System.lineSeparator())
                }
            }

        }

        def folder_path = '/empty/static'
        File folder = new File(folder_path)
        if (!folder.exists()) {
            folder.mkdirs()
        }

        File file = new File(folder_path + "/user_mobiles_level3.csv");
        if (!file.exists()) {
            file.createNewFile()
        }
        file.withWriterAppend { Writer writer ->
            writer.write(buf.toString())
            writer.flush()
            writer.close()
        }
    }

    def static financeChargeMonthStatic(int i) {
        Calendar cal = getCalendar()
        cal.add(Calendar.MONTH, -i)
        long firstDayOfCurrentMonth = cal.getTimeInMillis()  //当月第一天
        cal.add(Calendar.MONTH, -1)
        long firstDayOfLastMonth = cal.getTimeInMillis()  //上月第一天
        String ym = new Date(firstDayOfLastMonth).format("yyyyMM")
        String year = new Date(firstDayOfLastMonth).format("yyyy")
        def timebetween = [timestamp: [$gte: firstDayOfLastMonth, $lt: firstDayOfCurrentMonth]]
        //println " from ${new Date(firstDayOfLastMonth).format("yyyy-MM-dd HH:mm:ss")} to ${new Date(firstDayOfCurrentMonth).format("yyyy-MM-dd HH:mm:ss")}".toString()
        List<String> pcChannel = getChannel('1');
        List<String> andriodChannel = getChannel('2');

        //获得充值安卓用户
        List an_uids = finance_log.distinct('user_id', new BasicDBObject(timebetween).append('via', $$($ne: 'Admin')).append('qd', $$($in: andriodChannel)))
        //获得PC充值用户
        List pc_uids = finance_log.distinct('user_id', new BasicDBObject(timebetween).append('via', $$($ne: 'Admin')).append('qd', $$($in: pcChannel)))

        //本月新增用户
        Set<Integer> new_an_uids = getNewUids(an_uids, timebetween)
        Set<Integer> new_pc_uids = getNewUids(pc_uids, timebetween)
        //存量用户
        Set<Integer> old_an_uids = new HashSet<>();
        an_uids.each { if (!new_an_uids.contains(it)) old_an_uids.add(it as Integer) }
        Set<Integer> old_pc_uids = new HashSet<>();
        pc_uids.each { if (!new_pc_uids.contains(it)) old_pc_uids.add(it as Integer) }

        Long new_an_cny = getCnyByUids(new_an_uids.toList(), timebetween)
        Long new_pc_cny = getCnyByUids(new_pc_uids.toList(), timebetween)
        Long old_an_cny = getCnyByUids(old_an_uids.toList(), timebetween)
        Long old_pc_cny = getCnyByUids(old_pc_uids.toList(), timebetween)
        //本月新增用户金额
        //println " andriod uids : ${an_uids.size()} new_an_uids:${new_an_uids.size()}  old_an_uids : ${old_an_uids.size()} pc_uids : ${pc_uids.size()}  new_pc_uids : ${new_pc_uids.size()}  old_pc_uids : ${old_pc_uids.size()} ".toString()
        //println " new_an_cny : ${new_an_cny}  old_an_cny : ${old_an_cny}  new_pc_cny : ${new_pc_cny} old_pc_cny : ${old_pc_cny}".toString()
        println "${new Date(firstDayOfLastMonth).format("yyyy-MM")},${old_pc_uids.size()},${old_pc_cny},${new_pc_uids.size()},${new_pc_cny},${old_an_uids.size()},${old_an_cny},${new_an_uids.size()},${new_an_cny} "
    }

    /**
     * 获取上个月第一天和最后一天的时间戳
     * @param i
     * @return
     */
    private static Map getPreMonthMap(int i) {
        Map map = new HashMap()
        Calendar cal = getCalendar();
        cal.add(Calendar.MONTH, -i)
        Long curBegin = cal.getTimeInMillis() //这个月第一天
        cal.add(Calendar.MONTH, 1);
        cal.set(Calendar.DATE, 1);
        cal.add(Calendar.SECOND, -1);
        Long curEnd = cal.getTimeInMillis() //这个月最后一天
        map.put('curBegin', curBegin)
        map.put('curEnd', curEnd)
        return map
    }

    /**
     * 是否新增充值用户
     * @param distinctId
     * @return
     */
    static Integer isNewConsumer(Integer userId, Long timestamp) {
        Integer isNewConsumer = 0
        BasicDBObject expression = new BasicDBObject('user_id': userId, timestamp: [$lt: timestamp])
        Long count = finance_log.count(expression)
        if (count > 0) {
            isNewConsumer = 1
        }
        return isNewConsumer
    }

    /**
     * 导出pc和android 流水报表
     * @return
     */
    def static exportFinanceReport() {
        StringBuffer buf = new StringBuffer('日期,pc存量用户数, PC存量用户流失数,PC存量金额,PC存量ARPU,PC新增充值用户数,PC新增充值用户的充值金额,PC新增ARPU,')
        buf.append('android存量用户数, android存量用户流失数,android存量金额,android存量ARPU,android新增充值用户数,android新增充值用户的充值金额,android新增ARPU')
        buf.append(System.lineSeparator())
        int begin = 9
        while (begin-- > 0) {
            buildFinanceReportDataForPC(begin, buf)
            buildFinanceReportDataForAndroid(begin, buf)
        }

//        def folder_path = '/Users/monkey/'
        def folder_path = '/empty/static/'
        File folder = new File(folder_path)
        if (!folder.exists()) {
            folder.mkdirs()
        }

        File file = new File(folder_path + "/financeReport.csv");
        if (!file.exists()) {
            file.createNewFile()
        }
        file.withWriterAppend { Writer writer ->
            writer.write(buf.toString())
            writer.flush()
            writer.close()
        }
    }

    /**
     * 统计2015-12 ~ 2016-8 月的PC流水
     * @param i
     * @return
     */
    private static buildFinanceReportDataForPC(int i, StringBuffer buf) {
        Map map = getPreMonthMap(i)
        List<String> pcChannel = getChannel('1');
        Long curBegin = map.get('curBegin') // 这个月第一天
        Long curEnd = map.get('curEnd') // 这个月最后一天

        // 当月充值总金额
        Double currentAmount = 0
        // 当月新增充值用户
        Integer newReChargeUser = 0
        // 当月新增充值金额
        Double newReChargeAmount = 0
        // 当月新增充值APRM
        Double reChargeAPRM = 0

        BasicDBObject pcExpression = new BasicDBObject()
        pcExpression.append('via', $$($ne: 'Admin')).append('qd', $$($in: pcChannel)).append('timestamp', [$gte: curBegin, $lt: curEnd])
        def currentRechargeUsers = finance_log.find(pcExpression)
        Map<Integer, Double> newReChargeUserMap = new HashMap<Integer, Double>()
        Map<Integer, Double> allReChargeUserMap = new HashMap<Integer, Double>()
        currentRechargeUsers.each {
            BasicDBObject obj ->
                Double cny = obj.get("cny") == null ? 0 : obj.get("cny") as Double
                currentAmount = currentAmount + cny
                Integer userId = obj.get('user_id') as Integer
                if (isNewConsumer(userId, curBegin) == 1) {
                    // 用户在这个月前都没有充值过,则算是一个本月新增的充值用户,累计在本月充值的金额
                    if (newReChargeUserMap.containsKey(userId)) {
                        Double temp = newReChargeUserMap.get(userId)
                        newReChargeUserMap.put(userId, temp + cny)
                    } else {
                        newReChargeUserMap.put(userId, cny)
                    }
                }
                allReChargeUserMap.put(userId, cny)
        }
        newReChargeUserMap.each {
            // 循环累加计算新增用户充值金额
            newReChargeAmount = newReChargeAmount + it.value
        }

        // 计算新增充值APRM = 新增充值金额/新增充值用户数
        newReChargeUser = newReChargeUserMap.size()
        if (newReChargeUser != 0) {
            reChargeAPRM = newReChargeAmount / newReChargeUser
        }

        // 当月存量用户 = 当月充值用户数 - 新增充值用户数
//        Integer oldUser = allReChargeUserMap.size() - newReChargeUser
        Map<Integer, Double> storeUserMap = new HashMap<Integer, Double>()
        allReChargeUserMap.each {
            Integer key = it.key
            if (!newReChargeUserMap.containsKey(key)) {
                storeUserMap.put(key, it.value)
            }
        }
        // 当月存量用户 = 当月充值用户数 - 新增充值用户数
        Integer storeUser = storeUserMap.size()
        Integer temp = 0
        // 计算存量用户流失,读取preMonthStoreUserMap,没有代表第一个月,第一个月的存量流失用户为0
        preMonthStoreUserMapForPC.each {
            Integer userId = it.key
            if (allReChargeUserMap.containsKey(userId)) {
                // 如果上个月的存量用户在本月消费过则 + 1
                temp++
            }
        }
        // 存量用户流失 = 上月存量用户数 - 上月存量用户在本月发生消费行为的用户数
        Integer storeLoseUser = preMonthStoreUserMapForPC.size() - temp

        // 存量金额 = 当月充值总金额 - 新增用户充值金额
        Double storeAmount = currentAmount - newReChargeAmount

        // 存量ARPU = 存量金额 / 存量用户数
        Double storeReChargeARPU = 0
        if (storeUser != 0) {
            storeReChargeARPU = storeAmount / storeUserMap.size()
        }

        // 清空preMonthStoreUserMap,将本月存量用户ID赋值
        preMonthStoreUserMapForPC.clear()
        storeUserMap.each {
            preMonthStoreUserMapForPC.put(it.key, it.value)
        }
        String date = new Date(curBegin).format('yyyy/MM/dd') + '---' + new Date(curEnd).format('yyyy/MM/dd')
        buf.append(date).append(',')
        buf.append(storeUser).append(',')
        buf.append(storeLoseUser).append(',')
        buf.append(String.format("%.2f", storeAmount)).append(',')
        buf.append(String.format("%.2f", storeReChargeARPU)).append(',')
        buf.append(newReChargeUser).append(',')
        buf.append(String.format("%.2f", newReChargeAmount)).append(',')
        buf.append(String.format("%.2f", reChargeAPRM)).append(',')
    }

    /**
     * 统计2015-12 ~ 2016-8 月的Android流水
     * @param i
     * @return
     */
    private static buildFinanceReportDataForAndroid(int i, StringBuffer buf) {
        Map map = getPreMonthMap(i)
        List<String> androidChannel = getChannel('2');
        Long curBegin = map.get('curBegin') // 这个月第一天
        Long curEnd = map.get('curEnd') // 这个月最后一天

        // 当月充值总金额
        Double currentAmount = 0
        // 当月新增充值用户
        Integer newReChargeUser = 0
        // 当月新增充值金额
        Double newReChargeAmount = 0
        // 当月新增充值APRM
        Double reChargeAPRM = 0

        BasicDBObject androidExpression = new BasicDBObject()
        androidExpression.append('via', $$($ne: 'Admin')).append('qd', $$($in: androidChannel)).append('timestamp', [$gte: curBegin, $lt: curEnd])
        def currentRechargeUsers = finance_log.find(androidExpression)
        Map<Integer, Double> newReChargeUserMap = new HashMap<Integer, Double>()
        Map<Integer, Double> allReChargeUserMap = new HashMap<Integer, Double>()
        currentRechargeUsers.each {
            BasicDBObject obj ->
                Double cny = obj.get("cny") == null ? 0 : obj.get("cny") as Double
                currentAmount = currentAmount + cny
                Integer userId = obj.get('user_id') as Integer
                if (isNewConsumer(userId, curBegin) == 1) {
                    // 用户在这个月前都没有充值过,则算是一个本月新增的充值用户,累计在本月充值的金额
                    if (newReChargeUserMap.containsKey(userId)) {
                        Double temp = newReChargeUserMap.get(userId)
                        newReChargeUserMap.put(userId, temp + cny)
                    } else {
                        newReChargeUserMap.put(userId, cny)
                    }
                }
                allReChargeUserMap.put(userId, cny)
        }
        newReChargeUserMap.each {
            // 循环累加计算新增用户充值金额
            newReChargeAmount = newReChargeAmount + it.value
        }

        // 计算新增充值APRM = 新增充值金额/新增充值用户数
        newReChargeUser = newReChargeUserMap.size()
        if (newReChargeUser != 0) {
            reChargeAPRM = newReChargeAmount / newReChargeUser
        }

        // 当月存量用户 = 当月充值用户数 - 新增充值用户数
        Map<Integer, Double> storeUserMap = new HashMap<Integer, Double>()
        allReChargeUserMap.each {
            Integer key = it.key
            if (!newReChargeUserMap.containsKey(key)) {
                storeUserMap.put(key, it.value)
            }
        }
        // 当月存量用户 = 当月充值用户数 - 新增充值用户数
        Integer storeUser = storeUserMap.size()
        Integer temp = 0
        // 计算存量用户流失,读取preMonthStoreUserMap,没有代表第一个月,第一个月的存量流失用户为0
        preMonthStoreUserMapForAndroid.each {
            Integer userId = it.key
            if (allReChargeUserMap.containsKey(userId)) {
                // 如果上个月的存量用户在本月消费过则 + 1
                temp++
            }
        }
        // 存量用户流失 = 上月存量用户数 - 上月存量用户在本月发生消费行为的用户数
        Integer storeLoseUser = preMonthStoreUserMapForAndroid.size() - temp

        // 存量金额 = 当月充值总金额 - 新增用户充值金额
        Double storeAmount = currentAmount - newReChargeAmount

        // 存量ARPU = 存量金额 / 存量用户数
        Double storeReChargeARPU = 0
        if (storeUser != 0) {
            storeReChargeARPU = storeAmount / storeUserMap.size()
        }

        // 清空preMonthStoreUserMap,将本月存量用户ID赋值
        preMonthStoreUserMapForAndroid.clear()
        storeUserMap.each {
            preMonthStoreUserMapForAndroid.put(it.key, it.value)
        }
        buf.append(storeUser).append(',')
        buf.append(storeLoseUser).append(',')
        buf.append(String.format("%.2f", storeAmount)).append(',')
        buf.append(String.format("%.2f", storeReChargeARPU)).append(',')
        buf.append(newReChargeUser).append(',')
        buf.append(String.format("%.2f", newReChargeAmount)).append(',')
        buf.append(String.format("%.2f", reChargeAPRM))
        buf.append(System.lineSeparator())
    }


    private static List<String> getChannel(String client) {
        return channels.find($$('client': client), $$(_id: 1)).toArray().collect { it['_id'] as String }
    }

    private static List<String> getUsers() {
        return channels.find($$(_id: 1)).toArray().collect { it['_id'] as String }
    }

    /**
     * 过滤出本段时间内新增用户
     */
    private static Set<Integer> getNewUids(List<Integer> uids, Map timebetween) {
        Set<Integer> newUids = new HashSet<>(uids.size());
        newUids.addAll(users.find($$(timebetween).append('_id', $$($in: uids)), $$(_id: 1)).toArray().collect {
            it['_id'] as Integer
        })
        return newUids
    }

    private static Long getCnyByUids(List<Integer> uids, Map timebetween) {
        Long cny = 0l
        def match = [via: [$ne: 'Admin'], user_id: [$in: uids],]
        match.putAll(timebetween)
        def iter = finance_log.aggregate(
                new BasicDBObject('$match', match),
                new BasicDBObject('$project', [cny: '$cny', user_id: '$user_id']),
                new BasicDBObject('$group', [_id: null, cny: [$sum: '$cny']])
        ).results().iterator()

        if (iter.hasNext()) {
            def obj = iter.next()
            cny = obj['cny'] as Long
        }
        return cny;
    }

    /**
     * 导出聊天限制
     */
    def static exportChatSetting() {
        StringBuffer sb = new StringBuffer()
        sb.append('日期,聊天限制开启数').append(System.lineSeparator())
        Long begin, end = 0
        Map<Long, Long> map = getBeginAndEnd()
        map.each {
            begin = it.key
            end = it.value
            BasicDBObject searchExpression = new BasicDBObject('type': 'chat_limit', 'timestamp': [$gte: begin, $lte: end])
            List rooms = room_edit.distinct('room', searchExpression)
            Integer count = 0
            rooms.each {
                def room = it as Integer
                BasicDBObject lastRoomStatus = new BasicDBObject('room': room, 'type': 'chat_limit', 'timestamp': [$gte: begin, $lte: end])
                BasicDBObject sortExpression = new BasicDBObject('timestamp': -1)
                def result = room_edit.find(lastRoomStatus).sort(sortExpression).limit(1)
                result.each {
                    BasicDBObject obj ->
                        def data = obj.get('data') as Integer
                        if (data != null && data > 0) {
                            count++
                        }
                }
            }
            sb.append(new Date(begin).format('yyyy-MM-dd')).append(',')
            sb.append(count)
            sb.append(System.lineSeparator())
        }

        def folder_path = '/empty/static/'
        File folder = new File(folder_path)
        if (!folder.exists()) {
            folder.mkdirs()
        }

        File file = new File(folder_path + "/chatSettingReport.csv");
        if (!file.exists()) {
            file.createNewFile()
        }
        file.withWriterAppend { Writer writer ->
            writer.write(sb.toString())
            writer.flush()
            writer.close()
        }

    }


    def static exportStoreUser() {
        StringBuffer sb = new StringBuffer()
        sb.append('开始时间,一月,二月,三月,四月,五月,六月,七月,八月,九月,十月,十一月,十二月').append(System.lineSeparator())
        Map map = getBeginAndEndForMonth()
        Long begin, end = 0
        // 总充值用户,当月新增充值用户,存量用户
        Integer storeUser = 0
        map.each {
            Integer start = 12
            begin = it.key
            end = it.value
            sb.append(new Date(begin).format('yyyy-MM-dd') + '-' + new Date(end).format('yyyy-MM-dd')).append(',')
            while (start-- > 0) {
                List<Integer> userIds = new ArrayList<Integer>()
                BasicDBObject expression = new BasicDBObject()
                if (start == 11 && storeUserList.isEmpty()) {
                    List<Integer> tempList = new ArrayList<Integer>()
                    expression.append('via', $$($ne: 'Admin')).append('timestamp', [$gte: begin, $lt: end])
                    userIds = finance_log.distinct('user_id', expression)
                    storeUserList = (ArrayList<Integer>) userIds.clone()
                } else {
                    expression.append('timestamp', [$gte: begin, $lt: end]).append('user_id', $$($in: storeUserList))
                    userIds = finance_log.distinct('user_id', expression)
                    storeUserList.clear()
                    storeUserList = (ArrayList<Integer>) userIds.clone()
                }
                storeUser = storeUserList.size()
                sb.append(storeUser)
                if (start == 0) {
                    storeUserList.clear()
                }else{
                    sb.append(',')
                }
                begin = end
                Calendar calendar = Calendar.getInstance()
                calendar.setTimeInMillis(begin)
                calendar.add(Calendar.MONTH, 1)
                end = calendar.getTimeInMillis()
            }
            sb.append(System.lineSeparator())

        }


        def folder_path = '/empty/static/'
        File folder = new File(folder_path)
        if (!folder.exists()) {
            folder.mkdirs()
        }

        File file = new File(folder_path + "/storeUserReport.csv");
        if (!file.exists()) {
            file.createNewFile()
        }
        file.withWriterAppend { Writer writer ->
            writer.write(sb.toString())
            writer.flush()
            writer.close()
        }
    }

    /**
     * 获取日期方法
     * 本方法按需获取前30天的每天开始时间和结束时间
     * 例如 2016年8月15日00:00:00 - 8月15日23:59:59
     * 每一个map的key是开始,value是结束
     * @return
     */
    private static Map getBeginAndEnd() {
        Long begin, end = 0
        Integer lastDayAgo = 30
        Map<Long, Long> map = new TreeMap<Long, Long>()
        while (lastDayAgo-- > 0) {
            Calendar calendar = Calendar.getInstance()
            calendar.set(Calendar.HOUR_OF_DAY, 0)
            calendar.set(Calendar.MINUTE, 0)
            calendar.set(Calendar.SECOND, 0)
            calendar.set(Calendar.MILLISECOND, 0)
            calendar.add(Calendar.DATE, -lastDayAgo)
            begin = calendar.getTime().getTime()
            calendar.add(Calendar.DATE, 1)
            calendar.add(Calendar.SECOND, -1)
            end = calendar.getTime().getTime()
            map.put(begin, end)
        }
        return map
    }

    /**
     * 获取日期方法
     * 本方法按需获取前30天的每天开始时间和结束时间
     * 例如 2016年8月15日00:00:00 - 8月15日23:59:59
     * 每一个map的key是开始,value是结束
     * @return
     */
    private static Map getBeginAndEndForMonth() {
        Long begin, end = 0
        Integer lastDayAgo = 13
        Map<Long, Long> map = new TreeMap<Long, Long>()
        while (lastDayAgo-- > 0) {
            Calendar calendar = Calendar.getInstance()
            calendar.set(Calendar.DATE, 1)
            calendar.set(Calendar.HOUR_OF_DAY, 0)
            calendar.set(Calendar.MINUTE, 0)
            calendar.set(Calendar.SECOND, 0)
            calendar.set(Calendar.MILLISECOND, 0)
            calendar.add(Calendar.MONTH, -lastDayAgo)
            begin = calendar.getTime().getTime()
            calendar.add(Calendar.MONTH, 1)
            end = calendar.getTime().getTime()
            map.put(begin, end)
        }
        return map
    }

    static void chargeItunes(){
        StringBuffer buf = new StringBuffer()
        buf.append('date,id,cny,coin').append(System.lineSeparator())
        DBCursor usersCursor = finance_log.find($$(user_id:[$in:[16583767,16474574,15593546,15150349]],via:'itunes'), $$(user_id:1,'coin': 1, cny: 1, timestamp: 1)).batchSize(5000)
        while (usersCursor.hasNext()) {
            DBObject obj = usersCursor.next()
            buf.append(new Date(obj.get('timestamp') as Long).format('yyyy-MM-dd HH:mm:ss')).append(',')
            buf.append(obj.get('user_id')).append(',')
            buf.append(obj.get('cny')).append(',')
            buf.append(obj.get('coin'))
            buf.append(System.lineSeparator())
        }
        def folder_path = '/empty/static'
        File folder = new File(folder_path)
        if (!folder.exists()) {
            folder.mkdirs()
        }

        File file = new File(folder_path + "/itunes_logs.csv");
        if (!file.exists()) {
            file.createNewFile()
        }
        file.withWriterAppend { Writer writer ->
            writer.write(buf.toString())
            writer.flush()
            writer.close()
        }
    }

    //导出手工减币
    static exportHandSubtractoin() {
        StringBuffer sb = new StringBuffer()
        sb.append('操作日期,操作人,用户ID,柠檬数,备注').append(System.lineSeparator())

        DBCursor curosr = mongo.getDB('xy_admin').getCollection('ops').find(new BasicDBObject(type: 'finance_cut_coin')).batchSize(10000)
        def iter = curosr.iterator()
        while (iter.hasNext()) {
            def obj = iter.next()
            def data = obj['data'] as Map
            def session = obj['session'] as Map
            sb.append(new Date(obj['timestamp'] as Long).format('yyyy-MM-dd HH:mm:ss')).append(',')
            sb.append(session?.get('nick_name')).append(',')
            sb.append(data['user_id']).append(',')
            sb.append(data['coin']).append(',')
            sb.append(data['remark']?:'')
            sb.append(System.lineSeparator())
        }
        def folder_path = '/empty/static/'
        File folder = new File(folder_path)
        if (!folder.exists()) {
            folder.mkdirs()
        }

        File file = new File(folder_path + "/handSubtractReport.csv");
        if (!file.exists()) {
            file.createNewFile()
        }
        file.withWriterAppend { Writer writer ->
            writer.write(sb.toString())
            writer.flush()
            writer.close()
        }
    }

    //导出手工加币
    static exportAddCoin() {
        StringBuffer sb = new StringBuffer()
        sb.append('操作日期,操作人,用户ID,柠檬数,备注').append(System.lineSeparator())

        DBCursor curosr = mongo.getDB('xy_admin').getCollection('ops').find(new BasicDBObject(type: 'finance_add')).batchSize(10000)
        def iter = curosr.iterator()
        while (iter.hasNext()) {
            def obj = iter.next()
            def data = obj['data'] as Map
            def session = obj['session'] as Map
            sb.append(new Date(obj['timestamp'] as Long).format('yyyy-MM-dd HH:mm:ss')).append(',')
            sb.append(session?.get('nick_name')).append(',')
            sb.append(data['user_id']).append(',')
            sb.append(data['coin']).append(',')
            sb.append(data['remark']?:'')
            sb.append(System.lineSeparator())
        }
        def folder_path = '/empty/static/'
        File folder = new File(folder_path)
        if (!folder.exists()) {
            folder.mkdirs()
        }

        File file = new File(folder_path + "/handAddReport.csv");
        if (!file.exists()) {
            file.createNewFile()
        }
        file.withWriterAppend { Writer writer ->
            writer.write(sb.toString())
            writer.flush()
            writer.close()
        }
    }

    static void main(String[] args) {
        long l = System.currentTimeMillis()
        //exportStoreUser()
//        exportChatSetting()
//        exportFinanceReport()

        //exportUserEvent()
        //exportWeiXin_m()
//        richTopStatic()
//        5.times {
//            mvpStatic(it)
//        }
        //mvpStatic(0)
//        payStaticsByUserChannel()
        //earnedStatic();
        //costStatic();
        //chargeStatic();
/*        int i = 0
        while(i <= 20){
            chargeStatistic(i++)
        }*/
        //VIP和守护大于30天流水导出
        //exportVipAndGuard();
        //exportUsers();
        //countBagGifts();
        //jifen('baidu_jf');
        //jifen('dianle');
        //allStarInfo();
        //daylogin()
        //debug_logs();
        /*List<Integer> userIds = [17005428,
                                 27153695,
                                 24681537,
                                 23591959]
        userChargeStatistic(userIds)*/
        //userMobile(15000)
        //userMobile(50000)
//        println "日期,PC存量用户数,PC存量金额,PC新增用户数,PC新增金额,Android存量用户数,Android存量金额,Android新增用户数,Android新增金额 "
        //chargeItunes();
        exportHandSubtractoin()
        exportAddCoin()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${ExportStat.class.getSimpleName()},total cost  ${System.currentTimeMillis() - l} ms"
    }

}