#!/usr/bin/env groovy
package crontab.st

//@GrabResolver(name = 'restlet', root = 'http://192.168.31.253:8081/nexus/content/groups/public')
@GrabResolver(name = 'restlet', root = 'http://210.22.151.242:8081/nexus/content/groups/public')
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('org.apache.httpcomponents:httpclient:4.2.5'),
        @Grab('redis.clients:jedis:2.1.0'),
        @Grab('com.ttpod:https-util:1.0'),
        @Grab('com.sensorsdata.analytics.javasdk:SensorsAnalyticsSDK:2.0.3')
])
import com.mongodb.BasicDBObject
import com.mongodb.DBCollection
import com.mongodb.DBCursor
import com.mongodb.DBObject
import com.mongodb.Mongo
import com.mongodb.MongoURI
import com.sensorsdata.analytics.javasdk.SensorsAnalytics

import java.text.SimpleDateFormat

class SensorsApi {

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

    // mongodb static params
    static mongo = new Mongo(new MongoURI(getProperties('mongo.uri', 'mongodb://192.168.31.249:10000/?w=1') as String))
    static DBCollection timeLogs = mongo.getDB('xyrank').getCollection('timer_logs')
    static DBCollection users = mongo.getDB('xy').getCollection('users')
    static DBCollection dayLogin = mongo.getDB('xylog').getCollection('day_login')
    static DBCollection roomCost = mongo.getDB('xylog').getCollection('room_cost')
    static DBCollection followerLogs = mongo.getDB('xylog').getCollection('follower_logs')
    static DBCollection financeLog = mongo.getDB('xy_admin').getCollection('finance_log')
    static DBCollection channelPay = mongo.getDB('xy_admin').getCollection('channel_pay')
    static DBCollection channels = mongo.getDB('xy_admin').getCollection('channels')
    static DBCollection giftCategories = mongo.getDB('xy_admin').getCollection('gift_categories')
    static DBCollection xyUsers = mongo.getDB('xy_user').getCollection('users')

    // sensorsEvent
    static final String TIMER_NAME = 'sensorsTask'
    static final String REGISTER = 'register'
    static final String LOGIN = 'sign_in'
    static final String RECHARGE = 'top_up'
    static final String SEND_GIFT = 'send_present'
    static final String PLAY_GAME = 'play_game'
    static final String ITEM = 'product'
    static final String FOLLOW = 'follow'
    static final String PROFILE = 'profile'

    /*  histroy
    static final Long BEGIN = Date.parse("yyyy-MM-dd HH:mm:ss", "2014-06-25 00:00:00").getTime()  //2014-06-12 17:30:00
    static final Long END = null  //2016-08-17 00:00:00
    */
    //TODO begin 和 end 为空则取日志记录上次执行完毕时间
    static final Long BEGIN = null // 2014-06-12 17:30:00 == 1402565400000L
    static final Long END = null
    static final Integer MINUS_DAY = null
    static final Integer CLOSE_DAY = null

    static
    final Map viaMap = ['qq': '腾讯qq', 'local': '本地', 'sina': '微博', 'weixin': '微信', 'xiaomi': '小米', 'android': '安卓', 'robot': '机器人']
    static final Map sexMap = [0: '男', 1: '女', 2: '未知']
    static final Map platformMap = [1: 'Android', 2: 'iOS', 3: 'H5', 4: 'PC', 5: 'RIA']
    static final Map qdTypeMap = [1: 'PC', 2: 'Android', 4: 'iOS', 5: 'H5', 6: 'RIA']
    static final Map playGameMap = ['football_shoot': '点球', 'open_card': '翻牌', 'open_egg': '砸蛋', 'car_race': '赛车']
    static final Map productMap = ['buy_vip': 'vip', 'buy_guard': '守护', 'grab_sofa': '沙发', 'song': '点歌',
                                   'buy_car': '座驾', 'broadcast': '广播', 'buy_prettynum': '靓号', 'send_fortune': '财神', 'send_bell': '铃铛', 'level_up': '接生']
    static final Map livingTypeMap = [1: '传统', 2: '手机', 3: 'VR', 4: 'OBS']
    static Map qdMap = [:]
    static Map giftTypeMap = [:]

    static final Integer FETCH_BULK_SIZE = 5000
    static DAY_MILLON = 24 * 3600 * 1000L
    static final String STRING_DEFAULT = '其他'
    static final Double DOUBLE_DEFAULT = 0d
    static final Integer INTEGER_DEFAULT = 0

    static final String FILE_PATH = getProperties('file.path', '/Users/monkey/')
    static final String FILE = "${FILE_PATH}sensorsLog.${new Date().format('yyyyMMdd')}".toString()
    static final Writer writer = new BufferedWriter(new PrintWriter(new FileOutputStream(FILE)));
    static final SensorsAnalytics sa = new SensorsAnalytics(new SensorsAnalytics.ConsoleConsumer(writer));

    static void main(String[] args) {
        Long now = System.currentTimeMillis()
        initData()
        sensorsTask(now)
    }

    static void initData() {
        findAllQd()
        findAllGiftType()
    }

    static void sensorsTask(Long now) {
        try {
            println "${SensorsApi.class.getSimpleName()}:${new Date().format('yyyy-MM-dd HH:mm:ss')}:---- 数据上传任务开始 ------"
            Map<String, Long> map = getBeginAndEnd()
            Long begin = map['begin']
            Long end = map['end']
            println '开始时间 = ' + new Date(begin).format('yyyy-MM-dd HH:mm:ss')
            println '结束时间 = ' + new Date(end).format('yyyy-MM-dd HH:mm:ss')
            // sendProfile(PROFILE, begin, end)
            sendRegisterEvent(REGISTER, begin, end)
            sendLoginEvent(LOGIN, begin, end)
            sendReChargeEvent(RECHARGE, begin, end)
            sendPresentEvent(SEND_GIFT, begin, end)
            sendPlayGameEvent(PLAY_GAME, begin, end)
            sendProductEvent(ITEM, begin, end)
            sendFollowEvent(FOLLOW, begin, end)
            jobFinish(now)
        } catch (Exception e) {
            println e.getStackTrace()
        }
        finally {
            sa.shutdown();
            writer.flush();
            writer.close();
        }
    }

    /**
     * 发送profile信息
     */
    static void sendProfile(String eventName, Long begin, Long end) {
        BasicDBObject timeExpression = new BasicDBObject('timestamp': [$gte: begin, $lt: end], via: [$ne: 'robot'], 'isTest': [$ne: 'test'], '_id': [$ne: null])
        BasicDBObject fieldExpression = new BasicDBObject('_id': 1, 'timestamp': 1, 'qd': 1)
        DBCursor cursor = users.find(timeExpression, fieldExpression).batchSize(FETCH_BULK_SIZE)
        println "${SensorsApi.class.getSimpleName()}:${new Date(begin).format('yyyy-MM-dd HH:mm:ss')} to ${new Date(end).format('yyyy-MM-dd HH:mm:ss')}---------------------- [${eventName}] ---- 共${cursor.size()}条数据 ---------------------- "
        List list = new ArrayList()
        while (cursor.hasNext()) {
            def obj = cursor.next()
            Integer id = obj.get('_id') as Integer
            Long timestamp = obj.get('timestamp') as Long
            Map<String, Object> properties = new HashMap<String, Object>();
            properties.put('executor', id.toString());
            properties.put('getTime', new Date(timestamp))
            properties.put('timestamp', timestamp)
            String qdFormat = qdMap.get(obj['qd']) == null ? STRING_DEFAULT : qdMap.get(obj['qd'])
            properties.put('qd', qdFormat)
            properties.put('qdId', obj['qd'] == null ? STRING_DEFAULT : obj['qd'])
            BasicDBObject qdSearchExpression = new BasicDBObject('_id': obj['qd'])
            DBObject channel = channels.findOne(qdSearchExpression)
            Integer clientType = 1
            if (channel != null && channel['client'] != null) {
                clientType = channel['client'] as Integer
            }
            properties.put('qdType', qdTypeMap.get(clientType))
            list.add(properties)
            if (list.size() == FETCH_BULK_SIZE) {
                postProfile(list, eventName)
            }
        }

        if (!list.isEmpty()) {
            postProfile(list, eventName)
        }
        cursor.close()
    }

    /**
     * 发送注册事件
     */
    static void sendRegisterEvent(String eventName, Long begin, Long end) {
        BasicDBObject timeExpression = new BasicDBObject('timestamp': [$gte: begin, $lt: end], via: [$ne: 'robot'], 'isTest': [$ne: 'test'], '_id': [$ne: null])
        BasicDBObject fieldExpression = new BasicDBObject('_id': 1, 'via': 1, 'qd': 1, 'sex': 1, 'nick_name': 1, 'timestamp': 1, 'mobile_bind': 1)
        DBCursor cursor = users.find(timeExpression, fieldExpression).batchSize(FETCH_BULK_SIZE)
        println "${SensorsApi.class.getSimpleName()}:${new Date(begin).format('yyyy-MM-dd HH:mm:ss')} to ${new Date(end).format('yyyy-MM-dd HH:mm:ss')}---------------------- [${eventName}] ---- 共${cursor.size()}条数据 ---------------------- "
        List list = new ArrayList()
        while (cursor.hasNext()) {
            def obj = cursor.next()
            Integer id = obj.get('_id')
            Long timestamp = obj.get('timestamp') as Long
            Map<String, Object> properties = new HashMap<String, Object>();
            properties.put('$time', new Date(timestamp));
            properties.put('executor', id.toString());
            properties.put('via', obj.get('via') == null ? STRING_DEFAULT : viaMap.get(obj.get('via')) as String)
            properties.put('qd', obj.get('qd') == null ? STRING_DEFAULT : obj.get('qd') as String)
            String sex = sexMap.get(obj.get('sex') as Integer)
            properties.put('sex', sex == null ? STRING_DEFAULT : sex)
            properties.put('nickName', obj.get('nick_name') == null ? STRING_DEFAULT : obj.get('nick_name') as String)
            properties.put('timestamp', timestamp.toString())
            Boolean isMobileBind = obj.get('mobile_bind') as Boolean
            if (isMobileBind) {
                properties.put('mobile', getMobileById(id))
            } else {
                properties.put('mobile', STRING_DEFAULT)
            }
            String platform = getPlatForm(id)
            properties.put('client_type', platform)
            list.add(properties)
            if (list.size() == FETCH_BULK_SIZE) {
                postData(list, eventName)
            }
        }
        if (!list.isEmpty()) {
            postData(list, eventName)
        }
        cursor.close()
    }

    /**
     * 发送登陆事件
     */
    static void sendLoginEvent(String eventName, Long begin, Long end) {
        BasicDBObject timeExpression = new BasicDBObject('timestamp': [$gte: begin, $lt: end], 'user_id': [$ne: null])
        BasicDBObject fieldExpression = new BasicDBObject('_id': 1, 'user_id': 1, 'timestamp': 1, 'platform': 1)
        DBCursor cursor = dayLogin.find(timeExpression, fieldExpression).batchSize(FETCH_BULK_SIZE)
        println "${SensorsApi.class.getSimpleName()}:${new Date(begin).format('yyyy-MM-dd HH:mm:ss')} to ${new Date(end).format('yyyy-MM-dd HH:mm:ss')}---------------------- [${eventName}] ---- 共${cursor.size()}条数据 ---------------------- "
        List list = new ArrayList()
        while (cursor.hasNext()) {
            def obj = cursor.next()
            Long timestamp = obj.get('timestamp') as Long
            Integer id = obj.get('user_id') as Integer
            Map<String, Object> properties = new HashMap<String, Object>();
            properties.put('$time', new Date(timestamp));
            properties.put('executor', id.toString());
            properties.put('timestamp', timestamp.toString());
            String platform = getPlatForm(id)
            properties.put('client_type', platform)
            properties.put('login_type', getLoginType(id))
            list.add(properties)
            if (list.size() == FETCH_BULK_SIZE) {
                postData(list, eventName)
            }
        }
        if (!list.isEmpty()) {
            postData(list, eventName)
        }
        cursor.close()
    }

    /**
     * 发送充值事件
     */
    static void sendReChargeEvent(String eventName, Long begin, Long end) {
        BasicDBObject timeExpression = new BasicDBObject('timestamp': [$gte: begin, $lt: end], 'via': [$ne: 'Admin'], 'user_id': [$ne: null])
        BasicDBObject fieldExpression = new BasicDBObject('_id': 1, 'user_id': 1, 'to_id': 1, 'timestamp': 1, 'via': 1, 'cny': 1, 'coin': 1)
        DBCursor cursor = financeLog.find(timeExpression, fieldExpression).batchSize(FETCH_BULK_SIZE)
        println "${SensorsApi.class.getSimpleName()}:${new Date(begin).format('yyyy-MM-dd HH:mm:ss')} to ${new Date(end).format('yyyy-MM-dd HH:mm:ss')}---------------------- [${eventName}] ---- 共${cursor.size()}条数据 ---------------------- "
        List list = new ArrayList()
        while (cursor.hasNext()) {
            def obj = cursor.next()
            Long timestamp = obj.get('timestamp') as Long
            Integer id = obj.get('user_id') as Integer
            Map<String, Object> properties = new HashMap<String, Object>();
            String via = obj.get('via') as String
            properties.put('$time', new Date(timestamp));
            properties.put('pay_id', id as String)
            String platform = getPlatForm(id)
            properties.put('client_type', platform)
            properties.put('executor', id.toString());
            properties.put('pay_type', via == null ? '支付宝移动' : getChannelPay(via))
            properties.put('currency_number', obj.get('cny') == null ? DOUBLE_DEFAULT : obj.get('cny') as Double)
            properties.put('receiver_id', obj.get('to_id') == null ? STRING_DEFAULT : obj.get('to_id') as String)
            properties.put('lemon_number', obj.get('coin') == null ? INTEGER_DEFAULT : obj.get('coin') as Long)
            properties.put('first_recharge', isNewConsumer(id, timestamp))
            properties.put('timestamp', timestamp.toString())
            list.add(properties)
            if (list.size() == FETCH_BULK_SIZE) {
                postData(list, eventName)
            }
        }
        if (!list.isEmpty()) {
            postData(list, eventName)
        }
        cursor.close()
    }

    /**
     * 发送赠送礼物事件
     */
    static void sendPresentEvent(String eventName, Long begin, Long end) {
        BasicDBObject timeExpression = new BasicDBObject('timestamp': [$gte: begin, $lt: end], type: 'send_gift', 'session._id': [$ne: null], 'session.data': [$ne: null])
        BasicDBObject fieldExpression = new BasicDBObject('_id': 1, 'session': 1, 'timestamp': 1, 'cost': 1, 'room': 1)
        DBCursor cursor = roomCost.find(timeExpression, fieldExpression).batchSize(FETCH_BULK_SIZE)
        println "${SensorsApi.class.getSimpleName()}:${new Date(begin).format('yyyy-MM-dd HH:mm:ss')} to ${new Date(end).format('yyyy-MM-dd HH:mm:ss')}---------------------- [${eventName}] ---- 共${cursor.size()}条数据 ---------------------- "
        List list = new ArrayList()
        while (cursor.hasNext()) {
            def obj = cursor.next()
            Map<String, Object> properties = new HashMap<String, Object>();
            Map session = obj.get('session') as Map
            Map data = session.get('data') as Map
            String id = session.get('_id') as String
            Long timestamp = obj.get('timestamp') as Long
            String xyUserId = data.get('xy_user_id') as String
            String xyStarId = data.get('xy_star_id') as String
            properties.put('$time', new Date(timestamp));
            properties.put('timestamp', timestamp.toString());
            properties.put('executor', id.toString());
            Integer platform = session.get('platform') == null ? 1 : session.get('platform') as Integer
            properties.put('client_type', platformMap.get(platform))
            if (xyUserId == null) {
                properties.put('receiverId', xyStarId)
                properties.put('receiverType', '主播')
            } else {
                properties.put('receiverId', xyUserId)
                properties.put('receiverType', '观众')
            }

            properties.put('giftId', data.get('_id') as String)
            String categoryName = giftTypeMap.get(data.get('category_id')) == null ? STRING_DEFAULT : giftTypeMap.get(data.get('category_id'))
            properties.put('giftType', categoryName)
            properties.put('gift_count_number', data.get('count'))
            properties.put('gift_value_number', data.get('coin_price'))
            properties.put('giftName', data.get('name'))
            properties.put('charge_lemon', obj.get('cost') == null ? INTEGER_DEFAULT : obj.get('cost') as Long)
            properties.put('room', obj.get('room') == null ? STRING_DEFAULT : obj.get('room') as String)
            list.add(properties)
            if (list.size() == FETCH_BULK_SIZE) {
                postData(list, eventName)
            }
        }
        if (!list.isEmpty()) {
            postData(list, eventName)
        }
        cursor.close()
    }

    /**
     * 发送玩游戏事件
     */
    static void sendPlayGameEvent(String eventName, Long begin, Long end) {
        def condition = ['football_shoot', 'open_card', 'open_egg', 'car_race']
        BasicDBObject timeExpression = new BasicDBObject('timestamp': [$gte: begin, $lt: end], type: [$in: condition], 'session._id': [$ne: null])
        BasicDBObject fieldExpression = new BasicDBObject('_id': 1, 'type': 1, 'timestamp': 1, 'cost': 1, 'session': 1)
        DBCursor cursor = roomCost.find(timeExpression, fieldExpression).batchSize(FETCH_BULK_SIZE)
        println "${SensorsApi.class.getSimpleName()}:${new Date(begin).format('yyyy-MM-dd HH:mm:ss')} to ${new Date(end).format('yyyy-MM-dd HH:mm:ss')}---------------------- [${eventName}] ---- 共${cursor.size()}条数据 ---------------------- "
        List list = new ArrayList()
        while (cursor.hasNext()) {
            def obj = cursor.next()
            Map<String, Object> properties = new HashMap<String, Object>();
            Map session = obj.get('session') as Map
            String id = session.get('_id') as String
            Long timestamp = obj.get('timestamp') as Long
            properties.put('$time', new Date(timestamp));
            properties.put('executor', id.toString());
            properties.put('timestamp', timestamp.toString());
            properties.put('charge_lemon', obj.get('cost') == null ? INTEGER_DEFAULT : obj.get('cost') as Long)
            properties.put('game_type', obj.get('type') == null ? STRING_DEFAULT : playGameMap.get(obj.get('type')))
            Integer platform = session.get('platform') == null ? 1 : session.get('platform') as Integer
            properties.put('client_type', platformMap.get(platform))
            list.add(properties)
            if (list.size() == FETCH_BULK_SIZE) {
                postData(list, eventName)
            }
        }
        if (!list.isEmpty()) {
            postData(list, eventName)
        }
        cursor.close()
    }

    /**
     * 发送赠送道具事件
     */
    static void sendProductEvent(String eventName, Long begin, Long end) {
        def condition = ['buy_vip', 'buy_guard', 'grab_sofa', 'song', 'buy_car', 'broadcast', 'buy_prettynum']
        BasicDBObject timeExpression = new BasicDBObject('timestamp': [$gte: begin, $lt: end], type: [$in: condition], 'session._id': [$ne: null],)
        BasicDBObject fieldExpression = new BasicDBObject('_id': 1, 'type': 1, 'timestamp': 1, 'cost': 1, 'session': 1, 'room': 1)
        DBCursor cursor = roomCost.find(timeExpression, fieldExpression).batchSize(FETCH_BULK_SIZE)
        println "${SensorsApi.class.getSimpleName()}:${new Date(begin).format('yyyy-MM-dd HH:mm:ss')} to ${new Date(end).format('yyyy-MM-dd HH:mm:ss')}---------------------- [${eventName}] ---- 共${cursor.size()}条数据 ---------------------- "
        List list = new ArrayList()
        while (cursor.hasNext()) {
            def obj = cursor.next()
            Map session = obj.get('session') as Map
            Map<String, Object> properties = new HashMap<String, Object>();
            String id = session.get('_id') as String
            Long timestamp = obj.get('timestamp') as Long
            String type = obj.get('type') == null ? 'vip' : productMap.get(obj.get('type'))
            properties.put('$time', new Date(timestamp));
            properties.put('executor', id.toString());
            properties.put('timestamp', timestamp.toString());
            Integer platform = session.get('platform') == null ? 1 : session.get('platform') as Integer
            properties.put('client_type', platformMap.get(platform))
            properties.put('duration', '30')
            properties.put('charge_lemon', obj.get('cost') == null ? INTEGER_DEFAULT : obj.get('cost') as Long)
            properties.put('product_type', type)
            if (type.equals('守护')) {
                properties.put('guardedId', obj.get('room') == null ? STRING_DEFAULT : obj.get('room') as String)
            } else {
                properties.put('guardedId', STRING_DEFAULT)
            }
            list.add(properties)
            if (list.size() == FETCH_BULK_SIZE) {
                postData(list, eventName)
            }
        }
        if (!list.isEmpty()) {
            postData(list, eventName)
        }
        cursor.close()
    }

    /**
     * 发送关注事件
     */
    static void sendFollowEvent(String eventName, Long begin, Long end) {
        BasicDBObject timeExpression = new BasicDBObject('timestamp': [$gte: begin, $lt: end], 'user_id': [$ne: null])
        DBCursor cursor = followerLogs.find(timeExpression).batchSize(FETCH_BULK_SIZE)
        println "${SensorsApi.class.getSimpleName()}:${new Date(begin).format('yyyy-MM-dd HH:mm:ss')} to ${new Date(end).format('yyyy-MM-dd HH:mm:ss')}---------------------- [${eventName}] ---- 共${cursor.size()}条数据 ---------------------- "
        List list = new ArrayList()
        while (cursor.hasNext()) {
            def obj = cursor.next()
            String id = obj.get('user_id') as String
            Long timestamp = obj.get('timestamp') as Long
            Map<String, Object> properties = new HashMap<String, Object>();
            properties.put('$time', new Date(timestamp));
            properties.put('timestamp', timestamp.toString());
            properties.put('executor', id.toString());
            properties.put('starId', obj.get('star_id') == null ? STRING_DEFAULT : obj.get('star_id') as String);
            Boolean isFirstFollow = obj.get('first_time') as Boolean
            if (isFirstFollow) {
                properties.put('isFirstFollow', '是')
            } else {
                properties.put('isFirstFollow', '否')
            }
            String platForm = getPlatForm(id as Integer)
            properties.put('client_type', platForm)
            list.add(properties)
            if (list.size() == FETCH_BULK_SIZE) {
                postData(list, eventName)
            }
        }
        if (!list.isEmpty()) {
            postData(list, eventName)
        }
        cursor.close()
    }

    /**
     * 获取所有渠道
     */
    private static void findAllQd() {
        DBCursor cursor = channels.find()
        while (cursor.hasNext()) {
            def obj = cursor.next()
            qdMap.put(obj['_id'], obj['name'])
        }
        cursor.close()
    }

    /**
     * 获取所有礼物类型
     */
    private static void findAllGiftType() {
        DBCursor cursor = giftCategories.find()
        while (cursor.hasNext()) {
            def obj = cursor.next()
            giftTypeMap.put(obj['_id'], obj['name'])
        }
        cursor.close()
    }

    /**
     * 发送profile数据
     * @param list
     */
    private static postProfile(List<Map> list, String eventName) {
        for (Map properties : list) {
            String distinctId = properties.get('executor').toString()
            try {
                sa.profileSet(distinctId, properties)
            } catch (Exception e) {
                println(e.printStackTrace())
                throw new Exception('the message is not profileSet. the error object is ' + properties)
            }
        }
        list.clear()
    }

    /**
     * 发送数据
     * @param list
     * @param eventName
     * @return
     */
    private static postData(List<Map> list, String eventName) {
        for (Map properties : list) {
            String distinctId = properties.get('executor').toString()
            try {
                sa.track(distinctId, eventName, properties);
            } catch (Exception e) {
                println(e.printStackTrace())
                throw new Exception('the message is not track. the error object is ' + properties)
            }
        }
        list.clear()
    }

    /**
     * 任务完成
     * @param begin
     * @return
     */
    private static jobFinish(Long begin) {
        Long now = System.currentTimeMillis()
        Long totalCost = now - begin
        println 'now is ' + now
        println 'begin is ' + begin
        println 'totalCost is ' + totalCost
        def id = TIMER_NAME + "_" + new Date().format("yyyyMMdd")
        BasicDBObject saveExpression = new BasicDBObject(timer_name: TIMER_NAME, cost_total: totalCost, cat: 'day', unit: 'ms', timestamp: now)
        timeLogs.update(new BasicDBObject(_id: id), new BasicDBObject('$set': saveExpression), true, false);
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}:${SensorsApi.class.getSimpleName()}:finish cost ${totalCost} ms"
    }

    /**
     * 获取开始时间
     * @param eventName
     * @return
     */
    private static Long getBegin(String eventName) {
        Long begin = BEGIN
        if (BEGIN == null) {
            BasicDBObject sortExpression = new BasicDBObject()
            sortExpression.put('eventName.' + eventName, -1)
            List sensorTaskList = timeLogs.find(new BasicDBObject('timer_name': TIMER_NAME)).sort(sortExpression).limit(1).toArray();
            begin = BEGIN
            Map map = new HashMap()
            if (!sensorTaskList.isEmpty()) {
                def obj = sensorTaskList.get(0) as DBObject
                map = obj.get('eventName')
                if (map != null && map.get(eventName) != null) {
                    begin = map.get(eventName)
                }
            }
        }
        return begin
    }

    /**
     * 获取结束时间
     * @return
     */
    private static Long getEnd() {
        if (END == null) {
            return System.currentTimeMillis()
        } else {
            return END
        }
    }

    /**
     * 格式化日期
     * @param timestamp
     * @return
     */
    private static String timestampFomat(Long timestamp) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm");
        Date date = new Date(timestamp)
        String timestampFomat = sdf.format(date)
        return timestampFomat
    }

    /**
     * 获取手机号
     * @param distinctId
     * @return
     */
    private static String getMobileById(Integer distinctId) {
        String mobile = STRING_DEFAULT
        DBObject user = xyUsers.findOne(new BasicDBObject('_id': distinctId))
        if (user != null) {
            mobile = user.get('mobile') == null ? mobile : user.get('mobile')
        }
        return mobile
    }

    /**
     * 获取平台信息
     * @param distinctId
     * @return
     */
    private static String getPlatForm(Integer distinctId) {
        Integer platForm = 1
        BasicDBObject expression = new BasicDBObject('user_id': distinctId)
        BasicDBObject sortExpression = new BasicDBObject('timestamp': -1)
        DBCursor cursor = dayLogin.find(expression).sort(sortExpression).limit(1)
        while (cursor.hasNext()) {
            def obj = cursor.next()
            platForm = obj.get('platform') == null ? 1 : obj.get('platform') as Integer
        }
        return platformMap.get(platForm) as String
    }

    /**
     * 获取登陆方式
     * @param distinctId
     * @return
     */
    private static String getLoginType(Integer distinctId) {
        BasicDBObject expression = new BasicDBObject('_id': distinctId)
        BasicDBObject fieldExpression = new BasicDBObject('mobile_bind': 1, 'uname_bind': 1, 'via': 1)
        DBObject user = users.findOne(expression, fieldExpression)
        if (user == null) {
            return STRING_DEFAULT
        }
        String loginType = user.get('via') == null ? STRING_DEFAULT : viaMap.get(user.get('via')) as String
        if (user && user.get('mobile_bind') != null) {
            Boolean isBindMobile = user.get('mobile_bind') as Boolean
            if (isBindMobile) {
                loginType = '手机号'
            }
            return loginType
        }
        if (user && user.get('uname_bind') != null) {
            Boolean isBindUname = user.get('uname_bind') as Boolean
            if (isBindUname) {
                loginType = '么么号'
            }
            return loginType
        }
        return loginType
    }

    /**
     * 获取付款方式
     * @param via
     * @return
     */
    static String getChannelPay(String via) {
        BasicDBObject expression = new BasicDBObject('_id': via)
        DBObject obj = channelPay.findOne(expression)
        String viaFomat = '支付宝移动'
        if (obj) {
            viaFomat = obj.get('name') == null ? viaFomat : obj.get('name')
        }
        return viaFomat
    }

    /**
     * 是否新增充值用户
     * @param distinctId
     * @return
     */
    static String isNewConsumer(Integer distinctId, Long timestamp) {
        String isNewConsumer = '是'
        BasicDBObject expression = new BasicDBObject('user_id': distinctId, timestamp: [$lt: timestamp])
        Long count = financeLog.count(expression)
        if (count > 0) {
            isNewConsumer = '否'
        }
        return isNewConsumer
    }

    /**
     * 以当前时间0点为基准,计算前n天和截止日期
     * ALL代表是否全表扫描,true 则 开始时间从自定义的时间执行,结束时间为now
     *
     * @param minusDay 减去的天数
     * @param closeDay 截止日期
     * @return
     */
    static Map getBeginAndEnd() {
        Calendar now = Calendar.getInstance()
        Long begin,end = 0L
        now.setTime(new Date());
        now.set(Calendar.HOUR_OF_DAY, 0)
        now.set(Calendar.MINUTE, 0)
        now.set(Calendar.SECOND, 0)
        now.set(Calendar.MILLISECOND, 0)
        now.set(Calendar.DATE, now.get(Calendar.DATE) - (MINUS_DAY == null ? 1 : MINUS_DAY));
        begin = now.getTimeInMillis()
        end = now.getTimeInMillis() + DAY_MILLON * (CLOSE_DAY == null ? 1 : CLOSE_DAY)
        if (END != null) {
            end = END
        }
        if (BEGIN != null) {
            begin = BEGIN
        }
        return ['begin':begin,'end':end];
    }

}
