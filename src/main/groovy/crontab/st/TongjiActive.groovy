#!/usr/bin/env groovy
package crontab.st

import com.https.HttpsUtil
import com.mongodb.BasicDBObject
import com.mongodb.DBObject
import com.mongodb.Mongo
import com.mongodb.MongoURI
import groovy.json.JsonBuilder
@GrabResolver(name = 'restlet', root = 'http://210.22.151.242:8081/nexus/content/groups/public')
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
        @Grab('com.ttpod:https-util:1.0'),
])
import groovy.json.JsonSlurper
import org.apache.commons.lang.StringUtils
import java.security.MessageDigest

class TongjiActive {
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
    static historyMongo = new Mongo(new MongoURI(getProperties('mongo_history.uri', 'mongodb://192.168.31.246:27017/?w=1') as String))
    static day_login = mongo.getDB("xylog").getCollection("day_login")
    //static day_login = historyMongo.getDB("xylog_history").getCollection("day_login_history")

    static DAY_MILLON = 24 * 3600 * 1000L

    static long zeroMill = new Date().clearTime().getTime()
    static Long yesTday = zeroMill - DAY_MILLON
    static String YMD = new Date(yesTday).format("yyyyMMdd")

    // 友盟账号
    static final String UMENG_ACCOUNT = 'aiwanzhibo@qq.com'

    // 友盟密码
    static final String UMENG_PASSWARD = 'eC9CBNPz4LUWrL2'

    //友盟token
    static String AUTH_TOKEN = 'yudDFxUp8ZmLlyP9gfnv'

    //友盟秘钥
    static final String APP_KEY = '58ac09dd717c195234000979'

    static String request(String url) {
        HttpURLConnection conn = null;
        def jsonText = "";
        try {
            conn = (HttpURLConnection) new URL(url).openConnection()
            conn.setRequestMethod("POST")
            conn.setDoOutput(true)
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(5000);
            conn.connect()
            jsonText = conn.getInputStream().getText("UTF-8")

        } catch (Exception e) {
            println "request Exception : " + e;
        } finally {
            if (conn != null) {
                conn.disconnect();
                conn = null;
            }
        }
        return jsonText;
    }

    /**
     * 获取友盟的token
     * curl "http://api.umeng.com/authorize" --data "email=aiwanzhibo@qq.com&password=eC9CBNPz4LUWrL2" post请求
     */
    private static void umeng_token() {
        String url = "http://api.umeng.com/authorize?email=${UMENG_ACCOUNT}&password=${UMENG_PASSWARD}"
        String resp = request(url)
        if (StringUtils.isNotBlank(resp)) {
            JsonSlurper jsonSlurper = new JsonSlurper()
            Map map = jsonSlurper.parseText(resp) as Map
            if (map != null && map.containsKey('code')) {
                def code = map['code'] as Integer
                if (code == 200 && map.containsKey('auth_token')) {
                    AUTH_TOKEN = map['auth_token']
                }
            }
        }
        println("auth_token is ${AUTH_TOKEN}")
    }

    /**
     * 友盟接口请求有限制（15分钟300条）
     * @param i
     */
    static void fetchUmengData(int i) {
        long l = System.currentTimeMillis()
        def gteMill = yesTday - i * DAY_MILLON
        def date = new Date(gteMill)//
        def list = []
        def day = date.format("yyyyMMdd_")
        def coll = mongo.getDB('xy_admin').getCollection('stat_channels')
        def channelDB = mongo.getDB('xy_admin').getCollection('channels')
        def qdlist = channelDB.find(new BasicDBObject(is_close: false), new BasicDBObject(_id: 1))*._id

        [APP_KEY].each { String appkey ->
            def page = 1, per_page = 100
            def hasMore = true
            while (hasMore) {
                def data = null, result = "[]"
                for (int j = 0; j < 10; j++) {
                    try {
                        result = getChannels(appkey, per_page, page, date)
                        if (result != null) {
                            data = new JsonSlurper().parseText(result)
                            break
                        }
                        if (data == null) {
                            //若接口第一时间无法同步则等待一定时间再重新查询
                            Thread.sleep(2 * 60 * 1000L)
                        }
                    } catch (Exception e) {
                        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}"
                        println e.getStackTrace()
                        //若接口第一时间无法同步则等待一定时间再重新查询
                        Thread.sleep(30 * 1000L)
                    }

                }
                if (data.size() > 0) {
                    data.each { Map row ->
                        def channelId = row['channel'] as String
                        if (qdlist.contains(channelId)) {
                            //查询umeng自定义事件三日发言
                            def update = new BasicDBObject([active     : row['install'] as Integer,//新增用户
                                                            active_user: row['active_user'] as Integer,//日活
                                                            duration   : row['duration'] as String//使用时长
                            ])
                            def channel = channelDB.findOne(new BasicDBObject("_id", row['channel'] as String), new BasicDBObject("active_discount", 1))
                            def active = row['install'] as Integer
                            if (channel != null && active != null) {
                                //设置激活扣量cpa1
                                def discountMap = channel.removeField("active_discount") as Map
                                if (discountMap != null && discountMap.size() > 0) {
                                    def cpa = null
                                    def keyList = discountMap.keySet().toArray().sort { String a, String b ->
                                        Long.parseLong(b) <=> Long.parseLong(a)
                                    }
                                    for (it in keyList) {
                                        if (gteMill >= (it as Long)) {
                                            def discount = discountMap.get(it) as Integer
                                            cpa = new BigDecimal((double) (active * discount / 100)).toInteger()
                                            break
                                        }
                                    }
                                    if (cpa != null) update.append("cpa1", cpa)
                                }
                            }
                            row.put('update', update)
                            row.put('appkey', appkey)
                            //查询30日新增激活及日活
                            def new_active30 = 0, active30 = 0
                            coll.aggregate(
                                    new BasicDBObject('$match', [qd: row['channel'], timestamp: [$gte: gteMill - 30 * DAY_MILLON, $lt: gteMill + DAY_MILLON]]),
                                    new BasicDBObject('$project', [_id: '$qd', new_active: '$active', active: '$active_user']),
                                    new BasicDBObject('$group', [_id: '$_id', total_new_active: [$sum: '$new_active'], total_active: [$sum: '$active']])
                            ).results().each { BasicDBObject item ->
                                new_active30 = item.get('total_new_active') as Integer
                                active30 = item.get('total_active') as Integer
                            }
                            update.put("active30", new_active30 + update.get('active'))
                            update.put("active_user30", active30 + update.get('active_user'))
                            list.add(row)
                        }
                    }
                }
                if (data == null || data.size() < per_page) {
                    hasMore = false
                }
                page++
            }
        }
        list.each { Map row ->
            def appkey = row['appkey'] as String
            def update = row['update'] as BasicDBObject
            try {
                if (row['id'] != null) {
                    def count = getSpeechs(appkey, row['id'] as String, date)
                    update.put("speechs", count)
                }
            } catch (Exception e) {
                println "${new Date().format('yyyy-MM-dd HH:mm:ss')} ${row['channel']} :${row['id']} speechs error".toString()
            }
            coll.update(new BasicDBObject('_id', "${day}${row['channel']}".toString()), new BasicDBObject('$set', update))
        }

        def before = new Date(gteMill - DAY_MILLON)
        list.each { Map row ->
            def appkey = row['appkey'] as String
            try {
                if (row['id'] != null) {
                    def rateStr = getRetention(appkey, row['id'] as String, before)
                    def rate = new BigDecimal(rateStr).toDouble()
                    coll.update(new BasicDBObject('_id', "${before.format("yyyyMMdd_")}${row['channel']}".toString()), new BasicDBObject('$set', ["retention": rate]))
                }
            } catch (Exception e) {
                println "${new Date().format('yyyy-MM-dd HH:mm:ss')} ${row['channel']}:${row['id']} retention error".toString()
            }
        }
    }

    /**
     * 调用友盟渠道列表
     *
     * @param appkey
     * @param pageSize
     * @param page
     * @param date
     * @return
     */
    private static String getChannels(String appkey, Integer pageSize, Integer page, Date date) {
        def content = null
        InputStream is = null
        try {
            def url = new URL("http://api.umeng.com/channels?appkey=${appkey}&auth_token=${AUTH_TOKEN}" +
                    "&per_page=${pageSize}&page=${page}&date=${date.format('yyyy-MM-dd')}")
            def conn = (HttpURLConnection) url.openConnection()
            def responseCode = conn.getResponseCode()
            if (responseCode == 403) {
                println 'umeng channels sleep 3min'
                Thread.sleep(3 * 60 * 1000L)
                return getChannels(appkey, pageSize, page, date)
            }
            if (responseCode == 200) {
                def buffer = new StringBuffer()
                is = conn.getInputStream()
                is.eachLine('UTF-8') { buffer.append(it) }
                content = buffer.toString()
            }
        } finally {
            if (is != null) is.close()
        }
        return content
    }

    private static Integer getSpeechs(String appkey, String channelId, Date date) {
        def count = 0
        InputStream is = null
        try {
            String url_str = "http://api.umeng.com/events/parameter_list?appkey=${appkey}" +
                    "&auth_token=${AUTH_TOKEN}&period_type=daily&event_id=543ce217e8af9ceaa72f3847" +
                    "&start_date=${date.format('yyyy-MM-dd')}&end_date=${date.format('yyyy-MM-dd')}" +
                    "&channels=${channelId}"
            def url = new URL(url_str)

            //println "getSpeechs url: ${url_str}"
            def conn = (HttpURLConnection) url.openConnection()
            def responseCode = conn.getResponseCode()
            if (responseCode == 403) {
                println 'umeng parameter_list sleep 3min'
                Thread.sleep(3 * 60 * 1000L)
                return getSpeechs(appkey, channelId, date)
            }
            if (responseCode == 200) {
                def buffer = new StringBuffer()
                is = conn.getInputStream()
                is.eachLine('UTF-8') { buffer.append(it) }
                def content = buffer.toString()
                if (StringUtils.isNotBlank(content)) {
                    //println "getSpeechs content: ${content}"
                    def listObj = new JsonSlurper().parse(new StringReader(content)) as List
                    if (listObj != null) {
                        for (Map item : listObj) {
                            if ("新注册用户数发言率".equals(item.get('label') as String)) {
                                count = item.get('num') as Integer
                            }
                        }
                    }
                }
            }
        } finally {
            if (is != null) is.close()
        }
        return count
    }

    private static String getRetention(String appkey, String channelId, Date date) {
        def rate = "0"
        InputStream is = null
        try {
            def url = new URL("http://api.umeng.com/retentions?appkey=${appkey}&auth_token=${AUTH_TOKEN}" +
                    "&start_date=${date.format('yyyy-MM-dd')}&end_date=${date.format('yyyy-MM-dd')}&period_type=daily" +
                    "&channels=${channelId}")
            def conn = (HttpURLConnection) url.openConnection()
            def responseCode = conn.getResponseCode()
            if (responseCode == 403) {
                println 'umeng retentions sleep 3min'
                Thread.sleep(3 * 60 * 1000L)
                return getRetention(appkey, channelId, date)
            }
            if (responseCode == 200) {
                def buffer = new StringBuffer()
                is = conn.getInputStream()
                is.eachLine('UTF-8') { buffer.append(it) }
                def content = buffer.toString()
                if (StringUtils.isNotBlank(content)) {
                    def listObj = new JsonSlurper().parse(new StringReader(content)) as List
                    if (listObj != null && listObj.size() > 0) {
                        def obj = listObj[0] as Map
                        def retentionList = obj.get('retention_rate') as List
                        if (retentionList != null && retentionList.size() > 0) {
                            rate = retentionList[0] as String
                        }
                    }
                }

            }
        } finally {
            if (is != null) is.close()
        }
        return rate
    }

    /**
     * 1,7,30活跃统计
     * @param i
     */
    static void activeStatics(int i) {
        def gteMill = yesTday - i * DAY_MILLON
        def ltMill = gteMill + DAY_MILLON
        def YMD = new Date(gteMill).format('yyyyMMdd')
        def day7Mill = gteMill - 6 * DAY_MILLON
        def day30Mill = gteMill - 29 * DAY_MILLON
        def coll = mongo.getDB('xy_admin').getCollection('stat_channels')
        mongo.getDB('xy_admin').getCollection('channels').find(new BasicDBObject(),
                new BasicDBObject('_id', 1)).toArray().each { BasicDBObject obj ->

            def logins = day_login.find(new BasicDBObject(qd: obj['_id'], timestamp: [$gte: day30Mill, $lt: ltMill]), new BasicDBObject([user_id: 1, timestamp: 1])).toArray()
            Set<Integer> daySet = new HashSet<Integer>(200000)
            Set<Integer> day7Set = new HashSet<Integer>(2000000)
            Set<Integer> day30Set = new HashSet<Integer>(2000000)
            for (DBObject login : logins) {
                Integer uid = login.get("user_id") as Integer
                Long timestamp = login.get('timestamp') as Long
                day30Set.add(uid)
                if (timestamp >= day7Mill) {
                    day7Set.add(uid)
                }
                if (timestamp >= gteMill) {
                    daySet.add(uid)
                }
            }
            coll.update(new BasicDBObject('_id', "${YMD}_${obj['_id']}".toString()), new BasicDBObject('$set',
                    [daylogin: daySet.size(), day7login: day7Set.size(), day30login: day30Set.size()]
            ))
        }
    }

    /**
     * 1,3,7,30留存统计
     */
    static stayStatics(int i) {
        def gteMill = yesTday - i * DAY_MILLON
        def prefix = new Date(gteMill).format("yyyyMMdd_")
        def channels = mongo.getDB("xy_admin").getCollection("channels")
        def stat_channels = mongo.getDB("xy_admin").getCollection('stat_channels')

        channels.find(new BasicDBObject(), new BasicDBObject("_id": 1)).toArray().each { BasicDBObject qdObj ->
            String qd = qdObj.get("_id")
            def channel = stat_channels.findOne(new BasicDBObject('_id', prefix + qd))
            if (channel != null) {
                def map = new HashMap<String, Long>(4)
                [1, 3, 7, 30].each { Integer d ->
                    def allUids = channel?.get('regs') as Collection
                    if (allUids && allUids.size() > 0) {
                        Long gt = gteMill + d * DAY_MILLON
                        Integer count = 0;
                        if (gt <= yesTday) {
                            count = day_login.count(new BasicDBObject(user_id: [$in: allUids], timestamp:
                                    [$gte: gt, $lt: gt + DAY_MILLON]))
                        }
                        map.put("${d}_day".toString(), count)
                    }
                }
                if (map.size() > 0) {
                    stat_channels.update(new BasicDBObject('_id', "${prefix}${qd}".toString()),
                            new BasicDBObject('$set', new BasicDBObject("stay", map)))
                }
            }
        }
    }

    /**
     * 更新父渠道激活、扣量激活、发言数等信息
     */
    static parentQdstatic(int i) {
        def channel_db = mongo.getDB('xy_admin').getCollection('channels')
        def stat_channels = mongo.getDB('xy_admin').getCollection('stat_channels')
        def channels = channel_db.find(new BasicDBObject(parent_qd: [$ne: null]), new BasicDBObject(parent_qd: 1)).toArray()
        Map<String, DBObject> parentMap = new HashMap<String, DBObject>()
        for (DBObject obj : channels) {
            String parent_id = obj.get("parent_qd") as String
            parentMap.put(parent_id, obj)
        }

        for (String key : parentMap.keySet()) {
            DBObject obj = parentMap.get(key)
            Long begin = yesTday - i * DAY_MILLON
            def parent_id = obj.get("parent_qd") as String
            def childqds = channel_db.find(new BasicDBObject(parent_qd: parent_id), new BasicDBObject(_id: 1)).toArray()
            DBObject query = new BasicDBObject('qd', [$in: childqds.collect {
                ((Map) it).get('_id').toString()
            }]).append("timestamp", begin)
            def stat_child_channels = stat_channels.find(query).toArray()
            Integer cpa1 = 0
            Integer visitors = 0
            Integer active = 0
            Integer active_user = 0
            Integer speechs = 0
            Boolean hasVisitor = Boolean.FALSE
            int size = stat_child_channels.size()
            //println "stat_child_channels.size-------------->:$size"
            for (DBObject myObj : stat_child_channels) {
                Integer currentCpa1 = (myObj.get("cpa1") != null) ? myObj.get("cpa1") as Integer : 0
                cpa1 += currentCpa1
                Integer currentActive = (myObj.get("active") != null) ? myObj.get("active") as Integer : 0
                active += currentActive
                Integer currentActive_user = (myObj.get("active_user") != null) ? myObj.get("active_user") as Integer : 0
                active_user += currentActive_user
                Integer currentSpeechs = (myObj.get("speechs") != null) ? myObj.get("speechs") as Integer : 0
                speechs += currentSpeechs
                if (myObj.containsField("visitors")) {
                    hasVisitor = Boolean.TRUE
                    visitors += (myObj.get("visitors") != null) ? myObj.get("visitors") as Integer : 0
                }
            }
            def YMD = new Date(begin).format("yyyyMMdd")
            def st = new BasicDBObject(_id: "${YMD}_${parent_id}" as String, qd: parent_id, timestamp: begin)
            def incObject = new BasicDBObject(cpa1: cpa1, active: active, active_user: active_user, speechs: speechs)
            def setObject = new BasicDBObject(qd: parent_id, timestamp: begin)
            if (hasVisitor) incObject.append('visitors', visitors)
            setObject.putAll(incObject)
            stat_channels.findAndModify(st, null, null, false,
                    new BasicDBObject($set: setObject), true, true)
        }

    }

    private static String md5HexString(String content) {
        def hexDigits = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'] as char[];
        try {
            byte[] btInput = content.getBytes();
            // 获得MD5摘要算法的 MessageDigest 对象
            MessageDigest mdInst = MessageDigest.getInstance("MD5");
            // 使用指定的字节更新摘要
            mdInst.update(btInput);
            // 获得密文
            byte[] md = mdInst.digest();
            // 把密文转换成十六进制的字符串形式
            int j = md.length;
            char[] str = new char[j << 1];
            int k = 0;
            for (int i = 0; i < j; i++) {
                str[(k++)] = hexDigits[((0xF0 & md[i]) >>> 4)];
                str[(k++)] = hexDigits[(0xF & md[i])]
            }
            return new String(str);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null
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
                    accesskey: 'B6C91AE433B94D9ABA77936AEEF21900',
                    filter   : [// *数据筛选条件
                                platformids: [1, 2]
                    ]
            ]
            builder.setContent(queryContent)
            def response = HttpsUtil.postMethod(channellist, builder.toString())
            def channel = [:]
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
                if (StringUtils.isNotBlank(response)) {
                    def respMap = new JsonSlurper().parseText(response) as Map
                    if (respMap && 200 == respMap.get('status')) {
                        def channels = respMap.get('result') as List
                        channels.each { Map obj ->
                            def channelid = obj.get('channelid') as String
                            def newuser = (obj.get('newuser') ?: 0) as Integer
                            def channelName = channel.get(channelid) as String
                            //更新渠道激活信息
                            coll.update(new BasicDBObject('_id', "${date.format('yyyyMMdd')}_${channelName}".toString()), new BasicDBObject('$set', [active: newuser]))
                        }
                    }
                }
            }
        } catch (Exception e) {
            println "fetchTalkingData Exception:" + e
        }


    }

    static staticQdReport(int i) {
        def gteMill = yesTday - i * DAY_MILLON
        def date = new Date(gteMill)
        def prefix = date.format('yyyyMMdd_')
        def channels = mongo.getDB('xy_admin').getCollection('channels')
        def stat_daily = mongo.getDB('xy_admin').getCollection('stat_daily')
        def users = mongo.getDB('xy').getCollection('users')
        def finance_log = mongo.getDB('xy_admin').getCollection('finance_log')
        def stat_report = mongo.getDB('xy_admin').getCollection('stat_report')
        def channel_pc = [] as List, channel_android = [] as List, channel_ios = [] as List, channel_h5 = [] as List, channel_ria = [] as List
        //查询pc渠道id
        channels.find(new BasicDBObject(), new BasicDBObject(_id: 1, client: 1)).toArray().each { BasicDBObject obj ->
            def id = obj.get('_id') as String
            def client = obj.get('client') as String
            //if ('1'.equals(client) || '5'.equals(client) || '6'.equals(client)) {
            if ('1'.equals(client)) {
                channel_pc.add(id)
            }
            if ('2'.equals(client)) {
                channel_android.add(id)
            }
            if ('4'.equals(client)) {
                channel_ios.add(id)
            }
            /*if ('5'.equals(client)) {
                channel_h5.add(id)
            }
            if ('6'.equals(client)) {
                channel_ria.add(id)
            }*/
        }
        // 查询充值信息
        def pay = stat_daily.findOne(new BasicDBObject(_id: "${prefix}allpay".toString()), new BasicDBObject(first_pay: 0))
        // 查询当天登录数
        def login = stat_daily.findOne(new BasicDBObject(_id: "${prefix}alllogin".toString()))
        def reg_pc = new HashSet(10000), reg_mobile = new HashSet(500000)
        def pc_cur = new HashSet(1000), mobile_cur = new HashSet(10000)
        // 查询30天注册人数,顺便统计当天注册人数
        users.find(new BasicDBObject(timestamp: [$gte: gteMill - 29 * DAY_MILLON, $lt: gteMill + DAY_MILLON]),
                new BasicDBObject(_id: 1, timestamp: 1, qd: 1)).toArray().each { BasicDBObject obj ->
            def qd = obj.get('qd') ?: 'MM'
            def uid = obj.get('_id') as Integer
            def timestamp = obj.get('timestamp') as Long
            def insert = false
            if (channel_pc.contains(qd)) {
                insert = true
                if (timestamp > gteMill) {
                    pc_cur.add(uid)
                }
                reg_pc.add(uid)
            }
            if (channel_android.contains(qd)) {
                insert = true
                if (timestamp > gteMill) {
                    mobile_cur.add(uid)
                }
                reg_mobile.add(uid)
            }
            if (channel_ios.contains(qd)) {
                insert = true
            }
            if (!insert) {
                reg_mobile.add(uid)
            }
        }
        // 查询30天新充值用户
        def payed_user = new ArrayList(2000)
        stat_daily.find(new BasicDBObject(type: "allpay", timestamp: [$gte: gteMill - 29 * DAY_MILLON, $lt: gteMill + DAY_MILLON]), new BasicDBObject(first_pay: 1))
                .toArray().each { BasicDBObject obj ->
            def uids = (obj.get('first_pay') as List) ?: []
            payed_user.addAll(uids)
        }
        //每个用户30天内的充值金额
        def pc_total = 0, mobile_total = 0, pc_reg_total = 0, mobile_reg_total = 0
        def pc_pay_user = new HashSet(), mobile_pay_user = new HashSet()
        def pc_pay_reg = new HashSet(), mobile_pay_reg = new HashSet()
        finance_log.aggregate(
                new BasicDBObject('$match', [via: [$ne: 'Admin'], $or: [[user_id: [$in: payed_user]], [to_id: [$in: payed_user]]], timestamp: [$gte: gteMill - 29 * DAY_MILLON, $lt: gteMill + DAY_MILLON]]),
                new BasicDBObject('$project', [_id: '$user_id', to_id: '$to_id', qd: '$qd', cny: '$cny']),
                new BasicDBObject('$group', [_id: [fid: '$_id', tid: '$to_id'], qd: [$first: '$qd'], cny: [$sum: '$cny']])
        ).results().each { BasicDBObject obj ->
            def uid = obj.get('_id') as Map
            def fid = uid.get('fid') as Integer
            def tid = uid.get('tid') as Integer
            def qd = obj.get('qd') ?: 'MM'
            def cny = obj.get('cny') as Double
            if (channel_pc.contains(qd)) {
                pc_total += cny
                if (fid != null && fid == tid) pc_pay_user.add(fid)
                if (tid != null && tid != fid) pc_pay_user.add(tid)
            } else if (channel_android.contains(qd)) {
                mobile_total += cny
                if (fid != null && fid == tid) mobile_pay_user.add(fid)
                if (tid != null && tid != fid) mobile_pay_user.add(tid)
            } else if (channel_ios.contains(qd)) {
                //do nothing
            } else {
                mobile_total += cny
                if (fid != null && fid == tid) mobile_pay_user.add(fid)
                if (tid != null && tid != fid) mobile_pay_user.add(tid)
            }
            if (reg_pc.contains(fid)) {
                pc_reg_total += cny
                pc_pay_reg.add(fid)
            } else if (reg_pc.contains(tid)) {
                pc_reg_total += cny
                pc_pay_reg.add(tid)
            }
            if (reg_mobile.contains(fid)) {
                mobile_reg_total += cny
                mobile_pay_reg.add(fid)
            } else if (reg_mobile.contains(tid)) {
                mobile_reg_total += cny
                mobile_pay_reg.add(tid)
            }

        }
        def pc_map = new HashMap(), mobile_map = new HashMap()
        pc_map.put('type', 'pcreport')
        pc_map.put('timestamp', gteMill)
        pc_map.put('pay_user', pay?.get('user_pc')?.getAt('user') ?: 0)
        pc_map.put('pay_cny', pay?.get('user_pc')?.getAt('cny') ?: 0)
        pc_map.put('reg_user', pc_cur.size())
        pc_map.put('log_user', login?.get('pc_login')?.getAt('daylogin') ?: 0)
        pc_map.put('reg_pay_cny', pc_reg_total)//30天内注册用户充值金额
        pc_map.put('reg_user30', reg_pc.size())//30天内注册用户数
        pc_map.put('reg_pay_user', pc_pay_reg.size())//30天内注册充值用户数
        pc_map.put('first_pay_cny', pc_total)//30天内新增充值用户充值金额
        pc_map.put('first_pay_user', pc_pay_user.size())//30天内新增充值用户数
        stat_report.update(new BasicDBObject(_id: "${prefix}pcreport".toString()),
                new BasicDBObject($set: pc_map), true, false)
        //新增激活，激活用户
        def new_active = 0, active = 0
        try {
            def content = new URL("http://api.umeng.com/base_data?appkey=${APP_KEY}&auth_token=${AUTH_TOKEN}" +
                    "&date=${date.format('yyyy-MM-dd')}").getText("UTF-8")
            def obj = new JsonSlurper().parseText(content) as Map
            new_active = obj.get('new_users') as Integer
            active = obj.get('active_users') as Integer
        } catch (Exception e) {
            println e
        }
        mobile_map.put('type', 'mobilereport')
        mobile_map.put('timestamp', gteMill)
        mobile_map.put('pay_user', pay?.get('user_mobile')?.getAt('user') ?: 0)
        mobile_map.put('pay_cny', pay?.get('user_mobile')?.getAt('cny') ?: 0)
        mobile_map.put('reg_user', mobile_cur.size())
        mobile_map.put('log_user', login?.get('mobile_login')?.getAt('daylogin') ?: 0)
        mobile_map.put('reg_pay_cny', mobile_reg_total)
        mobile_map.put('reg_user30', reg_mobile.size())
        mobile_map.put('reg_pay_user', mobile_pay_reg.size())
        mobile_map.put('first_pay_cny', mobile_total)
        mobile_map.put('first_pay_user', mobile_pay_user.size())
        mobile_map.put('new_active', new_active ?: 0)
        mobile_map.put('active', active ?: 0)
        stat_report.update(new BasicDBObject(_id: "${prefix}mobilereport".toString()),
                new BasicDBObject($set: mobile_map), true, false)
    }

    static staticRetention(int i) {
        def gteMill = yesTday - i * DAY_MILLON
        def date = new Date(gteMill)
        def prefix = date.format('yyyyMMdd_')
        def channels = mongo.getDB('xy_admin').getCollection('channels')
        def users = mongo.getDB('xy').getCollection('users')
        def stat_report = mongo.getDB('xy_admin').getCollection('stat_report')
        def channel_pc = [] as List
        channels.find(new BasicDBObject('client', '1'), new BasicDBObject(_id: 1)).toArray().each { BasicDBObject obj ->
            def id = obj.get('_id') as String
            channel_pc.add(id)
        }
        //查询注册用户
        def pc_reg = new HashSet(), mobile_reg = new HashSet()
        users.find(new BasicDBObject(timestamp: [$gte: gteMill, $lt: gteMill + DAY_MILLON]),
                new BasicDBObject(_id: 1, timestamp: 1, qd: 1)).toArray().each { BasicDBObject obj ->
            def qd = obj.get('qd') ?: 'MM'
            def uid = obj.get('_id') as Integer
            if (channel_pc.contains(qd)) {
                pc_reg.add(uid)
            } else {
                mobile_reg.add(uid)
            }
        }
        def pc_retention = day_login.count(new BasicDBObject(user_id: [$in: pc_reg], timestamp: [$gte: gteMill + DAY_MILLON, $lt: gteMill + 2 * DAY_MILLON]))
        def mobile_retention = day_login.count(new BasicDBObject(user_id: [$in: mobile_reg], timestamp: [$gte: gteMill + DAY_MILLON, $lt: gteMill + 2 * DAY_MILLON]))
        def beforeStr = date.format('yyyy-MM-dd')
        def rate = 0 as Double
        try {
            def content = new URL("http://api.umeng.com/retentions?appkey=${APP_KEY}&auth_token=${AUTH_TOKEN}" +
                    "&start_date=${beforeStr}&end_date=${beforeStr}&period_type=daily").getText("UTF-8")
            if (StringUtils.isNotBlank(content)) {
                def listObj = new JsonSlurper().parse(new StringReader(content)) as List
                if (listObj != null && listObj.size() > 0) {
                    def obj = listObj[0] as Map
                    def retentionList = obj.get('retention_rate') as List
                    if (retentionList != null && retentionList.size() > 0) {
                        rate = new BigDecimal(retentionList[0] as String).toDouble()
                    }
                }
            }
        } catch (Exception e) {
            println e
        }
        stat_report.update(new BasicDBObject(_id: "${prefix}pcreport".toString()),
                new BasicDBObject($set: [reg_retention: pc_retention]))
        stat_report.update(new BasicDBObject(_id: "${prefix}mobilereport".toString()),
                new BasicDBObject($set: [reg_retention: mobile_retention, active_retention: rate]))
    }
    static Integer day = 0;

    static void main(String[] args) {
        long l = System.currentTimeMillis()
        long begin = l

//        /**
//         *

        //更新渠道的日、周、月活跃度
        l = System.currentTimeMillis()
        activeStatics(day)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   update qd activeStatics, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //更新渠道的1,3,7,30日留存率
        l = System.currentTimeMillis()
        31.times {
            stayStatics(it)
        }
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   update qd stayStatics, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)
        // 更新渠道的激活值
        l = System.currentTimeMillis()
        fetchUmengData(day)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   update qd fetchUmengData, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //03.父级渠道的统计
        l = System.currentTimeMillis()
        parentQdstatic(day)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   update qd parentQdstatic cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        // 更新IOS的激活（联运管理iOS版）
        l = System.currentTimeMillis()
        fetchTalkingData(day)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   update qd fetchTalkingData, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        // 运营关键数据统计（PC、手机），对应财务管理
        l = System.currentTimeMillis()
        staticQdReport(day)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   update qd staticQdReport cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        // 次日留存统计（对应财务管理-运营关键数据里边的三个留存数据）
        staticRetention(day + 1)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   update qd staticRetention cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)
//         */
        //落地定时执行的日志
        jobFinish(begin)
    }

    /**
     * 标记任务完成  用于运维监控
     * @return
     */
    private static jobFinish(Long begin) {
        def timerName = 'TongjiActive'
        Long totalCost = System.currentTimeMillis() - begin
        saveTimerLogs(timerName, totalCost)
        println "${new Date().format('yyyy-MM-dd')}:${TongjiActive.class.getSimpleName()}:finish   cost  ${System.currentTimeMillis() - begin} ms"
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