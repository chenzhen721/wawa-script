#!/usr/bin/env groovy
package crontab.st

import com.mongodb.BasicDBObject
import com.mongodb.DBCollection
import com.mongodb.DBCursor
import com.mongodb.DBObject
import com.mongodb.Mongo
import com.mongodb.MongoURI


//@GrabResolver(name = 'restlet', root = 'http://210.22.151.242:8081/nexus/content/groups/public')
@GrabResolver(name = 'restlet', root = 'http://192.168.31.253:8081//nexus/content/groups/public')
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('org.apache.httpcomponents:httpclient:4.2.5'),
        @Grab('redis.clients:jedis:2.1.0'),
        @Grab('com.ttpod:https-util:1.0'),
])
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus
import org.apache.http.NameValuePair
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.message.BasicNameValuePair
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams
import java.util.Map.Entry;
import org.apache.http.util.EntityUtils;

/**
 * 推荐系统 数据上报
 */
class Recommendation {
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
    static DBCollection users = mongo.getDB('xy').getCollection('users')
    static DBCollection rooms = mongo.getDB('xy').getCollection('rooms')
    static DBCollection room_cost = mongo.getDB("xylog").getCollection("room_cost")
    static DBCollection room_feather = mongo.getDB("xylog").getCollection("room_feather")
    static DBCollection followerLogs = mongo.getDB("xylog").getCollection("follower_logs")

    static DAY_MILLON = 24 * 3600 * 1000L

    static long zeroMill = new Date().clearTime().getTime()
    static Long yesTday = zeroMill - DAY_MILLON
    static String YMD = new Date(yesTday).format("yyyyMMdd")

    // 达观请求接口地址
    static String APP_NAME = 'meme';
    static String APP_ID = '1353115';
    static String POST_URL = "http://datareportapi.datagrand.com/data/${APP_NAME}"
    // 个性化推荐服务接口地址
    static String GET_RECOMMEND_STAR = "http://recapi.datagrand.com/personal/${APP_NAME}"

    // const
    static Long BEGIN = Date.parse("yyyy-MM-dd HH:mm:ss", "2014-06-25 00:00:00").getTime()
    static Long END = System.currentTimeMillis()
    static String ITEM = 'item'
    static String USER_ACTION = 'user_action'
    static String FOLLOW = 'follow'
    static String ONLNIE = 'liveUp'
    static String DOWN = 'liveOff'
    static String SEND_GIFT = 'sendGift'
    static String SEND_MEME = 'sendMeMe'

    static void main(String[] args) {
        Long start = System.currentTimeMillis()
        doDataGrandTask()
        Long totalCost = System.currentTimeMillis() - start
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}:${Recommendation.class.getSimpleName()}:finish cost ${totalCost} ms"
        updateTimerLogs(totalCost)
        // saveTestData()
    }

    /**
     * 达观任务
     * @return
     */
    def static doDataGrandTask() {
//        postStarData(STAR)
        postOnlineStarData(ONLNIE)
        postDownStarData(DOWN)
        postFollowData(FOLLOW)
        postSendGiftData(SEND_GIFT)
        postSendMeMeData(SEND_MEME)
//        getRecommendStar(50,1201175)
    }

    /**
     * 根据事件名获取最近一次执行时间,如果没有则取默认开始时间
     * 默认开始时间和默认结束时间需要自定义
     * @param eventName
     * @return
     */
    private static Map getTaskTime(String eventName) {
        Map<String, Long> map = new HashMap<>()
        Long begin = BEGIN
        Long end = END
        def timerLogsDB = mongo.getDB("xyrank").getCollection("timer_logs")
        def lastTime = eventName + '.lastTime'
        BasicDBObject expression = new BasicDBObject('timer_name': 'Recommendation')
        BasicDBObject sortExpression = new BasicDBObject()
        sortExpression.put(lastTime, -1)
        List lastTaskLog = timerLogsDB.find(expression).sort(sortExpression).limit(1).toArray()
        DBObject lastTask = lastTaskLog.isEmpty() ? null : lastTaskLog.get(0)
        if (lastTask && lastTask.get(eventName) != null) {
            Map event = lastTask.get(eventName) as Map
            if (event != null && event.get('lastTime') != null) {
                begin = event.get('lastTime') as Long
            }
        }
        map.put('begin', begin)
        map.put('end', end)
        return map
    }

    /**
     * 发送主播开播数据
     * @param eventName
     */
    def static void postOnlineStarData(String eventName) {
        Map<String, Long> map = getTaskTime(eventName)
        Long taskBegin = System.currentTimeMillis()
        Long begin = map.get('begin')
        Long end = map.get('end')
        BasicDBObject searchExpression = new BasicDBObject('timestamp': [$gte: begin, $lt: end], 'test': [$ne: true], 'temp': [$ne: true], 'live': true)
        BasicDBObject fieldExpression = new BasicDBObject('xy_star_id': 1, 'nick_name': 1, 'real_sex': 1, 'timestamp': 1, 'visiter_count': 1, 'bean': 1)
        BasicDBObject sortExpression = new BasicDBObject('timestamp': 1)
        DBCursor cursor = rooms.find(searchExpression, fieldExpression).sort(sortExpression).batchSize(5000)
        List list = new ArrayList(5000)
        println "-----[${eventName},共${cursor.size()} 条数据," +
                "${new Date(begin).format('yyyy-MM-dd HH:mm:ss')} --- ${new Date(System.currentTimeMillis()).format('yyyy-MM-dd HH:mm:ss')} ]-----"
        while (cursor.hasNext()) {
            def obj = cursor.next()
            Integer xyStarId = obj['xy_star_id']
            String nickName = obj['nick_name']
            Integer sex = obj['real_sex']
            Long timestamp = obj['timestamp']
            Long count = obj['visiter_count']
            Long bean = obj['bean']
            Map<String, Object> params = new HashMap<String, Object>()
            params.put('cmd', 'add');
            params.put('fields', ['itemid': xyStarId, 'nickName': nickName, 'sex': sex, 'timestamp': timestamp, 'visiterCount': count * 140, 'level': bean]);
            list.add(params)
            if (list.size() == 5000) {
                sendMessage(list, eventName, taskBegin, ITEM)
            }
        }
        sendMessage(list, eventName, taskBegin, ITEM)
        cursor.close()
    }

    /**
     * 发送主播关播数据
     * @param eventName
     */
    def static void postDownStarData(String eventName) {
        Map<String, Long> map = getTaskTime(eventName)
        Long taskBegin = System.currentTimeMillis()
        Long begin = map.get('begin')
        Long end = map.get('end')
        BasicDBObject searchExpression = new BasicDBObject('live_end_time': [$gte: begin, $lt: end], 'test': [$ne: true], 'temp': [$ne: true], 'live': false)
        BasicDBObject fieldExpression = new BasicDBObject('live_end_time': 1)
        BasicDBObject sortExpression = new BasicDBObject('live_end_time': 1)
        DBCursor cursor = rooms.find(searchExpression, fieldExpression).sort(sortExpression).batchSize(5000)
        List list = new ArrayList(5000)
        println "-----[${eventName},共${cursor.size()} 条数据," +
                "${new Date(begin).format('yyyy-MM-dd HH:mm:ss')} --- ${new Date(System.currentTimeMillis()).format('yyyy-MM-dd HH:mm:ss')} ]-----"
        while (cursor.hasNext()) {
            def obj = cursor.next()
            Integer xyStarId = obj['_id']
            Long timestamp = obj['live_end_time']
            Map<String, Object> params = new HashMap<String, Object>()
            params.put('cmd', 'delete');
            params.put('fields', ['itemid': xyStarId, 'timestamp': timestamp]);
            list.add(params)
            if (list.size() == 5000) {
                sendMessage(list, eventName, taskBegin, ITEM)
            }
        }
        sendMessage(list, eventName, taskBegin, ITEM)
        cursor.close()
    }

    /**
     * 发送主播数据
     */
    def static void postStarData(String eventName) {
        Map<String, Long> map = getTaskTime(eventName)
        Long taskBegin = System.currentTimeMillis()
        Long begin = map.get('begin')
        Long end = map.get('end')
        BasicDBObject searchExpression = new BasicDBObject('priv': 2, 'via': [$ne: 'robot'], 'timestamp': [$gte: begin, $lt: end], 'finance': [$ne: null])
        BasicDBObject fieldExpression = new BasicDBObject('_id': 1, 'nick_name': 1, 'finance': 1, 'timestamp': 1)
        BasicDBObject sortExpression = new BasicDBObject('timestamp': 1)
        DBCursor cursor = users.find(searchExpression, fieldExpression).sort(sortExpression).batchSize(5000)
        List list = new ArrayList(5000)
        println "-----[${eventName},共${cursor.size()} 条数据," +
                "${new Date(begin).format('yyyy-MM-dd HH:mm:ss')} --- ${new Date(System.currentTimeMillis()).format('yyyy-MM-dd HH:mm:ss')} ]-----"
        while (cursor.hasNext()) {
            def obj = cursor.next()
            Map finance = obj['finance'] as Map
            Long beanCountTotal = finance.get('bean_count_total') == null ? 0 : finance.get('bean_count_total')
            Map<String, Object> params = new HashMap<>()
            params.put('cmd', 'add');
            params.put('fields', ['itemid': obj['_id'], 'bean_count_total': beanCountTotal, 'title': obj['nick_name'], 'timestamp': obj['timestamp']]);
            list.add(params)
            if (list.size() == 5000) {
                sendMessage(list, eventName, taskBegin, ITEM)
            }
        }
        sendMessage(list, eventName, taskBegin, ITEM)
        cursor.close()
    }

    /**
     * 发送关注数据
     */
    def static void postFollowData(String eventName) {
        Map<String, Long> map = getTaskTime(eventName)
        Long taskBegin = System.currentTimeMillis()
        Long begin = map.get('begin')
        Long end = map.get('end')
        BasicDBObject searchExpression = new BasicDBObject('timestamp': [$gte: begin, $lt: end])
        BasicDBObject fieldExpression = new BasicDBObject('star_id': 1, 'user_id': 1, 'timestamp': 1)
        BasicDBObject sortExpression = new BasicDBObject('timestamp': 1)
        DBCursor cursor = followerLogs.find(searchExpression, fieldExpression).sort(sortExpression).batchSize(5000)
        List list = new ArrayList(5000)
        println "-----[${eventName},共${cursor.size()} 条数据," +
                "${new Date(begin).format('yyyy-MM-dd HH:mm:ss')} --- ${new Date(System.currentTimeMillis()).format('yyyy-MM-dd HH:mm:ss')} ]-----"
        while (cursor.hasNext()) {
            def obj = cursor.next()
            Map<String, Object> params = new HashMap<>()
            params.put('cmd', 'add');
            params.put('fields', ['actionid': obj['_id'], 'action_type': 'subscribe', 'itemid': obj['star_id'], 'userid': obj['user_id'], 'timestamp': obj['timestamp']]);
            list.add(params)
            if (list.size() == 5000) {
                sendMessage(list, eventName, taskBegin, USER_ACTION)
            }
        }
        sendMessage(list, eventName, taskBegin, USER_ACTION)
        cursor.close()
    }

    /**
     * 发送送礼数据
     */
    def static void postSendGiftData(String eventName) {
        Map<String, Long> map = getTaskTime(eventName)
        Long taskBegin = System.currentTimeMillis()
        Long begin = map.get('begin')
        Long end = map.get('end')
        BasicDBObject searchExpression = new BasicDBObject('type': 'send_gift', 'timestamp': [$gte: begin, $lt: end], 'session.data.xy_star_id': [$ne: null])
        BasicDBObject fieldExpression = new BasicDBObject('session': 1, 'data': 1, 'timestamp': 1)
        BasicDBObject sortExpression = new BasicDBObject('timestamp': 1)
        DBCursor cursor = room_cost.find(searchExpression, fieldExpression).sort(sortExpression).batchSize(5000)
        List list = new ArrayList(5000)
        println "-----[${eventName},共${cursor.size()} 条数据," +
                "${new Date(begin).format('yyyy-MM-dd HH:mm:ss')} --- ${new Date(System.currentTimeMillis()).format('yyyy-MM-dd HH:mm:ss')} ]-----"
        while (cursor.hasNext()) {
            def obj = cursor.next()
            Map session = obj['session'] as Map
            Map data = session['data'] as Map
            Map<String, Object> properties = new HashMap<>()
            properties.put('cmd', 'add');
            properties.put('fields', ['actionid' : obj['_id'], 'action_type': 'gift', 'userid': session.get('_id'), 'itemid': data.get('xy_star_id'),
                                      'timestamp': obj['timestamp'], 'giftId': data.get('_id'), 'giftName': data.get('name'),
                                      'giftCount': data.get('count')]);
            list.add(properties)
            if (list.size() == 5000) {
                sendMessage(list, eventName, taskBegin, USER_ACTION)
            }
        }
        sendMessage(list, eventName, taskBegin, USER_ACTION)
        cursor.close()
    }

    /**
     * 发送赠送么么哒数据
     */
    def static void postSendMeMeData(String eventName) {
        Map<String, Long> map = getTaskTime(eventName)
        Long taskBegin = System.currentTimeMillis()
        Long begin = map.get('begin')
        Long end = map.get('end')
        BasicDBObject searchExpression = new BasicDBObject('type': 'send_meme', 'timestamp': [$gte: begin, $lt: end],'session._id':[$ne:null])
        BasicDBObject fieldExpression = new BasicDBObject('session': 1, 'user_id': 1, 'timestamp': 1,'room':1)
        BasicDBObject sortExpression = new BasicDBObject('timestamp': 1)
        DBCursor cursor = room_feather.find(searchExpression, fieldExpression).sort(sortExpression).batchSize(5000)
        List list = new ArrayList(5000)
        println "-----[${eventName},共${cursor.size()} 条数据," +
                "${new Date(begin).format('yyyy-MM-dd HH:mm:ss')} --- ${new Date(System.currentTimeMillis()).format('yyyy-MM-dd HH:mm:ss')} ]-----"
        while (cursor.hasNext()) {
            def obj = cursor.next()
            Map session = obj['session'] as Map
            Map<String, Object> params = new HashMap<>()
            params.put('cmd', 'add');
            params.put('fields', ['actionid': obj['_id'], 'action_type': 'sendMM', 'itemid': obj['room'], 'userid': session['_id'], 'timestamp': obj['timestamp']]);
            list.add(params)
            if (list.size() == 5000) {
                sendMessage(list, eventName, taskBegin, USER_ACTION)
            }
        }
        sendMessage(list, eventName, taskBegin, USER_ACTION)
        cursor.close()
    }

    /**
     * 5000一条发送
     * @param list
     * @param eventName
     * @param taskBegin
     */
    private static void sendMessage(List list, String eventName, Long taskBegin, String eventKey) {
        if (!list.isEmpty()) {
            Long timestamp = 0
            Map tmp = list.get(list.size() - 1)
            Map obj = tmp.get('fields') as Map
            timestamp = obj.get('timestamp')
            postData(eventKey, JsonOutput.toJson(list), timestamp, eventName)
            list.clear()
        }
    }

    /**
     * 传输数据
     * @param tablename
     * @param table_content
     * @return
     * @throws ClientProtocolException
     * @throws IOException
     */
    public
    static void postData(String tableName, String tableContent, Long taskBegin, String eventName) {
        if (tableContent == null || tableContent == '[]') {
            println "—————— ${eventName} —————— 在这个时间范围内没有数据,不发送不记日志"
            return
        }
        String responseContent = "";
        Map params = new HashMap()
        params.put('appid', APP_ID)
        params.put('table_name', tableName)
        params.put('table_content', tableContent)
        HttpPost httpPost = new HttpPost(POST_URL);
        if (params != null) {
            List<NameValuePair> nvps = new ArrayList<NameValuePair>();
            Set<Entry<String, String>> paramEntrys = params.entrySet();
            for (Entry<String, String> entry : paramEntrys) {
                nvps.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
            }
            httpPost.setEntity(new UrlEncodedFormEntity(nvps, "utf-8"));
        }

        httpPost.setHeader("User-Agent", "datagrand/datareport/java sdk v1.0.0");
        httpPost.setHeader("Content-Type", "application/x-www-form-urlencoded");

        HttpClient httpClient = getHttpClient()

        HttpResponse response = httpClient.execute(httpPost);
        StatusLine status = response.getStatusLine();
        if (status.getStatusCode() >= HttpStatus.SC_MULTIPLE_CHOICES) {
            System.out.printf("Did not receive successful HTTP response: status code = {}, status message = {}", status.getStatusCode(), status.getReasonPhrase());
            httpPost.abort();
        }

        HttpEntity entity = response.getEntity();
        if (entity != null) {
            responseContent = EntityUtils.toString(entity, "utf-8");
            def jsonSlurper = new JsonSlurper()
            def result = jsonSlurper.parseText(responseContent)
            println 'response is ' + result
            println 'taskBegin is ' + taskBegin
            jobFinish(taskBegin, result, eventName)
            EntityUtils.consume(entity);
        } else {
            System.out.printf("Http entity is null! request url is {},response status is {}", POST_URL, response.getStatusLine());
        }
        httpClient.getConnectionManager().shutdown()
    }

    /**
     * 标记任务完成  用于运维监控
     * @return
     */
    private static jobFinish(Long taskBegin, Object result, String eventName) {
        def timerName = 'Recommendation'
        Long totalCost = System.currentTimeMillis() - taskBegin
        saveTimerLogs(timerName, totalCost, result, eventName)
    }

    //落地定时执行的日志
    private static saveTimerLogs(String timerName, Long totalCost, Object result, String eventName) {
        def timerLogsDB = mongo.getDB("xyrank").getCollection("timer_logs")
        def tmp = System.currentTimeMillis()
        def id = timerName + "_" + new Date().format("yyyyMMdd")
        String requestId = eventName + '.requestId'
        String field = eventName + '.totalCost'
        String status = eventName + '.status'
        String lastTime = eventName + '.lastTime'
        String errorMsg = eventName + '.errorMsg'

        BasicDBObject eventExpression = new BasicDBObject(timer_name: timerName, cat: 'day', unit: 'ms', timestamp: tmp)
        eventExpression.append(field, totalCost).append(requestId, result.request_id).append(status, result.status).append(errorMsg, result.errors)
        if (result.errors == null) {
            eventExpression.append(lastTime, tmp)
        }
        timerLogsDB.findAndModify(new BasicDBObject('_id', id), null, null, false, new BasicDBObject('$set', eventExpression), true, true)
    }

    /**
     * 最后更新执行时间
     * @param totalCost
     * @return
     */
    private static updateTimerLogs(Long totalCost) {
        def timerLogsDB = mongo.getDB("xyrank").getCollection("timer_logs")
        def id = 'Recommendation' + "_" + new Date().format("yyyyMMdd")
        def tmp = System.currentTimeMillis()
        BasicDBObject eventExpression = new BasicDBObject(cost_total: totalCost, timestamp: tmp)
        timerLogsDB.findAndModify(new BasicDBObject('_id', id), null, null, false, new BasicDBObject('$set', eventExpression), true, true)
    }

    /**
     * 获取所有推荐接口
     */
    def static void getRecommendStar(Integer size, Integer userId) {
        String url = GET_RECOMMEND_STAR + "?cnt=" + size + '&userid=' + userId
        HttpGet httpGet = new HttpGet(url)
        HttpResponse response = httpClient.execute(httpGet);
        StatusLine status = response.getStatusLine();
        if (status.getStatusCode() == HttpStatus.SC_OK) {
            HttpEntity entity = response.getEntity();
            String responseContent = EntityUtils.toString(entity, "utf-8");
            println 'response content is ' + responseContent
            EntityUtils.consume(entity);
        }
    }

    /**
     * 插入测试数据
     */
    def static saveTestData() {
        Integer userId = 3000000
        while (600000-- > 0) {
            Long now = System.currentTimeMillis()
            BasicDBObject expression = new BasicDBObject()
            expression.put('isTest', 'test')
            expression.put('_id', userId + '_' + now)
            expression.put('type', 'send_gift')
            expression.put('session', [_id: userId,data:[xy_star_id: userId, name: '测试礼物_' + userId, _id: userId - 299999, count: 20]])
            expression.put('data', [xy_star_id: userId, name: '测试礼物_' + userId, _id: userId - 299999, count: 20])
            expression.put('timestamp', now)
            room_cost.save(expression)
        }
    }

    /**
     * 获取httpClient
     * @return
     */
    def static HttpClient getHttpClient() {
        HttpClient httpClient = new DefaultHttpClient();
        HttpParams httpParams = httpClient.getParams();
        HttpConnectionParams.setSoTimeout(httpParams, 10 * 1000);
        HttpConnectionParams.setConnectionTimeout(httpParams, 10 * 1000);
        return httpClient
    }
}