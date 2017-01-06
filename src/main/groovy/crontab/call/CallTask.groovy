#!/usr/bin/env groovy
package crontab.st

import com.https.HttpsUtil
import com.mongodb.BasicDBObject
@GrabResolver(name = 'restlet', root = 'http://192.168.31.253:8081//nexus/content/groups/public')
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('org.apache.httpcomponents:httpclient:4.2.5'),
        @Grab('redis.clients:jedis:2.1.0'),
        @Grab('com.ttpod:https-util:1.0')
])
import com.mongodb.DBCollection
import com.mongodb.Mongo
import com.mongodb.MongoURI
import org.apache.commons.lang.StringUtils
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.params.HttpConnectionParams
import org.apache.http.params.HttpParams

/**
 * 扫描未确认的订单并且自动确认
 */
class CallTask {

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
    static DBCollection call_orders = mongo.getDB('xy_call').getCollection('orders')
    static DBCollection call_stars = mongo.getDB('xy_call').getCollection('stars')
    static DBCollection timeLogs = mongo.getDB('xyrank').getCollection('timer_logs')

    static Integer CALL_STATUS = 2
    static Integer CONFIRM_STATUS = 3
    static Integer CONFIRM_TYPE = 2
    static final Integer LIMIT = 500
    static final Long FIVE_DAY_MILLION = 60 * 60 * 24 * 5 * 1000L
    static final String TIMER_NAME = 'CallTask'
    static Integer PAY_STATUS = 1
    static final Long TEN_MINUTE_MILLION = 60 * 10 * 1000L
    static final String MSG = '亲爱的甜心,您有一单叫醒任务需要在10分钟后执行哦!用你独特的嗓音去唤醒Ta吧'
    private static final String ACCOUNT = "VIP-meme21";
    private static final String PSWD = "Txb123456";
    private static String API_URL = "http://222.73.117.156/msg/HttpBatchSendSM" + "?account=" + ACCOUNT + "&pswd=" + PSWD;

    static void main(String[] args) {
        Long timestamp = System.currentTimeMillis()
        autoUpdateOrder(timestamp)
        messageTips(timestamp)
        jobFinish(timestamp)
    }

    /**
     * 扫描已拨打电话未确认的订单
     * 超过5天未确认的订单自动确认
     * 确认后需要入账
     */
    static void autoUpdateOrder(Long timestamp) {
        println("${CallTask.class.getSimpleName()} --- ${new Date().format('yyyy-MM-dd hh:mm:ss')} --- begin scan orders ...")
        // 查询已拨打，并且超过5天用户还没有确认的订单
        def query = $$('status': CALL_STATUS, 'execute_time': [$lte: (timestamp - FIVE_DAY_MILLION)])
        def field = $$('_id': 1, 'execute_time': 1, 'amount': 1, 'star': 1)
        def sort = $$('execute_time': 1)
        def cursor = call_orders.find(query, field).sort(sort).limit(LIMIT)
        def success = 0
        def error = 0
        println("cursor size is ${cursor.size()}")

        while (cursor.hasNext()) {
            def obj = cursor.next()
            def id = obj['_id'] as String // 订单id
            def amount = obj['amount'] as Long // 订单金额
            def star = obj['star'] as Map
            def now = System.currentTimeMillis()
            def order_query = $$('_id', id)
            def update = $$('$set': $$('last_modify_time': now, 'confirm_time': now, 'confirm_type': CONFIRM_TYPE, 'status': CONFIRM_STATUS))
            if (star == null) {
                continue
            }

            // 确认成功后需要增加主播的总金额和可提现金额
            call_orders.update(order_query, update)
            def starId = star['_id'] as Integer
            def star_query = $$('_id', starId)
            def star_update_query = $$('$inc': ['finance.total_amount': amount, 'finance.valid_amount': amount])
            if (!call_stars.update(star_query, star_update_query).getN()) {
                println("${CallTask.class.getSimpleName()} has been an error , orderId is ${id} modify stars collections column amount is failure ,but order status is updated")
                error += 1
                continue
            }
            success += 1
        }

        println("${CallTask.class.getSimpleName()} --- ${new Date().format('yyyy-MM-dd hh:mm:ss')} --- autoUpdateOrder done,total is ${cursor.size()} ,success is ${success}, failure is ${error} ")
    }

    /**
     * 发送短信提醒甜心有订单即将来临
     */
    static void messageTips(Long timestamp) {
        println("${CallTask.class.getSimpleName()} --- ${new Date().format('yyyy-MM-dd hh:mm:ss')} --- begin scan messageTips task ...")

        def query = $$('status': PAY_STATUS, 'send_time': null, 'call_time': [$gt: timestamp, $lte: (timestamp + TEN_MINUTE_MILLION)])
        def field = $$('_id': 1, 'call_time': 1, 'star': 1)
        def sort = $$('call_time': 1)
        def cursor = call_orders.find(query, field).sort(sort).limit(LIMIT)
        println("cursor size is ${cursor.size()}")

        Set orderIds = new HashSet()
        Set mobiles = new HashSet()
        while (cursor.hasNext()) {
            def obj = cursor.next()
            def star = obj['star'] as Map
            if (star == null) {
                continue
            }
            def id = obj['_id']
            def starId = star['_id'] ? star['_id'] as Integer : 0
            def starMobile = star['mobile'] ? star['mobile'] as String : null
            if (StringUtils.isBlank(starMobile) || starId == 0) {
                continue
            }
            // 避免同一个主播收到多条短信
            mobiles.add(starMobile)
            orderIds.add(id)
        }

        // 构建发送号码
        String sendMobile = ''
        mobiles.each { it ->
            sendMobile += it + ','
        }
        if (StringUtils.isNotBlank(sendMobile)) {
            String mobile = sendMobile.substring(0, sendMobile.length() - 1)
            println("mobile params is ${mobile}")
            tipsAndUpdate(mobile, orderIds)
        }

        println("${CallTask.class.getSimpleName()} --- ${new Date().format('yyyy-MM-dd hh:mm:ss')} --- messageTips done")
    }

    /**
     *
     * @param msg
     * @return
     */
    private static void tipsAndUpdate(String mobile, Set orderIds) {
        String requestUrl = API_URL + '&mobile=' + mobile + '&needstatus=false&msg=' + MSG
        println("requestUrl is ${requestUrl}")
        HttpGet httpGet = new HttpGet(requestUrl);
        HttpClient httpClient = getHttpClient()
        httpClient.execute(httpGet);
        def timestamp = System.currentTimeMillis()
        def query = $$('_id': [$in: orderIds])
        def update_query = $$('$set': $$('last_modify_time': timestamp, 'send_time': timestamp))
        call_orders.update(query, update_query, false, true)
    }

    /**
     * 任务完成
     * @param begin
     * @return
     */
    private static jobFinish(Long begin) {
        Long now = System.currentTimeMillis()
        Long totalCost = now - begin
        def id = TIMER_NAME + "_" + new Date().format("yyyyMMdd")
        def query = $$('_id', id)
        def update_query = $$('$set': $$(timer_name: TIMER_NAME, cost_total: totalCost, cat: 'day', unit: 'ms', timestamp: now))
        timeLogs.update(query, update_query, true, false);
        println("${new Date().format('yyyy-MM-dd HH:mm:ss')}:${CallTask.class.getSimpleName()}:finish cost ${totalCost} ms")
    }

    public static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    public static BasicDBObject $$(Map map) {
        return new BasicDBObject(map);
    }

    public static HttpClient getHttpClient() {
        HttpClient httpClient = new DefaultHttpClient();
        HttpParams httpParams = httpClient.getParams();
        HttpConnectionParams.setSoTimeout(httpParams, 10 * 1000);
        HttpConnectionParams.setConnectionTimeout(httpParams, 10 * 1000);
        return httpClient
    }
}