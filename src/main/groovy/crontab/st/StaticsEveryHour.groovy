#!/usr/bin/env groovy
package crontab.st

import com.mongodb.BasicDBObject
import com.mongodb.DBCollection
import com.mongodb.DBObject
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
        @Grab('org.apache.httpcomponents:httpclient:4.2.5')
])

import com.mongodb.Mongo
import com.mongodb.MongoURI
import groovy.json.JsonSlurper
import org.apache.commons.lang.StringUtils
import org.apache.http.HttpEntity
import org.apache.http.HttpResponse
import org.apache.http.HttpStatus
import org.apache.http.StatusLine
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.params.HttpConnectionParams
import org.apache.http.params.HttpParams
import org.apache.http.util.EntityUtils

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/**
 * 每小时统计一份数据
 */
class StaticsEveryHour {

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

    static mongo = new Mongo(new MongoURI(getProperties('mongo.uri', 'mongodb://192.168.2.27:10000?w=1') as String))


    static HOUR_MILLION = 3600 * 1000L
    static DAY_MILLON = 24 * HOUR_MILLION

    static long zeroMill = new Date().clearTime().getTime()
    static Long yesTday = zeroMill - DAY_MILLON
    static String YMD = new Date(yesTday).format("yyyyMMdd")

    static DBCollection users = mongo.getDB('xy').getCollection('users')
    static DBCollection invitor_logs = mongo.getDB('xylog').getCollection('invitor_logs')
    static DBCollection stat_hourly = mongo.getDB('xy_admin').getCollection('stat_hourly')
    /**
     * 每小时监控邀请数据
     * @param i
     * @return
     */
    static inviteStatics(int i) {
        // 当前时间的前i + 1个小时
        def begin = startTime(-1 - i)
        def end = begin + HOUR_MILLION
        Calendar cal = Calendar.getInstance()
        cal.setTimeInMillis(begin)
        int hour = cal.get(Calendar.HOUR_OF_DAY)

        //一小时内分享获得的用户
        def ids = users.distinct('_id', $$(timestamp: [$gte: begin, $lt: end], qd: [$in: ['wawa_share_lianjie', 'wawa_share_erweima']]))
        //30分钟后这些用户分享获得的用户
        def invitors = [] as Set
        def invitees = 0
        invitor_logs.find($$(timestamp: [$gte: end, $lt: end + 30 * 60000L], invitor: [$in: ids])).toArray().each {BasicDBObject obj->
            if (obj['invitor'] != null) {
                invitors.add(obj['invitor'] as Integer)
            }
            invitees = invitees + 1
        }

        def _id = "${new Date(begin).format('yyyyMMddHH')}_share_users".toString()

        def update = $$(type: 'share_users', timestamp: begin, hour_of_day: hour, share_users: ids?.size() ?: 0, invitors: invitors.size(), invitees: invitees)

        stat_hourly.update($$(_id: _id), $$($set: update), true, false)
    }

    static startTime(int i) {
        Calendar cal = Calendar.getInstance()
        cal.clear(Calendar.MILLISECOND)
        if (i != 0) {
            cal.add(Calendar.HOUR_OF_DAY, i)
        }
        cal.set(Calendar.MINUTE, 0)
        cal.set(Calendar.SECOND, 0)
        cal.set(Calendar.MILLISECOND, 0)
        cal.getTime().getTime()
    }



    static Integer STAGE = 0

    static void main(String[] args) {
        long l = System.currentTimeMillis()
        long begin = l

        //查询每个小时的邀请统计
        l = System.currentTimeMillis()
        inviteStatics(STAGE)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   inviteStatics, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

        //落地定时执行的日志
        jobFinish(begin)
    }

    static String request(String url) {
        HttpURLConnection conn = null
        def jsonText = ""
        try {
            conn = (HttpURLConnection) new URL(url).openConnection()
            conn.setRequestMethod("GET")
            conn.setDoOutput(true)
            conn.setConnectTimeout(3000)
            conn.setReadTimeout(3000)
            conn.connect()
            jsonText = conn.getInputStream().getText("UTF-8")

        } catch (Exception e) {
            println "request Exception : " + e
        } finally {
            if (conn != null) {
                conn.disconnect()
            }
        }
        return jsonText
    }

    /**
     * 标记任务完成  用于运维监控
     * @return
     */
    private static jobFinish(Long begin) {
        def timerName = 'StaticsEveryHour'
        Long totalCost = System.currentTimeMillis() - begin
        saveTimerLogs(timerName, totalCost)
        println "${new Date().format('yyyy-MM-dd')}:${StaticsEveryHour.class.getSimpleName()}:finish  cost  ${System.currentTimeMillis() - begin} ms"
    }

    //落地定时执行的日志
    private static saveTimerLogs(String timerName, Long totalCost) {
        def timerLogsDB = mongo.getDB("xyrank").getCollection("timer_logs")
        def tmp = System.currentTimeMillis()
        def id = timerName + "_" + new Date().format("yyyyMMdd")
        def update = new BasicDBObject(timer_name: timerName, cost_total: totalCost, cat: 'day', unit: 'ms', timestamp: tmp)
        timerLogsDB.findAndModify(new BasicDBObject('_id', id), null, null, false, new BasicDBObject('$set', update), true, true)
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

    public static Object getData(String url, Map header) {
        String responseContent = ""
        HttpGet httpGet = new HttpGet(url)
        HttpClient httpClient = getHttpClient()

        HttpResponse response = httpClient.execute(httpGet)
        StatusLine status = response.getStatusLine()
        if (status.getStatusCode() >= HttpStatus.SC_MULTIPLE_CHOICES) {
            System.out.printf("Did not receive successful HTTP response: status code = {}, status message = {}", status.getStatusCode(), status.getReasonPhrase())
            httpGet.abort()
        }

        HttpEntity entity = response.getEntity()
        if (entity != null) {
            responseContent = EntityUtils.toString(entity, "utf-8")
            def jsonSlurper = new JsonSlurper()
            def result = jsonSlurper.parseText(responseContent)
            EntityUtils.consume(entity)
            return result
        } else {
            System.out.printf("Http entity is null! request url is {},response status is {}", url, response.getStatusLine())
        }
        httpClient.getConnectionManager().shutdown()
    }

    static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    static BasicDBObject $$(Map map) {
        return new BasicDBObject(map)
    }

}