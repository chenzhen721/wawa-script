#!/usr/bin/env groovy
package crontab.st

import com.mongodb.MongoURI
import groovy.json.JsonOutput
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
 *  家族成员贡献奖励发放
 *  每个小时发放一次
 */
class FamilyContribution {
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

    static final String WS_DOMAIN = getProperties('ws.domain', 'http://test-aiws.memeyule.com:6010')

    static DAY_MILLON = 24 * 3600 * 1000L
    static HOUR_MILLON = 3600 * 1000L

    static long zeroMill = new Date().clearTime().getTime()
    static members = mongo.getDB('xy_family').getCollection('members')
    static Long VALID_DAY = 7200

    //1:可发放, 2:待领取, 3:已领取
    static renewRewardStatus(){
        Long next_time = (System.currentTimeMillis() + VALID_DAY) - 5000
        members.updateMulti($$(award_status:3,next_time:[$lt:System.currentTimeMillis()]), $$($set:[award_status:1, next_time:next_time ]))
        members.updateMulti($$(award_status:null), $$($set:[award_status:1]))
        members.updateMulti($$(next_time:null), $$($set:[next_time:next_time]))
        //推送到直播间
    }
    private static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    private static BasicDBObject $$(Map map) {
        return new BasicDBObject(map);
    }

    static void main(String[] args) {
        long l = System.currentTimeMillis()
        long begin = l
        renewRewardStatus();
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  renewRewardStatus cost  ${System.currentTimeMillis() - l} ms"

        jobFinish(begin)
    }

    private static void publish(Map params) {
        String url = getPublishUrl();
        String content = JsonOutput.toJson(params)
        request_post(url, content)
    }


    private static String getPublishUrl() {
        return String.format("%s%s", WS_DOMAIN, "/api/publish/global/");
    }

    static String request_post(String url, String params) {
        HttpURLConnection conn = null;
        PrintWriter pw = null;
        BufferedReader br = null;
        def jsonText = "";
        try {
            conn = (HttpURLConnection) new URL(url).openConnection()
            conn.setRequestMethod("POST")
            conn.setDoOutput(true)
            conn.setConnectTimeout(5000);
            conn.setReadTimeout(5000);
            conn.setDoOutput(true);
            conn.setDoInput(true);
            pw = new PrintWriter(conn.getOutputStream());
            pw.print(params);
            pw.flush();
            br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String line;
            while ((line = br.readLine()) != null) {
                jsonText += line;
            }
        } catch (Exception e) {
            println("发送 POST 请求出现异常！" + e);
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
            if (pw != null) {
                pw.close()
            }
            if (br != null) {
                br.close()
            }
        }
        return jsonText;
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