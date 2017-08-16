#!/usr/bin/env groovy
package crontab.st

@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
])
import com.mongodb.BasicDBObject
import com.mongodb.Mongo
import com.mongodb.MongoURI
import com.mongodb.util.JSON
import groovy.json.JsonOutput
import redis.clients.jedis.Jedis

/**
 *  家族排行发放奖励，每晚8:30
 */
class FamilyRank {
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
    static final String api_domain = getProperties("api.domain", "http://test-aiapi.memeyule.com/")

    static family_rank() {
        def api_url = api_domain + "job/family_rank_snapshot".toString()
        def params = [key: '53cae7e7224a08885a7153058122647e']
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')} result : ${request_post(api_url, JSON.serialize(params))}"
    }

    private static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value)
    }

    private static BasicDBObject $$(Map map) {
        return new BasicDBObject(map)
    }

    static void main(String[] args) {
        long l = System.currentTimeMillis()
        long begin = l
        family_rank()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  reCaculateValue cost  ${System.currentTimeMillis() - l} ms"

        jobFinish(begin)
    }

    static String request_post(String url, String params) {
        println params
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
        println "${new Date().format('yyyy-MM-dd')}:${FamilyRank.class.getSimpleName()}:finish  cost  ${System.currentTimeMillis() - begin} ms"
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