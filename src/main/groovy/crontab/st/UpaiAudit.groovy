#!/usr/bin/env groovy
package crontab.st

@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
        @Grab('com.ttpod:ttpod-rest:1.0.4')
])
import com.mongodb.BasicDBObject
import com.mongodb.DBObject
import com.mongodb.Mongo
import com.mongodb.MongoURI
import com.ttpod.rest.common.util.http.HttpClientUtil
import com.ttpod.rest.common.util.http.HttpEntityHandler
import groovy.json.JsonSlurper
import org.apache.commons.lang.StringUtils
import org.apache.http.Header
import org.apache.http.HttpEntity
import org.apache.http.HttpResponse
import org.apache.http.HttpStatus
import org.apache.http.client.methods.HttpDelete
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpRequestBase
import org.apache.http.entity.ContentType
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.util.EntityUtils
import org.apache.http.client.HttpClient
import redis.clients.jedis.Jedis

import java.nio.charset.Charset

/**
 *  家族排行发放奖励，每晚8:30
 */
class UpaiAudit {
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

    private static final String IMG_API_URL =  "http://mon.memeyule.com/monapi/image/check"
    private static final String ILLAGE_CODE = "1"
    public static final String AUTHORIZATION = 'bGFpaG91OmxhaWhvdTEyMw==' //Base64(operator:password)
    public static final HttpClient httpClient = new DefaultHttpClient()
    public static final String URI = 'http://v0.api.upyun.com/laihou-chat/'
    public static final String NEXT_PAGE_KEY = 'x-upyun-list-iter'
    public static final String FINAL_PAGE_FLAG = 'g2gCZAAEbmV4dGQAA2VvZg'
    static final String jedis_host = getProperties("main_jedis_host", "192.168.31.236")
    static final Integer main_jedis_port = getProperties("main_jedis_port", 6379) as Integer
    static redis = new Jedis(jedis_host, main_jedis_port)

    static audit_pic() {
        def start = new Date().getTime() - 7 * 60 * 1000
        def end = new Date().getTime() - 1 * 60 * 1000
        def startMonth = new Date(start).format('yyyy_M')
        def endMonth = new Date(end).format('yyyy_M')
        def month = []
        def query = []
        if (startMonth == endMonth) {
            month.add(startMonth)
            query.add($$(action: 'room.chat.pub_pic', is_audit: [$ne: true], 'data.ts': [$gte: start, $lt: end]))
        } else {
            month.add(startMonth)
            query.add($$(action: 'room.chat.pub_pic', is_audit: [$ne: true], 'data.ts': [$gte: start, $lt: getFirstMonthDay(end)]))
            month.add(endMonth)
            query.add($$(action: 'room.chat.pub_pic', is_audit: [$ne: true], 'data.ts': [$gte: start, $lt: end]))
        }
        new Thread(new Runnable() {
            @Override
            void run() {
                int i = 0
                month.each {
                    def collection = mongo.getDB('chat_log').getCollection("${it}_room_publish")
                    collection.find(query.get(i++) as DBObject).each {BasicDBObject obj->
                        boolean is_audit = Boolean.TRUE
                        if (obj['data'] != null) {
                            def url = obj['data']['url'] as String
                            if (url != null && url.isNotEmpty()) {
                                if (identifyIsIllegalPic(url)) { //删除又拍云上的图片
                                    url = url.replaceAll(/^https?:\/\/[^\/]*\//, '')
                                    String response = deletePic("${URI}${url}".toString())
                                    if (response == null || response.trim() == '0') {
                                        is_audit = Boolean.FALSE
                                    }
                                }
                            }
                        }
                        if (is_audit) {
                            //TODO collection.update($$(_id: obj['_id']), $$($set: [is_audit: true]), false, false)
                        }
                        println 'is_audit:' + is_audit + ' obj:' + obj
                    }
                }
            }
        }).start()
    }

    static empty_folder() {
        def ym = new Date(zeroMill - 4 * DAY_MILLON).format('yyyyMMdd')
        String key = "laihou-chat:folder:uploads:${ym}"
        if (StringUtils.isNotBlank(redis.get(key))) {
            return
        }
        redis.set(key, '1')
        redis.expireAt(key, zeroMill - 6 * DAY_MILLON)

        new Thread(new Runnable() {
            @Override
            void run() {
                boolean deleteAll = true
                String xListLimit = null
                String folderPath = "${URI}uploads/${ym}".toString()
                while(true) {
                    HttpGet httpGetFolder = new HttpGet(folderPath)
                    def params = ['x-list-limit': '100']
                    if (xListLimit != null) {
                        params.put('x-list-iter', xListLimit)
                    }
                    params.put('Authorization', 'Basic ' + AUTHORIZATION)
                    Response response = doRequest(httpClient, httpGetFolder, params)
                    //处理content
                    if (StringUtils.isNotBlank(response.content)) {
                        response.content.split('\n').each {String line ->
                            String[] item = line.split('\t')
                            if (item.size() >=2 && 'N' == item[1]) { //F : folder
                                // TODO
                                println 'do delete pic:' + "${folderPath}${item[0]}".toString()
                                /*String res = deletePic("${folderPath}${item[0]}".toString())
                                if (res == null || res.trim() != '0') {
                                    println 'delete failed. path:' + "${folderPath}${item[0]}".toString()
                                    deleteAll = false
                                }*/
                            }
                        }
                    }
                    if (response == null || response.getHeader(NEXT_PAGE_KEY) == null) {
                        println 'some exception happened unwanted.'
                        deleteAll = false
                        break
                    }
                    xListLimit = response.getHeader(NEXT_PAGE_KEY)
                    println 'next page:' + xListLimit
                    if (xListLimit.equals(FINAL_PAGE_FLAG)) {
                        break
                    }
                }
                if (deleteAll) {
                    println 'del folder' //TODO
                    /*String response = deletePic(folderPath)
                    if (response != null && response.trim() == '0') {
                        return
                    }*/
                }
                redis.del(key)
            }
        }).start()
    }

    /**
     * 是否为非法图片
     * @param pic_url
     * @return
     */
    static Boolean identifyIsIllegalPic(String pic_url){
        try{
            String resp = request(IMG_API_URL+"?url="+pic_url, null)
            def json = new JsonSlurper().parseText(resp) as Map
            String data = json['data'] as String
            if(ILLAGE_CODE.equals(data)){
                return Boolean.TRUE
            }
        }catch (Exception e){
            println "identifyIsIllegalPic Exception : " + e
        }
        return Boolean.FALSE
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
        audit_pic()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  audit_pic cost  ${System.currentTimeMillis() - l} ms"
        l = System.currentTimeMillis()
        empty_folder()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  empty_folder cost  ${System.currentTimeMillis() - l} ms"

        jobFinish(begin)
    }

    private static long getFirstMonthDay(long time) {
        Calendar cal = Calendar.getInstance()//获取当前日期
        cal.setTimeInMillis(time)
        cal.set(Calendar.DAY_OF_MONTH, 1)//设置为1号,当前日期既为本月第一天
        cal.set(Calendar.HOUR_OF_DAY, 0)
        cal.set(Calendar.MINUTE, 0)
        cal.set(Calendar.SECOND, 0)
        cal.set(Calendar.MILLISECOND, 0)
        return cal.getTimeInMillis()
    }

    static String deletePic(String url) {
        HttpDelete httpDelete = new HttpDelete(url)
        def params = [:]
        params.put('Authorization', 'Basic ' + AUTHORIZATION)
        Response response = doRequest(httpClient, httpDelete, params)
        if (response != null) {
            return response.content
        }
        return null
    }

    static class Response {
        String content
        Header[] headers

        String getHeader(String name) {
            if (headers == null || headers.size() <= 0) {
                return null
            }
            for(Header header : headers) {
                if (header.name == name) {
                    return header.value
                }
            }
            return null
        }
    }

    static Response doRequest(HttpClient httpClient, HttpRequestBase request, Map<String, String> params) {
        final String forceCharset = 'UTF-8'
        return HttpClientUtil.http(httpClient, request, params, new HttpEntityHandler<Response>() {

            @Override
            Response handleResponse(HttpResponse response) throws IOException {
                def code = response.getStatusLine().getStatusCode()
                if(code != HttpStatus.SC_OK){
                    return null
                }
                Response resp = handle(response.getEntity())
                resp.headers = response.getAllHeaders()
                return resp
            }

            @Override
            Response handle(HttpEntity entity) throws IOException {
                byte[] content = EntityUtils.toByteArray(entity)
                if(forceCharset != null){
                    Response resp = new Response()
                    resp.content = new String(content,forceCharset)
                    return resp
                }
                String html
                Charset charset =null
                ContentType contentType = ContentType.get(entity)
                if(contentType !=null){
                    charset = contentType.getCharset()
                }
                if(charset ==null){
                    charset = Charset.forName("GB18030")
                }
                html = new String(content,charset)
                Response resp = new Response()
                resp.content = html
                return resp
            }

            @Override
            String getName() {
                return null
            }
        })
    }

    static String request(String url, String params) {
        println params
        def conn = null
        InputStream is = null
        def jsonText = ""
        try {
            conn = (HttpURLConnection) new URL(url).openConnection()
            def respCode = conn.getResponseCode()
            println "respCode: ${respCode}"
            if (200 == respCode) {
                def buffer = new StringBuffer()
                is = conn.getInputStream()
                is.eachLine('UTF-8') { buffer.append(it) }
                jsonText = buffer.toString()
            }
        } catch (Exception e) {
            println("error" + e)
        } finally {
            if (conn != null) {
                conn.disconnect()
            }
            if (is != null) {
                is.close()
            }
        }
        println jsonText
        return jsonText
    }

    /**
     * 标记任务完成  用于运维监控
     * @return
     */
    private static jobFinish(Long begin) {
        def timerName = 'LiveStat'
        Long totalCost = System.currentTimeMillis() - begin
        saveTimerLogs(timerName, totalCost)
        println "${new Date().format('yyyy-MM-dd')}:${Audit.class.getSimpleName()}:finish  cost  ${System.currentTimeMillis() - begin} ms"
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