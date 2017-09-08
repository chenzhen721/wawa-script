#!/usr/bin/env groovy
package crontab.st

@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
        @Grab('org.apache.httpcomponents:httpclient:4.2.3'),
        @Grab('org.apache.httpcomponents:httpcore:4.2.2'),
])
import com.mongodb.BasicDBObject
import com.mongodb.DBCollection
import com.mongodb.DBObject
import com.mongodb.Mongo
import com.mongodb.MongoURI
import org.apache.http.Header
import org.apache.http.HttpEntity
import org.apache.http.HttpResponse
import org.apache.http.client.HttpClient
import org.apache.http.client.ResponseHandler
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpRequestBase
import org.apache.http.client.params.ClientPNames
import org.apache.http.client.params.CookiePolicy
import org.apache.http.conn.ConnectTimeoutException
import org.apache.http.conn.scheme.Scheme
import org.apache.http.conn.scheme.SchemeRegistry
import org.apache.http.conn.ssl.SSLSocketFactory
import org.apache.http.entity.ContentType
import org.apache.http.impl.client.DecompressingHttpClient
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.impl.conn.PoolingClientConnectionManager
import org.apache.http.params.BasicHttpParams
import org.apache.http.params.CoreConnectionPNames
import org.apache.http.params.CoreProtocolPNames
import org.apache.http.params.HttpParams
import org.apache.http.util.EntityUtils

import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManager
import javax.net.ssl.X509TrustManager
import java.nio.charset.Charset
import java.security.cert.CertificateException
import java.security.cert.X509Certificate
import java.util.concurrent.TimeUnit

/**
 *  哥布林
 */
class Goblin {
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
    public static final HttpClient httpClient = new DefaultHttpClient()
    static final String USER_AGENT = "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.4 (KHTML, like Gecko) Safari/537.4";
    public static final Charset UTF8 =Charset.forName("UTF-8")

    static DBCollection xy_users = mongo.getDB("xy").getCollection("users")
    static DBCollection xy_family = mongo.getDB("xy_family").getCollection("familys")
    static DBCollection award_logs = mongo.getDB('xylog').getCollection('user_award_logs') //道具奖励日志
    static DBCollection stat_goblin = mongo.getDB('xy_admin').getCollection('stat_goblin') //哥布林统计

    /**
     * params: start/min/max/period/times
     * @return
     */
    static goblin_action() {
        new Thread(new Runnable() {
            @Override
            void run() {
                String goblin_action = api_domain + "job/goblin_action".toString()
                HttpGet httpGet = new HttpGet(goblin_action)
                println "job/goblin_action:" + doRequest(httpClient, httpGet, null)
            }
        }).start()
    }

    static goblin_rank() {
        def start = System.currentTimeMillis() - 40 * 60 * 1000L
        def end = System.currentTimeMillis() - 20 * 60 * 1000L
        def query = $$(type: 'goblin_ack', status: 1, is_rank: [$ne: true], timestamp: [$lte: end])
        def batchIds = award_logs.distinct('batch_id', query)
        for(String id : batchIds) {
            def users = []
            def familyId = null
            def update = new BasicDBObject()
            Integer count = 0, cash = 0
            award_logs.aggregate([
                    $$($match: [type: 'goblin_ack', status: 1, batch_id: id]),
                    $$($project: [user_id: '$user_id', family_id: '$family_id', batch_id: '$batch_id', dual: '$dual', cash: '$award.cash']),
                    $$($group: [_id: '$user_id', count: [$sum: '$dual'], family_id: [$first: '$family_id'], cash: [$sum: '$cash'], batch_id: [$first: '$batch_id']]),
                    $$($sort: [count: -1])
            ]).results().each {BasicDBObject obj ->
                if (familyId == null) {
                    familyId = obj['family_id'] as Integer
                }
                users.add([user_id: obj['_id'], count: obj['count'], cash: obj['cash']])
                count = count + (obj['count'] as Integer)
                cash = cash + (obj['cash'] as Integer)
            }
            award_logs.updateMulti($$(batch_id: id), $$($set: [is_rank: true]))
            if (users != null && !users.isEmpty()) {
                for(Map user : users) {
                    DBObject obj = xy_users.findOne(user['user_id'] as Integer, $$(nick_name: 1, level: 1, pic: 1, 'family.family_priv': 1))
                    obj.removeField('_id')
                    user.putAll(obj.toMap())
                }
            }
            def _id = "${id}_${System.currentTimeMillis()}".toString()
            update.put('_id', _id)
            update.put('users', users)
            update.put('count', count)
            update.put('cash', cash)
            update.put('timestamp', Long.parseLong(id.split('_')[1]))
            update.put('create_at', System.currentTimeMillis())
            update.put('family', xy_family.findOne($$(_id: familyId), $$(name: 1, pic: 1)))
            stat_goblin.update($$(_id: _id), update, true, false)
        }
    }


    private static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value)
    }

    private static BasicDBObject $$(Map map) {
        return new BasicDBObject(map)
    }

    static class Response {
        HttpResponse httpResponse
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

        @Override
        public String toString() {
            return "Response{" +
                    "content='" + content + '\'' +
                    ", headers=" + Arrays.toString(headers) +
                    '}';
        }
    }

    static Response doRequest(HttpClient httpClient, HttpRequestBase request, Map<String, String> params) {
        final String forceCharset = 'UTF-8'
        return http(httpClient, request, params, new ResponseHandler<Response>() {

            @Override
            Response handleResponse(HttpResponse response) throws IOException {
                def code = response.getStatusLine().getStatusCode()
                Response resp = new Response()
                resp.httpResponse = response
                /*if(code != HttpStatus.SC_OK){
                    return resp
                }*/
                resp.content = handle(response.getEntity())
                resp.headers = response.getAllHeaders()
                return resp
            }

            String handle(HttpEntity entity) throws IOException {
                byte[] content = EntityUtils.toByteArray(entity)
                if(forceCharset != null){
                    Response resp = new Response()
                    resp.content = new String(content,forceCharset)
                    return resp
                }
                Charset charset =null
                ContentType contentType = ContentType.get(entity)
                if(contentType !=null){
                    charset = contentType.getCharset()
                }
                if(charset ==null){
                    charset = Charset.forName("GB18030")
                }
                return new String(content,charset)
            }
        })
    }

    static void main(String[] args) {
        long l = System.currentTimeMillis()
        long begin = l
        //goblin_action()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  goblin_action cost  ${System.currentTimeMillis() - l} ms"

        l = System.currentTimeMillis()
        goblin_rank()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  goblin_rank cost  ${System.currentTimeMillis() - l} ms"

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
        println "${new Date().format('yyyy-MM-dd')}:${Goblin.class.getSimpleName()}:finish  cost  ${System.currentTimeMillis() - begin} ms"
    }

    //落地定时执行的日志
    private static saveTimerLogs(String timerName, Long totalCost) {
        def timerLogsDB = mongo.getDB("xyrank").getCollection("timer_logs")
        def tmp = System.currentTimeMillis()
        def id = timerName + "_" + new Date().format("yyyyMMdd")
        def update = new BasicDBObject(timer_name: timerName, cost_total: totalCost, cat: 'day', unit: 'ms', timestamp: tmp)
        timerLogsDB.findAndModify(new BasicDBObject('_id', id), null, null, false, new BasicDBObject('$set', update), true, true)
    }

    static <T> T  http(HttpClient  client, HttpRequestBase request, Map<String,String> headers, ResponseHandler<T> handler)
            throws IOException {
        if(headers !=null &&  ! headers.isEmpty()){
            for (Map.Entry<String,String> kv : headers.entrySet()){
                request.addHeader(kv.getKey(),kv.getValue())
            }
        }
        try{
            return client.execute(request,handler,null)
        }catch (ConnectTimeoutException e){
            println " catch ConnectTimeoutException ,closeExpiredConnections &  closeIdleConnections for 30 s. " + e
            client.getConnectionManager().closeExpiredConnections()
            client.getConnectionManager().closeIdleConnections(30, TimeUnit.SECONDS)
        }
        return null
    }

    static void initHttpClient() {

        PoolingClientConnectionManager cm = new PoolingClientConnectionManager(createSchemeRegistry())
        cm.setMaxTotal(800)
        cm.setDefaultMaxPerRoute(200)

        /*cm.setMaxPerRoute(new HttpRoute(new HttpHost("localhost")),500)
        cm.setMaxPerRoute(new HttpRoute(new HttpHost("127.0.0.1")),500)
        cm.setMaxPerRoute(new HttpRoute(new HttpHost("aiapi.memeyule.com")),500)
        cm.setMaxPerRoute(new HttpRoute(new HttpHost("aiuser.memeyule.com")),500)*/
        HttpParams defaultParams = new BasicHttpParams()

        defaultParams.setLongParameter(ClientPNames.CONN_MANAGER_TIMEOUT, 5000)
        defaultParams.setIntParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 5000)//连接超时
        defaultParams.setIntParameter(CoreConnectionPNames.SO_TIMEOUT, 5000)//读取超时

        defaultParams.setParameter(ClientPNames.COOKIE_POLICY, CookiePolicy.IGNORE_COOKIES)
        defaultParams.setParameter(CoreProtocolPNames.HTTP_CONTENT_CHARSET,UTF8.name())
        defaultParams.setParameter(CoreProtocolPNames.USER_AGENT,USER_AGENT)

        HttpClient client = new DefaultHttpClient(cm, defaultParams)
        client = new DecompressingHttpClient(client)
        //httpClient = client
    }

    private static SchemeRegistry createSchemeRegistry() {
        SchemeRegistry sr = new SchemeRegistry()
        try {
            SSLContext ctx = SSLContext.getInstance("TLS")
            X509TrustManager tm = new X509TrustManager() {

                @Override
                public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
                }

                @Override
                public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
                }

                @Override
                public X509Certificate[] getAcceptedIssuers() {
                    return null
                }
            }
            ctx.init(null, [tm] as TrustManager[], null)
            SSLSocketFactory ssf = new SSLSocketFactory(ctx, SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER)
            sr.register(new Scheme("https", 443, ssf))
        } catch (Exception ex) {
        }
        return sr
    }

}