#!/usr/bin/env groovy
package tmp

import com.mongodb.BasicDBObject
import com.mongodb.DBCollection
import com.mongodb.DBObject
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
        @Grab(group = 'net.sf.json-lib', module = 'json-lib', version = '2.3', classifier = 'jdk15'),
])
import com.mongodb.MongoURI
import com.mongodb.Mongo
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;


/**
 * 又拍云定时删除任务
 */
public class Upyun {

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

    static mongo = new Mongo(new MongoURI(getProperties('mongo.uri', 'mongodb://192.168.31.246:27017/?w=1') as String))

    static DBCollection ban_photos =  mongo.getDB("xy").getCollection("ban_photos")

    public static final String ED_AUTO = "v0.api.upyun.com";

    static long zeroMill = new Date().clearTime().getTime()

    static void main(String[] args)
    {
        long l = System.currentTimeMillis()
        long begin = l
        ban_photos.find(new BasicDBObject('upai_bucket':bucketName,'del_time' : new BasicDBObject('$gte':zeroMill)))
                    .limit(5000).toArray().each {DBObject obj ->
            def path = obj['_id'] as String
            println path
            if(deleteFile(path)){
                ban_photos.remove(new BasicDBObject('_id', path))
            }
        }
        //落地定时执行的日志
        l = System.currentTimeMillis()
        def timerName = 'upyun'
        Long totalCost = System.currentTimeMillis() - begin
        saveTimerLogs(timerName,totalCost)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  save timer_logs , cost  ${System.currentTimeMillis() - l} ms"

        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${Upyun.class.getSimpleName()}, cost  ${System.currentTimeMillis() -begin} ms"
    }

    //落地定时执行的日志
    private static saveTimerLogs(String timerName,Long totalCost)
    {
        def timerLogsDB =  mongo.getDB("xyrank").getCollection("timer_logs")
        def tmp = System.currentTimeMillis()
        def id = timerName + "_"  + new Date().format("yyyyMMdd")
        def update = new BasicDBObject(timer_name:timerName,cost_total:totalCost,cat:'hour',unit:'ms',timestamp:tmp)
        timerLogsDB.findAndModify(new BasicDBObject('_id',id), null, null, false,new BasicDBObject('$set',update),true, true)
    }

    public static String MD5(String s) {
        def hexDigits =  ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'] as char[];

        try {
            byte[] btInput = s.getBytes();
            // 获得MD5摘要算法的 MessageDigest 对象
            MessageDigest mdInst = MessageDigest.getInstance("MD5");
            // 使用指定的字节更新摘要
            mdInst.update(btInput);
            // 获得密文
            byte[] md = mdInst.digest();
            // 把密文转换成十六进制的字符串形式
            int j = md.length;
            def str = new char[j * 2];
            int k = 0;
            for (int i = 0; i < j; i++) {
                byte byte0 = md[i];
                str[k++] = hexDigits[byte0 >>> 4 & 0xf];
                str[k++] = hexDigits[byte0 & 0xf];
            }
            return new String(str).toLowerCase();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }


    /** 默认的编码格式 */
    private static final String UTF8 = "UTF-8";

    /** SKD版本号 */
    private static final String VERSION = "2.0";

    /** 路径的分割符 */
    private static final String SEPARATOR = "/";

    private static final String AUTHORIZATION = "Authorization";
    private static final String DATE = "Date";
    private static final String CONTENT_LENGTH = "Content-Length";

    private static final String METHOD_HEAD = "HEAD";
    private static final String METHOD_GET = "GET";
    private static final String METHOD_DELETE = "DELETE";
    // 默认不开启debug模式
    private static final boolean debug = false;
    // 默认的超时时间：30秒
    private static final int timeout = 5 * 1000;
    // 默认为自动识别接入点
    private static final String apiDomain = ED_AUTO;
    // 空间名
    private static final String bucketName = "img-nest";
    // 操作员名
    private static final String userName = 'devadmin';
    // 操作员密码
    private static final  String password = 'devadmin';
    /**
     * 删除文件
     *
     * @param filePath
     *            文件路径（包含文件名）
     *
     * @return true or false
     */
    public static boolean deleteFile(String filePath) {
        return HttpAction(METHOD_DELETE, formatPath(filePath)) != null;
    }

    private static boolean isEmpty(String str) {
        return str == null || str.length() == 0;
    }


    private static String formatPath(String path) {

        if (!isEmpty(path)) {
            // 去除前后的空格
            path = path.trim();

            // 确保路径以"/"开头
            if (!path.startsWith(SEPARATOR)) {
                return SEPARATOR + bucketName + SEPARATOR + path;
            }
        }

        return SEPARATOR + bucketName + path;
    }

    /**
     * 连接处理逻辑
     *
     * @param method
     *            请求方式 {GET, POST, PUT, DELETE}
     * @param uri
     *            请求地址
     */
    private static String HttpAction(String method, String uri) {

        String result = null;
        HttpURLConnection conn = null;

        try {
            // 获取链接
            URL url = new URL("http://" + apiDomain + uri);
            conn = (HttpURLConnection) url.openConnection();

            // 设置必要参数
            conn.setConnectTimeout(timeout);
            conn.setRequestMethod(method);
            conn.setUseCaches(false);
            if(!method.equals(METHOD_DELETE) && !method.equals(METHOD_HEAD) && !method.equals(METHOD_GET)){
                conn.setDoOutput(true);
            }

            // 设置时间
            conn.setRequestProperty(DATE, getGMTDate());

            long contentLength = 0;
                conn.setRequestProperty(CONTENT_LENGTH, "0");


            // 设置签名
            conn.setRequestProperty(AUTHORIZATION,sign(conn, uri, contentLength));

            // 创建链接
            conn.connect();

            int code = conn.getResponseCode();

            println "code :" + code
            if(code == 200){
                result = "200"
            }
        } catch (IOException e) {
            println e.printStackTrace();
            // 操作失败
            return null;

        } finally {
            if (conn != null) {
                conn.disconnect();
                conn = null;
            }
        }

        return result;
    }

    private static String getGMTDate() {
        SimpleDateFormat formater = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss 'GMT'", Locale.US);
        formater.setTimeZone(TimeZone.getTimeZone("GMT"));
        return formater.format(new Date());
    }

    private static String sign(HttpURLConnection conn, String uri, long length) {
        String sign = conn.getRequestMethod() + '&' + uri + '&' + conn.getRequestProperty(DATE) + '&' + length + '&' + MD5(password);
        return "UpYun " + userName + ":" + MD5(sign);
    }


}
