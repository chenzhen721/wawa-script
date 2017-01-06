#!/usr/bin/env groovy

@Grapes([
@Grab('org.mongodb:mongo-java-driver:2.14.2'),
@Grab('commons-lang:commons-lang:2.6'),
@Grab('redis.clients:jedis:2.1.0'),
])
import com.mongodb.Mongo
import com.mongodb.MongoURI
import com.mongodb.BasicDBObject
import com.mongodb.DBCursor
import com.mongodb.DBObject
/**
 * 延迟、遗漏订单检测补偿
 */
class DelayOrderCheck {
    static Properties props = null;
    static String profilepath="/empty/crontab/db.properties";

    static getProperties(String key, Object defaultValue){
        try {
            if(props == null){
                props = new Properties();
                props.load(new FileInputStream(profilepath));
            }
        } catch (Exception e) {
            println e;
        }
        return props.get(key, defaultValue)
    }

    static long zeroMill = new Date().clearTime().getTime()
    static DAY_MILLON = 24 * 3600 * 1000L
    def static mongo  = new Mongo(new MongoURI(getProperties('mongo.uri','mongodb://192.168.31.249:27017/?w=1') as String))
    static final String api_domain = getProperties("api.domain", "http://localhost:8080/")
    def static order_logs = mongo.getDB("xylog").getCollection("order_logs");

    def static final Long MIN_MILLS = 60 * 1000l

    static api_url = new URL(api_domain+"pay/delay_order_fix")

    static WEEK_MILLON = 7 * DAY_MILLON
    private static final Integer TIME_OUT = 10 * 60 * 1000;

    //2分钟 5分钟 15分钟 1小时 4小时 8小时 24小时 48小时
    def static final List<Long> CHECK_COUNTS_MIN = [5*MIN_MILLS, 15*MIN_MILLS, 60*MIN_MILLS, 4*60*MIN_MILLS, 8*60*MIN_MILLS, 24*60*MIN_MILLS, 3*24*60*MIN_MILLS];
    def static orderCheck(){
        //Long now = System.currentTimeMillis();
        //TODO 清除超过三天的订单 移到SysEveryDay
        //order_logs.remove(new BasicDBObject(timestamp:[$lte:now - (3*24*60*MIN_MILLS)]));

        /*DBCursor orderList = order_logs.find(new BasicDBObject(status:1, checkpoint:[$lte:now]),new BasicDBObject(checkpoint:1, checkcount:1)).batchSize(1000)
        Integer total = 0
        Integer successTotal = 0
        Integer failTotal = 0
        while (orderList.hasNext()){
            total++;
            def order = orderList.next();
            String orderId = order['_id'] as String
            Integer checkcount = (order['checkCount'] ?: 0) as Integer
            //记录下次检测时间
            BasicDBObject updateInfo = null;
            //处理订单
            if(!process(orderId)){
                failTotal++;
                if(checkcount < CHECK_COUNTS_MIN.size()){
                    Long checkpoint = now + CHECK_COUNTS_MIN[checkcount]
                    updateInfo = new BasicDBObject('$set':[checkpoint:checkpoint, modify_time:now]).append('$inc', new BasicDBObject(checkcount:1))
                    order_logs.update(new BasicDBObject('_id', orderId), updateInfo);
                }
            }else{
                successTotal++;
            }
        }*/
        //println "${new Date().format('yyyy-MM-dd HH:mm:ss')} total ${total}, successTotal : ${successTotal}, failTotal: ${failTotal}"
        HttpURLConnection conn = null;
        def jsonText = "";
        try{
            conn = (HttpURLConnection)api_url.openConnection()
            conn.setRequestMethod("GET")
            conn.setDoOutput(true)
            conn.setConnectTimeout(TIME_OUT);
            conn.setReadTimeout(TIME_OUT);
            conn.connect()
            jsonText = conn.getInputStream().getText("UTF-8")
        }catch (Exception e){
            println "staticWeek Exception : " + e;
        }finally{
            if (conn != null) {
                conn.disconnect();
                conn = null;
            }
        }
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')} result : ${jsonText}"
    }

    //TODO 测试用，生成订单
    static List<String> vias = ['ali_m', 'weixin_m', 'ali_pc', 'weixin_m', 'weixin_h5', 'weixin_m']
    static Boolean generate(){
        3.times {
            vias.each {String via ->
                Long times = System.currentTimeMillis();
                String order_id = "31952149_${times}_meme${via}".toString()
                BasicDBObject orderInfo = new BasicDBObject('_id', order_id);
                orderInfo.put('timestamp', times);
                orderInfo.put("checkpoint",(times + (5 * MIN_MILLS)));
                orderInfo.put("checkcount",0);
                orderInfo.put("via", via);
                orderInfo.put("status", 1);
                order_logs.insert(orderInfo) ;
            }
        }
    }

    static void main(String[] args){
        long l = System.currentTimeMillis()
        long begin = l
        //generate();
        orderCheck();
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DelayOrderCheck.class.getSimpleName()}, cost  ${System.currentTimeMillis() - begin} ms"
    }



}