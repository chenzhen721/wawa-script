#!/usr/bin/env groovy
package crontab.tmp

import com.mongodb.BasicDBObject
import com.mongodb.DBCollection
import com.mongodb.DBCursor
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
])
import com.mongodb.Mongo
import com.mongodb.DBObject
import com.mongodb.MongoURI
import groovy.json.JsonOutput
import groovy.json.*

import java.text.SimpleDateFormat

/**
 * 神策历史数据导出
 */
class ShenceExportData {
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
    static historyDB = historyMongo.getDB('xylog_history')
    static DAY_MILLON = 24 * 3600 * 1000L

    static long zeroMill = new Date().clearTime().getTime()
    static Long yesTday = zeroMill - DAY_MILLON
//    static String YMD = new Date(yesTday).format("yyyyMMdd")
    static room_cost = mongo.getDB('xylog').getCollection('room_cost')
    static trade_logs = mongo.getDB('xylog').getCollection('trade_logs')
    static room_cost_2014 = historyDB.getCollection('room_cost_2014')
    static room_cost_2015 = historyDB.getCollection('room_cost_2015')
    static room_cost_2016 = historyDB.getCollection('room_cost_2016')
    static finance_log = mongo.getDB('xy_admin').getCollection('finance_log')
    static users = mongo.getDB("xy").getCollection("users")
    static lottery_logs = mongo.getDB('xylog').getCollection('lottery_logs')

    public static final String ls = System.lineSeparator();

    public static BasicDBObject $$(String key,Object value){
        return new BasicDBObject(key,value);
    }

    public static BasicDBObject $$(Map map){
        return new BasicDBObject(map);
    }

    /**
     * {
     "distinct_id": "12345",
     "time": 1434556935000,
     "type": "track",
     "event": "SignUp",
     "properties": {
     “source":"H5"
     }
     }

     {
     "distinct_id": "12345",
     "type": "profile_set",
     "time": 1434556935000,
     "properties": {
     “source":"H5",
     "$signup_time": "2015-06-26 11:43:15.610"
     }
     }

     * @return
     */
    static registerUser(){
        def startStr = "2016-03-01 000000"
        def endStr = "2016-04-01 000000"
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HHmmss")
        def stime = 0L
        def etime = 0L
        try {
            stime = sdf.parse(startStr).getTime()
            etime = sdf.parse(endStr).getTime()
        } catch (Exception e) {
            println e
        }
        def query = new BasicDBObject([timestamp: ['$gte': stime, '$lt': etime]])
        DBCursor cursor = users.find(query, $$(qd:1,timestamp:1)).batchSize(5000)
        def buf = new StringBuffer()
        while (cursor.hasNext()) {
            def user = cursor.next()
            Map data = warpData(user['_id'].toString(),user['timestamp'] as Long, "track", "SignUp",["source":user['qd']]);
            buf.append(JsonOutput.toJson(data)).append(ls)
            String date = new Date(user['timestamp'] as Long).format("yyyy-MM-dd HH:mm:ss.SSS")
            data = warpData(user['_id'].toString(),user['timestamp'] as Long, "profile_set", null, ["source":user['qd'],'$signup_time':date]);
            buf.append(JsonOutput.toJson(data)).append(ls)
        }
        write2File(buf.toString(), 'users_2016-03');
    }


    static chargeUser(){
        def startStr = "2016-03-01 000000"
        def endStr = "2016-06-01 000000"
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HHmmss")
        def stime = 0L
        def etime = 0L
        try {
            stime = sdf.parse(startStr).getTime()
            etime = sdf.parse(endStr).getTime()
        } catch (Exception e) {
            println e
        }
        def query = new BasicDBObject([timestamp: ['$gte': stime, '$lt': etime], via:[$ne:'Admin']])
        DBCursor cursor = finance_log.find(query, $$(to_id:1,via:1,cny:1,timestamp:1)).batchSize(5000)
        def buf = new StringBuffer()
        while (cursor.hasNext()) {
            def user = cursor.next()
            Map prop = ["rechargeMethod":user['via'], rechargeValue:user['cny']];
            Map data = warpData(user['to_id'].toString(),user['timestamp'] as Long, "track", "recharge",prop);
            buf.append(JsonOutput.toJson(data)).append(ls)
        }
        write2File(buf.toString(), 'recharge');
    }

    /**
     * {
     "distinct_id": "12345",
     "time": 1434556935000,
     "type": "track",
     "event": "sendGift",
     "properties": {
         “giftId”:"9999998",
         “giftType”:“金柠檬”,
         “giftCount”:“50”,
         “giftValue”:“100”,
         “anchorId”:“77778”,
         “roomId”:“20108676”,
         “receiverId”:“54321”
     }
     }

     */
    static giftUser(){
        def startStr = "2016-03-01 000000"
        def endStr = "2016-04-01 000000"

        String pic_folder = "/empty/static/"
        def fold = new File(pic_folder)
        File file = new File(fold,"send_gift_2016-03.json".toString())
        if (!file.exists()) {
            file.createNewFile();
        }

        FileWriter fw = new FileWriter(file);
        FileOutputStream  outSTr = new FileOutputStream(file);
        BufferedOutputStream bf =new BufferedOutputStream(outSTr, 24576);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HHmmss")
        def stime = 0L
        def etime = 0L
        try {
            stime = sdf.parse(startStr).getTime()
            etime = sdf.parse(endStr).getTime()
        } catch (Exception e) {
            println e
        }
        def query = new BasicDBObject([timestamp: ['$gte': stime, '$lt': etime], type:'send_gift'])
        DBCursor cursor = room_cost_2016.find(query, $$(session:1,room:1,timestamp:1)).batchSize(20000)
        //def buf = new StringBuffer()
        while (cursor.hasNext()) {
            def user = cursor.next()
            def session = user['session'] as Map
            def userId = session['_id'] as String
            def gData = session['data'] as Map
            def room = user['room']
            def receiverId = gData['xy_user_id'] ?: gData['xy_star_id']
            Map prop = ["giftId":gData['_id'], giftType:gData['category_id'], giftCount:gData['count'], giftValue:gData['coin_price'],
                        anchorId:userId, roomId:room.toString(), receiverId:receiverId];
            Map data = warpData(userId, user['timestamp'] as Long, "track", "sendGift",prop);
            //buf.append(JsonOutput.toJson(data)).append(ls)
            //file.append(JsonOutput.toJson(data))
            //file.append(ls)

            //fw.write(JsonOutput.toJson(data))
            //fw.write(ls)
            bf.write(JsonOutput.toJson(data).getBytes())
            bf.write(ls.getBytes())
        }
        bf.flush();
        bf.close();

        //fw.flush();
        //fw.close();
        //write2File(buf.toString(), 'send_gift_2016-03');
    }


    static fileTest(){
        def startStr = "2016-01-01 000000"
        def endStr = "2016-06-01 000000"

        String pic_folder = "/empty/static/"
        def fold = new File(pic_folder)
        File file = new File(fold,"send_gift_1.json".toString())
        if (!file.exists()) {
            file.createNewFile();
        }

        FileWriter fw = new FileWriter(file);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HHmmss")
        def stime = 0L
        def etime = 0L
        try {
            stime = sdf.parse(startStr).getTime()
            etime = sdf.parse(endStr).getTime()
        } catch (Exception e) {
            println e
        }
        def query = new BasicDBObject([timestamp: ['$gte': stime, '$lt': etime], type:'send_gift'])
        DBCursor cursor = room_cost_2016.find(query, $$(session:1,room:1,timestamp:1)).batchSize(10000)
        List<Map> list = new ArrayList(20000)
        while (cursor.hasNext()) {
            def user = cursor.next()
            def session = user['session'] as Map
            def userId = session['_id'] as String
            def gData = session['data'] as Map
            def room = user['room']
            def receiverId = gData['xy_user_id'] ?: gData['xy_star_id']
            Map prop = ["giftId":gData['_id'], giftType:gData['category_id'], giftCount:gData['count'], giftValue:gData['coin_price'],
                        anchorId:userId, roomId:room.toString(), receiverId:receiverId];
            Map data = warpData(userId, user['timestamp'] as Long, "track", "sendGift",prop);
            list.add(data)
        }

        long begin = System.currentTimeMillis()
        list.each {Map data ->
            fw.write(JsonOutput.toJson(data))
            fw.write(ls)
        }
        fw.flush();
        fw.close();
        println "file writer cost : ${System.currentTimeMillis() - begin}"

        file = new File(fold,"send_gift_2.json".toString())

        begin = System.currentTimeMillis()
        list.each {Map data ->
            file.append(JsonOutput.toJson(data))
            file.append(ls)
        }
        println "file append cost : ${System.currentTimeMillis() - begin}"

        file = new File(fold,"send_gift_3.json".toString())
        FileOutputStream  outSTr = new FileOutputStream(file);
        BufferedOutputStream bf =new BufferedOutputStream(outSTr);

        begin = System.currentTimeMillis()
        list.each {Map data ->
            bf.write(JsonOutput.toJson(data).getBytes())
            bf.write(ls.getBytes())
        }
        bf.flush();
        bf.close();
        println "file BufferedOutputStream cost : ${System.currentTimeMillis() - begin}"
        //write2File(buf.toString(), 'send_gift_2016-03');
    }
    static Map warpData(String distinct_id, Long time, String type, String event, Map properties){
        Map data = new HashMap();
        data['distinct_id'] = distinct_id
        data['time'] = time
        data['type'] = type
        if(event != null)
            data['event'] = event
        data['properties'] = properties
        return data;
    }

    static void write2File(String jsonText, String fileName){
        String pic_folder = "/empty/static/"
        def fold = new File(pic_folder)
        File file = new File(fold,"${fileName}.json".toString())
        if (!file.exists()) {
            file.createNewFile();
        }

        file.setText(jsonText,"UTF-8")
    }

    static void main(String[] args) {
        long l = System.currentTimeMillis()
        //registerUser();
        //chargeUser();
        giftUser();
        //fileTest();
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${ShenceExportData.class.getSimpleName()},total cost  ${System.currentTimeMillis() - l} ms"
    }

}