#!/usr/bin/env groovy
package st

import com.mongodb.BasicDBObject

/**
 * Author: monkey
 * Date: 2017/3/23
 */
import com.mongodb.DBCollection
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
])

import com.mongodb.Mongo
import com.mongodb.MongoURI

/**
 * 每天统计一份数据
 */
class Recovery {

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

    // 2017-03-31
    static Long begin = 1490889600000L
    // 2017-04-06
    static Long end = 1491408000000L
    static Integer AprilFoolGiftId = 9
    static DAY_MILLON = 24 * 3600 * 1000L
    static DBCollection room_cost = mongo.getDB('xylog').getCollection('room_cost')

    // 每天 每个人 送了 多少愚人节礼物,
    static void aprilFool_stat() {
        StringBuffer usrBuf = new StringBuffer("日期,用户id,送礼个数").append(System.lineSeparator())
        StringBuffer starBuf = new StringBuffer("日期,主播id,收礼个数").append(System.lineSeparator())
        while (begin < end) {
            def timestamp = begin + DAY_MILLON
            def query = $$('timestamp': ['$gte': begin, '$lt': timestamp], 'session.data._id': AprilFoolGiftId)
            def sender = room_cost.aggregate(
                    $$('$match': query),
                    $$('$project': ['user_id':'$session._id','count':'$session.data.count']),
                    $$('$group': ['_id':'$user_id',count: [$sum: '$count']])
            ).results().iterator()

            while (sender.hasNext()){
                def obj = sender.next()
                usrBuf.append(new Date(begin).format('yyyy-MM-dd')).append(",")
                usrBuf.append(obj['_id'].toString()).append(",")
                usrBuf.append(obj['count'].toString()).append(System.lineSeparator())
            }

            def receiver = room_cost.aggregate(
                    $$('$match': query),
                    $$('$project': ['user_id':'$session.data.xy_star_id','count':'$session.data.count']),
                    $$('$group': ['_id':'$user_id',count: [$sum: '$count']])
            ).results().iterator()

            while (receiver.hasNext()){
                def obj = receiver.next()
                starBuf.append(new Date(begin).format('yyyy-MM-dd')).append(",")
                starBuf.append(obj['_id'].toString()).append(",")
                starBuf.append(obj['count'].toString()).append(System.lineSeparator())
            }

            begin += DAY_MILLON
        }

        println("usrBuf is ${usrBuf}")
        println("starBuf is ${starBuf}")

        createFile(usrBuf, null, '/april_fool_sender.csv')
        createFile(starBuf, null, '/april_fool_receiver.csv')
    }

    /**
     * 创建文件
     * @param sb
     * @param folder_path
     * @param exportFileName
     */
    private static void createFile(StringBuffer sb, String folder_path, String exportFileName) {
        if (folder_path == null) {
            folder_path = '/empty/static/'
        }
        File folder = new File(folder_path)
        if (!folder.exists()) {
            folder.mkdirs()
        }
    println("file path is ${folder_path + exportFileName}")
        File file = new File(folder_path + exportFileName);
        if (!file.exists()) {
            file.createNewFile()
        }

        file.withWriterAppend { Writer writer ->
            writer.write(sb.toString())
            writer.flush()
            writer.close()
        }
    }

    private static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    private static BasicDBObject $$(Map map) {
        return new BasicDBObject(map);
    }

    static void main(String[] args) {
        long l = System.currentTimeMillis()
        aprilFool_stat()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   aprilFool_stat, cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

    }

}