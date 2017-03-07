#!/usr/bin/env groovy
import com.mongodb.DB
import com.mongodb.DBCollection
import com.mongodb.MongoURI
@Grapes([
@Grab('org.codehaus.groovy.modules.http-builder:http-builder:0.6'),
@Grab('org.mongodb:mongo-java-driver:2.10.1')
])
import groovyx.net.http.*
import org.apache.commons.lang.math.RandomUtils

import java.util.concurrent.atomic.AtomicInteger
import static groovyx.net.http.ContentType.*
import com.mongodb.Mongo
import com.mongodb.BasicDBObject



class RegisterRobot {

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

    static final long[] userLevels = [0,0,0,0,
                                      1000,1000,1000,1000,1000,
                                      5000,5000,5000,
                                        15000,
                                        30000,
                                        50000,
                                        80000]

    static register = new HTTPBuilder( 'http://test-aiuser.memeyule.com/register/robot' ,JSON)
    static xingYuanUser = new HTTPBuilder( 'http://localhost:8080/user/info',JSON)
    static flushUser = new HTTPBuilder( 'http://localhost:8080/java/flushuser',JSON)
    static users = mongo.getDB("xy").getCollection("users")
    static xy_user = mongo.getDB("xy_user").getCollection("users")

    static String filePath = "/empty/crontab/fansi.csv"

    //注册机器人
    static generateRobot(){
        int robot_id = 1023956

        def inc = new AtomicInteger()

        new File(filePath).splitEachLine(','){row->
            String username = "${inc.incrementAndGet()}@robot.com"
            register.post(query:[ username :username ,pwd:'memeqwert2wsx',via:'robot']){resp, json ->
                String  access_token = json.data.access_token
                String  tuid = json.data._id
                Boolean isUseDefault = RandomUtils.nextInt(10) < 6
                String  nick_name = isUseDefault ? json.data.nick_name : row[0] as String
                String pic =  isUseDefault ? json.data.pic : row[1] as String
                Integer uid = robot_id--
                Integer sex = row[2] as Integer
                Long coin_spend_total = RandomUtils.nextInt(10) < 8 ? 0 : userLevels[RandomUtils.nextInt(userLevels.length)]
                def info = [tuid:tuid as Integer, priv:3, mm_no:uid, user_name: username, via:'robot', pic:pic, sex:sex,
                            nick_name: nick_name, "finance.coin_spend_total":coin_spend_total,timestamp:System.currentTimeMillis()];
                println uid + " : " + info
                users.update(new BasicDBObject("_id",uid),new BasicDBObject('$set',info), true, false)
                xy_user.update(new BasicDBObject("_id",tuid as Integer),new BasicDBObject('$set',[nick_name:nick_name,uid:uid,spend:coin_spend_total,pic:pic]), false, false)
            }
        }

    }

    static void main(String[] args) {
        long l = System.currentTimeMillis()
        generateRobot()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   FinanceClean, cost  ${System.currentTimeMillis() - l} ms"
    }
}