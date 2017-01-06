#!/usr/bin/env groovy
import com.mongodb.DBObject
@Grapes([
@Grab('org.mongodb:mongo-java-driver:2.10.1')
])

import java.util.concurrent.atomic.AtomicInteger
import com.mongodb.Mongo
import com.mongodb.MongoURI
import com.mongodb.BasicDBObject


def mongo  = new Mongo(new MongoURI('mongodb://192.168.1.36:10000,192.168.1.37:10000,192.168.1.38:10000/?w=1&slaveok=true'))
def users = mongo.getDB("xy").getCollection("users")
def xyuser = mongo.getDB("xy_user").getCollection("users")

def cats = [0, 500,5000,10000]

def inc = new AtomicInteger()
int robot_id = 1023956

// password meme1234qwer

new File("/empty/crontab/fansi.cvs").splitEachLine(',') { row ->
//new File("E:/2339/project/api.2339.com/projects/star-script/src/main/groovy/robot/fansi.csv").splitEachLine(',') { row ->
    Integer type=row[2] as Integer
    DBObject user = xyuser.findOne(new BasicDBObject("userName","${inc.incrementAndGet()}@robot.com".toString()),
            new BasicDBObject([_id:1,"userName":1,"pic":1]))
    if(user != null){
        int id = robot_id--
        BasicDBObject u = new BasicDBObject('_id', robot_id--)
        u.append('tuid', user['_id'] as Integer)
        u.append('user_name', user['userName'] as String)
        u.append('pic', user['pic'] as String)
        u.append('via', "robot")
        u.append('nick_name', row[0] as String)
        u.append('status', true)
        u.append('priv', 3)
        u.append('sex', 2)
        u.append('timestamp', System.currentTimeMillis())
        u.append('finance', new BasicDBObject('coin_spend_total', cats[type]))
        users.insert(u)
    }

}
