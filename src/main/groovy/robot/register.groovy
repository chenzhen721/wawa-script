#!/usr/bin/env groovy

@Grapes([
@Grab('org.codehaus.groovy.modules.http-builder:http-builder:0.6'),
@Grab('org.mongodb:mongo-java-driver:2.10.1')
])
import groovyx.net.http.*
import java.util.concurrent.atomic.AtomicInteger
import static groovyx.net.http.ContentType.*
import com.mongodb.Mongo
import com.mongodb.BasicDBObject

/*
def register = new HTTPBuilder( 'http://user.2339.com/register/robot' ,JSON)
def xingYuanUser = new HTTPBuilder( 'http://api.2339.com/user/info',JSON)
def flushUser = new HTTPBuilder( 'http://api.2339.com/java/flushuser',JSON)
def mongo = new Mongo("192.168.8.223", 10009)
def users = mongo.getDB("xy").getCollection("users")
 */


def register = new HTTPBuilder( 'http://localhost:8082/register/robot' ,JSON)
def xingYuanUser = new HTTPBuilder( 'http://localhost:8080/user/info',JSON)
def flushUser = new HTTPBuilder( 'http://localhost:8080/java/flushuser',JSON)
def mongo = new Mongo("192.168.8.119", 27017)
def users = mongo.getDB("xy").getCollection("robotusers")

def cats = [500,5000,10000]
def inc = new AtomicInteger()

new File("E:/2339/project/api.memeyule.com/projects/star-script/src/main/groovy/robot/fansi.csv").splitEachLine(','){row->
    register.post(query:[ username :"${inc.incrementAndGet()}@robot.com" ,pwd:'memeqwert2wsx',via:'robot']){resp, json ->
        println json
        String  access_token = json.data.access_token
        if (access_token){
            xingYuanUser.get(query: [access_token:access_token]){r,user->
                Integer uid = user.data._id
                Integer type=row[2] as Integer
                users.update(new BasicDBObject("_id",uid),new BasicDBObject('$set',
                        [via:'robot',pic:row[1],sex:type,nick_name: row[0],"finance.coin_spend_total":cats[type]]))
                if(row[0] != json.data.nick_name){
                    //flush
                    flushUser.get(query: [id1:uid])
                }
            }
        }
    }
}

