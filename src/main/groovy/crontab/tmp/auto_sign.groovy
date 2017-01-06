#!/usr/bin/env groovy
package tmp

@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
        @Grab(group = 'net.sf.json-lib', module = 'json-lib', version = '2.3', classifier = 'jdk15'),
])
import com.mongodb.MongoURI
import groovy.json.JsonSlurper
import com.mongodb.BasicDBObject
import com.mongodb.Mongo
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPubSub
import net.sf.json.JSONObject
import java.security.MessageDigest

class auto_sign {

    static url = "http://api.memeyule.com"
    def final static users = [1201053 : "15cda64a6a7ea44b7f2f3a2d718e91e2",
                              1201066 : "a9b531c7fa68cb039e7598e2750c55ca",
                              2731113 : "ead2be4ad5e2a9cbd0f171f11aa9021b",
                              1201054 : "5967e1e8e8f23e1ff6685ac6dfc7f4f3",
                              1204657 : "dfba9ed9b5dfad5de2fc2e79ea320891"
    ] as Map

    static sign(String access_token){
        println "day_login  "+ new URL(url+'/user/day_login/'+access_token).getText("utf-8")
        println "amass "+new URL(url+"/feather/amass/${access_token}").getText("utf-8")
        println "mission "+new URL(url+"/mission/award?access_token=${access_token}&mission_id=sign_daily&qd=MM").getText("utf-8")
        println "check "+ new URL(url+"/sign/check?access_token=${access_token}").getText("utf-8")
        println "chest "+new URL(url+"/sign/chest?access_token=${access_token}").getText("utf-8")
        [0, 1, 2, 3, 4].each {
            println new URL(url+'/sign/award/'+access_token+'/'+it).getText("utf-8")
        }

    }

    static auto_sign_go (){
        users.each {k,String access_token ->
            sign(access_token)
        }
    }

    static void main(String[] args)
    {
        auto_sign_go()
    }


}