#!/usr/bin/env groovy
package crontab.tmp

import com.mongodb.BasicDBObject
@Grapes([
@Grab('org.mongodb:mongo-java-driver:2.14.2'),
@Grab('commons-lang:commons-lang:2.6'),
@Grab('redis.clients:jedis:2.1.0'),
@Grab(group = 'net.sf.json-lib', module = 'json-lib', version = '2.3', classifier = 'jdk15')
]) import com.mongodb.Mongo
import com.mongodb.MongoURI

import java.text.SimpleDateFormat
import com.mongodb.DBCollection
import com.mongodb.DBObject

/**
 *初始化用户绑定手机任务
 *
 */
class InitUserMission
{
   //static mongo = new Mongo("192.168.1.156", 10000)
    //static mongo = new Mongo("192.168.31.252", 27017)
   static mongo  = new Mongo(new MongoURI('mongodb://192.168.1.36:10000,192.168.1.37:10000,192.168.1.38:10000/?w=1&slaveok=true'))



    static initUser(){
        mongo.getDB("xy").getCollection("users").update(new BasicDBObject('mobile_bind',false).append('mission.mobile_bind',0),
                new BasicDBObject('$unset', new BasicDBObject('mission.mobile_bind', 1)), false, true)
    }



    static void main(String[] args)
    {
        long l = System.currentTimeMillis()
        initUser();
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitUserMission.class.getSimpleName()} InitRoomUsrFilling----------->:finish "

    }

}