#!/usr/bin/env groovy
package crontab.st

import com.mongodb.BasicDBObject
import com.mongodb.DBCollection
@Grapes([
@Grab('org.mongodb:mongo-java-driver:2.14.2'),
@Grab('commons-lang:commons-lang:2.6'),
@Grab('redis.clients:jedis:2.1.0'),
]) import com.mongodb.Mongo

import java.text.SimpleDateFormat

/**
 *
 *
 * date: 13-10-16 下午2:46
 * @author: haigen.xiong@ttpod.com
 */
class StaticsBaiduXml {

    //static mongo = new Mongo("127.0.0.1", 10000)
    //static mongo = new Mongo(new com.mongodb.MongoURI('mongodb://10.0.5.32:10000,10.0.5.33:10000,10.0.5.34:10000/?w=1&slaveok=true'))


    static baiduStaticsXml()
    {
        def xmlText = new URL("http://api.memeyule.com/baidu/static_star_xml?size=1000").getText("utf-8")
        def fold = new File("/empty/www.2339.com/baidu/")
        if (!fold.exists()) {
            fold.mkdir()
        }
        new File(fold,"baidu.xml").setText(xmlText,"utf-8")
       // new File(fold,new Date().format('yyyy-MM-dd')+'.xml').setText(xmlText,"utf-8")
    }



    public static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    static void main(String[] args) {

        long l = System.currentTimeMillis()

        baiduStaticsXml()

        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   StaticsBaiduXml, cost  ${System.currentTimeMillis() - l} ms"

    }

}

