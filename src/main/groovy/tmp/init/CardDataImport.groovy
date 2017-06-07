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



class CardDataImport {

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
    static cards = mongo.getDB("xy_admin").getCollection("cards")

    static String filePath = "/empty/crontab/game_data_v1.csv"


    static importData(){
        //cards.remove(new BasicDBObject("_id",[$ne: null]))
        new File(filePath).splitEachLine(','){row->
            if(!row[0].equals("id")){
                int i = 1
                def info = ["status": 1,type:row[i++] as Integer, category:row[i++] as Integer, level : row[i++] as Integer,
                            next_level_id : row[i++] as String,  levelup : row[i++] as Integer,
                            coin_rate:row[i++] as Double,coin_min:row[i++] as Integer,coin_max:row[i++] as Integer,
                            cash_rate:row[i++] as Double,cash_min:row[i++] as Integer,cash_max:row[i++] as Integer,
                            exp_rate:row[i++] as Double,exp_min:row[i++] as Integer,exp_max:row[i++] as Integer,
                            diamond_rate:row[i++] as Double,diamond_min:row[i++] as Integer,diamond_max:row[i++] as Integer,
                            ack_rate:row[i++] as Double,ack_min:row[i++] as Integer,ack_max:row[i++] as Integer,
                            def_rate:row[i++] as Double,def_min:row[i++] as Integer,def_max:row[i++] as Integer,
                            steal_rate:row[i++] as Double,steal_min:row[i++] as Integer,steal_max:row[i++] as Integer];
                println info
                cards.update(new BasicDBObject("_id",row[0] as String),new BasicDBObject('$set',info), true, false)
            }
        }

    }

    static void main(String[] args) {
        long l = System.currentTimeMillis()
        importData()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   importData, cost  ${System.currentTimeMillis() - l} ms"
    }
}