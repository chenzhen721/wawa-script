#!/usr/bin/env groovy

@GrabResolver(name = 'restlet', root = 'http://192.168.31.253:8081/nexus/content/groups/public')
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
])
import com.mongodb.Mongo
import com.mongodb.MongoURI
import com.mongodb.BasicDBObject
import org.apache.commons.lang.math.RandomUtils
import redis.clients.jedis.Jedis


/**
 * MongoCreateIndex
 */

class StarRaceTest{
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

    static mongo  = new Mongo(new MongoURI(getProperties('mongo.uri','mongodb://192.168.31.246:27017/?w=1') as String))
    static api_url  = getProperties('api.domain','http://localhost:8080/')

    static final String jedis_host = getProperties("main_jedis_host", "192.168.31.246")
    static final String chat_jedis_host = getProperties("chat_jedis_host", "192.168.31.246")
    static final String live_jedis_host = getProperties("live_jedis_host", "192.168.31.246")
    static final String user_jedis_host = getProperties("user_jedis_host", "192.168.31.246")

    static final Integer main_jedis_port = getProperties("main_jedis_port",6379) as Integer
    static final Integer chat_jedis_port = getProperties("chat_jedis_port",6379) as Integer
    static final Integer live_jedis_port = getProperties("live_jedis_port",6379) as Integer
    static final Integer user_jedis_port = getProperties("user_jedis_port",6379) as Integer

    static mainRedis = new Jedis(jedis_host, main_jedis_port, 50000)
    static liveRedis = new Jedis(live_jedis_host, live_jedis_port, 50000)
    static userRedis = new Jedis(user_jedis_host, user_jedis_port, 50000)

    // test
    //static access_token='1b29cd74980ebe1f415f406bbd174450'
    static access_token='c321fa5b8d03780e45bd7b52a5a6456d'
    static users = mongo.getDB("xy").getCollection("users");
    static rooms = mongo.getDB("xy").getCollection("rooms");

    static List<Integer> stars;
    static void main(String[] args) {
        //goRace()
        printlnRoundWinner();
    }

    static goRace(){
        initStar();
        init();
        round1();
        round2();
        round3();
        round4();
        round5();
        
    }

    static init(){
        println new URL("${api_url}starrace2016/init_data?access_token=${access_token}").getText("utf-8")
    }

    static round1(){
        println new URL("${api_url}starrace2016/change?access_token=${access_token}&date=2016-12-01%2000:00:01").getText("utf-8")
        sendGiftToStar()
        println new URL("${api_url}starrace2016/change?access_token=${access_token}&date=2016-12-06%2000:00:01").getText("utf-8")
        Thread.sleep(1000)
    }

    static round2(){
        sendGiftToStar()
        println new URL("${api_url}starrace2016/change?access_token=${access_token}&date=2016-12-07%2000:00:01").getText("utf-8")
        sendGiftToStar()
        println new URL("${api_url}starrace2016/change?access_token=${access_token}&date=2016-12-08%2000:00:01").getText("utf-8")
        sendGiftToStar()
        println new URL("${api_url}starrace2016/change?access_token=${access_token}&date=2016-12-09%2000:00:01").getText("utf-8")
        sendGiftToStar()
        println new URL("${api_url}starrace2016/change?access_token=${access_token}&date=2016-12-10%2000:00:01").getText("utf-8")
        sendGiftToStar()
        println new URL("${api_url}starrace2016/change?access_token=${access_token}&date=2016-12-11%2000:00:01").getText("utf-8")
        new URL("${api_url}starraceinfo2016/star_rank?size=20&round=2").getText("utf-8")
        Thread.sleep(1000)
    }

    static round3(){
        println new URL("${api_url}starrace2016/change?access_token=${access_token}&date=2016-12-12%2000:00:01").getText("utf-8")
        sendGiftToStar()
        println new URL("${api_url}starrace2016/change?access_token=${access_token}&date=2016-12-18%2000:00:01").getText("utf-8")
        new URL("${api_url}starraceinfo2016/star_rank?size=20&round=3").getText("utf-8")
        Thread.sleep(1000)
    }

    static round4(){
        println new URL("${api_url}starrace2016/change?access_token=${access_token}&date=2016-12-19%2000:00:01").getText("utf-8")
        sendGiftToStar()
        println new URL("${api_url}starrace2016/change?access_token=${access_token}&date=2016-12-20%2000:00:01").getText("utf-8")
        new URL("${api_url}starraceinfo2016/star_rank?size=20&round=4").getText("utf-8")
        Thread.sleep(1000)
    }

    static round5(){
    
        println new URL("${api_url}starrace2016/change?access_token=${access_token}&date=2016-12-22%2000:00:01").getText("utf-8")
        sendGiftToStar()
        println new URL("${api_url}starrace2016/change?access_token=${access_token}&date=2016-12-23%2000:00:01").getText("utf-8")
        new URL("${api_url}starraceinfo2016/star_rank?size=20&round=5").getText("utf-8")
        
        sendGiftToStar()
        println new URL("${api_url}starrace2016/change?access_token=${access_token}&date=2016-12-24%2000:00:01").getText("utf-8")
        new URL("${api_url}starraceinfo2016/star_rank?size=20&round=5").getText("utf-8")
      
       
        sendGiftToStar()
        println new URL("${api_url}starrace2016/change?access_token=${access_token}&date=2016-12-25%2000:00:01").getText("utf-8")
        new URL("${api_url}starraceinfo2016/star_rank?size=20&round=5").getText("utf-8")
         
        sendGiftToStar()
        println new URL("${api_url}starrace2016/change?access_token=${access_token}&date=2016-12-26%2000:00:01").getText("utf-8")
        new URL("${api_url}starraceinfo2016/star_rank?size=20&round=5").getText("utf-8")
        
        sendGiftToStar()
        println new URL("${api_url}starrace2016/change?access_token=${access_token}&date=2016-12-27%2000:00:01").getText("utf-8")
        new URL("${api_url}starraceinfo2016/star_rank?size=20&round=5").getText("utf-8")
       
        sendGiftToStar()
        println new URL("${api_url}starrace2016/change?access_token=${access_token}&date=2016-12-28%2000:00:01").getText("utf-8")
        new URL("${api_url}starraceinfo2016/star_rank?size=20&round=5").getText("utf-8")
        
        sendGiftToStar()
        println new URL("${api_url}starrace2016/change?access_token=${access_token}&date=2016-12-29%2000:00:01").getText("utf-8")
        new URL("${api_url}starraceinfo2016/star_rank?size=20&round=5").getText("utf-8")
        
    }

    static void initStar(){
        List ids = users.find(new BasicDBObject(priv:2, status:true), new BasicDBObject(_id:1)).sort(new BasicDBObject(_id:1)).toArray()*._id
        stars =  rooms.find(new BasicDBObject(_id : [$in:ids]), new BasicDBObject(_id:1)).sort(new BasicDBObject(_id:1)).limit(100).toArray()*._id
        println stars.size();
    }

    static void sendGiftToStar(){
        stars.each {Integer starId ->
            sendGift(starId, RandomUtils.nextInt(500))
        }
    }
    static sendGift(Integer starId, Integer count){
        def jsonText = new URL("${api_url}room/send_gift?access_token=${access_token}&id1=${starId}&id2=154&count=${count}").getText("utf-8")
        //println jsonText
    }

    static void printlnRoundWinner(){
        def round1 = mainRedis.smembers("2016NianDuStar:race:star:wins:1")
        def round2 =  mainRedis.smembers("2016NianDuStar:race:star:wins:2")
        def round3 =  mainRedis.smembers("2016NianDuStar:race:star:wins:3")
        def round4 =  mainRedis.smembers("2016NianDuStar:race:star:wins:4")

        println "====================== round 1 =================== "
        println round1
        println "====================== round 2 =================== "
        println round2
        println "====================== round 3 =================== "
        println round3
        println "====================== round 4 =================== "
        println round4


        println "r1 : ${round1.containsAll(round2)}  ${round1.containsAll(round3)}  ${round1.containsAll(round4)} "
        println "r2 : ${round2.containsAll(round3)}  ${round2.containsAll(round4)}"
        round2.removeAll(round3)
        println "r3 : ${round2.containsAll(round4)}"
    }
}



