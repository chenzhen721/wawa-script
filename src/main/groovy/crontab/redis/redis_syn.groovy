#!/usr/bin/env groovy
package crontab

@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
])
import redis.clients.jedis.Jedis

/**
 * redis 核心用户数据迁移
 */
class redis_syn {

    static Properties props = null;
    static String profilepath="/empty/crontab/db.properties";

    static getProperties(String key, Object defaultValue){
        try {
            if(props == null){
                props = new Properties();
                props.load(new FileInputStream(profilepath));
            }
        } catch (Exception e) {
            println e;
        }
        return props.get(key, defaultValue)
    }


    static final String jedis_host = getProperties("main_jedis_host", "192.168.31.246")
    static final String chat_jedis_host = getProperties("chat_jedis_host", "192.168.31.246")
    static final String live_jedis_host = getProperties("live_jedis_host", "192.168.31.246")
    static final String user_jedis_host = getProperties("user_jedis_host", "192.168.31.246")

    static final Integer main_jedis_port = getProperties("main_jedis_port",6379) as Integer
    static final Integer chat_jedis_port = getProperties("chat_jedis_port",6379) as Integer
    static final Integer live_jedis_port = getProperties("live_jedis_port",6379) as Integer
    static final Integer user_jedis_port = getProperties("user_jedis_port",6379) as Integer

    static redis = new Jedis(jedis_host, main_jedis_port)
    static chatRedis = new Jedis(chat_jedis_host,chat_jedis_port)
    static userRedis = new Jedis(user_jedis_host,user_jedis_port, 50000)
    static liveRedis = new Jedis(live_jedis_host,live_jedis_port)
    //static final long delay = 45 * 1000L
    static long zeroMill = new Date().clearTime().getTime()
    static DAY_MILLON = 24 * 3600 * 1000L

    /**
     *
     room:guarder:starId
     user:${user_id}:car
     guard:car:${roomId}:${user_id}
     all:room:chat:limit

     * @param arg
     */
    public static main(arg) {
       /*println "connection 41.."
       println redis.info();
       println "connection 44.."
       println userRedis.info()*/

       /*synCar();
       synGuardCar();
       synRoomGuarder();
       synchatLimit();*/

        //String starId = "5156886";
        //Double ponits = 34627;
        //redis.zadd("2015YearCeremony:race:star:rank:6", ponits, starId)
        //println redis.smembers("sign:chest:prettnum:list")
        //println redis.llen("sign:chest:prettnum:list")
       // synKickShutup();
        //redis.set("card:game:limited:3028703", '25100')
        //println redis.get("card:game:limited:3028703")
        //redis.expire("card:game:limited:3028703", 35159)
        //println redis.ttl("card:game:limited:3028703")
        //println userRedis.hgetAll('token:83a85ddddb0ea4ab2d1d1e26f4ed1b8c')
        //checkTokenErorrInfo();
        //println redis.ttl("index:recommend:data:expire:6")
    }


    static void synCar(){
        Set<String> carKeys =redis.keys("user:*:car")
        println "user cars key ====:  " + carKeys
        /*carKeys.each {String key ->
            def carId = redis.get(key).toString()
            def time = redis.ttl(key).toInteger()
            println "carId:${carId}  time: ${time}"
            userRedis.setex(key, time, carId)
        }
        println "new user cars ================="
        Set<String> newcarKeys =userRedis.keys("user:*:car")
        newcarKeys.each {String key ->
            def carId = userRedis.get(key).toString()
            def time = userRedis.ttl(key).toInteger()
            println "carId:${carId}  time: ${time}"
        }*/
    }

    static void synRoomGuarder(){
        Set<String> carKeys = redis.keys("room:guarder:*")
        println "keys guarder ====:  " + carKeys
        /*carKeys.each {String key ->
            try{
                def members = redis.smembers(key)
                def time = redis.ttl(key).toInteger()
                println "members:${members}  time: ${time}"
                members.each {String userId ->
                    userRedis.sadd(key, userId)
                }
            }catch (Exception e){

            }
        }*/
    }

    static void synGuardCar(){
        Set<String> carKeys =redis.keys("guard:car:*")
        println "guard cars key ====:  " + carKeys
        /*carKeys.each {key ->
            def carId = redis.get(key)
            def time = redis.ttl(key)
            println "carId:${carId}  time: ${time}"
        }*/

    }


    static void synchatLimit(){
        println "synchatLimit ====:  " + redis.hgetAll("all:room:chat:limit")
    }


    static void synKickShutup(){
        Set<String> keys =redis.keys("room:*:shutup:*")
        keys.each {String key ->
            def value = redis.get(key).toString()
            def time = redis.ttl(key).toInteger()
            println "${key}:${value}  time: ${time}"
            if(time > 0)
                userRedis.setex(key, time, value)
        }
        Set<String> kickkeys =redis.keys("room:*:kick:*")
        kickkeys.each {String key ->
            def value = redis.get(key).toString()
            def time = redis.ttl(key).toInteger()
            println "${key}:${value}  time: ${time}"
            if(time > 0)
                userRedis.setex(key, time, value)
        }
    }

    //检测错误token用户信息
    static checkTokenErorrInfo(){
        Set<String> keys =userRedis.keys("token:*")
        keys.each {String key ->
            Map datas = userRedis.hgetAll(key)
            def time = userRedis.ttl(key).toInteger()
            if(datas["_id"] == null){
                userRedis.del(key)
                println "${datas}  time: ${time}"
            }
        }
    }
}
