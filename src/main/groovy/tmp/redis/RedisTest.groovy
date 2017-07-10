#!/usr/bin/env groovy

@Grapes([
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
])

import groovy.json.JsonSlurper
import redis.clients.jedis.BinaryClient
import redis.clients.jedis.Connection
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPubSub
import redis.clients.jedis.JedisPoolConfig
import redis.clients.jedis.JedisPool
/**
 * redis 测试
 */

class RedisTest{
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

    static Map<String, String> userCounts = Collections.emptyMap();
    static Map<String, String> userTimes = Collections.emptyMap();

    static void main(String[] args){
        //def day = new Date().format('yyyyMMdd')
        /*def day = '20160508'
        String room_user_count_key = "live:room:user:count:${day}".toString();
        String room_user_time_key = "live:room:user:count:times:${day}".toString();
        userCounts = liveRedis.hgetAll(room_user_count_key)
        userTimes = liveRedis.hgetAll(room_user_time_key)
        println "userCounts : ${userCounts},  userTimes: ${userTimes}"

        String star_id = "11524665"
        if(userCounts.size() > 0 && userTimes.size() > 0){
            Integer userCount = (userCounts[star_id] ?: 0 )as Integer
            Integer userTime = (userTimes[star_id] ?: 0)  as Integer
            println "${star_id} : ${userCount}, ${userTime}  ${(userCount / userTime) as Integer}"
        }*/
        /*
        Set<String> keys =mainRedis.keys("star:recomm:rank:*")
        keys.each {String key ->
            printZset(key);
        }
        */
        /*int last_week = geWeekOfYear(-1)
        int index = 0;
        while(index < 60){
            if(index != last_week && index != last_week+1){
                Set<String> keys =mainRedis.keys("room:guarder:rank:week:${index}:*".toString())
                keys.each {String key ->
                    println key;
                    mainRedis.del(key)
                }
            }
            index++;
        }*/
        //printZset('race:star:rank:app_meme_luck_gift:1', mainRedis)
        //userRedis.sadd("room:guarder:24275864", "29735214");
        //println userRedis.smembers("room:guarder:24275864")
        //String key = "authcode:smsmobile:85254149858"
        //liveRedis.del(key)
        /*println liveRedis.get(key)
        println liveRedis.ttl(key)
        println liveRedis.get("authcode:smsmobile:total:85254149858")
        println liveRedis.get("authcode:smsip:210.22.151.242, 192.168.1.34")
        println liveRedis.get("authcode:smsip:210.22.151.242, 192.168.1.35")*/
        //println liveRedis.smembers("limit:smsmobile:list")
        //liveRedis.srem("authcode:limit:smsmobile:list","85254149858")
        //println liveRedis.sismember("authcode:limit:smsmobile:list","85254149858")
        //println mainRedis.get("weixin:wx719ebd95f287f861:token")
        //mainRedis.del("weixin:wx719ebd95f287f861:token")
        /*Map starAndUsers = userRedis.hgetAll('week:star:award:list')
        Set<String> stars = new HashSet<>();
        starAndUsers.each {String star, String priv ->
            if(priv.equals("1")){
                println star + ":" + priv
                stars.add("room:week:star:users:${star}".toString());
            }
        }
        String room_week_star_key = "room:week:star:users:*".toString()
        Set<String> keys = userRedis.keys(room_week_star_key)
        keys.each {String key ->
            if(!stars.contains(key)){
                //userRedis.del(key)
            }
            printSet(key, userRedis);
        }*/
        //printZset('2016NianDuStar:race:star:rank:2:2016-12-06:0', mainRedis)
        //printZset('2016NianDuStar:race:star:rank:2:2016-12-07:1', mainRedis)
        //printZset('2016NianDuStar:race:star:rank:2:2016-12-08:2', mainRedis)
        //printZset('2016NianDuStar:race:star:rank:2:2016-12-10:3', mainRedis)
        //mainRedis.zrem('2016NianDuStar:race:star:rank:2:2016-12-10:3','22881601')
        //printSet('2016NianDuStar:race:star:pks:3', mainRedis)
        /*printZset('2016NianDuStar:race:star:rank:2:2016-12-10:0', mainRedis)
        printZset('2016NianDuStar:race:star:rank:2:2016-12-10:2', mainRedis)
        printZset('2016NianDuStar:race:star:rank:2:2016-12-10:1', mainRedis)
        printZset('2016NianDuStar:race:star:rank:2:2016-12-10:3', mainRedis)*/
        //println printSet('2016NianDuStar:race:star:promoteds:5',mainRedis)
        //22149607:27482733, 26175714:8168991, 2912313:25974419
        /*mainRedis.hset('room:recommend:list:user', '22149607', '27482733_0')
        mainRedis.hset('room:recommend:list:user', '26175714', '8168991_0')
        mainRedis.hset('room:recommend:list:user', '2912313', '25974419_0')
        println mainRedis.hgetAll('room:recommend:list:user')*/
        //printZset('2016NianDuStar:race:star:rank:3', mainRedis)
        println mainRedis.get("laihou:family:ack:1254788")
        println mainRedis.ttl("laihou:family:ack:1254788")
    }

    static String[] keys = ['2016XuebiRace:race:user:rank:total']
    static removeGarbigeKey(){
        mainRedis.del(keys)
    }
    static printZset(String key, Jedis redis){
        Set<redis.clients.jedis.Tuple> ranks = redis.zrevrangeWithScores(key, 0, -1)
        println key
        for(redis.clients.jedis.Tuple rank : ranks){
            println rank.getElement() + " : " + rank.getScore();
        }
    }


    static printSet(String key, Jedis redis){
        println key + ":" + redis.ttl(key)
        Set<String> members = redis.smembers(key)
        for(String member : members){
            println member;
        }
    }

    private static int geWeekOfYear(int amount) {
        Calendar cal =  Calendar.getInstance()
        //cal.setTime(Date.parse("yyyy-MM-dd HH:mm" ,"2015-05-11 00:02"))
        cal.setFirstDayOfWeek(Calendar.MONDAY)
        cal.add(Calendar.WEEK_OF_YEAR, amount)
        return cal.get(Calendar.WEEK_OF_YEAR);
    }
}



