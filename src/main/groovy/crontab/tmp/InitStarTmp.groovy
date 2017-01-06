#!/usr/bin/env groovy
package crontab.tmp

import com.mongodb.BasicDBObject
import com.mongodb.DBObject
@Grapes([
@Grab('org.mongodb:mongo-java-driver:2.14.2'),
@Grab('commons-lang:commons-lang:2.6'),
@Grab('redis.clients:jedis:2.1.0'),
]) import com.mongodb.Mongo

/**
 *
 *
 * date: 13-2-28 下午2:46
 * @author: yangyang.cong@ttpod.com
 */
class InitStarTmp {

    //static mongo = new Mongo("127.0.0.1", 10000)
    static mongo  = new Mongo(new com.mongodb. MongoURI('mongodb://10.0.5.32:10000,10.0.5.33:10000,10.0.5.34:10000/?w=1&slaveok=true'))
    static DAY_MILLON = 24 * 3600 * 1000L
    static long zeroMill = new Date().clearTime().getTime()

    static apply(Integer xy_user_id,String nick_name)
    {
        def tmp = System.currentTimeMillis()
        def id = xy_user_id +"_"+ tmp
        Integer brokerId = 14978061
        def apply = new BasicDBObject(_id:id,broker:brokerId,sex:0,status:2,nick_name:nick_name,
                real_name:'李在元',bank:'中国工商银行',bank_location:'北京中国工商银行地安门支行',
                bank_name:'中国工商银行',bank_id:'6212260200036221295',bank_user_name:'隋洋',
                sfz:'370205198608082538',tel:'18605320571',qq:'99128270',address:'',lastmodif:tmp,xy_user_id:xy_user_id)

        int applyN =  mongo.getDB("xy_admin").getCollection("applys").save(apply).getN()
        println "applyN---------->:${applyN}"
        def userDB = mongo.getDB("xy").getCollection("users")
        int userN =  userDB.update(new BasicDBObject(_id:xy_user_id),new BasicDBObject('$set',new BasicDBObject(priv:2,star:
                new BasicDBObject(room_id:xy_user_id,timestamp:tmp,broker:brokerId,sex:0)))).getN()
        println "userN---------->:${userN}"

        def room =  new BasicDBObject( _id:xy_user_id,xy_star_id:xy_user_id,live:Boolean.FALSE, bean : 0, visiter_count:0,found_time:tmp,room_ids:xy_user_id.toString(),nick_name: nick_name ,real_sex:0)
        int roomN = mongo.getDB("xy").getCollection("rooms").save(room).getN()
        println "roomN---------->:${roomN}"

        def brokerN = userDB.update(new BasicDBObject(_id:brokerId),
                new BasicDBObject($addToSet:['broker.stars':xy_user_id],$inc:['broker.star_total':1])).getN()
        println "brokerN---------->:${brokerN}"

        /*def jsonText = new URL("http://api.ttxiu.com/java/flushuser/"+xy_user_id).getText("utf-8")
        println "jsonText---------->:${jsonText}"*/
    }



    static void main(String[] args)
    {
        long l = System.currentTimeMillis()

        def userDB = mongo.getDB("xy").getCollection("users")
        def users = userDB.find(new BasicDBObject(tuid:['$in': [234291732,
                234291733,
                234291734,
                234291735,
                234291736,
                234291737,
                234291738,
                234291739,
                234291740,
                234291741,
                234291742,
                234291743,
                234291744,
                234291745,
                234291746,
                234291747,
                234291748,
                234291749,
                234291750,
                234291752,
                234291753,
                234291754,
                234291755,
                234291756,
                234291757,
                234291758,
                234291759,
                234291760,
                234291761,
                234291762,
                234291763,
                234291764,
                234291765,
                234291766,
                234291767,
                234291768,
                234291769,
                234291770,
                234291771,
                234291772,
                234291773,
                234291774,
                234291775,
                234291776,
                234291777,
                234291778,
                234291779,
                234291780,
                234291781,
                234291782,
                234291783,
                234291784,
                234291785,
                234291787,
                234291788,
                234291789,
                234291790,
                234291791,
                234291792,
                234291793,
                234291794,
                234291795,
                234291796,
                234291797,
                234291798,
                234291799,
                234291800,
                234291801,
                234291802,
                234291803,
                234291804,
                234291805,
                234291806,
                234291807,
                234291808,
                234291809,
                234291810,
                234291811,
                234291812,
                234291813,
                234291814,
                234291816,
                234291817,
                234291818,
                234291819,
                234291820,
                234291821,
                234291822,
                234291823,
                234291824,
                234291826,
                234291828,
                234291829,
                234291830,
                234291831,
                234291832]]).append('priv',2),new BasicDBObject(_id:1,nick_name:1)).toArray()
        for(DBObject user: users)
        {
            Integer  xy_user_id = user.get("_id") as Integer
            String  nick_name = user.get("nick_name") as String
            apply(xy_user_id,nick_name)
        }

        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitStarTmp.class.getSimpleName()},apply cost  ${System.currentTimeMillis() -l} ms"

    }



}