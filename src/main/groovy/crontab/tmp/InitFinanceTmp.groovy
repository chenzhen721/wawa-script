#!/usr/bin/env groovy
package crontab.tmp

import com.mongodb.BasicDBObject
import com.mongodb.DBObject
@Grapes([
@Grab('org.mongodb:mongo-java-driver:2.14.2'),
@Grab('commons-lang:commons-lang:2.6'),
@Grab('redis.clients:jedis:2.1.0'),
]) import com.mongodb.Mongo
import com.mongodb.MongoURI

import java.text.SimpleDateFormat

/**
 *
 *
 * date: 13-2-28 下午2:46
 * @author: yangyang.cong@ttpod.com
 */
class InitFinanceTmp {

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
    static mongo  = new Mongo(new MongoURI(getProperties('mongo.uri','mongodb://192.168.31.246:27017/?w=1') as String))

    static DAY_MILLON = 24 * 3600 * 1000L
    static long zeroMill = new Date().clearTime().getTime()

    static Long yesTday = zeroMill - DAY_MILLON

   static userRemain()
    {
        def coinQ = new BasicDBObject('finance.coin_count':[$gt:0])
        def coinList = mongo.getDB("xy").getCollection("users").
                find(coinQ,new BasicDBObject('finance.coin_count':1))
                .toArray()
       def coin = coinList.sum {it.finance.coin_count} as Long
       println "totalcoin:---->:${coin}"

        def spendQ = new BasicDBObject('finance.coin_spend_total':[$gt:0])
        def spendList = mongo.getDB("xy").getCollection("users").
                find(spendQ,new BasicDBObject('finance.coin_spend_total':1))
                .toArray()

       def spend_coin =  spendList.sum {it.finance.coin_spend_total} as Long
       println "spend_coin---->:${spend_coin}"

        def starQ = new BasicDBObject('finance.bean_count':[$gt:0],priv:2)
        def starList = mongo.getDB("xy").getCollection("users").
                find(starQ,new BasicDBObject('finance.bean_count',1))
                .toArray()
        def star_bean = starList.sum {it.finance.bean_count?:0} as Long
        println "star_bean---->:${star_bean}"

        def starObtainQ = new BasicDBObject('finance.bean_count_total':[$gt:0],priv:2)
        def starObtainList = mongo.getDB("xy").getCollection("users").
                find(starObtainQ,new BasicDBObject('finance.bean_count_total',1))
                .toArray()
        def star_obtain_bean = starObtainList.sum {it.finance.bean_count_total?:0} as Long
        println "star_obtain_bean---->:${star_obtain_bean}"

        def usrQ = new BasicDBObject('finance.bean_count':[$gt:0],priv:[$nin:[2]])
        def userList = mongo.getDB("xy").getCollection("users").
                find(usrQ,new BasicDBObject('finance.bean_count',1))
                .toArray()
        def usr_bean = userList.sum {it.finance.bean_count?:0} as Long
        println "usr_bean---->:${usr_bean}"

        def usrObtainQ = new BasicDBObject('finance.bean_count_total':[$gt:0],priv:[$nin:[2]])
        def usrObtainList = mongo.getDB("xy").getCollection("users").
                find(usrObtainQ,new BasicDBObject('finance.bean_count_total',1))
                .toArray()
        def usr_obtain_bean = usrObtainList.sum {it.finance.bean_count_total?:0} as Long
        println "usr_obtain_bean---->:${usr_obtain_bean}"

        def  coll = mongo.getDB('xy_admin').getCollection('stat_finance_tmp')
        def id = "finance_" + new Date(yesTday).format("yyyyMMdd")
        def obj =  new BasicDBObject(_id:id,total_coin:coin,coin_spend_total:spend_coin,
                star_total_bean:star_bean,star_obtain_bean:star_obtain_bean,
                usr_total_bean:usr_bean,usr_obtain_bean:usr_obtain_bean,
                timestamp:System.currentTimeMillis(),sj:new Date(System.currentTimeMillis()).format("yyyy-MM-dd HH:mm:ss"))
        coll.save(obj)
    }

    static tmpDianLe(){
        def finance_log = mongo.getDB('xy_admin').getCollection('finance_log')
        def trade_logs = mongo.getDB('xylog').getCollection('trade_logs')
        def dianle = finance_log.find(new BasicDBObject(via:"dianle", "shop": "dianle")).toArray()
        def list = new ArrayList(500)
        dianle.each {
            def obj =  new BasicDBObject(_id:it['_id'])
            obj.append('uid',it['user_id'])
            obj.append('via','dianle')
            obj.append('time',it['timestamp'])
            obj.append('resp',[qd:it['qd'],coin:it['coin'],remark:it['remark']])
            list.add(obj)
        }
        trade_logs.insert(list)
        finance_log.remove(new BasicDBObject(via:"dianle", "shop": "dianle"))
    }


    static void main(String[] args)
    {
        long l = System.currentTimeMillis()

        long begin = l
        // userRemain()
        tmpDianLe()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitFinanceTmp.class.getSimpleName()},tmpDianLe cost  ${System.currentTimeMillis() -l} ms"

        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitFinanceTmp.class.getSimpleName()},------------>:finish cost  ${System.currentTimeMillis() -begin} ms"
    }

}