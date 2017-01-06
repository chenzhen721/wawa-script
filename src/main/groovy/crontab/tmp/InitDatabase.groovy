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
 *
 * 数据清除
 * 临时文件
 *
 */
class InitDatabase
{
   //static mongo = new Mongo("127.0.0.1", 10000)
   //static mongo = new Mongo("192.168.8.119", 27017)
   //static mongo  = new Mongo(new com.mongodb. MongoURI('mongodb://10.0.5.32:10000,10.0.5.33:10000,10.0.5.34:10000/?w=1&slaveok=true'))
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

    static mongo  = new Mongo(new MongoURI(getProperties('mongo.uri','mongodb://192.168.31.249:27017/?w=1') as String))

    static DAY_MILLON = 24 * 3600 * 1000L

    static long zeroMill = new Date().clearTime().getTime()

    def static xy_tables = ["ban_photos","familys","mails","msgs","operate_logs","photos","rooms","songs"]

    def static admin_tables = ['accuse','applys','bean_ops','cars_bak','channel_users','channels','clocks','config_bak','family_applys','finance_dailyReport','finance_log','gift_categories_bak','gifts_bak','informs','medals_bak','messages','missions_bak','newyear_chances','notices_bak','oplog','oplog.$_id_','oplogs','ops','posters_bak','properties_bak','rooms_whisper','spam_info','stat_brokers','stat_channels','stat_daily','stat_finance_tmp','stat_lives','stat_month','sys_config_bak','union_photos','voting','withdrawl_log']
    
    def static active_tables = ['active_award_logs','charge_day','christmas_circle','cost_logs','qq_share','qq_share_logs','soccer_logs','soccer_match','user_applys','user_voting','voting_log']
    
    def static topic_tables = ['comments','reminds','topics']

    def static sing_tables = ['apply_logs',' award_logs',' exp_logs',' gift_logs',' point_logs',' rank_logs',' rounds',' score_logs',' sing_logs']

    def static union_tables = ['cost_log','orders_weixin','stat_cost','stat_login']

    def static rank_tables = ["active_value","family",'family_star','family_users','feather','gift','gift_last',
                              'posters','sing_star','sing_user','song,star','timer_logs','user','user_room']

    def static log_tables = ['accuse_logs','apple_devices','awards_login','broker_ops','card_logs',
                             'day_login','declarations','exchange_log','family_member_cost','family_rank',
                             'feedbacks','labels','lottery_logs','medal_award_logs','medal_logs','member_applys',
                             'mission_logs','obtain_gifts','praise','room_cost','room_cost_day_star',
                             'room_cost_day_usr','room_cost_family','room_cost_star','room_cost_usr',
                             'room_edit','room_feather','room_feather_day','room_fensi_cost','room_luck',
                             'share_logs','special_gifts','trade_logs','treasure_logs','upai_notify']

    static cleanUser(){
        mongo.getDB("xy_user").getCollection("users").remove(new BasicDBObject("via",new BasicDBObject('$ne': 'robot')))
    }

    static cleanXY(){
       //mongo.getDB("xy").getCollection("users").remove(new BasicDBObject("via",new BasicDBObject('$ne': 'robot')))
        mongo.getDB("xy").getCollectionNames().each {
            String colName = it as String;
            if(xy_tables.contains(colName)){
                println colName +": "+mongo.getDB("xy").getCollection(colName).count()
                mongo.getDB("xy").getCollection(colName).remove(new BasicDBObject())
                println colName +" remove: "+mongo.getDB("xy").getCollection(colName).count()
            }
            //用户表
        }
        //mongo.getDB("xy").getCollection("users").drop()
        //mongo.getDB("xy").getCollection("pretty1000").drop()

    }



    static cleanXYRank(){
        mongo.getDB("xyrank").getCollectionNames().each {
            String colName = it as String;
            if(rank_tables.contains(colName)){
                println colName +": "+mongo.getDB("xyrank").getCollection(colName).count()
                mongo.getDB("xyrank").getCollection(colName).remove(new BasicDBObject())
                println colName +" remove: "+mongo.getDB("xyrank").getCollection(colName).count()
            }
        }
    }

    static cleanXYLog(){
        //mongo.getDB("xylog").getCollection("room_cost").drop()
        //mongo.getDB("xylog").getCollection("room_luck").drop()
        mongo.getDB("xylog").getCollectionNames().each {
            String colName = it as String;
            if(log_tables.contains(colName)){
                println colName +": "+mongo.getDB("xylog").getCollection(colName).count()
                mongo.getDB("xylog").getCollection(colName).remove(new BasicDBObject())
                println colName +" remove: "+mongo.getDB("xylog").getCollection(colName).count()
            }
        }
    }

    static clean(String col, List tables){
        mongo.getDB(col).getCollectionNames().each {
            String colName = it as String;
            if(tables.contains(colName)){
                println colName +": "+mongo.getDB(col).getCollection(colName).count()
                mongo.getDB(col).getCollection(colName).remove(new BasicDBObject())
                println colName +" remove: "+mongo.getDB(col).getCollection(colName).count()
            }
        }
    }


    static void main(String[] args)
    {
        long l = System.currentTimeMillis()
        cleanUser();
        cleanXY();
        cleanXYLog();
        cleanXYRank();

        //clean("xy_admin",admin_tables)
        clean("xyactive",active_tables)
        clean("xytopic",topic_tables)
        clean("xy_sing",sing_tables)
        clean("xy_union",union_tables)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${InitDatabase.class.getSimpleName()} InitRoomUsrFilling----------->:finish "

    }

}