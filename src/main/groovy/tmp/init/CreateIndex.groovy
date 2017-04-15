#!/usr/bin/env groovy
package crontab.st

import com.mongodb.BasicDBObject
import com.mongodb.DB
@GrabResolver(name = 'restlet', root = 'http://192.168.31.253:8081//nexus/content/groups/public')
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('org.apache.httpcomponents:httpclient:4.2.5'),
        @Grab('redis.clients:jedis:2.1.0'),
        @Grab('com.ttpod:https-util:1.0'),
])
import com.mongodb.DBCollection
import com.mongodb.DBObject
import com.mongodb.Mongo
import com.mongodb.MongoURI

/**
 * 爱玩项目初始化脚本
 */
class CreateIndex {
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

    static mongo = new Mongo(new MongoURI(getProperties('mongo.uri', 'mongodb://192.168.31.231:20000,192.168.31.236:20000,192.168.31.231:20001/?w=1&slaveok=true') as String))
    static DBCollection users = mongo.getDB('xy').getCollection('users')

    /**
     * 初始化爱玩直播游戏数据库脚本
     * @param args
     */
    static void main(String[] args) {
        buildGameRoundsIndex()
        buildUserBetIndex()
        buildUserLotteryIndex()
        //shop 订单表，加钻石，减钻石,主播分成表
        buildOrderIndex()
        buildDiamondLogIndex()
        buildDiamondCostLogIndex()
        buildStarAwardLogIndex()

        //game_log 红包现金领取日志 红包兑换日志，红包现金提现日志
        buildRedPacketLogIndex()
        buildRedPacketCostLogIndex()
        buildRedPacketApplyLogIndex()
    }
    /**
     * 构建game_log下game_round集合的索引
     */
    private static void buildGameRoundsIndex() {
        DBCollection rounds = mongo.getDB('game_log').getCollection('game_round')
        def indexName = '_round_room_live_timestamp_'
        def round_room_live_timestamp_index = $$('round_id': 1, 'room_id': 1, 'live_id': 1, 'timestamp': -1)
        createIfNotAbsent(rounds,round_room_live_timestamp_index,indexName)

    }

    /**
     * 用户下注
     */
    private static void buildUserBetIndex() {
        DBCollection user_bet = mongo.getDB('game_log').getCollection('user_bet')
        def indexName = '_user_round_room_live_timestamp_'
        def user_round_room_live_timestamp_index = $$('user_id': 1, 'round_id': 1, 'room_id': 1, 'live_id': 1, 'timestamp': -1)
        createIfNotAbsent(user_bet,user_round_room_live_timestamp_index,indexName)

    }

    /**
     * 用户奖励
     */
    private static void buildUserLotteryIndex() {
        DBCollection user_lottery = mongo.getDB('game_log').getCollection('user_lottery')
        def indexName = '_user_round_room_live_timestamp_'
        def user_round_room_live_timestamp_index = $$('user_id': 1, 'round_id': 1, 'room_id': 1, 'live_id': 1, 'timestamp': -1)
        createIfNotAbsent(user_lottery,user_round_room_live_timestamp_index,indexName)

    }

    /**
     * 订单表
     */
    private static void buildOrderIndex(){
        DBCollection orders = mongo.getDB('shop').getCollection('orders')
        def indexName = '_user_status_product_mobile_last_timestamp_'
        def orders_index = $$('user_id': 1, 'status': 1, 'product_id': 1, 'mobile':1,'last_modify': -1, 'timestamp': -1)
        createIfNotAbsent(orders,orders_index,indexName)
    }

    /**
     * 获得钻石表
     */
    private static void buildDiamondLogIndex(){
        DBCollection diamondLog = mongo.getDB('xy_admin').getCollection('diamond_logs')
        def indexName = '_user_room_diamond_type_'
        def diamond_log_index = $$('user_id': 1, 'diamond_count': 1, 'type':1, 'timestamp': -1)
        createIfNotAbsent(diamondLog,diamond_log_index,indexName)

    }

    /**
     * 钻石消费表
     */
    private static void buildDiamondCostLogIndex(){
        DBCollection diamondCostLog = mongo.getDB('xy_admin').getCollection('diamond_cost_logs')
        def indexName = '_user_room_diamond_type_'
        def diamond_cost_log_index = $$('user_id': 1, 'diamond_count': 1, 'type':1, 'timestamp': -1)
        createIfNotAbsent(diamondCostLog,diamond_cost_log_index,indexName)
    }

    /**
     * 钻石消费表
     */
    private static void buildStarAwardLogIndex(){
        DBCollection starAwardLog = mongo.getDB('game_log').getCollection('star_award_logs')
        def star_award_index = $$('room_id': 1,  'timestamp': -1)
        def indexName = '_room_timestamp_'
        createIfNotAbsent(starAwardLog,star_award_index,indexName)
    }

    /**
     * 红包领取日志
     */
    private static void buildRedPacketLogIndex(){
        DBCollection redPacketLogs = mongo.getDB('game_log').getCollection('red_packet_logs')
        def red_packet_logs_index = $$('user_id': 1,  'timestamp': -1,'date':1,'acquire_type':1,'type':1)
        def indexName = '_user_date_type_acquire_type_timestamp_'
        createIfNotAbsent(redPacketLogs,red_packet_logs_index,indexName)
    }

    /**
     * 红包消费日志
     */
    private static void buildRedPacketCostLogIndex(){
        DBCollection redPacketCostLogs = mongo.getDB('game_log').getCollection('red_packet_cost_logs')
        def red_packet_cost_logs_index = $$('user_id': 1,  'timestamp': -1,'date':1,'cost_type':1,'type':1)
        def indexName = '_user_date_type_cost_type_timestamp_'
        createIfNotAbsent(redPacketCostLogs,red_packet_cost_logs_index,indexName)
    }

    /**
     * 红包提现日志表
     */
    private static void buildRedPacketApplyLogIndex(){
        DBCollection redPacketApplyLogs = mongo.getDB('game_log').getCollection('red_packet_apply_logs')
        def red_packet_apply_logs_index = $$('user_id': 1,  'timestamp': -1,'date':1,'account':1,'status':1)
        def indexName = '_user_date_status_account_timestamp_'
        createIfNotAbsent(redPacketApplyLogs,red_packet_apply_logs_index,indexName)
    }


    /**
     * 根据索引名称判断是否有索引 没有则创建
     * @param collection
     * @param indexInfo
     * @param indexName
     */
    private static void createIfNotAbsent(DBCollection collection,DBObject indexInfo,String indexName){
        def isExists = Boolean.TRUE
        collection.indexInfo.each {
            if(it.name == indexName){
                isExists = Boolean.FALSE
            }
        }
        if(isExists){
            collection.createIndex(indexInfo,indexName)
        }
    }

    public static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    public static BasicDBObject $$(Map map) {
        return new BasicDBObject(map);
    }
}