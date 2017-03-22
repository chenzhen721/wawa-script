#!/usr/bin/env groovy
package crontab.st

import com.mongodb.BasicDBObject
@GrabResolver(name = 'restlet', root = 'http://192.168.31.253:8081//nexus/content/groups/public')
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('org.apache.httpcomponents:httpclient:4.2.5'),
        @Grab('redis.clients:jedis:2.1.0'),
        @Grab('com.ttpod:https-util:1.0'),
])
import com.mongodb.DBCollection
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
    }

    /**
     * 构建game_log下game_round集合的索引
     */
    private static void buildGameRoundsIndex() {
        DBCollection rounds = mongo.getDB('game_log').getCollection('game_round')

        /** 组合索引 **/
        def round_room_live_timestamp_index = $$('round_id': 1, 'room_id': 1, 'live_id': 1, 'timestamp': -1)
        rounds.createIndex(round_room_live_timestamp_index, 'round_room_live_timestamp_')
    }

    /**
     * 用户下注
     */
    private static void buildUserBetIndex() {
        DBCollection user_bet = mongo.getDB('game_log').getCollection('user_bet')
        /** 组合索引 **/

        def user_round_room_live_timestamp_index = $$('user_id': 1, 'round_id': 1, 'room_id': 1, 'live_id': 1, 'timestamp': -1)
        user_bet.createIndex(user_round_room_live_timestamp_index, 'user_round_room_live_timestamp_')
    }

    /**
     * 用户奖励
     */
    private static void buildUserLotteryIndex() {
        DBCollection user_lottery = mongo.getDB('game_log').getCollection('user_lottery')
        /** 组合索引 **/

        def user_round_room_live_timestamp_index = $$('user_id': 1, 'round_id': 1, 'room_id': 1, 'live_id': 1, 'timestamp': -1)
        user_lottery.createIndex(user_round_room_live_timestamp_index, 'user_round_room_live_timestamp_')
    }

    /**
     * 订单表
     */
    private static void buildOrderIndex(){
        DBCollection orders = mongo.getDB('shop').getCollection('orders')

        def orders_index = $$('user_id': 1, 'status': 1, 'product_id': 1, 'mobile':1,'last_modify': -1, 'timestamp': -1)
        orders.createIndex(orders_index, '_user_status_product_mobile_last_timestamp_')
    }

    /**
     * 获得钻石表
     */
    private static void buildDiamondLogIndex(){
        DBCollection diamondLog = mongo.getDB('xy_admin').getCollection('diamond_logs')

        def diamond_log_index = $$('user_id': 1, 'diamond_count': 1, 'type':1, 'timestamp': -1)
        diamondLog.createIndex(diamond_log_index, '_user_room_diamond_type_')
    }

    /**
     * 钻石消费表
     */
    private static void buildDiamondCostLogIndex(){
        DBCollection diamondCostLog = mongo.getDB('xy_admin').getCollection('diamond_cost_logs')

        def diamond_cost_log_index = $$('user_id': 1, 'diamond_count': 1, 'type':1, 'timestamp': -1)
        diamondCostLog.createIndex(diamond_cost_log_index, '_user_room_diamond_type_')
    }

    /**
     * 钻石消费表
     */
    private static void buildStarAwardLogIndex(){
        DBCollection starAwardLog = mongo.getDB('game_log').getCollection('star_award_logs')

        def star_award_index = $$('room_id': 1,  'timestamp': -1)
        starAwardLog.createIndex(star_award_index, '_room_timestamp_')
    }

    public static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    public static BasicDBObject $$(Map map) {
        return new BasicDBObject(map);
    }
}