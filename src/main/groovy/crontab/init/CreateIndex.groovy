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

    static mongo = new Mongo(new MongoURI(getProperties('mongo.uri', 'mongodb://192.168.31.231:10000,192.168.31.236:10000,192.168.31.231:10001/?w=1&slaveok=true') as String))
    static DBCollection users = mongo.getDB('xy').getCollection('users')

    /**
     * 初始化爱玩直播游戏数据库脚本
     * @param args
     */
    static void main(String[] args) {
        init()
        buildGameRoundsIndex()
        buildUserBetIndex()
        buildUserLottery()
    }

    private static void init(){
        mongo.getDB('game_log').dropDatabase()
        mongo.getDB('game_log').createCollection('game_round',null)
        mongo.getDB('game_log').createCollection('user_bet',null)
        mongo.getDB('game_log').createCollection('user_lottery',null)
    }

    /**
     * 构建game_log下game_round集合的索引
     */
    private static void buildGameRoundsIndex() {
        DBCollection rounds = mongo.getDB('game_log').getCollection('game_round')
        /** 普通索引**/
        def timestamp_index = $$('timestamp', -1)
        rounds.createIndex(timestamp_index,'_timestamp_')

        // 用于查询某一类游戏出现的数量
        def game_id_index = $$('game_id',1)
        rounds.createIndex(game_id_index,'_game_id_')

        // 查询单个房间的所有游戏场次
        def room_id_index = $$('room_id':1)
        rounds.createIndex(room_id_index,'_room_id_')


        /** 组合索引 **/
        // 用于查询某一个房间某一场的某一局游戏
        def round_live_room_index = $$('round_id': 1,'room_id': 1)
        rounds.createIndex(round_live_room_index, '_round_live_room_')

        // 查看某个房间的游戏情况
        def room_index = $$('timestamp':-1)
        rounds.createIndex(room_index, '_room_')

        // 用于查询某个时间的结果集
        def result_index = $$('room_id':1, 'live_id': 1,'result':1,'timestamp':-1)
        rounds.createIndex(result_index,'_result_timestamp_')


        /** 唯一索引 **/
        def unique_roundId = $$('round_id': 1)
        rounds.createIndex(unique_roundId,'_round_id_', true)
    }

    /**
     * 用户下注
     */
    private static void buildUserBetIndex() {
        DBCollection user_bet = mongo.getDB('game_log').getCollection('user_bet')
        /** 普通索引**/
        def timestamp_index = $$('timestamp', -1)
        user_bet.createIndex(timestamp_index,'_timestamp_')

        // 查询单个房间的所有游戏场次
        def room_id_index = $$('room_id':1)
        user_bet.createIndex(room_id_index,'_room_id_')


        /** 组合索引 **/
        // 查询某个用户的下注情况
        def user_timestamp_index = $$('user_id': 1,'timestamp': -1,)
        user_bet.createIndex(user_timestamp_index, '_user_timestamp_')

        // 查看某一场直播的所有用户下注情况
        def live_timestamp_index = $$('live_id':1,'timestamp':-1)
        user_bet.createIndex(live_timestamp_index, '_live_timestamp_')

        // 查询某个房间的所有用户下注情况
        def room_timestamp_index = $$('room_id':1,'timestamp':-1)
        user_bet.createIndex(room_timestamp_index, '_room_timestamp_')


        /** 唯一索引 **/
        def unique_roundId = $$('round_id': 1)
        user_bet.createIndex(unique_roundId,'_round_id_', true)
    }


    /**
     * 用户奖励
     */
    private static void buildUserLottery() {
        DBCollection user_lottery = mongo.getDB('game_log').getCollection('user_lottery')
        // 普通索引
        def timestamp_index = $$('timestamp', -1)
        user_lottery.createIndex(timestamp_index,'_timestamp_')

        // 组合索引
        // 查询用户游戏结果日志
        def user_timestamp_index = $$('user_id': 1,'timestamp': -1)
        user_lottery.createIndex(user_timestamp_index, '_user_timestamp_')

        // 查看用户某一种游戏的游戏结果日志
        def user_timestamp_game_index = $$('user_id': 1,'game_id':1,'timestamp': -1)
        user_lottery.createIndex(user_timestamp_game_index, '_user_timestamp_game_')

        // 唯一索引
        def unique_roundId = $$('round_id': 1)
        user_lottery.createIndex(unique_roundId,'_round_id_', true)
    }

    public static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    public static BasicDBObject $$(Map map) {
        return new BasicDBObject(map);
    }
}