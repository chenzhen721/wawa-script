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

    private static void init() {
        mongo.getDB('game_log').dropDatabase()
        mongo.getDB('game_log').createCollection('game_round', null)
        mongo.getDB('game_log').createCollection('user_bet', null)
        mongo.getDB('game_log').createCollection('user_lottery', null)
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
    private static void buildUserLottery() {
        DBCollection user_lottery = mongo.getDB('game_log').getCollection('user_lottery')
        /** 组合索引 **/

        def user_round_room_live_timestamp_index = $$('user_id': 1, 'round_id': 1, 'room_id': 1, 'live_id': 1, 'timestamp': -1)
        user_lottery.createIndex(user_round_room_live_timestamp_index, 'user_round_room_live_timestamp_')

    }

    public static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    public static BasicDBObject $$(Map map) {
        return new BasicDBObject(map);
    }
}