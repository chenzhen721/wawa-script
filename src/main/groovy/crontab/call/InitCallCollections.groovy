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
import com.mongodb.Mongo
import com.mongodb.MongoURI

/**
 * 初始化xy_call的集合和索引
 */
class InitCallCollections {

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

    static mongo = new Mongo(new MongoURI(getProperties('mongo.uri', 'mongodb://192.168.31.249:10000/?w=1') as String))
    static DB CALL_DB = mongo.getDB('xy_call')
    static final String USERS_COLLECTION = 'users'
    static final String STARS_COLLECTION = 'stars'
    static final String APPLY_COLLECTION = 'stars_apply'
    static final String ORDER_COLLECTION = 'orders'
    static final String CASH_LOGS_COLLECTION = 'cash_logs'
    static final String FINANCE_LOG_COLLECTION = 'finance_logs'


    static void main(String[] args) {
        Long begin = System.currentTimeMillis()
        println("${InitCallCollections.class.getSimpleName()} --- ${new Date().format('yyyy-MM-dd hh:mm:ss')} --- begin init call collections ...")
        initCollections()
        Long totalCost = System.currentTimeMillis() - begin
        println("${new Date().format('yyyy-MM-dd HH:mm:ss')}:${InitCallCollections.class.getSimpleName()}:finish cost ${totalCost} ms")    }

    /**
     * 创建文档集合
     */
    private static initCollections() {
        def users = CALL_DB.getCollection(USERS_COLLECTION)
        def stars = CALL_DB.getCollection(STARS_COLLECTION)
        def starsApply = CALL_DB.getCollection(APPLY_COLLECTION)
        def orders = CALL_DB.getCollection(ORDER_COLLECTION)
        def cashLogs = CALL_DB.getCollection(CASH_LOGS_COLLECTION)
        def financeLogs = CALL_DB.getCollection(FINANCE_LOG_COLLECTION)

        createUsersIndex(users)
        createStarsIndex(stars)
        createStarsApplyIndex(starsApply)
        createOrdersIndex(orders)
        createCashLogsIndex(cashLogs)
        createFinanceLogsIndex(financeLogs)
    }

    /**
     * 创建users索引
     */
    private static void createUsersIndex(DBCollection users) {
        // 普通索引
        def timestamp_index = $$('timestamp', -1)
        users.createIndex(timestamp_index, '_timestamp_')

        def enable_index = $$('enable': 1)
        users.createIndex(enable_index, '_enable_')

        // 组合索引
        def nick_name_sex = $$('nick_name': 1, 'sex': 1)
        users.createIndex(nick_name_sex, '_nick_name_sex_')
        // 唯一索引
        def unique_openId = $$('open_id': 1)
        def unique_unionId = $$('union_id': 1)
        users.createIndex(unique_openId, '_open_id_', true)
        users.createIndex(unique_unionId, '_union_id_', true)
    }

    /**
     * 创建stars索引
     * @param stars
     */
    private static void createStarsIndex(DBCollection stars) {
        // 普通索引
        def timestamp_index = $$('timestamp', -1)
        stars.createIndex(timestamp_index, '_timestamp_')

        def enable_index = $$('enable': 1)
        stars.createIndex(enable_index, '_enable_')

        // 组合索引
        def nick_name_sex = $$('nick_name': 1, 'sex': 1)
        stars.createIndex(nick_name_sex, '_nick_name_sex_')

        // 唯一索引
        def unique_openId = $$('open_id': 1)
        def unique_unionId = $$('union_id': 1)
        stars.createIndex(unique_openId, '_open_id_', true)
        stars.createIndex(unique_unionId, '_union_id_', true)

        // 子文档索引
        def finance_index = $$('finance': 1)
        def cash_info_index = $$('cash_info': 1)
        stars.createIndex(finance_index, '_finance_')
        stars.createIndex(cash_info_index, '_cash_info_')
    }

    /**
     * 创建stars_apply索引
     * @param starsApply
     */
    private static void createStarsApplyIndex(DBCollection starsApply) {
        // 普通索引
        def timestamp_index = $$('timestamp', -1)
        def user_id_index = $$('user_id': 1)
        def status_index = $$('status': 1)
        starsApply.createIndex(timestamp_index, '_timestamp_')
        starsApply.createIndex(user_id_index, '_user_id_')
        starsApply.createIndex(status_index, '_status_')

        // 组合索引
        def nick_name_sex = $$('nick_name': 1, 'sex': 1)
        starsApply.createIndex(nick_name_sex, '_nick_name_sex_')
    }

    /**
     * 创建orders索引
     * @param orders
     */
    private static void createOrdersIndex(DBCollection orders) {
        // 普通索引
        def status_index = $$('status': 1)
        def timestamp_index = $$('timestamp', -1)
        orders.createIndex(status_index, '_status_')
        orders.createIndex(timestamp_index, '_timestamp_')

        // 组合索引
        def call_time_status_index = $$('call_time': 1, 'status': 1)
        orders.createIndex(call_time_status_index, '_call_time_status_')

        // 子文档索引
        def users_index = $$('users': 1)
        def stars_index = $$('stars': 1)
        orders.createIndex(users_index, '_finance_')
        orders.createIndex(stars_index, '_cash_info_')
    }

    /**
     * 创建finance_logs索引
     * @param financeLogs
     */
    private static void createFinanceLogsIndex(DBCollection financeLogs){
        // 普通索引
        def order_id_index = $$('order_id':1)
        def user_id_index=$$('user_id':1)
        def star_id_index = $$('star_id':1)
        def timestamp_index = $$('timestamp', -1)
        financeLogs.createIndex(order_id_index,'_order_id_')
        financeLogs.createIndex(user_id_index,'_user_id_')
        financeLogs.createIndex(star_id_index,'_star_id_')
        financeLogs.createIndex(timestamp_index,'_timestamp_')

        // 组合索引
        def user_id_timestamp_index = $$('timestamp':-1,'user_id':1)
        def star_id_timestamp_index = $$('timestamp':-1,'star_id':1)
        financeLogs.createIndex(user_id_timestamp_index,'_user_id_timestamp_')
        financeLogs.createIndex(star_id_timestamp_index,'_star_id_timestamp_')
    }

    /**
     * 创建cash_info索引
     * @param cashLogs
     */
    private static void createCashLogsIndex(DBCollection cashLogs){
        // 普通索引
        def timestamp_index = $$('timestamp', -1)
        def status_index = $$('status', -1)
        def user_id_index = $$('user_id_index', -1)
        def amount_index = $$('amount_index', -1)
        cashLogs.createIndex(timestamp_index,'_timestamp_')
        cashLogs.createIndex(status_index,'_status_')
        cashLogs.createIndex(user_id_index,'_user_id_')
        cashLogs.createIndex(amount_index,'_amount_')

        // 组合索引
        def user_id_timestamp_status_index = $$('user_id':1,'timestamp':-1,'status':1)
        def user_id_amount_index = $$('user_id':1,'amount':1)
        cashLogs.createIndex(user_id_timestamp_status_index,'_user_id_timestamp_status_')
        cashLogs.createIndex(user_id_amount_index,'_user_id_amount_')

        // 子文档索引
        def cash_info_index = $$('cash_info':1)
        cashLogs.createIndex(cash_info_index,'_cash_info_')

    }

    public static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    public static BasicDBObject $$(Map map) {
        return new BasicDBObject(map);
    }

}