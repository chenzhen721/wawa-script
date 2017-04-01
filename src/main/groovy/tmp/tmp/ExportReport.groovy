#!/usr/bin/env groovy
package crontab.tmp

import com.mongodb.*
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
])
import com.mongodb.Mongo

/**
 * 导出运营踢出的需求报表
 */
class ExportReport {
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
    static historyMongo = new Mongo(new MongoURI(getProperties('mongo_history.uri', 'mongodb://192.168.31.246:27017/?w=1') as String))
    static historyDB = historyMongo.getDB('xylog_history')
    static DAY_MILLON = 24 * 3600 * 1000L

    static long zeroMill = new Date().clearTime().getTime()
    static Long yesTday = zeroMill - DAY_MILLON
    static room_cost = mongo.getDB('xylog').getCollection('room_cost')
    static room_edit = mongo.getDB('xylog').getCollection('room_edit')
    static trade_logs = mongo.getDB('xylog').getCollection('trade_logs')
    static day_login = mongo.getDB('xylog').getCollection('day_login')
    static debug_logs = mongo.getDB('xylog').getCollection('debug_logs')
    static room_cost_2014 = historyDB.getCollection('room_cost_2014')
    static room_cost_2015 = historyDB.getCollection('room_cost_2015')
    static room_cost_2016 = historyDB.getCollection('room_cost_2016')
    static finance_log = mongo.getDB('xy_admin').getCollection('finance_log')
    static applys = mongo.getDB('xy_admin').getCollection('applys')
    static users = mongo.getDB("xy").getCollection("users")
    static rooms = mongo.getDB("xy").getCollection("rooms")
    static xy_users = mongo.getDB("xy_user").getCollection("users")
    static lottery_logs = mongo.getDB('xylog').getCollection('lottery_logs')
    static invite_logs = mongo.getDB('xyactive').getCollection('invite_logs')
    static invite_rank = mongo.getDB('xyactive').getCollection('invite_rank')
    static invite_users  = mongo.getDB('xyactive').getCollection('invite_users')
    static channels = mongo.getDB('xy_admin').getCollection('channels')
    static final Integer MAX_LEVEL = 29
    static
    final long[] BEAN_COUNT_TOTAL = [0, 1000, 5000, 15000, 30000, 50000, 80000, 150000, 300000, 500000, 700000, 1000000,
                                     1500000, 2000000, 2500000, 3500000, 5000000, 7000000, 10000000, 15000000, 21000000, 28000000,
                                     36000000, 45000000, 55000000, 70000000, 108000000, 168000000, 258000000, 458000000,4580000000000];
    static
    final String[] LEVEL = ['庶民', '1富', '2富', '3富', '4富', '5富', '6富', '7富', '8富', '9富', '10富', '举人', '贡士', '进士', '知府',
                            '巡抚', '总督', '尚书', '太傅', '太师', '丞相', '藩王', '郡王', '亲王', '诸侯', '王爷', '皇帝', '大帝', '玉帝', '天尊'];

    static Map<String, Long> LEVEL_MAP = ['庶民': 0, '1富': 0, '2富': 0, '3富': 0, '4富': 0, '5富': 0, '6富': 0, '7富': 0, '8富': 0, '9富': 0, '10富': 0, '举人': 0, '贡士': 0, '进士': 0, '知府': 0,
                                             '巡抚': 0, '总督': 0, '尚书': 0, '太傅': 0, '太师': 0, '丞相': 0, '藩王': 0, '郡王': 0, '亲王': 0, '诸侯': 0, '王爷': 0, '皇帝': 0, '大帝': 0, '玉帝': 0, '天尊': 0];


    /**
     * 用户邀请数据
     */
    static void inviteStatistic(){
        //日期
        Set<String> datas = new HashSet<>();
        //发起邀请人数
        def Map<String, Integer> invites = new HashMap<>();
        //获取返币人次
        def Map<String, Integer> retruns = new HashMap<>();
        //获取返币人数
        def Map<String, Set<Integer>> retrunUsers = new HashMap<>();
        invite_users.find().sort(new BasicDBObject(timestamp: -1)).toArray().each {DBObject inviteUser ->
            Long    timestamp = inviteUser['timestamp'] as Long
            String date =  new Date(timestamp).format("yyyy-MM-dd")
            Integer count = invites.get(date) ?: 0
            invites.put(date, ++count)
            datas.add(date)
        }
        invite_logs.find().sort(new BasicDBObject(timestamp: -1)).toArray().each {DBObject invitedUser ->
            Long  timestamp = invitedUser['timestamp'] as Long
            Integer  user_id = invitedUser['user_id'] as Integer
            String date =  new Date(timestamp).format("yyyy-MM-dd")
            Integer count = retruns.get(date) ?: 0
            retruns.put(date, ++count)

            Set<Integer> users = (retrunUsers.get(date) ?: new HashSet<>()) as Set;
            users.add(user_id)
            retrunUsers.put(date, users)
            datas.add(date)
        }
        datas.each {String date ->
            String str =  date << ',' << (invites[date] ?: 0)<< ',' << (retruns[date] ?: 0)
            Integer retrunUser = 0
            if(retrunUsers.get(date) != null){
                retrunUser = retrunUsers.get(date).size()
            }
            str = str << ',' << retrunUser
            println str
        }
    }

    def static void saveTestData() {
        Long begin = 1468771200000L
        Random rand = new Random();
        for (int i = 0; i < 20000000; i++) {
            Integer randNum = rand.nextInt(29);
            BasicDBObject expression = new BasicDBObject()
            expression.put('_id', 3000000 + i)
            expression.put('mm_no', 3000000 + i)
            expression.put('tuid', 30000000 + i)
            expression.put('user_name', 'monkey_' + i)
            expression.put('via', 'android')
            expression.put('sex', 2)
            expression.put('status', true)
            expression.put('nick_name', 'monkey_' + i)
            expression.put('finance', [coin_spend_total: BEAN_COUNT_TOTAL[randNum]])
            expression.put('priv', 3)
            expression.put('timestamp', begin)
            expression.put('isTest', 'test')
            users.save(expression)
        }
    }

    def static void saveLoginTestData() {
        Long begin = 1468771200000L
        BasicDBObject expression = new BasicDBObject(timestamp: begin)
        DBCursor cursor = users.find(expression)
        while (cursor.hasNext()) {
            def obj = cursor.next()
            Integer userId = obj.get('_id')
            def id = '20160718_' + userId
            BasicDBObject saveExpression = new BasicDBObject()
            saveExpression.put('_id', '20160718_' + userId)
            saveExpression.put('user_id', userId)
            saveExpression.put('timestamp', begin)
            saveExpression.put('qd', 'MM')
            saveExpression.put('ip', '192.168.31.2')
            saveExpression.put('isTest', 'test')
            day_login.save(saveExpression)
        }
    }

    /**
     * 导出某段时间的用户登陆信息
     */
    def static void exportLoginInfo() {
        StringBuffer sb = new StringBuffer('等级')
        sb.append(System.lineSeparator())
        Set<Integer> userSet = new HashSet<Integer>()
        Long begin = 1468771200000L
        Long end = 1471449600000L
        BasicDBObject searchExpression = new BasicDBObject(timestamp: [$gte: begin, $lt: end])
        DBCursor dbCursor = day_login.find(searchExpression).batchSize(5000)
        while (dbCursor.hasNext()) {
            def obj = dbCursor.next()
            Integer userId = obj.get('user_id')
            userSet.add(userId)
        }
        dbCursor.close()


        println '一共有' + userSet.size() + '个用户登陆过'
        Map<Integer, Long> userMap = new HashMap<Integer, Long>(userSet.size())
        List list = new ArrayList()

        userSet.each {
            Integer userId = it
            list.add(userId)
            if (list.size() == 1000) {
                loadData(list, userMap)
            }
        }
        loadData(list, userMap)
        userMap.each {
            Integer userId = it.key
            Long coinSpendTotal = it.value
            // 计算等级
            String levelName = userLevel(coinSpendTotal)
            Integer count = LEVEL_MAP.get(levelName)
            LEVEL_MAP.put(levelName, ++count)
        }

        LEVEL_MAP.each {
            sb.append(it.key).append(',').append(it.value).append(System.lineSeparator())
        }

        createFile(sb, null, '/userLevel.csv')
    }

    static  exportLoginInfo_v2() {
        StringBuffer sb = new StringBuffer('等级')
        sb.append(System.lineSeparator())
        Set<Integer> userSet = new HashSet<Integer>()
        Long begin = 1468771200000L
        Long end = 1471449600000L
        BasicDBObject searchExpression = new BasicDBObject(timestamp: [$gte: begin, $lt: end])
        DBCursor dbCursor = day_login.find(searchExpression).batchSize(5000)
        while (dbCursor.hasNext()) {
            def obj = dbCursor.next()
            Integer userId = obj.get('user_id')
            userSet.add(userId)
        }
        dbCursor.close()

        def userIdList = userSet.toArray()
        println userIdList.size()
        int i = 0;
        for (int j = 1; j < BEAN_COUNT_TOTAL.size() ; j++) {
            Long g = BEAN_COUNT_TOTAL[i++];
            Long l = BEAN_COUNT_TOTAL[j];
            Long iStart = 0
            Long iEnd = 100000
            while (iEnd <= userIdList.size()){
                BasicDBObject expressionCount = new BasicDBObject(_id: [$in: userSet.toArray()[iStart..iEnd]], priv: 3, 'finance.coin_spend_total':[$gte:g, $lt:l])
                String levelName = userLevel(g)
                Long count = LEVEL_MAP.get(levelName)?:0
                LEVEL_MAP.put(levelName, count + users.count(expressionCount))

                iStart = iEnd
                iEnd += 100000
                iEnd = iEnd > userIdList.size() ? (userIdList.size()-1) : iEnd
                if(iStart >= (userIdList.size()-1)) break;

            }

        }

        LEVEL_MAP.each {
            sb.append(it.key).append(',')
            sb.append(it.value)
            sb.append(System.lineSeparator())
        }

        createFile(sb, null, '/userLevel.csv')
    }

    /**
     * 读取数据
     * @param list
     * @param userMap
     */
    private static void loadData(List list, Map userMap) {
        BasicDBObject fieldExpression = new BasicDBObject('finance': 1)
        BasicDBObject expression = new BasicDBObject(_id: [$in: list], priv: 3)
        DBCursor cursor = users.find(expression, fieldExpression)
        while (cursor.hasNext()) {
            def user = cursor.next()
            if (user) {
                Integer userId = user.get('_id')
                Map finance = user.get('finance') as Map
                Long coinSpendTotal = finance.get('coin_spend_total') == null ? 0L : finance.get('coin_spend_total') as Long
                userMap.put(userId, coinSpendTotal)
            }
        }
        cursor.close()
        list.clear()
    }

    /**
     * 统计流失用户的等级分布情况
     */
    def static void lostUserLevelInfo(Long begin, Long end, Integer nextMonth) {
        BasicDBObject expression = new BasicDBObject(timestamp: [$gte: begin, $lt: end], via: [$ne: 'Admin'])
        List currentMonthReChargeUser = finance_log.distinct('user_id', expression)
        // 当月总充值人数
        Integer userCount = currentMonthReChargeUser.size()
        // 留存用户
        List storeUserList = new ArrayList()
        // 基于当月充值人数在下个月充值人数
        while (nextMonth-- > 0) {
            Map map = getNextMonth(begin)
            begin = map.get('begin')
            end = map.get('end')
            if (!storeUserList.isEmpty()) {
                currentMonthReChargeUser.clear()
                currentMonthReChargeUser = storeUserList.clone()
            }
            BasicDBObject expression2 = new BasicDBObject(timestamp: [$gte: begin, $lt: end], user_id: [$ne: currentMonthReChargeUser])
            storeUserList = finance_log.distinct('user_id', expression2)
            List lostUser = currentMonthReChargeUser - storeUserList

            // 根据流失用户 计算其等级分布情况,并且将留存用户记录
            lostUser.each {

            }
        }

    }

    /**
     * 基于变量获取下个月的月初和月尾
     * @param current
     */
    private static Map getNextMonth(Long current) {
        println 'current is ' + current
        Map<Long, Long> map = new TreeMap<Long, Long>()
        Calendar calendar = Calendar.getInstance()
        calendar.setTimeInMillis(current)
        calendar.set(Calendar.DATE, 1)
        calendar.set(Calendar.HOUR_OF_DAY, 0)
        calendar.set(Calendar.MINUTE, 0)
        calendar.set(Calendar.SECOND, 0)
        calendar.set(Calendar.MILLISECOND, 0)
        calendar.add(Calendar.MONTH, 1)
        Long begin = calendar.getTimeInMillis()
        calendar.add(Calendar.MONTH, 1)
        Long end = calendar.getTimeInMillis()
        map.put('begin', begin)
        map.put('end', end)
        return map
    }

    /**
     *
     * @return
     */
    private static Map getBeginToEnd() {
        Long begin, end = 0
        Integer lastDayAgo = 13
        Map<Long, Long> map = new TreeMap<Long, Long>()
        while (lastDayAgo-- > 0) {
            Calendar calendar = Calendar.getInstance()
            calendar.set(Calendar.DATE, 1)
            calendar.set(Calendar.HOUR_OF_DAY, 0)
            calendar.set(Calendar.MINUTE, 0)
            calendar.set(Calendar.SECOND, 0)
            calendar.set(Calendar.MILLISECOND, 0)
            calendar.add(Calendar.MONTH, -lastDayAgo)
            begin = calendar.getTime().getTime()
            calendar.add(Calendar.MONTH, 1)
            end = calendar.getTime().getTime()
            map.put(begin, end)
        }
        return map
    }

    /**
     * 计算用户等级
     * @param coin
     * @return
     */
    private static String userLevel(long coin) {
        Integer index = 0
        for (int i = 1; i < 29; i++) {
            if (coin < (BEAN_COUNT_TOTAL[i])) {
                index = i - 1;
                return LEVEL[index]
            }
        }

        return LEVEL[MAX_LEVEL - 1]
    }

    /**
     * 创建文件
     * @param sb
     * @param folder_path
     * @param exportFileName
     */
    private static void createFile(StringBuffer sb, String folder_path, String exportFileName) {
        if (folder_path == null) {
            folder_path = '/empty/static/'
        }
        File folder = new File(folder_path)
        if (!folder.exists()) {
            folder.mkdirs()
        }

        File file = new File(folder_path + exportFileName);
        if (!file.exists()) {
            file.createNewFile()
        }

        file.withWriterAppend { Writer writer ->
            writer.write(sb.toString())
            writer.flush()
            writer.close()
        }
    }

    //特定代理下主播每月新增可提现VC
    static void brokerStarEarnedVc(List<Integer> brokerIDs){
        brokerIDs.each {Integer brokerId ->
            List<Integer> starIDs = getStarIdsFromBoker(brokerId)
            println "${brokerId} : star size : ${starIDs.size()} "
            /*int begin = 11
            while(begin-- > 0){
                starsEarnedVcPerMonth(begin, starIDs)
            }*/
            starsEarnedVcPerMonth(0, starIDs)
        }
    }

    static List<Integer> getStarIdsFromBoker(Integer brokerId){
        return users.find(new BasicDBObject('priv':2,'star.broker':brokerId),new BasicDBObject('_id':1)).toArray()*._id
    }

    static Long starsEarnedVcPerMonth(Integer i, List<Integer> starIDs){
        Calendar cal = getCalendar()
        cal.add(Calendar.MONTH, -i)
        long firstDayOfCurrentMonth = cal.getTimeInMillis()  //当月第一天
        cal.add(Calendar.MONTH, -1)
        long firstDayOfLastMonth = cal.getTimeInMillis()  //上月第一天
        String ym = new Date(firstDayOfLastMonth).format("yyyyMM")
        String year = new Date(firstDayOfLastMonth).format("yyyy")

        //获取代理旗下主播
        def query = [timestamp: [$gte: firstDayOfLastMonth, $lt: firstDayOfCurrentMonth], user_id:[$in:starIDs]]
        Long vc_total = starEarnedVc(query) - starFrozenVc(query)
        //println "${new Date(firstDayOfLastMonth).format("yyyy-MM-dd HH:mm:ss")} to ${new Date(firstDayOfCurrentMonth).format("yyyy-MM-dd HH:mm:ss")}".toString()
        println "${ym} : ${vc_total}"
        return vc_total
    }

    private static Calendar getCalendar() {
        Calendar cal = Calendar.getInstance()//获取当前日期
        cal.set(Calendar.DAY_OF_MONTH, 1)//设置为1号,当前日期既为本月第一天
        cal.set(Calendar.HOUR_OF_DAY, 0)
        cal.set(Calendar.MINUTE, 0)
        cal.set(Calendar.SECOND, 0)
        cal.set(Calendar.MILLISECOND, 0)
        return cal
    }

    static DBCollection stat_lives = mongo.getDB('xy_admin').getCollection('stat_lives')
    //主播赚取vc
    static Long starEarnedVc(Map query){
        Long total = 0;
        //30 * 60

        def res = stat_lives.aggregate(
                new BasicDBObject('$match', query),
                new BasicDBObject('$project', [user_id:'$user_id', earned: '$earned']),
                new BasicDBObject('$group', [_id: '$user_id', earned: [$sum: '$earned']]) //top N 算法
        )
        Iterable objs = res.results()
        objs.each {row ->
            total += row.earned
        }
        return total;
    }

    //主播未新增超过30,000VC主播冻结VC
    private final static Integer VAIL_VC = 30000
    static Long starFrozenVc(Map query){
        Long total = 0;
        def res = stat_lives.aggregate(
                new BasicDBObject('$match', query),
                new BasicDBObject('$project', [user_id:'$user_id', earned: '$earned']),
                new BasicDBObject('$group', [_id: '$user_id', earned: [$sum: '$earned']]) //top N 算法
        )
        Iterable objs = res.results()
        objs.each {row ->
            Long earned = row.earned
            if(earned < VAIL_VC){
                total += earned
            }
        }
        return total;
    }

    // test
    // static List<Integer> brokerIds = [1315023,1305799,1201928];
    //static List<Integer> brokerIds = [5874151,31292678,32077539,35027968,28237607,15604258,31952177];
    static List<Integer> brokerIds = [1205786,1205728];


    static void main(String[] args) {
        //exportLoginInfo()
        //inviteStatistic();
        brokerStarEarnedVc(brokerIds);
    }
}