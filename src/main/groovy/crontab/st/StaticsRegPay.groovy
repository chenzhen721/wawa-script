#!/usr/bin/env groovy
package crontab.st

import com.mongodb.BasicDBObject
import com.mongodb.DBCollection
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
])
import com.mongodb.Mongo
import com.mongodb.MongoURI

/**
 * 新增用户付费统计
 */
class StaticsRegPay {

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

    static mongo = new Mongo(new MongoURI(getProperties('mongo.uri', 'mongodb://192.168.2.27:10000/?w=1') as String))
    static DAY_MILLON = 24 * 3600 * 1000L
    static long zeroMill = new Date().clearTime().getTime()
    static Long yesTday = zeroMill - DAY_MILLON
    static String YMD = new Date(yesTday).format("yyyyMMdd")
    static DBCollection stat_regpay = mongo.getDB('xy_admin').getCollection('stat_regpay')
    static DBCollection users = mongo.getDB('xy').getCollection('users')
    static DBCollection finance_log_DB = mongo.getDB('xy_admin').getCollection('finance_log')
    static DBCollection day_login = mongo.getDB("xylog").getCollection("day_login")

    /**
     * regs:[] //注册IDs
     * reg_count:  注册人数
     * payuser_current: //截止到今天付费总人数
     * paytotal_current: //截止到今天付费总金额
     * paycount_current: //截止到今天付费总次数
     * payuserlogin_current: //截止到今天总登录天数
     * pay_last5: //最近5日内充值人数
     * //当日注册用户中付费的 平均登录天数（payuserlogin_current/payuser_current）
     * //复购次数 paycount_current/payuser_current
     * pay_user_count: //当日新增付费
     * pay_total: //总金额
     * pay_user_count1: //次日新增付费
     * pay_total1: //总金额
     * pay_user_count3: //三日新增付费
     * pay_total3: //总金额
     * pay_user_count7: //七日新增付费
     * pay_total7: //总金额
     * pay_user_count30: //30日新增付费
     * pay_total30: //总金额
     *
     * @param i
     */
    static regStatics(int i){
        def begin = yesTday - i * DAY_MILLON
        if (begin < 1511107200000) return
        def end = begin + DAY_MILLON
        def YMD = new Date(begin).format('yyyyMMdd')
        //def regs = users.count(new BasicDBObject(timestamp: [$gte: begin, $lt: end]))
        def total_regs = []
        users.aggregate([
                $$('$match', [timestamp: [$gte: begin, $lt: end]]),
                $$('$project', [user_id: '$_id', qd: '$qd']),
                $$('$group', [_id: '$qd', user_id: [$addToSet: '$user_id']])
        ]).results().each {BasicDBObject obj->
            //qd信息,  对应的users
            def qd = obj['_id'] as String
            def regs = obj['user_id'] as Set
            total_regs.addAll(regs)
            def update = [type: 'qd', qd: qd, timestamp: begin, regs: regs, reg_count: regs.size()]
            [1, 3, 7, 30].each {
                update.put("pay_user_count${it}".toString(), 0)
                update.put("pay_total${it}".toString(), 0)
            }
            stat_regpay.update($$(_id: "${YMD}_${qd}_regpay".toString()), $$($set: update), true, false)
        }

        def update = [type: 'total', timestamp: begin, regs: total_regs, reg_count: total_regs.size()]
        [1, 3, 7, 30].each {
            update.put("pay_user_count${it}".toString(), 0)
            update.put("pay_total${it}".toString(), 0)
        }
        stat_regpay.update($$(_id: "${YMD}_regpay".toString()), $$($set: update), true, false)
    }

    /**
     * i天的n日新增付费
     * @param i 查询哪一天
     * @param n 第n日的新增付费
     * @return
     */
    static regPayStatics(int i, int n) {
        if (n > i) return
        def begin = yesTday - i * DAY_MILLON
        def YMD = new Date(begin).format('yyyyMMdd')
        def paybegin = begin + n * DAY_MILLON
        def payend = paybegin + DAY_MILLON
        Integer total_count = 0
        Double total_cny = 0
        stat_regpay.find($$(type: 'qd', timestamp: begin)).toArray().each {BasicDBObject obj ->
            def regs = obj['regs'] as Set
            def pay = regpay(regs, paybegin, payend)
            def update = $$("pay_user_count" + (n == 0 ? "": n), pay['pay_user_count']) //用户数
            update.put("pay_total" + (n == 0 ? "": n), pay['pay_total']) //总金额
            stat_regpay.update($$(_id: obj['_id']), $$($set: update), false, false)
            total_count = total_count + (pay['pay_user_count'] as Integer)
            total_cny = total_cny + (pay['pay_total'] as Integer)
        }
        def update = $$("pay_user_count" + (n == 0 ? "": n), total_count) //用户数
        update.put("pay_total" + (n == 0 ? "": n), total_cny) //总金额
        stat_regpay.update($$(_id: "${YMD}_regpay".toString()), $$($set: update), false, false)
    }

    /**
     * i天的用户, n日前5天内充值人数
     * @return
     */
    static regpay_last5(int i, int n) {
        if (n > i || i == 0 || (i >= 5 && i < n + 5)) return
        def begin = yesTday - i * DAY_MILLON //i天记录
        def YMD = new Date(begin).format('yyyyMMdd')
        def payend = yesTday - (n - 1) * DAY_MILLON //n日截止
        def payymd = new Date(payend - DAY_MILLON).format('yyyyMMdd')
        def last5begin = payend - 5 * DAY_MILLON //n前5日
        last5begin = last5begin < begin ? begin : last5begin
        def pay_user_count = 0
        stat_regpay.find($$(type: 'qd', timestamp: begin)).toArray().each {BasicDBObject obj ->
            def regs = obj['regs'] as Set
            Map pay = regpay(regs, last5begin, payend)
            def update = new BasicDBObject()
            if (n == 0) {
                update.put('pay_last5', pay['pay_user_count'])
            }
            //记录history
            if (last5begin >= begin) {
                update.put("history.${payymd}.pay_last5".toString(), pay['pay_user_count'])
            }
            stat_regpay.update($$(_id: obj['_id']), $$($set: update), false, false)
            pay_user_count = pay_user_count + (pay['pay_user_count'] as Integer)
        }
        def update = new BasicDBObject()
        if (n == 0) {
            update.put('pay_last5', pay_user_count)
        }
        //记录history
        if (last5begin >= begin) {
            update.put("history.${payymd}.pay_last5".toString(), pay_user_count)
        }
        stat_regpay.update($$(_id: "${YMD}_regpay".toString()), $$($set: update), false, false)
    }

    /**
     * i天的用户截止到n日的数据
     * @return
     */
    static regpay_till_current(int i, int n) {
        if (n > i) return
        def begin = yesTday - i * DAY_MILLON
        def YMD = new Date(begin).format('yyyyMMdd')
        def payend = yesTday - (n - 1) * DAY_MILLON
        def payymd = new Date(payend - DAY_MILLON).format('yyyyMMdd')
        Integer pay_user_count = 0, pay_count = 0, total_days = 0
        Double pay_total = 0
        stat_regpay.find($$(type: 'qd', timestamp: begin)).toArray().each {BasicDBObject obj ->
            def regs = obj['regs'] as Set
            def pay = regpay(regs, null, payend)
            def update = new BasicDBObject()

            //截止到当前付费的用户, 30天内最近一次登录时间距离注册日的天数总和
            def total_uids = pay['uids'] as Set
            def days = 0 as Integer
            def loginbegin = begin
            def loginend = payend

            total_uids.each {Integer id ->
                def logins = day_login.find($$(user_id: id, timestamp: [$gte: loginbegin, $lt: loginend])).sort($$(timestamp: -1)).limit(1).toArray()
                def time = logins[0]['timestamp'] as Long
                days = days + (((time - begin) / DAY_MILLON) as Double).toInteger() + 1
            }

            def user_count = pay['pay_user_count'] as Integer
            def rate = user_count != 0 ? (days / user_count) as Double : 0
            if (n == 0) { //保存最新的
                update.put('payuser_current', pay['pay_user_count']) //付费人数
                update.put('paytotal_current', pay['pay_total']) //付费金额
                update.put('paycount_current', pay['pay_count']) //付费次数
                update.put('payuserlogin_rate_current', rate) //总登录天数
            }
            update.put("history.${payymd}.payuser_current".toString(), pay['pay_user_count'])
            update.put("history.${payymd}.paytotal_current".toString(), pay['pay_total'])
            update.put("history.${payymd}.paycount_current".toString(), pay['pay_count'])
            update.put("history.${payymd}.payuserlogin_rate_current".toString(), rate)

            stat_regpay.update($$(_id: obj['_id']), $$($set: update), false, false)
            pay_user_count = pay_user_count + (pay['pay_user_count'] as Integer)
            pay_count = pay_count + (pay['pay_count'] as Integer)
            pay_total = pay_total + (pay['pay_total'] as Integer)
            total_days = total_days + days
        }

        def update = new BasicDBObject()
        def rate = pay_user_count != 0 ? total_days / pay_user_count as Double : 0
        if (n == 0) { //保存最新的
            update.put('payuser_current', pay_user_count) //付费人数
            update.put('paytotal_current', pay_total) //付费金额
            update.put('paycount_current', pay_count) //付费次数
            update.put('payuserlogin_rate_current',  rate) //总登录天数
        }
        update.put("history.${payymd}.payuser_current".toString(), pay_user_count)
        update.put("history.${payymd}.paytotal_current".toString(), pay_total)
        update.put("history.${payymd}.paycount_current".toString(), pay_count)
        update.put("history.${payymd}.payuserlogin_rate_current".toString(), rate)
        //println update
        stat_regpay.update($$(_id: "${YMD}_regpay".toString()), $$($set: update), false, false)
    }

    //查询用户regs，从begin到end 的 付费用户 总用户数 总付费金额
    private static Map regpay(def regs, def begin, def end) {
        def timestamp = [:]
        if (begin != null) {
            timestamp.put('$gte', begin)
        }
        if (end != null) {
            timestamp.put('$lt', end)
        }
        if (timestamp.isEmpty()) return
        def query = $$(user_id: [$in: regs], via: [$ne: 'Admin'])
        query.put('timestamp', timestamp)
        def total_uids = new HashSet()
        Double total_cny = 0
        Integer pay_count = 0
        finance_log_DB.find(query).toArray().each { BasicDBObject obj ->
            def cny = obj.cny as Double
            total_cny = total_cny + cny
            total_uids.add(obj.user_id)
            pay_count = pay_count + 1
        }
        return [uids: total_uids, pay_user_count: total_uids.size(), pay_total: total_cny, pay_count: pay_count]
    }

    public static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    public static BasicDBObject $$(Map map) {
        return new BasicDBObject(map)
    }

    static Integer DAY = 0

    static void main(String[] args) {
        try {
            long l = System.currentTimeMillis()
            //regStatics(DAY)
            println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   regStatics, cost  ${System.currentTimeMillis() - l} ms"

            l = System.currentTimeMillis()
            [0, 1, 3, 7, 30].each {Integer i->
                [0, 1, 3, 7, 30].each {Integer n->
                    //regPayStatics(i + DAY, n)
                }
            }
            println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   regPayStatics, cost  ${System.currentTimeMillis() - l} ms"

            l = System.currentTimeMillis()
            /*5.times { Integer i ->
                regpay_last5(i + DAY, 0)
            }*/
            49.times {Integer i ->
                i.times {Integer n->
                    //regpay_last5(i, n)
                }
            }
            println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   regpay_last5, cost  ${System.currentTimeMillis() - l} ms"

            l = System.currentTimeMillis()
            /*60.times { Integer i->
                regpay_till_current(i + DAY, 0)
            }*/
            49.times {Integer i ->
                i.times { Integer n->
                    regpay_till_current(i, n)
                }
            }
            println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   regpay_till_current, cost  ${System.currentTimeMillis() - l} ms"

        } catch (Exception e){
            println "Exception : " + e
        }

    }

}

