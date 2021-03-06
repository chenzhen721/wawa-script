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
    static DBCollection stat_order = mongo.getDB('xy_admin').getCollection('stat_order')
    static DBCollection stat_daily = mongo.getDB('xy_admin').getCollection('stat_daily')
    static DBCollection users = mongo.getDB('xy').getCollection('users')
    static DBCollection finance_log_DB = mongo.getDB('xy_admin').getCollection('finance_log')
    static DBCollection day_login = mongo.getDB("xylog").getCollection("day_login")
    static DBCollection diamond_add_logs = mongo.getDB("xylog").getCollection("diamond_add_logs")
    static DBCollection diamond_cost_logs = mongo.getDB("xylog").getCollection("diamond_cost_logs")
    static DBCollection apply_post_logs = mongo.getDB("xylog").getCollection("apply_post_logs")
    static DBCollection catch_record = mongo.getDB("xy_catch").getCollection("catch_record")
    static DBCollection catch_toy = mongo.getDB("xy_catch").getCollection("catch_toy")

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

    /**
     * 统计钻石积分等
     *
     * diamond_add_current 到目前为止领取钻石数
     * diamond_user_current 到目前为止获得赠送的人数
     * invite_user_current 到目前为止邀请好友数
     * invite_diamond_current 到目前为止邀请好友获得的钻石数
     * diamond_cost_current 到目前为止消耗的钻石数
     * charge_award_current 充值优惠钻石
     * admin_add_current admin补单
     *
     * @return
     */
    static diamondPresentStatics(int i, int n) {
        if (n > i) return
        def begin = yesTday - i * DAY_MILLON
        def YMD = new Date(begin).format('yyyyMMdd')
        def diamondend = yesTday - (n - 1) * DAY_MILLON
        def payymd = new Date(diamondend - DAY_MILLON).format('yyyyMMdd')

        Integer diamond_add_total = 0, diamond_user_total = 0, invite_user_total = 0, invite_diamond_total = 0,
                diamond_cost_total = 0, charge_award_total = 0, admin_add_total = 0
        Set uids_total = new HashSet()
        //每个渠道数据，然后汇总到总表
        stat_regpay.find($$(type: 'qd', timestamp: begin)).toArray().each { BasicDBObject obj ->
            def regs = obj['regs'] as Set
            def diamond_add_current = 0
            //def diamond_user_current = 0
            def invite_user_current = 0
            def invite_diamond_current = 0
            def diamond_cost_current = 0
            def charge_award_current = 0
            def sign_diamond_current = 0
            def admin_add_current = 0
            def uids = [] as Set
            def update = new BasicDBObject()
            //除充值以外的钻石
            diamond_add_logs.aggregate([
                    $$('$match', [user_id: [$in: regs], timestamp: [$lt: diamondend]]),
                    $$('$group', [_id: '$type', diamond: [$sum: '$award.diamond'], users: [$addToSet: '$user_id'], count: [$sum: 1]])
            ]).results().each {BasicDBObject item->
                def diamond = item['diamond'] as Integer ?: 0
                def users = item['users'] as Set ?: []
                uids.addAll(users)
                diamond_add_current = diamond_add_current + diamond
                //diamond_user_current = diamond_user_current + count
                if ('invite_diamond' == item['_id']) {
                    invite_diamond_current = invite_diamond_current + diamond
                    invite_user_current = invite_user_current + users.size()
                }
                if ('sign_diamond' == item['_id']) {
                    sign_diamond_current = sign_diamond_current + diamond
                }
            }

            //用户消耗的钻石数
            diamond_cost_logs.aggregate([
                    $$('$match', [user_id: [$in: regs], timestamp: [$lt: diamondend]]),
                    $$('$group', [_id: null, diamond: [$sum: '$cost']])
            ]).results().each {BasicDBObject item ->
                def diamond = item['diamond'] as Integer ?: 0
                diamond_cost_current = diamond_cost_current + diamond
            }

            //充值奖励钻石
            finance_log_DB.aggregate([
                    $$('$match', [user_id: [$in: regs], via: [$ne: 'Admin'], 'ext.award': [$gt: 0], timestamp: [$lt: diamondend]]),
                    $$('$group', [_id: null, diamond: [$sum: '$ext.award']])
            ]).results().each {BasicDBObject item ->
                charge_award_current = item['diamond'] as Integer ?: 0
            }

            //Admin奖励钻石
            finance_log_DB.aggregate([
                    $$('$match', [user_id: [$in: regs], via: 'Admin', timestamp: [$lt: diamondend]]),
                    $$('$group', [_id: null, diamond: [$sum: '$diamond']])
            ]).results().each {BasicDBObject item ->
                admin_add_current = item['diamond'] as Integer ?: 0
            }

            def diamond_user_current = uids.size()
            if (n == 0) { //保存最新的
                update.put('diamond_add_current', diamond_add_current) //
                update.put('diamond_user_current', diamond_user_current) //
                update.put('invite_user_current', invite_user_current) //
                update.put('invite_diamond_current', invite_diamond_current) //
                update.put('diamond_cost_current', diamond_cost_current) //
                update.put('charge_award_current', charge_award_current + sign_diamond_current) //
                update.put('admin_add_current', admin_add_current) //
            }
            update.put("history.${payymd}.diamond_add_current".toString(), diamond_add_current)
            update.put("history.${payymd}.diamond_user_current".toString(), diamond_user_current)
            update.put("history.${payymd}.invite_user_current".toString(), invite_user_current)
            update.put("history.${payymd}.invite_diamond_current".toString(), invite_diamond_current)
            update.put("history.${payymd}.diamond_cost_current".toString(), diamond_cost_current)
            update.put("history.${payymd}.charge_award_current".toString(), charge_award_current + sign_diamond_current)
            update.put("history.${payymd}.admin_add_current".toString(), admin_add_current)
            stat_regpay.update($$(_id: obj['_id']), $$($set: update), false, false)

            diamond_add_total = diamond_add_total + diamond_add_current
            diamond_user_total = diamond_user_total + diamond_user_current
            invite_user_total = invite_user_total + invite_user_current
            invite_diamond_total = invite_diamond_total + invite_diamond_current
            diamond_cost_total = diamond_cost_total + diamond_cost_current
            charge_award_total = charge_award_total + charge_award_current + sign_diamond_current
            admin_add_total = admin_add_total + admin_add_current
            uids_total.addAll(uids)
        }

        def update = new BasicDBObject()
        if (n == 0) { //保存最新的
            update.put('diamond_add_current', diamond_add_total) //
            update.put('diamond_user_current', uids_total.size()) //
            update.put('invite_user_current', invite_user_total) //
            update.put('invite_diamond_current', invite_diamond_total) //
            update.put('diamond_cost_current', diamond_cost_total) //
            update.put('charge_award_current', charge_award_total) //
            update.put('admin_add_current', admin_add_total) //
        }
        update.put("history.${payymd}.diamond_add_current".toString(), diamond_add_total)
        update.put("history.${payymd}.diamond_user_current".toString(), uids_total.size())
        update.put("history.${payymd}.invite_user_current".toString(), invite_user_total)
        update.put("history.${payymd}.invite_diamond_current".toString(), invite_diamond_total)
        update.put("history.${payymd}.diamond_cost_current".toString(), diamond_cost_total)
        update.put("history.${payymd}.charge_award_current".toString(), charge_award_total)
        update.put("history.${payymd}.admin_add_current".toString(), admin_add_total)

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

    /**
     * 订单统计
     * total_pay: 总营收
     * total_cost: 总成本
     * order_count: 寄出单数
     * goods_count: 商品个数
     * goods_cost: 商品价值
     * postage: 快递费用
     * user_count: 邮寄用户数
     *
     * @param i
     * @return
     */
    static orderStatics(int i) {
        def begin = yesTday - i * DAY_MILLON
        def end = begin + DAY_MILLON
        def YMD = new Date(begin).format('yyyyMMdd')
        int postage = 0, goods_cost = 0, total_cost = 0, order_count = 0, goods_count = 0
        Set uids = new HashSet()
        apply_post_logs.find($$(push_time: [$gte: begin, $lt: end])).toArray().each {BasicDBObject obj ->
            def toys = obj['toys'] as List
            def count = toys.size()
            order_count = order_count + 1
            goods_count = goods_count + count
            postage = postage + cost(count)
            toys.each {BasicDBObject toy ->
                def record_id = toy['record_id'] as String
                def record = catch_record.findOne($$(_id: record_id), $$(cost: 1))
                if (record != null) {
                    goods_cost = goods_cost + (record['cost'] as Integer)
                }
                /*def toy_cost = catch_toy.findOne($$(_id: toy['_id'] as Integer), $$(cost: 1))
                if (toy_cost != null) {
                    total_cost = total_cost + (toy_cost['cost'] as Integer)
                }*/
            }
            uids.add(obj['user_id'] as Integer)
        }
        total_cost = postage + goods_cost

        //总充值额度
        def finance = stat_daily.findOne("${YMD}_finance".toString()) ?: [:]

        def update = $$(timestamp: begin, type: 'order', total_pay: finance['total'] as Integer ?: 0, postage: postage, goods_cost: goods_cost,
                goods_count: goods_count, total_cost: total_cost, order_count: order_count, user_count: uids.size(), _id: "${YMD}_order".toString())
        stat_daily.update($$(_id: "${YMD}_order".toString()), $$($set: update), true, false)
    }

    private static int cost(int count) {
        if (count <= 2) {
            return 0
        }
        if (count >= 3 && count <= 6) {
            return 7
        }
        if (count > 6 && count <= 9) {
            return 14
        }
        if (count > 9) {
            return 21
        }
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
            [0, 1, 3, 7, 30].each { Integer i ->
                [0, 1, 3, 7, 30].each { Integer n ->
                    regPayStatics(i + DAY, n)
                }
            }
            println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   regPayStatics, cost  ${System.currentTimeMillis() - l} ms"

            l = System.currentTimeMillis()
            5.times { Integer i ->
                regpay_last5(i + DAY, 0)
            }
            /*for(int i = 0; i < 62; i++) {
                for(int n = 0; n <= i; n ++) {
                    regpay_last5(i, n)
                }
            }*/
            println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   regpay_last5, cost  ${System.currentTimeMillis() - l} ms"

            l = System.currentTimeMillis()
            60.times { Integer i->
                regpay_till_current(i + DAY, 0)
            }
            /*for(int i = 0; i < 62; i++) {
                for(int n = 0; n <= i; n ++) {
                    regpay_till_current(i, n)
                }
            }*/
            println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   regpay_till_current, cost  ${System.currentTimeMillis() - l} ms"

            l = System.currentTimeMillis()
            60.times { Integer i->
                diamondPresentStatics(i + DAY, 0)
            }
            /*for(int i = 0; i < 62; i++) {
                for(int n = 0; n <= i; n ++) {
                    diamondPresentStatics(i, n)
                }
            }*/
            println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   diamondPresentStatics, cost  ${System.currentTimeMillis() - l} ms"

            l = System.currentTimeMillis()
            orderStatics(DAY)
            println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   orderStatics, cost  ${System.currentTimeMillis() - l} ms"

        } catch (Exception e){
            println "Exception : " + e
        }

    }

}

