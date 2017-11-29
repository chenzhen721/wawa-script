#!/usr/bin/env groovy
package crontab.st

import com.mongodb.BasicDBObject
import com.mongodb.DBCollection
import com.mongodb.DBObject
@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
])
import com.mongodb.Mongo
import com.mongodb.MongoURI

/**
 * 关于钻石的统计
 */
class DiamondMonthStat {
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

    static DBCollection diamond_dailyReport_stat = mongo.getDB('xy_admin').getCollection('diamond_dailyReport_stat')

    static DBCollection diamond_month_stat = mongo.getDB('xy_admin').getCollection('diamond_month_stat')
    static DAY_MILLON = 24 * 3600 * 1000L
    static long zeroMill = new Date().clearTime().getTime()
    static Long yesTday = zeroMill - DAY_MILLON

    /**
     * 统计钻石的入账，出账，总账
     */
    private static void DiamondMonthStatic(int i) {
        Calendar cal = getCalendar()
        cal.add(Calendar.MONTH, -i)
        long firstDayOfCurrentMonth = cal.getTimeInMillis()  //当月第一天
        cal.add(Calendar.MONTH, -1)
        long firstDayOfLastMonth = cal.getTimeInMillis()  //上月第一天
        String ym = new Date(firstDayOfLastMonth).format("yyyyMM")
        def timebetween = [timestamp: [$gte: firstDayOfLastMonth, $lt: firstDayOfCurrentMonth]]
        println("timebetween is ${timebetween}")

        def total = 0
        def inc_total = 0
        def desc_total = 0
        def inc_detail = new HashMap<String, Long>()
        def desc_detail = new HashMap<String, Long>()
        def query = $$(timebetween)
        def cursors = diamond_dailyReport_stat.find(query)
        while (cursors.hasNext()) {
            def obj = cursors.next()
            total += obj.containsField('total') ? obj['total'] as Long : 0L
            inc_total += obj.containsField('inc_total') ? obj['inc_total'] as Long : 0L
            desc_total += obj.containsField('desc_total') ? obj['desc_total'] as Long : 0L
            def current_inc_map = obj.containsField('inc_detail') ? obj['inc_detail'] as Map<String,Long> : [:]
            current_inc_map.each {
                k, v ->
                    println("k is ${k},v is ${v}")
                    if (inc_detail.containsKey(k.toString())) {
                        v = v + inc_detail.get(k.toString())
                    }
                    inc_detail.put(k.toString(), v)
                    println("inc_detail is ${inc_detail}")
            }

            def current_dec_map = obj.containsField('desc_detail') ? obj['desc_detail'] as Map<String,Long> : [:]
            current_dec_map.each {
                k, v ->
                    if (desc_detail.containsKey(k.toString())) {
                        v = v + desc_detail.get(k.toString())
                    }
                    desc_detail.put(k.toString(), v)
            }
        }

        // 本月期初结余=上月期末结余
        def begin_surplus = lastMonthSurplus(i)
        // 本月期末月结余 = 本月期初结余 + 总增加的钻石 - 总减少钻石
        def end_surplus = begin_surplus + total

        println "${new Date(firstDayOfLastMonth).format("yyyy-MM-dd")} begin_surplus:${begin_surplus} + total:${total} = ${end_surplus}"

        def myId = "${ym}_diamond_dailyReport_stat".toString()

        def row = $$('_id': myId, 'inc_total': inc_total, 'desc_total': desc_total, 'total': total, 'timestamp': firstDayOfLastMonth, date: ym,
                'inc_detail': inc_detail, 'desc_detail': desc_detail, 'begin_surplus': begin_surplus, 'end_surplus': end_surplus)

        diamond_month_stat.update($$(_id: myId), $$('$set', row), true, false)
    }

    /**
     * 上月起初结余
     * @param begin
     * @return
     */
    static Long lastMonthSurplus(int i) {
        Calendar cal = getCalendar()
        cal.add(Calendar.MONTH, -i)
        cal.add(Calendar.MONTH, -2)
        long firstDayOfLastMonth = cal.getTimeInMillis()  //上月第一天
        String ym = new Date(firstDayOfLastMonth).format("yyyyMM")
        //println "lastMonthSurplus ${ym} "
        def last_month = diamond_dailyReport_stat.findOne($$(_id: "${ym}_finance".toString()))
        return (last_month?.get('end_surplus') ?: 0) as Long;
    }

    final static Integer month = -1;
    final static Integer day = 0;

    static void main(String[] args) {
        long l = System.currentTimeMillis()
        long begin = l

        DiamondMonthStatic(month)
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ${DiamondMonthStat.class.getSimpleName()},DiamondMonthStatic cost  ${System.currentTimeMillis() - l} ms"
        Thread.sleep(1000L)

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


    protected static BasicDBObject $$(String key, Object value) {
        return new BasicDBObject(key, value);
    }

    protected static BasicDBObject $$(Map map) {
        return new BasicDBObject(map)
    }

}