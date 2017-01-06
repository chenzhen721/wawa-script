#!/usr/bin/env groovy
package tmp

@Grapes([
@Grab('org.mongodb:mongo-java-driver:2.10.1'),
@Grab('commons-lang:commons-lang:2.6'),
@Grab('redis.clients:jedis:2.1.0'),
])
import com.mongodb.*
import redis.clients.jedis.Jedis

//def mongo = new Mongo("192.168.1.156", 10000)
//def final String jedis_host = "127.0.0.1" //test

//def mongo = new Mongo("192.168.31.246", 27017)

def final String jedis_host = "192.168.1.100"
def mongo = new Mongo(new MongoURI('mongodb://192.168.1.36:10000,192.168.1.37:10000,192.168.1.38:10000/?w=1&slaveok=true'))


def mainRedis = new Jedis(jedis_host,6379)
def liveRedis = new Jedis(jedis_host,6380)


def final Long DAY_MILL = 24*3600*1000L
//[vip:-1,vip_expires: (vip_days * ShopController.DAY_MILL+System.currentTimeMillis())]
def apply = mongo.getDB('xy_admin').getCollection('applys')
def finance_log = mongo.getDB('xy_admin').getCollection('finance_log')
def users = mongo.getDB('xy').getCollection('users')
def rooms = mongo.getDB('xy').getCollection('rooms')
def day_login = mongo.getDB('xylog').getCollection('day_login')
def lottery_logs = mongo.getDB('xylog').getCollection('lottery_logs')
def forbidden_logs = mongo.getDB('xylog').getCollection('forbidden_logs')
def xy_user = mongo.getDB('xy_user').getCollection('users')
def pretty4sale = mongo.getDB('xy').getCollection('pretty4sale')

users.updateMulti(new BasicDBObject('status',false).append('finance.coin_spend_total', new BasicDBObject('$gte': 0)),
        new BasicDBObject('$set',new BasicDBObject('status',true)))
//rooms.updateMulti(new BasicDBObject('tags',new BasicDBObject('$ne',null)),new BasicDBObject('$unset',new BasicDBObject('tags',1)))
//users.update(new BasicDBObject('_id',1201589),
//           new BasicDBObject('$set',new BasicDBObject("medals.124":1413973460785)))

/*
def Map mapPro = [11:"北京",12:"天津",13:"河北",14:"山西",15:"内蒙古",21:"辽宁",
                  22:"吉林",23:"黑龙江",31:"上海",32:"江苏",33:"浙江",34:"安徽",35:"福建",36:"江西",
                  37:"山东",41:"河南",42:"湖北",43:"湖南",44:"广东",45:"广西",46:"海南",50:"重庆",
                  51:"四川",52:"贵州",53:"云南",54:"西藏",61:"陕西",62:"甘肃",63:"青海",64:"宁夏",
                  65:"新疆",72:"台湾",81:"香港",91:"澳门",]
int count = 0
apply.find(new BasicDBObject('status',2),
        new BasicDBObject([sfz:1,xy_user_id:1])).limit(3000).toArray().each{
    Integer uid = it['xy_user_id'] as Integer
    if(it['sfz']){
        Integer num = Integer.valueOf(it['sfz'].toString().substring(0,2))
        println uid+":"+num +":"+ mapPro[num]
        rooms.update(new BasicDBObject('xy_star_id',uid),
                new BasicDBObject('$set',new BasicDBObject("address.province":mapPro[num])))
    }
    count++
}
println count

*/

/*
rooms.find(new BasicDBObject(mm_no:null),new BasicDBObject("xy_star_id": "1")).toArray().each {
    Integer id = it['_id'] as Integer
    rooms.update(new BasicDBObject("_id": id), new BasicDBObject('$set',new BasicDBObject("mm_no":id)), false, false)
}*/
/*
def ad_ips = 'ad_ips'
def String nick_name = "．C0M"
def query = new BasicDBObject(timestamp: new BasicDBObject('$gte': 1421769600000))
        .append("finance.coin_count", new BasicDBObject('$lte': 0))
        .append('status',true)
        .append('qd','MM')
        .append("nick_name", new BasicDBObject('$regex': nick_name))

def adusers = users.find(query, new BasicDBObject('_id':1)).limit(3000).toArray()
def Map<String, Integer> ips = new HashMap()
def ip_count = 0


adusers.each {
    def userid = it['_id'] as Integer
    //println userid

    users.update(new BasicDBObject('_id',userid),
           new BasicDBObject('$set',new BasicDBObject("status":false)))
    def key = "user:${userid}:access_token".toString()
    String token = mainRedis.get(key)
    println "token:${token}".toString()
    if (token) {
        mainRedis.del(key)
        mainRedis.del("token:"+token)
    }

    //IP 封杀

    day_login.find(new BasicDBObject('user_id',userid)).toArray().each {
        String uid = it['ip'] as String
        println "uid:${uid}".toString()
        if(liveRedis.ttl(uid) <= 0){
            liveRedis.set("uidblack:${uid}".toString(), userid.toString())
            liveRedis.expire("uidblack:${uid}".toString(), 99999999)
        }

    }
    //IP收集
    day_login.find(new BasicDBObject('user_id',userid)).toArray().each {
        ip_count++
        String ip = it['ip'] as String
        String user_id = it['user_id'] as String
        Integer count = 1
        if(ips.containsKey(ip)){
            count = ips.get(ip)
        }
        ips.put(ip, (count+1))
        forbidden_logs.update(new BasicDBObject('_id',ad_ips),new BasicDBObject('$addToSet',["ips":ip]), true, false)
    }
}
println adusers.size()
println "ip_count : ${ip_count}"
println "ips : "+ips
*/

/*
//解封
day_login.find(new BasicDBObject('user_id',3617096)).toArray().each {
    String uid = it['ip'] as String
    println "uid:${uid}".toString()
    liveRedis.del("uidblack:${uid}".toString())
}
*/


/*
//恶意签到刷币
def query = new BasicDBObject('status',true)
            .append('mobile_bind',false)
            .append('_id', new BasicDBObject('$gte': 5000000))
            .append('uname_bind',true)
            .append('via','local')
            .append('qd','MM')
            .append('priv',3)
            .append('pic','http://img.2339.com/22/6/1403510731734.jpg')
            .append('timestamp', new BasicDBObject('$gte': 1417363200000))
            .append('finance.coin_count', 0)
            .append('finance.feather_count', null)
            .append('finance.coin_spend_total', null)
            .append("user_name", new BasicDBObject('$regex': '^[A-Za-z]*$'))
            .append("nick_name", new BasicDBObject('$regex': '^[A-Za-z]*$'))
            .append("address", null)
def adusers = users.find(query, new BasicDBObject('_id':1,'nick_name':1,'user_name':1)).limit(10000).toArray()

adusers.each {
    def userid = it['_id'] as Integer
    def nick_name = it['nick_name'] as String
    def user_name = it['user_name'] as String

    if(nick_name.equalsIgnoreCase(user_name)){
    println it

        users.update(new BasicDBObject('_id',userid),
                new BasicDBObject('$set',new BasicDBObject("status":false)))
        def key = "user:${userid}:access_token".toString()
        String token = mainRedis.get(key)
        println "token:${token}".toString()
        if (token) {
            mainRedis.del(key)
            mainRedis.del("token:"+token)
        }

    }

}

println adusers.size()
*/

/*
def query = new BasicDBObject('mm_no',null)
        .append('via','local')
        .append('regTime', new BasicDBObject('$gt': 1418572800000))
        .append('regTime', new BasicDBObject('$lte': System.currentTimeMillis() - 5 * 60 * 1000))
        .append("nickname", new BasicDBObject('$regex': '^[A-Za-z]*$'))
def adusers = xy_user.find(query, new BasicDBObject('_id':1)).limit(20000).toArray()
def tuids = adusers.collect{((Map) it).get('_id') as String}
println tuids.size()
def AD_UIDS_KEY = "ad:user:ids"
mainRedis.del(AD_UIDS_KEY)
tuids.each {String tuid ->
    mainRedis.sadd(AD_UIDS_KEY, tuid)
}
*/
/**
 *
 {aggregate : "applys", pipeline : [{$match : {timestamp:{$gte: 1420045200000,$lt:1428940800000}}},
 {$project:{userId:'$uid'}},
 {$group:{_id:'$userId', total:{$sum: 1}}},
 {$sort: {total:-1}},
 {$limit : 10}
 ]}
 */
mongo.getDB('xy_friend').getCollection('applys').aggregate(
        new BasicDBObject('$match', new BasicDBObject('timestamp':[$gte: 1420045200000])),
        new BasicDBObject('$project', [userId: '$uid']),
        new BasicDBObject('$group', [_id: '$userId', total: [$sum: 1]]),
        new BasicDBObject('$sort', [total:-1]),
        new BasicDBObject('$limit', 100)
).results().each {
    def o = new BasicDBObject(it as Map)
    println o.get('_id') + ":" + o.get('total')
    def _id = o.get('_id') as Integer
    def total = o.get('total') as Long
    if(total>= 1000){
        users.update(new BasicDBObject('_id',_id),
                new BasicDBObject('$set',new BasicDBObject("status":false)))
        def key = "user:${_id}:access_token".toString()
        String token = mainRedis.get(key)
        println "token:${token}".toString()
        if (token) {
            mainRedis.del(key)
            mainRedis.del("token:"+token)
        }
        mongo.getDB('xy_friend').getCollection('applys').remove(new BasicDBObject(uid:_id))
    }

}

Integer family_id = 5352744;
Integer priv = 2;
users.update(new BasicDBObject(_id:1881736),
        new BasicDBObject($set:['family.family_id':family_id,
                                'family.family_priv':priv,
                                'family.timestamp':1435271353913,
                                'family.lastmodif':1436543855130]))