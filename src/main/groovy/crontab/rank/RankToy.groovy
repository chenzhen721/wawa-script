#!/usr/bin/env groovy
import com.mongodb.DBObject
@Grapes([
@Grab('org.mongodb:mongo-java-driver:2.14.2'),
@Grab('commons-lang:commons-lang:2.6'),
@Grab('redis.clients:jedis:2.1.0'),
])
import com.mongodb.Mongo
import com.mongodb.MongoURI
import com.mongodb.BasicDBObject

/**
 * 抓中娃娃排行榜
 */
class RankToy {
    static Properties props = null;
    static String profilepath="/empty/crontab/db.properties";

    static getProperties(String key, Object defaultValue){
        try {
            if(props == null){
                props = new Properties();
                props.load(new FileInputStream(profilepath));
            }
        } catch (Exception e) {
            println e;
        }
        return props.get(key, defaultValue)
    }

    static mongo  = new Mongo(new MongoURI(getProperties('mongo.uri','mongodb://192.168.31.249:27017/?w=1') as String))

    //需要排除的礼物ID 铃铛礼物ID
    static List EXCLUTION_GIFTS_IDS = [586,587,588]

    static Map getThisSunWeek(){
        def cal =  Calendar.getInstance()
        cal.set(Calendar.DAY_OF_WEEK,2)
        if (cal.getTimeInMillis() > System.currentTimeMillis()){
            cal.add(Calendar.DAY_OF_YEAR,-7)
        }
        [$gte: cal.getTime().clearTime().getTime()]
    }
    static timestamp_between = getThisSunWeek();

    /**
     * 获得第几周
     * @param timestamp 传入同步的时间戳
     * @return
     */
    public static Integer getWeekOfYear(Long timestamp){
        Calendar cal =  Calendar.getInstance();
        cal.setFirstDayOfWeek(Calendar.MONDAY);
        if(timestamp != null){
            cal.setTimeInMillis(timestamp);
        }
        return cal.get(Calendar.WEEK_OF_YEAR);
    }

    public static BasicDBObject $$(String key,Object value){
        return new BasicDBObject(key,value);
    }

    public static BasicDBObject $$(Map map){
        return new BasicDBObject(map);
    }

    static bulidRank(def giftId) {
        def query = [type: "send_gift",
                     "session.data._id":giftId,'session.data.xy_star_id':[$ne:null],
                     timestamp: timestamp_between]
        def res = mongo.getDB("xylog").getCollection("room_cost").aggregate(
                new BasicDBObject('$match', query),
                new BasicDBObject('$project', [_id: '$session.data.xy_star_id',timestamp:'$timestamp',
                        count:'$session.data.count',earned:'$session.data.earned'
                ]),
                new BasicDBObject('$group', [_id: '$_id',
                        count: [$sum: '$count'],
                        earned:[$sum: '$earned'],
                        timestamp:[$max:'$timestamp']
                ]),
                new BasicDBObject('$sort', [count:-1,timestamp:1])
        )
        return res.results().iterator()
    }

    private final static Long SLEEP_TIME = 5 * 1000l
    private final static Integer MAX_THRESHOLD = 1
    static gift_rank(List<DBObject> gifts){
        String cat = "week"
        int threshold = 0;
        def giftRank = mongo.getDB("xyrank").getCollection("gift")
        for (DBObject gift : gifts){
            def gift_id = gift.get("_id") as Integer
            Iterator objs = bulidRank(gift_id)
            def star_count =(gift.get("star_count") ?: 9999999) as Integer //周星礼物上限
            Boolean star =(gift.get("star") ?: false) as Boolean //是否为周星礼物
            int i = 0
            def list = new ArrayList(100)
            while (objs.hasNext())
            {
                def obj =  objs.next()
                def count = obj.get("count") as Integer
                def rank = ++i

                def user_id = obj.get("_id") as Integer
                //周星礼物上限判断
                if(star ){
                    if(star_count > 0 && count > star_count){
                        obj.put("count", star_count)
                        rank = 1
                    }
                    //修正周星礼物数量
                    amend_week_star(user_id, gift_id, count)
                }

                obj.put("real_count", count)
                obj.put("user_id",obj.get("_id"))
                obj.put("_id","${gift_id}_${cat}_${obj.get('user_id')}".toString())
                obj.put("gift_id",gift_id)
                obj.put("pic_url",gift.get("pic_url"))
                obj.put("name",gift.get("name"))
                obj.put("cat",cat)
                obj.put("rank",rank)
                obj.put("sj",new Date())
                list<< obj
            }
            giftRank.remove(new BasicDBObject("cat" : cat, gift_id : gift_id))
            if(list.size() > 0)
                giftRank.insert(list)

            if(threshold++ >= MAX_THRESHOLD){
                Thread.sleep(SLEEP_TIME)
                threshold = 0;
            }
        }
    }

    /**
     * 修正周星礼物数量
     * 预防每周周星设置延迟问题， 导致周星及时榜单未记录周星数量
     * @param user_id
     * @param gift_id
     */
    static amend_week_star(Integer user_id, Integer gift_id, Integer count){
        def week_stars = mongo.getDB("xyrank").getCollection('week_stars')
        def week = getWeekOfYear(System.currentTimeMillis())
        week_stars.update($$('week': week, user_id : user_id, gift_id : gift_id), $$('$set':[count:count]), false, false)
    }

    /**
     * 周星前三用户贡献榜
     * @param gifts
     * @return
     */
    static week_gift_user_rank(List<DBObject> gifts){
        def week_stars = mongo.getDB("xyrank").getCollection('week_stars')
        gifts.each{
            Boolean star =(it.get("star") ?: false) as Boolean //是否为周星礼物
            if(star){
                def week = getWeekOfYear(System.currentTimeMillis())
                Integer gift_id = it['_id'] as Integer
                def ranks = week_stars.find($$('week': week, gift_id: gift_id),
                        $$(gift_id: 1,user_id:1, count:1)).sort($$("count", -1)).limit(5).toArray()
                ranks.each {
                    def _id = it['_id']
                    Integer star_id = it['user_id'] as Integer
                    Iterator objs=  bulidUserRank(star_id, gift_id)
                    while (objs.hasNext()) {
                        def obj = objs.next()
                        def user_id = obj.get("_id") as Integer
                        def count = obj.get("count") as Integer
                        week_stars.update($$(_id:_id), $$('$set':[most_user:user_id, most_user_count:count]))
                    }
                }
            }
        }
    }

    static bulidUserRank(Integer star_id, Integer gift_id) {

        def res = mongo.getDB("xylog").getCollection("room_cost").aggregate(
                new BasicDBObject('$match', [type: "send_gift",
                                             "session.data._id":gift_id,'session.data.xy_star_id':star_id,
                                             timestamp: timestamp_between]),
                new BasicDBObject('$project', [_id: '$session._id', count:'$session.data.count'
                ]),
                new BasicDBObject('$group', [_id: '$_id',count: [$sum: '$count']
                ]),
                new BasicDBObject('$sort', [count:-1]),
                new BasicDBObject('$limit',1)
        )
        return res.results().iterator()
    }

    static void main(String[] args)
    {
        long l = System.currentTimeMillis()
        List<DBObject> gifts = mongo.getDB("xy_admin").getCollection("gifts")
                .find(new BasicDBObject("status",true).append('_id', new BasicDBObject('$nin',EXCLUTION_GIFTS_IDS))).toArray()

        long begin = l
        gift_rank(gifts);
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  gift rank, cost  ${System.currentTimeMillis() -l} ms"

        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}  cost  ${System.currentTimeMillis() -begin} ms"

    }

}