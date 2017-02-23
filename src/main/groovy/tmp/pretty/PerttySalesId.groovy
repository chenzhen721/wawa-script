#!/usr/bin/env groovy
package crontab.pretty

@Grapes([
    @Grab('org.mongodb:mongo-java-driver:2.14.2')
])
import com.mongodb.BasicDBObject
import com.mongodb.DBCollection
import com.mongodb.DBCursor
import com.mongodb.Mongo
import com.mongodb.MongoURI

/**
 * 靓号库生存
 */
class PerttySalesId {
    //static String[] STARTS_WITH = ["198", "199", "200" ];

    static String[] AAA = ["000", "111", "222", "333", "444", "555", "666", "777", "888", "999"];

    static String[] AAAA = ["0000", "1111", "2222", "3333", "4444", "5555", "6666", "7777", "8888", "9999"];


    static String[] ABC = [ "123456","2345","3456","87654321"];

    //static String[] LOVE = ["520", "521","1314","3344"];
    static String[] LOVE = ["520", "521","3344", "5201314","1314", "521314"];

    static String[][] GOOD = [ AAAA, ABC,LOVE];


    static enum Type{
        Unkonw,生日,豹子,顺子,爱情,三联对,稀少数字
    }

    public static int pretty(String id) {

       /* for (String start : STARTS_WITH) {
            if (id.startsWith(start)) {
                return Type.生日.ordinal();
            }
        }*/

        for (int i = 0; i < 3; i++) {
            for (String good : GOOD[i]) {
                if (id.contains(good)) {
                    return 2 + i;
                }
            }
        }

        char[] chars =  id.toCharArray();


        if (chars[0]==chars[1] && chars[2]==chars[3]&& chars[4]==chars[5]){
            return Type.三联对.ordinal();
        }
        if (chars[1]==chars[2] && chars[3]==chars[4]&& chars[5]==chars[6]){
            return Type.三联对.ordinal();
        }

        if (ABCChar(chars) > 3) {
            return Type.顺子.ordinal();
        }

        if (diffChar(chars) <= 3) {
            return Type.稀少数字.ordinal();
        }

        return 0;
    }


    public static int diffChar(char[] str) {
        Arrays.sort(str);
        char begin = '\0';
        int i = 0;
        for (char c : str) {
            if (c != begin) {
                i++;
                begin = c;
            }
        }
        return i;
    }

    public static int ABCChar(char[] str) {
        char begin = '\0';
        int i = 0;
        for (char c : str) {
            if (c == begin+1) {
                i++;
            }
            begin = c;
        }
        return i;
    }

    // 9876
    public static int ABCReveresChar(char[] str) {
        char begin = '\0';
        int i = 0;
        for (char c : str) {
            if (c == begin-1) {
                i++;
            }
            begin = c;
        }
        return i;
    }


    public static generate(double length, int price){
        long l = System.currentTimeMillis()
        DBCollection coll = mongo.getDB("xy").getCollection("pretty4sale");
        int count = 0
        int begin = Math.pow(10, length-1)
        for (int i = begin; i < begin * 10; i++) {
            def info = new BasicDBObject("_id", i)
                    .append("sale", 1)
                    .append("show", Boolean.FALSE)
                    .append("length", length)
                    .append("price", price * 100)
            //coll.insert(info);
            count++;
        }
        println count;
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ReserveId, cost  ${System.currentTimeMillis() -l} ms"
    }

    public static update(double length){
        long l = System.currentTimeMillis()
        DBCollection coll = mongo.getDB("xy").getCollection("pretty4sale");
        int count = 0
        int begin = Math.pow(10, length-1)
        for (int i = begin; i < begin * 10; i++) {
            //coll.insert(info);
            def prettyNum= coll.findOne(new BasicDBObject("_id": i, show_price:null),new BasicDBObject("price": 1,show_price:1))
            Long price = prettyNum?.get('price') as Long
            Long show_price = prettyNum?.get('show_price') as Long
            if(show_price == null || show_price <= 0)
                coll.update(new BasicDBObject("_id": i, show_price:null), new BasicDBObject('$set', new BasicDBObject("show_price", price)))
            count++;
        }
        println count;
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ReserveId, cost  ${System.currentTimeMillis() -l} ms"
    }

    public static fourSpcNumber(){
        int aabbCount = 0
        int ababCount = 0
        int abcdCount = 0
        int begin = 1000
        DBCollection coll = mongo.getDB("xy").getCollection("pretty4sale");
        for (int i = begin; i < begin * 10; i++) {
            Integer price = 800
            def chars = i.toString().toCharArray()
            //aabb
            if (chars[0]==chars[1] && chars[2]==chars[3] && (chars[1] != chars[2])){
                price = 3000
                aabbCount++
            }
            //abab
            else if(chars[0]==chars[2] && chars[1]==chars[3] && (chars[0]!=chars[1] && chars[2]!=chars[3])){
                price = 3000

                ababCount++
            }
            def info = new BasicDBObject("_id", i)
                    .append("sale", 1)
                    .append("show", Boolean.FALSE)
                    .append("length", 4)
                    .append("price", price * 100)
            coll.save(info);
        }
        println "aabbCount: ${aabbCount}";
        println "ababCount: ${ababCount}";
        println "abcdCount: ${abcdCount}";
    }

    private static initData(){
        //初始化日志
        def pretty_users = mongo.getDB("xylog").getCollection('pretty_users')
        def users =  mongo.getDB("xy").getCollection("users");
        pretty_users.find().toArray().each {BasicDBObject num ->
            if(num['_id'].toString().isNumber()){
                def uid = num['_id'] as Integer
                def number = num['num'] as Integer
                def log_id = "${uid}_${number}".toString()
                num['_id'] = log_id
                num['userId'] = uid
                pretty_users.save(num)
                pretty_users.remove(new BasicDBObject("_id", uid))
                //设置用户靓号库
                users.update(new BasicDBObject("_id", uid),new BasicDBObject('$addToSet', new BasicDBObject("mm_nos", number)),false, false)
            }
        }

    }

    public static batchUpdatePrice(List<Integer> nums, Long price){
        long l = System.currentTimeMillis()
        DBCollection coll = mongo.getDB("xy").getCollection("pretty4sale");
        coll.updateMulti(new BasicDBObject("_id", ['$in':nums]), new BasicDBObject('$set', [price:price,show_price:price]))
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ReserveId, cost  ${System.currentTimeMillis() -l} ms"
    }

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

    public static void main(String[] args) throws Exception {
        //DBCollection coll = mongo.getDB("xy").getCollection("pretty4sale");
        //coll.drop()
        //generate(4, 800)
        //fourSpcNumber()
        //generate(5, 500)
        //generate(6, 300)

        //update(4)
        //update(5)
        //update(6)


        //为了支持一用户多靓号 初始化数据
        //initData()

        //批量修改靓号价格
        List<Integer> nums5 = [11000,11222,11333,11444,11555,11666,11777,11888,11999,22000,22111,22333,22444,22555,22666,22777,22888,22999,33000,33111,33222,33444,33555,33666,33777,33888,33999,44000,44111,44222,44333,44555,44666,44777,44888,44999,55000,55111,55222,55333,55444,55666,55777,55888,55999,66000,66111,66222,66333,66444,66555,66777,66888,66999,77000,77111,77222,77333,77444,77555,77666,77888,77999,88000,88111,88222,88333,88444,88555,88666,88777,88999,99000,99111,99222,99333,99444,99555,99666,99777,99888,11100,11122,11133,11144,11155,11166,11177,11188,11199,22200,22211,22233,22244,22255,22266,22277,22288,22299,33300,33311,33322,33344,33355,33366,33377,33388,33399,44400,44411,44422,44433,44455,44466,44477,44488,44499,55500,55511,55522,55533,55544,55566,55577,55588,55599,66600,66611,66622,66633,66644,66655,66677,66688,66699,77700,77711,77722,77733,77744,77755,77766,77788,77799,88800,88811,88822,88833,88844,88855,88866,88877,88899,99900,99911,99922,99933,99944,99955,99966,99977,99988,10001,12221,13331,14441,15551,16661,17771,18881,19991,20002,21112,23332,24442,25552,26662,27772,28882,29992,30003,31113,32223,34443,35553,36663,37773,38883,39993,40004,41114,42224,43334,45554,46664,47774,48884,49994,50005,51115,52225,53335,54445,56665,57775,58885,59995,60006,61116,62226,63336,64446,65556,67776,68886,69996,70007,71117,72227,73337,74447,75557,76667,78887,79997,80008,81118,82228,83338,84448,85558,86668,87778,89998,90009,91119,92229,93339,94449,95559,96669,97779,98889,10000,12222,13333,14444,15555,16666,17777,18888,19999,20000,21111,23333,24444,25555,26666,27777,28888,29999,30000,31111,32222,34444,35555,36666,37777,38888,39999,40000,41111,42222,43333,45555,46666,47777,48888,49999,50000,51111,52222,53333,54444,56666,57777,58888,59999,60000,61111,62222,63333,64444,65555,67777,68888,69999,70000,71111,72222,73333,74444,75555,76666,78888,79999,80000,81111,82222,83333,84444,85555,86666,87777,89999,90000,91111,92222,93333,94444,95555,96666,97777,98888,11110,11112,11113,11114,11115,11116,11117,11118,11119,22220,22221,22223,22224,22225,22226,22227,22228,22229,33330,33331,33332,33334,33335,33336,33337,33338,33339,44440,44441,44442,44443,44445,44446,44447,44448,44449,55550,55551,55552,55553,55554,55556,55557,55558,55559,66660,66661,66662,66663,66664,66665,66667,66668,66669,77770,77771,77772,77773,77774,77775,77776,77778,77779,88880,88881,88882,88883,88884,88885,88886,88887,88889,99990,99991,99992,99993,99994,99995,99996,99997,99998,10101,12121,13131,14141,15151,16161,17171,18181,19191,20202,21212,23232,24242,25252,26262,27272,28282,29292,30303,31313,32323,34343,35353,36363,37373,38383,39393,40404,41414,42424,43434,45454,46464,47474,48484,49494,50505,51515,52525,53535,54545,56565,57575,58585,59595,60606,61616,62626,63636,64646,65656,67676,68686,69696,70707,71717,72727,73737,74747,75757,76767,78787,79797,80808,81818,82828,83838,84848,85858,86868,87878,89898,90909,91919,92929,93939,94949,95959,96969,97979,98989]
        batchUpdatePrice(nums5, 80000);

        List<Integer> nums4 = [1000,1222,1333,1444,1555,1666,1777,1888,1999,2000,2111,2333,2444,2555,2666,2777,2888,2999,3000,3111,3222,3444,3555,3666,3777,3888,3999,4000,4111,4222,4333,4555,4666,4777,4888,4999,5000,5111,5222,5333,5444,5666,5777,5888,5999,6000,6111,6222,6333,6444,6555,6777,6888,6999,7000,7111,7222,7333,7444,7555,7666,7888,7999,8000,8111,8222,8333,8444,8555,8666,8777,8999,9000,9111,9222,9333,9444,9555,9666,9777,9888,1011,1211,1311,1411,1511,1611,1711,1811,1911,2022,2122,2322,2422,2522,2622,2722,2822,2922,3033,3133,3233,3433,3533,3633,3733,3833,3933,4044,4144,4244,4344,4544,4644,4744,4844,4944,5055,5155,5255,5355,5455,5655,5755,5855,5955,6066,6166,6266,6366,6466,6566,6766,6866,6966,7077,7177,7277,7377,7477,7577,7677,7877,7977,8088,8188,8288,8388,8488,8588,8688,8788,8988,9099,9199,9299,9399,9499,9599,9699,9799,9899,1101,1121,1131,1141,1151,1161,1171,1181,1191,2202,2212,2232,2242,2252,2262,2272,2282,2292,3303,3313,3323,3343,3353,3363,3373,3383,3393,4404,4414,4424,4434,4454,4464,4474,4484,4494,5505,5515,5525,5535,5545,5565,5575,5585,5595,6606,6616,6626,6636,6646,6656,6676,6686,6696,7707,7717,7727,7737,7747,7757,7767,7787,7797,8808,8818,8828,8838,8848,8858,8868,8878,8898,9909,9919,9929,9939,9949,9959,9969,9979,9989,1110,1112,1113,1114,1115,1116,1117,1118,1119,2220,2221,2223,2224,2225,2226,2227,2228,2229,3330,3331,3332,3334,3335,3336,3337,3338,3339,4440,4441,4442,4443,4445,4446,4447,4448,4449,5550,5551,5552,5553,5554,5556,5557,5558,5559,6660,6661,6662,6663,6664,6665,6667,6668,6669,7770,7771,7772,7773,7774,7775,7776,7778,7779,8880,8881,8882,8883,8884,8885,8886,8887,8889,9990,9991,9992,9993,9994,9995,9996,9997,9998]
        batchUpdatePrice(nums4, 150000);
    }
}
