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
 * date: 13-3-11 下午6:21
 * @author: yangyang.cong@ttpod.com
 */
class ReserveId {
    //static String[] STARTS_WITH = ["198", "199", "200" ];

    static String[] AAA = ["000", "111", "222", "333", "444", "555", "666", "777", "888", "999"];

    static String[] AAAA = ["0000", "1111", "2222", "3333", "4444", "5555", "6666", "7777", "8888", "9999"];

    static String[] ABC = [ "123456","2345","3456","87654321","1234567"];

    //static String[] LOVE = ["520", "521","1314","3344"];
    static String[] LOVE = ["520", "521","3344", "5201314","1314", "521314"];

    static String[][] GOOD = [AAA, AAAA, ABC,LOVE];


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

        if (chars[2]==chars[3] && chars[4]==chars[5]&& chars[6]==chars[7]){
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

    static mongo  = new Mongo(new MongoURI(getProperties('mongo.uri','mongodb://192.168.31.246:27017/?w=1') as String))

    public static void main(String[] args) throws Exception {
     long l = System.currentTimeMillis()
        DBCollection coll = mongo.getDB("xy").getCollection("pretty1000");
        //coll.drop()
        coll = mongo.getDB("xy").getCollection("pretty1000");
        int count = 0;
        for (int i = 10000000; i < 100000000; i++) {
            int p = pretty(String.valueOf(i));
            if (p > 0) {
                count++;
                coll.insert(new BasicDBObject("_id", i).append("t", p));
            }
        }
        println count;
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   ReserveId, cost  ${System.currentTimeMillis() -l} ms"

    }
}
