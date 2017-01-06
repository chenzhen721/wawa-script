#!/usr/bin/env groovy
package crontab.tmp

import com.mongodb.BasicDBObject
import com.mongodb.DBCollection
import com.mongodb.Mongo
@Grapes([
@Grab('org.mongodb:mongo-java-driver:2.14.2'),
@Grab('commons-lang:commons-lang:2.6'),
@Grab('redis.clients:jedis:2.1.0'),
@Grab('net.coobird:thumbnailator:0.4.7'),
])
import groovy.json.JsonSlurper
import org.apache.commons.lang.StringUtils

import javax.imageio.ImageIO
import javax.servlet.http.HttpServletRequest
import java.awt.image.BufferedImage
import net.coobird.thumbnailator.Thumbnails

class  UpdateStarPic{

    //static mongo = new Mongo("192.168.8.119", 27017)
    //static mongo = new Mongo("127.0.0.1", 10000)
    static mongo  = new Mongo(new com.mongodb. MongoURI('mongodb://10.0.5.32:10000,10.0.5.33:10000,10.0.5.34:10000/?w=1&slaveok=true'))

    static String pic_domain = "http://img.sumeme.com/"

    static File pic_folder = new File("/empty/upload/")

    public  static final BasicDBObject ROOM_QUERY = new BasicDBObject(pic_url: [$exists: true], test: [$ne: true])

    public static DBCollection users(){return mongo.getDB('xy').getCollection("users");}
    public static DBCollection rooms(){return mongo.getDB('xy').getCollection("rooms");}


    def static batchPic(){
        rooms().find(ROOM_QUERY, new BasicDBObject("pic_url",1)).toArray().each {
            def map = new HashMap()
            String url =  it["pic_url"] as String
            Integer _id = it["_id"] as Integer
            if(StringUtils.isNotEmpty(url)){
                //W:231 ,H:142
                map.put("union_pic.bd_231X142", cutImage(url, 231, 142))
                //W:172, H:107
                map.put("union_pic.bd_172X107", cutImage(url, 172, 107))

                users().update(new BasicDBObject('_id',_id),new BasicDBObject('$set',map),false,false)
            }
        }
    }

    def static String cutImage(String allow_url, int rw, int rh){
        String fpath = "";
        try{
            def url = new URL(allow_url)
            BufferedImage img = ImageIO.read(url)
            fpath = url.getPath().replace('.jpg','').substring(1)+"_${rw}${rh}.jpg"
            File file = new File(pic_folder,fpath)
            file.getParentFile().mkdirs()
            Thumbnails.of(img).size(rw,rh).keepAspectRatio(false).toFile(file)
        }catch(Exception e){
            return fpath
        }
        return fpath
    }

    static void main(String[] args)
    {
        long l = System.currentTimeMillis()
        batchPic()
        println "${new Date().format('yyyy-MM-dd HH:mm:ss')}   UpdateStarPic, cost  ${System.currentTimeMillis() -l} ms"
    }
}


