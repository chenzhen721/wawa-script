#!/usr/bin/env groovy
package crontab.sys

@Grapes([
@Grab('org.mongodb:mongo-java-driver:2.14.2'),
@Grab('commons-lang:commons-lang:2.6'),
@Grab('redis.clients:jedis:2.1.0'),
])
import groovy.json.JsonBuilder

/**
 * 从 mongodb 中查询 live:true 检查redis中 LIVE_HEART 状态
 * date: 13-3-5 下午10:03
 * @author: yangyang.cong@ttpod.com
 */
import redis.clients.jedis.Jedis

import java.text.SimpleDateFormat



def day = args[0]
def url = "http://show.dongting.com/notice/${args[1]}"
def  msg = args[2]
def fmt = new SimpleDateFormat('yyyy-MM-dd')
if(System.currentTimeMillis() < fmt.parse(day).getTime()){
    new Jedis("10.0.3.5").publish("ALLchannel",
            new JsonBuilder(action:'sys.notice',data_d:[msg:msg,url:url]).toString())
}



