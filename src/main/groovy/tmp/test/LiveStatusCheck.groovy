#!/usr/bin/env groovy
package crontab.test

@Grapes([
@Grab('org.mongodb:mongo-java-driver:2.14.2'),
@Grab('commons-lang:commons-lang:2.6'),
@Grab('redis.clients:jedis:2.1.0'),
])
import com.mongodb.Mongo

/**
 * 从 mongodb 中查询 live:true 检查redis中 LIVE_HEART 状态
 * date: 13-3-5 下午10:03
 * @author: yangyang.cong@ttpod.com
 */
import redis.clients.jedis.Jedis

def M = new Mongo("192.168.8.223",10009)
def mongo = M.getDB("xy")


def logRoomEdit =M.getDB("xylog").getCollection("room_edit")

def rooms = mongo.getCollection("rooms")
def users = mongo.getCollection("users")
def songs = mongo.getCollection("songs")

def redis = new Jedis("192.168.8.223")


