
//npm install socket.io-client redis mongodb

var ioc = require('socket.io-client'),
    redis = require('redis'),http = require('http'),
    F = require('util').format;

const PWD = 'ttpodqwert2wsx' ,P = function(){};
var redis = redis.createClient(6379,'10.0.5.201');
var cleanIds = ['0?','1?','20','21','22','23'];
cleanIds.forEach(function(idp){
    redis.keys('user:10'+idp+'???:rooms',function(err,user_rooms){
        user_rooms.forEach(function(user_room){
            redis.smembers(user_room,function(err,roomIds){
                var userId = user_room.split(":")[1]
                roomIds.forEach(function(roomId){
                    redis.srem('room:'+roomId+':users',userId,P)
                    console.log("remove user:" + userId +" from Room:"+roomId)
                })
                redis.del(user_room,P)
            })
        })
    })
})

const USERS = [];
const INVS = [60000,70000,300000];

const ROBOTS ={};

Array.prototype.rand = function() {
    return this[Math.floor(Math.random()*this.length)]
}
require('mongodb').MongoClient.connect("mongodb://10.0.5.32:10000,10.0.5.33:10000,10.0.5.34:10000/xy?replicaSet=ttpod&readPreference=secondary",
    function(err, db) {
        var db_user = db.collection("users")
        var db_room = db.collection("rooms")
        db_user.find({via:"robot",sex:1},{user_name:1}).batchSize(2000).toArray(function(e, docs) {USERS[1]=docs})
        db_user.find({via:"robot",sex:2},{user_name:1}).batchSize(1000).toArray(function(e, docs) {USERS[2]=docs})
        db_user.find({via:"robot",sex:0},{user_name:1}).batchSize(9000).toArray(function(e, docs) {

            USERS[0]=docs;

            (function(){
                db_room.find({},{live:1}).toArray(function(err, rooms) {
                    if(rooms&&rooms.every)
                        rooms.every(function(room) {
                            var roomId = room._id
                            if(!room.live){
                                closeRoom(roomId)
                                return true
                            }
                            if(!ROBOTS[roomId]){
                                console.log("机器人开始进入房间:" + roomId)
                                ROBOTS[roomId] = new Robot(roomId)
                            }
                            return true
                        });
                })

                //60秒后重新获取房间检查
                setTimeout(arguments.callee, 60000)
            })()
        })
    })


console.log("OK.")
function doLogin(loginURL,call){
    http.get(loginURL, function(res) {
        var buffers = [] // res.pipe(process.stdout);
        res.on('data', function (trunk) {buffers.push(trunk)})
        res.on('end', function () {
            //console.log(loginURL+'\n'+buffers.toString())
            var data = JSON.parse(buffers.toString()).data
            //console.log(data)
            var token = data ? data.access_token : null
            if(token){
                doLogin('http://api.show.dongting.com/user/info/'+token,call)
            }else if(call){
                call()
            }
        });
    })
}


function tryEnterRoom(roomId,level){
    var user = USERS[level].rand()
    if(null == user){
        return
    }
    redis.ttl(F('room:%s:live',roomId),function(e,ttl){
        if(ttl>5){
            joinRoom(roomId,user)
        }else{ closeRoom(roomId)
        }
    })
}
function closeRoom(roomId){
    var robot = ROBOTS[roomId]
    if(robot){
        console.log(' robot  destroy . [ for room is close : '+roomId)
        robot.destroy()
        delete ROBOTS[roomId]
    }
}

const bl={} ;//用户不重复使用
function joinRoom(roomId,user){
    var userId  = user._id
    redis.get(F('user:%s:access_token',userId),function(e,token){
        if(token){
            if(bl[userId]){
                return
            }
            bl[userId]=token;
            var url = F('http://127.0.0.1:7002?room_id=%s&access_token=%s',roomId,token);
            var socket = ioc.connect(url,{'force new connection': true})
            socket.on('error',function(d){console.log('Error Enter Room : ' +d)})
            console.log(userId +' : '+ user.user_name + ' Enter Room : ' +roomId)
            setTimeout(function(){
                socket.disconnect()
                redis.del(F('user:%s:access_token',userId),redis.print)
                redis.del('token:'+bl[userId],redis.print)
                delete bl[userId]
                console.log(userId +' : '+ user.user_name + ' Leave Room : ' +roomId)
            },300000)
        }else{
            var login = 'http://ttus.ttpod.com/login?user_name='+user.user_name+'&password='+PWD
            doLogin(login,function(){
                console.log(user._id +' login  AND join ')
                joinRoom(roomId,user)
            })
        }
    })
}

function Robot(roomId){
    for(var i=0;i<3;i++){tryEnterRoom(roomId,i)}

    this.inv0 = setInterval(function(){tryEnterRoom(roomId,0)},INVS[0]);
    this.inv1 = setInterval(function(){tryEnterRoom(roomId,1)},INVS[1]);
    this.inv2 = setInterval(function(){tryEnterRoom(roomId,2)},INVS[2]);

    this.destroy=function(){
        console.log(' clearInterval 0: '+ this.inv0)
        clearInterval(this.inv0)
        console.log(' clearInterval 1: '+ this.inv1)
        clearInterval(this.inv1)
        console.log(' clearInterval 2: '+ this.inv2)
        clearInterval(this.inv2)
    }
}

