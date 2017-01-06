var http = require('http');


['a', 'b', 'c'].every(function(e) {
    console.log(e)
    return true
});

(function(){
    http.get("http://ttus.ttpod.com/login?user_name=1362650267080@robot.com&password=ttpodqwert2wsx", function(res) {
//        res.pipe(process.stdout);
//    console.log(res)
//    console.log("Got response: " + res.statusCode);
        var buffers = []
        res.on('data', function (data) {
            buffers.push(data)
        });
        res.on('end', function () {
            console.log(JSON.parse(buffers.toString()))
        });
    }).on('error',function(e){console.log(e.message)})
    setTimeout(arguments.callee, 5000)
} )()




//var rest = require('restler');
//
//rest.post('http://service.com/login', {
//    data: { username: 'foo', password: 'bar' }
//}).on('complete', function(data, response) {
//        if (response.statusCode == 200) {
//            // you can get at the raw response like this...
//        }
//});
//
//rest.get('http://twaud.io/api/v1/users/danwrong.json').on('complete', function(data) {
//    require('util').puts(data[0].message); // auto convert to object
//});