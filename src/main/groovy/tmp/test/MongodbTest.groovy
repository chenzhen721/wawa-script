#!/usr/bin/env groovy

@Grapes([
        @Grab('org.mongodb:mongo-java-driver:2.14.2'),
        @Grab('commons-lang:commons-lang:2.6'),
        @Grab('redis.clients:jedis:2.1.0'),
])
import com.mongodb.Mongo
import com.mongodb.MongoURI
import com.mongodb.BasicDBObject

import java.util.concurrent.BrokenBarrierException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

/**
 * MongodbTest
 */

class MongodbTest{
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

    //3.0 mongodb://192.168.31.249:30000
    //def static mongo  = new Mongo(new MongoURI(getProperties('mongo.uri','mongodb://192.168.31.249:10000/?w=1') as String))
    //def static mongo = new Mongo(new MongoURI('mongodb://192.168.31.249:30000/?w=1' as String))
    def static mongo  = new Mongo(new MongoURI('mongodb://192.168.31.249:10000/?w=1' as String))
    public static users_test = mongo.getDB("iibench").getCollection("users");
    static Map<String, String> userCounts = Collections.emptyMap();
    static Map<String, String> userTimes = Collections.emptyMap();

    static final Integer insert_threads = 30
    static final Integer query_threads = 30
    static final Integer update_threads = 30

    static final Integer insert_counts = 800000
    static final Integer query_counts = 800000
    static final Integer update_counts = 500000

    static ExecutorService insert_executor = Executors.newFixedThreadPool(insert_threads);
    static ExecutorService query_executor = Executors.newFixedThreadPool(query_threads);
    static ExecutorService update_executor = Executors.newFixedThreadPool(update_threads);

    static void main(String[] args){
        CountDownLatch doneSignal = new CountDownLatch(insert_threads);

        Integer id = 10000000
        Integer count = 0;

        CyclicBarrier insertBarrier = new CyclicBarrier(insert_threads);
        users_test.drop();
        println "insert begin..."
        insert_threads.times {
            count++;
            insert_executor.submit(new Runner(insertBarrier, doneSignal, '插入', new Insert(insert_counts, '插入', id*count)))
        }
        doneSignal.await()
        println "insert job is done"


        users_test.drop();
        createIndex();
        doneSignal = new CountDownLatch(insert_threads);
        println "insert with index begin..."
        id = 10000000
        count = 0;
        insert_threads.times {
            count++;
            insert_executor.submit(new Runner(insertBarrier, doneSignal, '插入', new Insert(insert_counts, '插入 with index', id*count)))
        }
        doneSignal.await()
        println "insert with index done..."



        doneSignal = new CountDownLatch(query_threads + update_threads);
        id = 10000000
        count = 0;
        //多个线程同时更新
        CyclicBarrier barrier = new CyclicBarrier(query_threads+update_threads);
        update_threads.times {
            update_executor.submit(new Runner(barrier, doneSignal, '更新', new Update(update_counts, '更新', id*count)))
        }

        id = 10000000
        count = 0;
        //多个线程同时查找
        query_threads.times {
            query_executor.submit(new Runner(barrier, doneSignal, '查询', new Query(query_counts, '查询', id*count)))
        }

        doneSignal.await()


        println "all job is done"
    }

    private static void createIndex(){
        users_test.createIndex(new BasicDBObject(tuid:1,nick_name:1,status:1))
        users_test.createIndex(new BasicDBObject(nick_name:1,timestamp:1))
        users_test.createIndex(new BasicDBObject(a:1,b:1))
        users_test.createIndex(new BasicDBObject(a:1,b:1,c:1))
        users_test.createIndex(new BasicDBObject(a:1,b:1,c:1,timestamp:1))
    }
}

class Insert extends Work{
    private id;
    public Insert(Integer count, String jobName, Integer id){
        super(count, jobName)
        this.id = id;
    }
    public void doSpecifSomething(){
        def users_test = MongodbTest.users_test;
        users_test.insert(generateUsers(id++))
    }
    public static BasicDBObject generateUsers(Integer id){
        return new BasicDBObject(
                _id: id,
                tuid: "tuid-"+id,
                nick_name: "nick_name"+id,
                a: "a"+id,
                b: "b"+id,
                c: "c"+id,
                d: "d"+id,
                status : true,
                priv : 3,
                sex: 1,
                timestamp:System.currentTimeMillis()
        )
    }
}

class Query extends Work{
    private id;
    public Query(Integer count, String jobName, Integer id){
        super(count, jobName)
        this.id = id;
    }
    public void doSpecifSomething(){
        def users_test = MongodbTest.users_test;
        users_test.find(new BasicDBObject(_id : id++))
    }
}

class Update extends Work{
    private id;
    public Update(Integer count, String jobName, Integer id){
        super(count, jobName)
        this.id = id;
    }
    public void doSpecifSomething(){
        def users_test = MongodbTest.users_test;
        users_test.update(new BasicDBObject(_id : id++), new BasicDBObject('$set',[priv:2, sex: 0,timestamp:System.currentTimeMillis()]))
    }
}


abstract class Work{
    protected Integer count;
    private String jobName;
    public Work(){
        this(10,'jobName');
    }
    public Work(Integer count, String jobName){
        this.count = count;
        this.jobName = jobName;
    }
    public void doSomething(){
        long begin = System.currentTimeMillis()
        Integer total = count
        while (count-- > 0){
            doSpecifSomething();
        }
        long end = System.currentTimeMillis() - begin
        println "${jobName}, count:${total}, cost time total: ${end} ms, avg : ${end/total} ms"
    }
    public abstract void doSpecifSomething();
}



class Runner implements Runnable {
    // 一个同步辅助类，它允许一组线程互相等待，直到到达某个公共屏障点 (common barrier point)
    private CyclicBarrier barrier;

    private final CountDownLatch doneSignal;

    private String name;

    private Work work;

    public Runner(CyclicBarrier barrier,CountDownLatch doneSignal, String name, Work work) {
        super();
        this.barrier = barrier;
        this.doneSignal = doneSignal;
        this.name = name;
        this.work = work;
    }

    @Override
    public void run() {
        try {
            Thread.sleep(1000 * (new Random()).nextInt(8));
            barrier.await();
            work.doSomething();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        }finally{
            doneSignal.countDown();
        }
    }
}

