# Raft Based Distributed Cache
基于Raft论文实现的,github有个中文的[Raft中文翻译版](https://github.com/maemual/raft-zh_cn)<br>
翻译的很准,不过简化了好多,感觉还是英文的比较好,英文的读的慢思考时间更多。<br>
学习用的应该有bug的,包括查资料,写代码,测试用了大概十多天的样子吧<br>
整体思路就是由Raft算法维持集群状态的一致性（节点丢失，增加问题）
一致性hash将数据分散到各个节点上


# 支持的功能
* leader选举.不管是刚启动还是leader丢失,都能保证一轮选举选出新leader
* 集群成员动态更变,保证集群半数以上server活着,依然能对外提供服务
* 基于一致性hash保证缓存数据的负载均衡,客户端连接也是连接到目标数据节点上,处
* 客户端由于配置了集群的信息,也可以生成一致性hash,虽然跟服务器可能不一致,但是正常情况是命中率很高.<br>
  在服务器没有节点丢失是100%命中,即出现不命中的情况,会同步服务的节点状况,发起二次请求,不影响后续的请求.
* 缓存数据的备份(内存快照+写操作的日志 = 完整缓存数据)


# 技术点
* Raft算法
* 一致性hash
* Netty,集群节点之间的通信使用的Netty
* NIO.(处理客户端请求是基于Java原生的nio实现,ProtoBuf对象的序列化,反序列化.<br>之前也是用的Netty实现的,由于是异步通信框架最后自己做了同步处理,然后性能测试的时候不到redis的1/20) 
* 并发编程


# Quick Start
* chmod -R 777 ./out
* cd out
* ./build.sh 5    可以快速在本机上部署五个实例的集群(必须写正常的参数,shell脚本不太会,没处理异常的)
* ./kill_all.sh 5 可以关闭这五台集群(必须正确的参数)
* ./kill.sh 1     可以关闭id = 1 的一个实例(必须正确的参数)
* ./start.sh 1    可以开启id = 1 的一个实例(必须正确的参数)
* ./client.sh     简单的命令行客户端
* 反复创建(执行build.sh) 而没有对应关掉的话,自己jps 查看下手动kill
* build.sh 之后一定要有kill，才能再build,pid文件存在创建实例的时候会自己退出
* Client Demo
```java
public class Demo {
    public static void main(String[] args) {
        CacheClient cacheClient = new CacheClient.newBuilder()
                .setNumberOfReplicas(CacheOptions.numberOfReplicas)
                .setNewNode("1:127.0.0.1:8081:9091")
                .setNewNode("2:127.0.0.1:8082:9092")
                .setNewNode("3:127.0.0.1:8083:9093")
                //.setNewNode("4:127.0.0.1:8084:9094")
                //.setNewNode("5:127.0.0.1:8085:9095")
                .build();
        boolean flag  = cacheClient.set("name", "quan",-1);
        if (flag) {
            String val = cacheClient.get("name");
            System.out.println(val);
        }
        flag = cacheClient.set("sex", "man", 1234506);
        System.out.println(flag);
        System.out.println("name=" + cacheClient.get("name") + " sex = " + cacheClient.get("sex"));

        flag = cacheClient.del("sex");
        System.out.println(flag);
        flag = cacheClient.del("name");
        System.out.println(flag);

        System.out.println("name=" + cacheClient.get("name") + " sex = " + cacheClient.get("sex"));

        System.out.println(cacheClient.status(null));
        cacheClient.close();
    }
}

```

# 程序运行流程
* load node 
* load snaphot 
* load raft logs
* 加载缓存备份数据,恢复到之前的状态
* init thread pool
* 启动监听其它节点消息的线程
* 启动监听客户端的消息的线程
* 启动异步写日志线程


# 总结(遇到的坑)
* lock.lock() 一定要在finally里面unlock 释放锁，状态转换的时候可能会中断该线程，导致锁没有释放
    最后其他线程无限等待状态
* lock 里面不要放io这种耗时操作，io 阻塞后的线程切换并不会释放锁，切换别的线程拿不到锁，也是白搞
* 本来是用map维护socket连接的，最后自己粗心了，创建socket之后没有put进去,导致监听该socket连接的server
    创建过多连接，导致线程池线程耗完，无法处理后续连接
* 考虑所有的情况，不要在编码的时候合并流程（即使该流程是不会发生的）,可以在编码完成后再合并


# 性能测试(只是跟redis对比测试)
* 该程序是在本机上跑的三个实例的集群,redis 单机的.
* 对于100w 次的请求没有任何请求异常
* 每秒能处理读请求    7.5w   redis 9.5w
* 每秒能处理写请求    5.6w   redis 8.5w
* 每秒能处理删除请求  7.3w   redis 9.3w
* 一致性hash的数据倾斜问题,在虚拟节点开到200个的时候,即hash环上有1000个节点的时候
* 对于100w 次的请求 每个节点处理的数据都是 20w +/- 50 基本没有数据倾斜问题
* cpu利用率峰值100% ,redis 100%.
* 即使对收到的消息直接返回,不做任何处理依然没有redis快,语言本身的性能差异摆在哪里的.
* 该程序测试代码
```java
public class ClientTest {
    public static void main(String[] args) {
        ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(10, 10, 0,
                TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
        long start = System.currentTimeMillis();
        final CountDownLatch countDownLatch = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) {
            poolExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    CacheClient cacheClient = new CacheClient.newBuilder()
                            .setNumberOfReplicas(CacheOptions.numberOfReplicas)
                            .setNewNode("1:127.0.0.1:8081:9091")
                            .setNewNode("2:127.0.0.1:8082:9092")
                            .setNewNode("3:127.0.0.1:8083:9093")
                            /*.setNewNode("4:127.0.0.1:8084:9094")
                            .setNewNode("5:127.0.0.1:8085:9095")*/
                            .build();
                    int success = 0;
                    for (int j = 0; j < 100000; j++) {
                        //UserMessageProto.ResponseMessage responseMessage = cacheClient.doSet(getRandomString(),getRandomString(),-1);
                        UserMessageProto.ResponseMessage responseMessage = cacheClient.doGet(getRandomString());
                        //UserMessageProto.ResponseMessage responseMessage = cacheClient.doDel(getRandomString());
                        if (responseMessage.getResponseType() == UserMessageProto.ResponseType.SUCCESS) {
                            success++;
                        }
                        //del 13381 ms
                        //get 13283 ms
                        //set 17862 ms
                    }
                    System.out.println(success);
                    cacheClient.close();
                    countDownLatch.countDown();
                }
            });
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(System.currentTimeMillis() - start);
        poolExecutor.shutdown();
    }

    /**
     * 获取一个随机的字符串.
     *
     * @return string
     */
    private static String getRandomString() {
        char[] chars = {
                'a', 'b', 'c', 'd', 'e',
                'f', 'g', 'h', 'i', 'j',
                'k', 'l', 'm', 'n', 'o',
                'p', 'q', 'r', 's', 't',
                'u', 'v', 'w', 'x', 'y',
                'z'
        };
        Random random = new Random();
        int num = random.nextInt(20) + 2;
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < num; i++) {
            int index = random.nextInt(26);
            builder.append(chars[index]);
        }
        return builder.toString();
    }
}
```
* Redis 测试代码
```java
public class RedisTest {
    public static void main(String[] args) {
        ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(10, 10, 0,
                TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
        long start = System.currentTimeMillis();
        final CountDownLatch countDownLatch = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) {
            poolExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    Jedis jedis = new Jedis("127.0.0.1");
                    for (int j = 0; j < 100000; j++) {
                        //jedis.set(getRandomString(), getRandomString());
                        //String val = jedis.get(getRandomString());
                        jedis.del(getRandomString());
                        //set 11698 ms
                        //get 10510 ms
                        //del 10662 ms
                    }
                    jedis.close();
                    countDownLatch.countDown();
                }
            });
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(System.currentTimeMillis() - start);
        poolExecutor.shutdown();
    }

    /**
     * 获取一个随机的字符串.
     *
     * @return string
     */
    private static String getRandomString() {
        char[] chars = {
                'a', 'b', 'c', 'd',
                'e', 'f', 'g', 'h',
                'i', 'j', 'k', 'l',
                'm', 'n', 'o', 'p',
                'q', 'r', 's', 't',
                'u', 'v', 'w', 'x',
                'y', 'z'
        };
        Random random = new Random();
        int num = random.nextInt(20) + 2;
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < num; i++) {
            int index = random.nextInt(26);
            builder.append(chars[index]);
        }
        return builder.toString();
    }
}

```

# 从数据一致性到系统架构的认识(猜想)
以前我总觉的mysql 这种主从复制集群模式,跟单机没啥区别,因为数据需要同步,主服务器写入的数据还是会到达从服务器<br>
这种集群能提供的性能跟单机差不多.现在自己实现了下,一致性算法,更能明显感觉到,需要提供数据一致性的功能集群(特别是对一致性要求高的)服务器
他能提供的最大性能是单机的k倍(k值可能就是2,3 这种小数字),不像web之类的服务器,它可以一直扩展下去,最后整个服务器集群提供的性能线性上升
所以这导致了随着用户的增加,整个系统的压力瓶颈会在mysql这里,所以后面出现了缓存,消息队列等就是为了避免大量请求到mysql数据库,导致数据库的崩溃.
为了将需要维护的一致性数据量减少,采用分库的形式,就是让每个mysql集群维护一部分的数据,每个集群之间没有数据交叉.
根据功能,服务拆分整个系统的数据库,就进一步演变成了,现在微服务的架构模式(微服務最大的好处是解耦降低系統設計的复杂度).
分库,或者微服务架构最后出现了新的问题,分布式事务.<br>

......


