import constant.CacheOptions;
import core.client.CacheClient;
import core.message.UserMessageProto;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


public class ClientTest {
    public static void main(String[] args) {

        /**
         * 测试结果说明(只是跟redis 对比测试):
         * 0. 该程序是在本机上跑的五个实例的集群,redis 单机的.
         * 1. 对于100w 次的请求没有任何请求异常
         * 2. 每秒能处理读请求    9.0w   redis 9.5w
         * 3. 每秒能处理写请求    7.1w   redis 9.3w
         * 4. 每秒能处理删除请求  8.3w   redis 8.5w
         * 5. 一致性hash的数据倾斜问题,在虚拟节点开到200个的时候,即hash环上有1000个节点的时候
         *     对于100w 次的请求 每个节点处理的数据都是 20w +/- 50 基本没有数据倾斜问题
         * 6. cpu利用率峰值100% ,redis 100%.
         *
         *
         */
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
                            .build();
                    int success = 0;
                    for (int j = 0; j < 100000; j++) {
                        //UserMessageProto.ResponseMessage responseMessage = cacheClient.doSet(getRandomString(),getRandomString(),-1);
                        UserMessageProto.ResponseMessage responseMessage = cacheClient.doGet(getRandomString());
                        //UserMessageProto.ResponseMessage responseMessage = cacheClient.doDel(getRandomString());
                        //System.out.println(responseMessage);
                        /*if (responseMessage.getResponseType() == UserMessageProto.ResponseType.SUCCESS) {
                            success++;
                        }*/
                        //del 12174 ms
                        //get 11002 ms
                        //set 13862 ms
                    }
                    //System.out.println(success);
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
