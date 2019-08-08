
import client.CacheClient;
import constant.CacheOptions;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


public class CacheServerTest {
    public static void main(String[] args) {

        ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(10, 10, 0,
                TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
        final StringBuilder builder = new StringBuilder();
        for (int i = 0;i< 29;i++) {
            builder.append("0123456789abcdefghijklmnopqrstuvwxyz");
        }
        long start = System.currentTimeMillis();
        final CountDownLatch countDownLatch = new CountDownLatch(10);
        for (int  i = 0; i < 10; i++) {
            poolExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    CacheClient cacheClient = new CacheClient.newBuilder()
                            .setNumberOfReplicas(CacheOptions.numberOfReplicas)
                            //.setNewNode("3:127.0.0.1:9093")
                            //这里值设置一个服务器地址,请求无效,会同步服务器集群状态
                            .setNewNode("1:127.0.0.1:9001")
                            //.setNewNode("1:127.0.0.1:9091")
                            .build();
                    /*for (int j = 0;j<10;j++) {
                        String val = cacheClient.get( "name" + j);
                        System.out.println(val);
                    }*/
                    for (int j = 0;j<500000;j++) {
                        cacheClient.put("name" + j + getRandomString(),builder.toString(),-1);
                    }
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
        int num = random.nextInt(5) + 2;
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < num; i++) {
            int index = random.nextInt(26);
            builder.append(chars[index]);
        }
        return builder.toString();
    }
}
