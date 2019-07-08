import core.client.CacheClient;
import core.message.UserMessageProto;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class NioTest {
    public static void main(String[] args) {
        ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(10,10,0,
                TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
        long start = System.currentTimeMillis();
        final CountDownLatch countDownLatch = new CountDownLatch(10);
        for (int i =0;i < 10;i++){
            poolExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    CacheClient cacheClient = new CacheClient.newBuilder()
                            .setNumberOfReplicas(1)
                            .setNewNode("6:127.0.0.1:8086:9097")
                            .build();
                    for (int j = 0;j<100000;j++) {
                        UserMessageProto.ResponseMessage responseMessage = cacheClient.doGet(getRandomString());
                    }
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
        System.out.println(System.currentTimeMillis()-start);
        poolExecutor.shutdown();
        /**
         * 在对数据不做任何处理的是java依然没有redis块,100w请求耗时12934ms
         */
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
