import redis.clients.jedis.Jedis;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
                    //int count = 0;
                    for (int j = 0; j < 100000; j++) {
                        jedis.set(getRandomString(), getRandomString());
                        //String val = jedis.get(getRandomString());
                        //jedis.del(getRandomString());
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
