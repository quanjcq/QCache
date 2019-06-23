import com.google.protobuf.InvalidProtocolBufferException;
import core.client.CacheClient;
import core.message.UserMessageProto;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 对于 100w次的请求最少会有400w次的序列化反序列化操作,测试这个需要多久时间
 * 测试结果为:2100ms左右,对象根据反射生成(还创建过多)的比较耗时,如果能省掉这2000ms 就能达到redis的读写性能
 *
 * 如果改写Protobuf 代码,使得创建对象的时候可以通过,已有对象里面set值,而不用反复创建对象应该能有很大性能提升
 */
public class ProtoBufPerformanceTest {
    public static void main(String[] args) {
        ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(8,10,0,
                TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
        long start = System.currentTimeMillis();
        final CountDownLatch countDownLatch = new CountDownLatch(10);
        for (int i =0;i < 10;i++){
            poolExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    for (int j = 0;j<400000;j++) {
                        UserMessageProto.UserMessage userMessage = UserMessageProto.UserMessage
                                .newBuilder()
                                .setMessageType(UserMessageProto.MessageType.GET)
                                .setGetMessage(UserMessageProto.GetMessage
                                                .newBuilder()
                                                .setKey(getRandomString())
                                                .build())
                                .build();
                        //序列化
                        byte[] data = userMessage.toByteArray();
                        //反序列化
                        try {
                            UserMessageProto.UserMessage userMessage1 = UserMessageProto.UserMessage.parseFrom(data);
                            if (!userMessage.equals(userMessage1)) {
                                System.out.println("no");
                            }
                        } catch (InvalidProtocolBufferException ex) {
                            ex.printStackTrace();
                        }
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
