import common.QLinkedBlockingQueue;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class QueueTest {
    @Test
    public void queueTest(){
        final QLinkedBlockingQueue<Integer> queue = new QLinkedBlockingQueue<Integer>();
        ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(3,3,0,
                TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
        final CountDownLatch countDownLatch = new CountDownLatch(2);
        long start = System.currentTimeMillis();
        poolExecutor.execute(new Runnable() {
            @Override
            public void run() {
                for (int i = 0;i< 100000;i++) {
                    //System.out.println("a");

                        queue.put(i);


                }
                countDownLatch.countDown();;
                System.out.println("complete");
                System.out.println("size " + queue.size());

            }
        });

        poolExecutor.execute(new Runnable() {
            @Override
            public void run() {
                for (int i = 0;i< 100000;i++) {
                        queue.poll();

                    //System.out.println(i);
                }
                countDownLatch.countDown();
                System.out.println("complete2");

            }
        });

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(queue.size());
        System.out.println(System.currentTimeMillis() - start);
        poolExecutor.shutdown();

    }
}
