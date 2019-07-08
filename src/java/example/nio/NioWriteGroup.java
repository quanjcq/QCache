package nio;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 负责管理所有写线程
 */
public class NioWriteGroup {
    private int num;
    private NioWriteLoop[] writeLoops;
    private AtomicLong atomicLong = new AtomicLong(0);
    private CountDownLatch countDownLatch;

    public NioWriteGroup(int num) {
        this.num = num;
        writeLoops = new NioWriteLoop[num];
        for (int i = 0; i < num; i++) {
            writeLoops[i] = new NioWriteLoop();
        }
    }

    public void setCountDownLatch(CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
    }

    /**
     * 获取一个NioWriteLoop 处理写任务
     *
     * @return NioWriteLoop
     */
    private NioWriteLoop writeLoop() {
        return writeLoops[(int) (atomicLong.getAndIncrement() % num)];
    }

    /**
     * shutdown.
     */
    public void shutdown() {
        for (NioWriteLoop writeLoop : writeLoops) {
            writeLoop.shutdown();
        }
    }

    /**
     * 找一个NioWriteLoop,并把任务交给他处理
     *
     * @param task task.
     */
    public void addTask(Object task) {
        NioWriteLoop nioWriteLoop = writeLoop();
        nioWriteLoop.setCountDownLatch(countDownLatch);
        nioWriteLoop.addTask(task);
    }

    /**
     * 运行所有NioWriteLoop
     *
     * @param threadPoolExecutor pool
     */
    public void runAll(ThreadPoolExecutor threadPoolExecutor) {
        for (int i = 0; i < num; i++) {
            threadPoolExecutor.execute(writeLoops[i]);
        }
    }
}
