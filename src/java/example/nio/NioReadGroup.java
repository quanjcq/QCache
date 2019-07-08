package nio;


import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 负责管理所有读线程的.
 */
public class NioReadGroup {
    private int num;
    private NioReadLoop[] readLoops;
    private AtomicLong atomicLong = new AtomicLong(0);

    public NioReadGroup(int num) {
        this.num = num;
        readLoops = new NioReadLoop[num];
        for (int i = 0; i < num; i++) {
            readLoops[i] = new NioReadLoop();
        }
    }

    private NioReadLoop readLoop() {
        return readLoops[(int) (atomicLong.getAndIncrement() % num)];
    }

    /**
     * shutdown
     */
    public void shutdown() {
        for (NioReadLoop readLoop : readLoops) {
            readLoop.shutdown();
        }
    }

    /**
     * 添加任务.
     *
     * @param nioChannel NioChannel
     */
    public void addTask(NioChannel nioChannel) {
        readLoop().addTask(nioChannel);
    }

    /**
     * 运行所有任务
     *
     * @param threadPoolExecutor pool
     */
    public void runAll(ThreadPoolExecutor threadPoolExecutor) {
        for (int i = 0; i < num; i++) {
            threadPoolExecutor.execute(readLoops[i]);
        }
    }
}
