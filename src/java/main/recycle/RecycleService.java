package recycle;

import common.UtilAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.CacheFileGroup;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class RecycleService implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(RecycleService.class);
    /**
     * 垃圾标记.
     */
    private Mark mark;
    private AtomicBoolean isShutdown = new AtomicBoolean(true);
    private CacheFileGroup cacheFileGroup;
    private ScheduledFuture scheduledFuture;
    private ScheduledExecutorService scheduledExecutorService;

    private AtomicBoolean canWrite;
    private AtomicBoolean canRead;

    public RecycleService(Mark mark,
                          CacheFileGroup cacheFileGroup,
                          AtomicBoolean canWrite,
                          AtomicBoolean canRead) {
        this.mark = mark;
        this.cacheFileGroup = cacheFileGroup;
        this.canWrite = canWrite;
        this.canRead = canRead;
        this.scheduledExecutorService = UtilAll.getScheduledExecutorService();
    }

    /**
     * 运行RecycleService.
     */
    public void start() {
        if (isShutdown.compareAndSet(true,false)) {
            scheduledFuture = scheduledExecutorService.scheduleWithFixedDelay(this,
                    RecycleOptions.RECYCLE_DELAY,
                    RecycleOptions.RECYCLE_DELAY,
                    TimeUnit.MILLISECONDS);
        }
    }
    @Override
    public void run() {
        if (!isShutdown()) {
            logger.info("Cache File Size : {} (M) ",cacheFileGroup.getWriteSize() / (1024 * 1024));
            if (cacheFileGroup.getWriteSize() < UtilAll.getTotalMemorySize() >> 2) {
                return;
            }
            int num = mark.mark();
            cacheFileGroup.deleteMessageAddCount(num);
            logger.info("delete num: {},total num:{} ",cacheFileGroup.getTotalDelete(),cacheFileGroup.getTotalMessage());
            if (1.0 * cacheFileGroup.getTotalDelete() / 1.0 * cacheFileGroup.getTotalMessage() < RecycleOptions.DEFAULT_FACTOR) {
                //垃圾占比比较少,不运行垃圾回收
                return;
            }
            //System.out.println(1.0 * cacheFileGroup.getTotalDelete() / 1.0 * cacheFileGroup.getTotalMessage());

            //关闭服务器的写请求
            canWrite.set(false);
            //通过sleep 使得正在执行的写请求,能够正常执行完成
            //这种方式实现多线程安全,可以使得主线程更快
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                logger.warn(e.toString());
            }
            //重建cache文件
            cacheFileGroup.rebulid();

            //关闭读请求
            canRead.set(false);
            //通过sleep 使得正在执行的读请求,能够正常执行完成
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                logger.warn(e.toString());
            }
            cacheFileGroup.setRebulidEffective();

            //开启服务的读写功能
            canWrite.set(true);
            canRead.set(true);
        }
    }

    /**
     * RecycleService 是否关闭.
     * @return bool.
     */
    public boolean isShutdown() {
        return isShutdown.get();
    }

    /**
     * RecycleService.
     */
    public void shutdown(){
        if (isShutdown.compareAndSet(false,true)) {
            if (!scheduledFuture.isDone() || !scheduledFuture.isCancelled()) {
                scheduledFuture.cancel(true);
            }
        }
    }

}
