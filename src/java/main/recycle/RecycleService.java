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

    /**
     * 磁盘有个异步刷盘的机制,在回收期间暂停磁盘的刷盘
     */
    private AtomicBoolean canFlush;
    private AtomicBoolean canRead;

    public RecycleService(Mark mark,
                          CacheFileGroup cacheFileGroup,
                          AtomicBoolean canFlush,
                          AtomicBoolean canRead) {
        this.mark = mark;
        this.cacheFileGroup = cacheFileGroup;
        this.canFlush = canFlush;
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
        logger.debug("recycle service start!");
    }
    @Override
    public void run() {
        if (!isShutdown()) {
            logger.debug("Cache File Size : {} (M) ",cacheFileGroup.getWriteSize() / (1024 * 1024));
            if (cacheFileGroup.getWriteSize() < UtilAll.getTotalMemorySize() >> 2) {
                return;
            }
            int num = mark.mark();
            cacheFileGroup.deleteMessageAddCount(num);
            logger.debug("delete num: {},total num:{} ",cacheFileGroup.getTotalDelete(),cacheFileGroup.getTotalMessage());
            if (1.0 * cacheFileGroup.getTotalDelete() / 1.0 * cacheFileGroup.getTotalMessage() < RecycleOptions.DEFAULT_FACTOR) {
                //垃圾占比比较少,不运行垃圾回收
                return;
            }
            //关闭服务的刷盘
            canFlush.set(false);
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

            //资源切换
            cacheFileGroup.setRebulidEffective();

            //开启服务的读功能
            canRead.set(true);
            canFlush.set(true);
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
