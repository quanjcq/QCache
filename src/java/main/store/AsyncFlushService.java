package store;

import common.UtilAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import store.checkpoint.CheckPoint;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class AsyncFlushService implements Runnable{
    private static Logger logger = LoggerFactory.getLogger(AsyncFlushService.class);
    private volatile boolean isShutdown = true;

    private CacheFileGroup cacheFileGroup;

    private CheckPoint checkPoint;

    private ScheduledFuture future;

    public AsyncFlushService(CacheFileGroup cacheFileGroup,CheckPoint checkPoint) {
        this.cacheFileGroup = cacheFileGroup;
        this.checkPoint = checkPoint;
    }

    public void start(){
        if (isShutdown()) {
            future = UtilAll.getScheduledExecutorService().scheduleWithFixedDelay(this,
                    StoreOptions.FLUSH_TIME,
                    StoreOptions.FLUSH_TIME,
                    TimeUnit.MILLISECONDS);
            isShutdown = false;
            logger.debug("AsyncFlushService start!");
        }
    }

    @Override
    public void run() {
        if (!isShutdown() && cacheFileGroup.getCanFlush().get()) {
            cacheFileGroup.flush();
            //记录刷盘时间
            checkPoint.setFlush(System.currentTimeMillis());
            logger.info("Async flush ok!");
        }
    }

    public boolean isShutdown(){
        return isShutdown;
    }

    /**
     * 关闭该服务
     */
    public void shutdown() {
        if (!isShutdown() && future != null) {
            isShutdown = true;
            if (!future.isDone() || !future.isCancelled()) {
                future.cancel(true);
            }
            logger.debug("AsyncFlushService shutdown!");
        }
    }
}
