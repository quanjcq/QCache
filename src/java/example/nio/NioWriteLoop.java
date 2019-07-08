package nio;

import common.QLinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class NioWriteLoop implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(NioWriteLoop.class);
    private QLinkedBlockingQueue<Object> writeCache = new QLinkedBlockingQueue<Object>();
    private volatile CountDownLatch countDownLatch = null;
    private volatile boolean isShutdown = false;

    public NioWriteLoop() {

    }

    public void setCountDownLatch(CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void run() {
        while (!isShutdown) {

            Object task = writeCache.poll();

            if (task instanceof TaskEntity) {
                //具体消息
                TaskEntity taskEntity = (TaskEntity) task;
                taskEntity.getChannel().write(taskEntity.getMsg());
            } else {
                //未读完的消息
                logger.info("not a complete package");
            }

            if (countDownLatch != null) {
                countDownLatch.countDown();
            }

        }
    }

    /**
     * shutdown
     */
    public void shutdown() {
        isShutdown = true;
    }

    /**
     * 添加任务.
     *
     * @param task task
     */
    public void addTask(Object task) {
        writeCache.put(task);
    }


}
