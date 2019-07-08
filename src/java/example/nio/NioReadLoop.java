package nio;

import java.util.concurrent.LinkedBlockingQueue;

public class NioReadLoop implements Runnable {
    private volatile boolean isShutdown = false;
    /**
     * 存放含有读事件的Channel.
     * 注:这个队列只适合单读,单写的环境
     */
    private LinkedBlockingQueue<NioChannel> task = new LinkedBlockingQueue<NioChannel>();

    @Override
    public void run() {
        while (!isShutdown) {
            try {
                NioChannel nioChannel = task.take();
                nioChannel.read();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }


        }
    }


    /**
     * shutdown.
     */
    public void shutdown() {
        isShutdown = true;
    }

    /**
     * 添加任务.
     *
     * @param nioChannel NioChannel
     */
    public void addTask(NioChannel nioChannel) {

        try {
            task.put(nioChannel);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
