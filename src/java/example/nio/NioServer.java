package nio;

import com.google.protobuf.MessageLite;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * NioServer
 */
public abstract class NioServer {
    /**
     * 处理客户端连接
     */
    protected AcceptorClientHandler acceptorClientHandler;
    /**
     * 处理读事件
     */
    protected NioEventLoop nioEventLoop;
    /**
     * 处理写任务
     */
    protected NioWriteGroup nioWriteGroup;
    /**
     * 处理读任务
     */
    protected NioReadGroup nioReadGroup;
    /**
     * 对于非完整数据包填充这个
     */
    protected Object object = new Object();
    protected LinkedBlockingQueue<Object> readCache = new LinkedBlockingQueue<Object>();
    protected MessageLite messageLite;
    private int port;
    private int threadNum = Runtime.getRuntime().availableProcessors() * 3;
    /**
     * 所有任务最终是放在线程池里面执行的
     */
    protected ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(threadNum, threadNum * 3, 60,
            TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());

    public NioServer readGroup(NioReadGroup nioReadGroup) {
        this.nioReadGroup = nioReadGroup;
        return this;
    }

    public NioServer writeGroup(NioWriteGroup nioWriteGroup) {
        this.nioWriteGroup = nioWriteGroup;
        return this;
    }


    public NioServer messageLite(MessageLite messageLite) {
        this.messageLite = messageLite;
        return this;
    }

    public NioServer bind(int port) {
        this.port = port;
        return this;
    }

    public NioServer start() {
        validate();
        acceptorClientHandler = new AcceptorClientHandler(nioReadGroup, nioWriteGroup, messageLite, readCache, port);
        acceptorClientHandler.run(poolExecutor);
        nioReadGroup.runAll(poolExecutor);

        //处理读到的数据,并生成写数据
        MessageProcess messageProcess = getInstance();
        messageProcess.run(poolExecutor);

        nioWriteGroup.runAll(poolExecutor);

        return this;
    }

    /**
     * 验证参数是否设置
     */
    private void validate() {

        if (nioReadGroup == null) {
            throw new RuntimeException("NioReadGroup Not set");
        }

        if (port == 0) {
            throw new RuntimeException("port Not set");
        }


        if (nioWriteGroup == null) {
            throw new RuntimeException("NioWriteGroup Not set");
        }

        if (readCache == null) {
            throw new RuntimeException("readCache Not set");
        }

        if (messageLite == null) {
            throw new RuntimeException("messageLite Not set");
        }

    }

    /**
     * shutdown
     */
    public void shutdown() {
        acceptorClientHandler.shutdown();
        nioEventLoop.shutdown();
        nioReadGroup.shutdown();
        nioWriteGroup.shutdown();
    }

    /**
     * 获取MessageProcess 实例对象.
     *
     * @return MessageProcess
     */
    public abstract MessageProcess getInstance();

    public abstract class MessageProcess implements Runnable {
        protected volatile boolean isShutdown = false;

        /**
         * run.
         *
         * @param poolExecutor pool
         */
        public void run(ThreadPoolExecutor poolExecutor) {
            if (!isShutdown) {
                poolExecutor.execute(this);
            }
        }

        /**
         * shutdown
         */
        public void shutdown() {
            isShutdown = true;
        }
    }

}
