package nio_3;

import com.google.protobuf.MessageLite;

import java.nio.channels.SelectableChannel;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

public class NioEventLoopGroup {

    /**
     * 用于多个NioEventLoop 同步.
     */
    private final Object sync = new Object();
    /**
     * 子线程数量
     */
    private int num;
    private NioEventLoop[] children;
    private MessageLite messageLite;

    private AtomicLong count  = new AtomicLong(0);
    private volatile boolean isShutdown = false;

    public NioEventLoopGroup(int num){
        this.num = num;
        children = new NioEventLoop[num];
        //启动子线程
    }

    public NioEventLoopGroup setMessageLite(MessageLite messageLite){
        this.messageLite = messageLite;
        for (int i = 0;i< num;i++) {
            children[i] = new NioEventLoop(this,messageLite);
        }
        return this;
    }

    public NioEventLoopGroup(){
        this(Runtime.getRuntime().availableProcessors());
    }

    /**
     * run.
     * @param poolExecutor pool.
     */
    public void run(ThreadPoolExecutor poolExecutor) {
        if (messageLite == null) {
            throw  new RuntimeException("messageLite can not be empty");
        }
        for (int i = 0;i<num;i++) {
            children[i].run(poolExecutor);
        }
    }
    /**
     * 是否关闭.
     * @return bool.
     */
    public boolean isShutdown(){
        return isShutdown;
    }

    /**
     * 关闭.
     */
    public void shutdown(){
        for (NioEventLoop eventLoop:children) {
            //依次关闭所有子线程
            eventLoop.shutdown();
        }
    }

    /**
     * 获取一个NioEventLoop 实例.
     * @return NioEventLoop
     */
    public NioEventLoop getEventLoop(){
        return children[(int)(count.getAndIncrement() % num)];
    }

    /**
     * 将连接注册到其中一个NioEventLoop.
     * @param channel channel.
     * @param interestOps interestOps.
     */
    public void register(SelectableChannel channel, int interestOps) {
        NioEventLoop nioEventLoop = getEventLoop();
        nioEventLoop.register(channel,interestOps);
    }


    public Object getSync() {
        return sync;
    }


}
