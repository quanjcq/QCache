package remote;

import constant.CacheOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import remote.message.RemoteMessage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.*;

/**
 * 连接对象
 */
public class NioChannel {
    private static Logger logger = LoggerFactory.getLogger(NioChannel.class);
    private Selector selector;
    /**
     * 记录上次读数据,或者写数据的时间,对于长时间没有数据来往的channel,将会被关闭.
     */
    private long transferTime = System.currentTimeMillis();

    //超过多久没有通信的将会被关闭
    private long maxKeepTime = CacheOptions.maxKeepTime;

    /**
     * 每个channel 初始分配1024个byte read缓冲区,不够再分配更大的
     */
    private int readBufSize = 1024;
    private ByteBuffer readBuffer = ByteBuffer.allocateDirect(readBufSize);


    /**
     * 每个channel 初始分配1024个byte write缓冲区,不够再分配更大的
     */
    private int writeBufSize = 1024;
    private ByteBuffer writeBuf = ByteBuffer.allocateDirect(writeBufSize);

    /**
     * 消息体长度
     */
    private short bodyLen = -1;

    /**
     * 包长度信息占两个字节
     */
    private ByteBuffer head = ByteBuffer.allocateDirect(2);


    /**
     * selector key 可唯一标识一个Channel
     */
    private SelectionKey selectionKey;
    /**
     * java nio SocketChannel
     */
    private SocketChannel channel;
    /**
     * 管理该Channel的 ChannelGroup
     */
    private NioChannelGroup nioChannelGroup;



    public NioChannel(NioChannelGroup nioChannelGroup,SocketChannel socketChannel) {

        this.nioChannelGroup = nioChannelGroup;
        this.channel = socketChannel;
    }

    /**
     * 将连接注册到Selector.
     *
     * @param selector    selector
     * @param interestOps interestOps
     * @return SelectionKey
     */
    public SelectionKey register(Selector selector, int interestOps) {
        this.selector = selector;
        try {
            this.selectionKey =  channel.register(selector, interestOps,this);
        } catch (ClosedChannelException e) {
            logger.error("can not register {}", e);
        }
        return null;
    }

    /**
     * 返回channel.
     *
     * @return channel
     */
    public SelectableChannel channel() {
        return this.channel;
    }

    /**
     * 获取该channel的selector.
     *
     * @return selector
     */
    public Selector selector() {
        return selector;
    }

    /**
     * 关闭该channel
     */
    public void close() {
        try {

            nioChannelGroup.remove(this);

            channel.close();
            selectionKey.cancel();
        } catch (IOException e) {
            logger.error("close channel error {}", e);
        }
    }

    /**
     * 写数据
     * @param message message
     */
    public void write(RemoteMessage message) {
        int bodySize = message.getSerializedSize();

        if (bodySize > Short.MAX_VALUE) {
            throw new RuntimeException("msg length must not bigger than  " + Short.MAX_VALUE);
        }

        if (bodySize > writeBufSize) {
            writeBufSize = bodySize;
            writeBuf = ByteBuffer.allocateDirect(writeBufSize);
        }
        writeBuf.clear();
        //将对象序列化放入writeBuf
        message.encode(writeBuf);

        writeBuf.flip();
        try {
            channel.write(writeBuf);
        } catch (IOException e) {
            this.close();
            logger.error("write error: {}", e);
        }
        transferTime = System.currentTimeMillis();

    }


    /**
     * 读数据
     */
    public ByteBuffer read() {

        //读取头信息
        if (bodyLen == -1) {
            try {
                int size = channel.read(head);
                if (size == -1) {
                    close();
                }
                if (head.position() == 2) {
                    //读满了两个字节
                    head.flip();
                    bodyLen = head.getShort();

                    head.clear();
                    readBuffer.clear();
                }
            } catch (IOException e) {
                logger.error("read error: {} ", e);
            }
        }
        if (bodyLen > 0) {
            try {
                if (bodyLen > readBufSize) {
                    //重新分配cache
                    readBufSize = bodyLen;
                    readBuffer = ByteBuffer.allocate(readBufSize);
                }
                readBuffer.limit(bodyLen);
                //长度信息
                readBuffer.putShort(bodyLen);
                int size = channel.read(readBuffer);
                if (size == -1) {
                    close();
                }
            } catch (IOException e) {
                logger.error(e.toString());
            }
            if (completePackage()) {
                //读完了一个完整的数据
                readBuffer.flip();
                bodyLen = -1;
                return readBuffer;
            }
        }
        transferTime = System.currentTimeMillis();
        return null;
    }

    /**
     * 是否有一个完整包数据
     *
     * @return bool
     */
    private boolean completePackage() {
        return bodyLen == readSize();
    }

    /**
     * 读取到的body长度
     *
     * @return 已经读到的body size
     */
    private int readSize() {
        return readBuffer.position();
    }


    /**
     * 获取消息体的长度,若为 -1 说明没有读取到包的长度信息,继续读取
     *
     * @return int
     */
    private int bodyLen() {
        return bodyLen;
    }

    /**
     * 获取selectorKey.
     *
     * @return selectionKey.
     */
    public SelectionKey selectionKey() {
        return this.selectionKey;
    }

    public void interestOps(int interestOps) {
        selectionKey.interestOps(interestOps);
    }

    /**
     * 连接是否可以关闭.[超过多久没有通信的连接可以关闭]
     * @return bool.
     */
    public boolean canClosed(){
        return System.currentTimeMillis() - transferTime > maxKeepTime;
    }

    @Override
    public int hashCode() {
        return this.selectionKey.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof NioChannel)) {
            return false;
        }
        return this.selector.equals(((NioChannel) obj).selector());
    }

    @Override
    public String toString() {
        return "ip:" + channel.socket().getRemoteSocketAddress().toString();
    }

    public ByteBuffer getReadBuffer() {
        return readBuffer;
    }
}
