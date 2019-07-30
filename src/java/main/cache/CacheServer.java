package cache;

import common.Node;
import common.UtilAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.ConsistentHash;
import raft.RaftServer;
import recycle.RecycleService;
import remote.NioChannel;
import remote.NioServer;
import remote.message.RemoteMessage;
import remote.message.RemoteMessageType;
import store.AofLogService;
import store.CacheFileGroup;
import store.RaftLogMessageService;
import store.RaftStateMachineService;
import store.checkpoint.CheckPoint;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class CacheServer extends NioServer {
    private static Logger logger = LoggerFactory.getLogger(CacheServer.class);
    /**
     * 一致性hash
     */
    private ConsistentHash consistentHash;

    /**
     * 集群
     */
    private List<Node> nodes;
    /**
     * 该节点信息
     */
    private Node myNode;

    /**
     * Raft集群服务
     */
    private RaftServer raftServer;

    /**
     * checkPoint
     */
    private CheckPoint checkPoint;

    /**
     * raftLog
     */
    private RaftLogMessageService raftLogMessageService;

    /**
     * raft stateMachine
     */
    private RaftStateMachineService raftStateMachineService;

    /**
     * CacheFile,存放缓存数据
     */
    private CacheFileGroup cacheFileGroup;

    /**
     * Cache 数据操作写日志
     */
    private AofLogService aofLogService;
    /**
     * 是否可读
     */
    private AtomicBoolean canRead = new AtomicBoolean(true);

    /**
     * 是否可写.过期数据或者被删除,只做标记并没有在物理上删除,所以需要回收这部分数据.
     * 在回收空间的时候是采用屏蔽所有写操作,将数据写入新文件中,读请求还是读原来数据,
     * 保证回收空间不影响读
     */
    private AtomicBoolean canWrite = new AtomicBoolean(true);

    /**
     * 内存回收.
     */
    private RecycleService recycleService;
    public CacheServer() {

    }

    @Override
    public void start() {
        if (aofLogService == null
                || cacheFileGroup == null
                || consistentHash == null
                || recycleService == null
                || myNode == null
                || nodes == null
        ) {
            throw new RuntimeException("args not set");
        }
        this.port = myNode.getListenClientPort();
        super.start();
        startRaft();
        recycleService.start();

    }

    private void startRaft() {
        if (nodes.size() == 1) {
            //只有一个节点,就不启动Raft服务
            consistentHash.add(myNode);
            return;
        }
        if (!validate()) {
            throw new RuntimeException("args not set");
        }
        raftServer = new RaftServer();
        raftLogMessageService.setRaftStateMachineService(raftStateMachineService);
        raftServer.setNodes(nodes);
        raftServer.setMyNode(myNode);
        raftServer.setCheckPoint(checkPoint);
        raftServer.setConsistentHash(consistentHash);
        raftServer.setRaftLogMessageService(raftLogMessageService);
        raftServer.setRaftStateMachineService(raftStateMachineService);
        raftServer.start();
        logger.info("raft server start");
    }

    private boolean validate() {
        return consistentHash != null
                && checkPoint != null
                && raftLogMessageService != null
                && raftStateMachineService != null;
    }

    @Override
    protected void processReadKey(SelectionKey selectionKey) {
        NioChannel nioChannel = nioChannelGroup.findChannel(selectionKey);
        if (nioChannel == null) {
            logger.info("Channel already closed! {}", nioChannel.toString());
        } else {
            ByteBuffer readBuf = nioChannel.read();
            if (readBuf == null) {
                return;
            }
            switch (RemoteMessage.getRemoteMessageType(readBuf)) {
                case PUT:
                    doPut(nioChannel);
                    break;
                case GET:
                    doGet(nioChannel);
                    break;
                case DEL:
                    doDel(nioChannel);
                    break;
                case UN_KNOWN:
                    doUnKnown(nioChannel);
                    break;
                case STATUS:
                    doStatus(nioChannel);
                    break;
            }


        }
    }

    /**
     * 处理put请求
     *
     * @param nioChannel nioChannel
     */
    private void doPut(NioChannel nioChannel) {
        RemoteMessage remoteMessage = RemoteMessage.decode(nioChannel.getReadBuffer(), false);
        String key = remoteMessage.getKey();
        if (needSwitchNode(key)) {
            //当前请求不在本机上,客户端的配置跟服务器上不一致
            remoteMessage.setMessageType(RemoteMessage.getTypeByte(RemoteMessageType.SWITCH_NODE));
            ByteBuffer byteBuffer = consistentHash.serializedConsistentHash();
            if (byteBuffer == null) {
                remoteMessage.setResponse(new byte[0]);
            } else {
                remoteMessage.setResponse(byteBuffer.array());
            }
            nioChannel.write(remoteMessage);
            return;
        }
        //不可写
        if (!canWrite.get()) {
            remoteMessage.setMessageType(RemoteMessage.getTypeByte(RemoteMessageType.NOT_WRITE));
            nioChannel.write(remoteMessage);
            return;
        }
        boolean flag = cacheFileGroup.put(key, remoteMessage.getVal(), remoteMessage.getTimeOut());

        if (flag) {
            remoteMessage.setMessageType(RemoteMessage.getTypeByte(RemoteMessageType.OK));
        } else {
            remoteMessage.setMessageType(RemoteMessage.getTypeByte(RemoteMessageType.ERROR));
        }
        nioChannel.write(remoteMessage);
    }

    /**
     * 处理get请求
     *
     * @param nioChannel nioChannel
     */
    private void doGet(NioChannel nioChannel) {
        RemoteMessage remoteMessage = RemoteMessage.decode(nioChannel.getReadBuffer(), false);
        String key = remoteMessage.getKey();
        if (needSwitchNode(key)) {
            //当前请求不在本机上,客户端的配置跟服务器上不一致
            remoteMessage.setMessageType(RemoteMessage.getTypeByte(RemoteMessageType.SWITCH_NODE));
            ByteBuffer byteBuffer = consistentHash.serializedConsistentHash();
            if (byteBuffer == null) {
                remoteMessage.setResponse(new byte[0]);
            } else {
                remoteMessage.setResponse(byteBuffer.array());
            }
            nioChannel.write(remoteMessage);
            return;
        }
        //不可读
        if (!canRead.get()) {
            //服务器不可读的时间只发生的,Cache文件重建后,将新建的文件指向该文件,时间很短,通过sleep,继续向下执行
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
        //不可读
        if (!canRead.get()) {
            remoteMessage.setMessageType(RemoteMessage.getTypeByte(RemoteMessageType.NOT_READ));
            nioChannel.write(remoteMessage);
            return;
        }
        String val = cacheFileGroup.get(key);

        if (val == null) {
            remoteMessage.setMessageType(RemoteMessage.getTypeByte(RemoteMessageType.KEY_NOT_EXIST));
        } else {
            remoteMessage.setMessageType(RemoteMessage.getTypeByte(RemoteMessageType.GET_R));
            remoteMessage.setResponse(UtilAll.string2Byte(val,"UTF-8"));
        }
        nioChannel.write(remoteMessage);
    }

    /**
     * 处理del请求
     *
     * @param nioChannel nioChannel
     */
    private void doDel(NioChannel nioChannel) {
        RemoteMessage remoteMessage = RemoteMessage.decode(nioChannel.getReadBuffer(), false);
        String key = remoteMessage.getKey();
        if (needSwitchNode(key)) {
            //当前请求不在本机上,客户端的配置跟服务器上不一致
            remoteMessage.setMessageType(RemoteMessage.getTypeByte(RemoteMessageType.SWITCH_NODE));
            ByteBuffer byteBuffer = consistentHash.serializedConsistentHash();
            if (byteBuffer == null) {
                remoteMessage.setResponse(new byte[0]);
            } else {
                remoteMessage.setResponse(byteBuffer.array());
            }
            nioChannel.write(remoteMessage);
            return;
        }
        //不可读
        if (!canWrite.get()) {
            remoteMessage.setMessageType(RemoteMessage.getTypeByte(RemoteMessageType.NOT_WRITE));
            nioChannel.write(remoteMessage);
            return;
        }
        boolean flag = cacheFileGroup.del(key);
        if (flag) {
            remoteMessage.setMessageType(RemoteMessage.getTypeByte(RemoteMessageType.OK));
        } else {
            remoteMessage.setMessageType(RemoteMessage.getTypeByte(RemoteMessageType.KEY_NOT_EXIST));
        }
        nioChannel.write(remoteMessage);
    }

    /**
     * 处理未知请求.
     *
     * @param nioChannel nioChannel
     */
    private void doUnKnown(NioChannel nioChannel) {
        RemoteMessage remoteMessage = RemoteMessage.getInstance(false);
        remoteMessage.setMessageType(RemoteMessage.getTypeByte(RemoteMessageType.UN_KNOWN));
        nioChannel.write(remoteMessage);
    }

    /**
     * 查看集权状态
     * @param nioChannel channel
     */
    private void doStatus(NioChannel nioChannel) {
        RemoteMessage remoteMessage = RemoteMessage.getInstance(false);
        remoteMessage.setMessageType(RemoteMessage.getTypeByte(RemoteMessageType.STATUS_R));
        if (raftServer != null) {
            String response = "------------------------------------------" +"\n"+
                    "State:" + raftServer.getState() + "\n" +
                    "Term:" + raftServer.getCurrentTerm() + "\n" +
                    "Id:" + myNode.getNodeId() + "\n" +
                    "Ip:" + myNode.getIp() + "\n" +
                    "Port:" + myNode.getListenClientPort() + "\n"+
                    "Alive Servers:"+"\n";
            StringBuilder builder = new StringBuilder();
            builder.append(response);
            for (Node node:consistentHash.getAliveNodes()){
                String temp = "server." + node.getNodeId()+"=" + node.getIp() + ":" + node.getListenClientPort() + "\n";
                builder.append(temp);
            }
            builder.append("------------------------------------------"+"\n");
            remoteMessage.setResponse(UtilAll.string2Byte(builder.toString(),"UTF-8"));
        } else {
            String response = "not in cluster model";
            remoteMessage.setResponse(UtilAll.string2Byte(response,"UTF-8"));
        }
        nioChannel.write(remoteMessage);
    }

    /**
     * 当前key 是否在本机.
     *
     * @param key key
     * @return bool
     */
    private boolean needSwitchNode(String key) {
        return !myNode.equals(consistentHash.get(key));
    }

    public void setConsistentHash(ConsistentHash consistentHash) {
        this.consistentHash = consistentHash;
    }

    public void setNodes(List<Node> nodes) {
        this.nodes = nodes;
    }

    public void setMyNode(Node myNode) {
        this.myNode = myNode;
    }

    public void setCheckPoint(CheckPoint checkPoint) {
        this.checkPoint = checkPoint;
    }

    public void setRaftLogMessageService(RaftLogMessageService raftLogMessageService) {
        this.raftLogMessageService = raftLogMessageService;
    }

    public void setRaftStateMachineService(RaftStateMachineService raftStateMachineService) {
        this.raftStateMachineService = raftStateMachineService;
    }

    public void setCacheFileGroup(CacheFileGroup cacheFileGroup) {
        this.cacheFileGroup = cacheFileGroup;
    }

    public void setAofLogService(AofLogService aofLogService) {
        this.aofLogService = aofLogService;
    }

    public void setRecycleService(RecycleService recycleService) {
        this.recycleService = recycleService;
    }

    public AtomicBoolean getCanRead() {
        return canRead;
    }

    public AtomicBoolean getCanWrite() {
        return canWrite;
    }
}
