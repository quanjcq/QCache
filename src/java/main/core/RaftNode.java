package core;

import com.google.protobuf.MessageLite;
import common.*;
import constant.CacheOptions;
import constant.RaftOptions;
import constant.RaftState;
import core.cache.CacheData;
import core.cache.CacheDataInt;
import core.cache.CacheDataString;
import core.cache.backup.BackUpAof;
import core.cache.backup.BackUpRdb;
import core.message.LastRaftMessage;
import core.message.RaftMessageProto;
import core.message.UserMessageProto;
import core.nio.NioChannel;
import core.nio.NioServer;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import log.RaftLog;
import log.RaftLogParse;
import log.RaftLogUnCommit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.channels.SelectionKey;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by quan on 2019/4/24.
 * 该类是负责的功能有：
 * 选举
 * 监听其他node(pre_candidate,candidate,leader)消息
 * 处理客户端请求
 */
public class RaftNode {
    private static Logger log = LoggerFactory.getLogger(RaftNode.class);
    //该节点为candidate时候获取的选票信息
    private final Set<Short> votes = new TreeSet<Short>();
    //该节点为pre_candidate时候获取的选票信息
    private final Set<Short> preVotes = new TreeSet<Short>();
    //状态，初始的时候为follower
    private volatile RaftState state = RaftState.FOLLOWER;
    // 服务器任期号(term)（初始化为 0，持续递增）
    private long currentTerm = 0;
    // 在当前获得选票的候选人的Id，若这个值为负数，说明他没有把票投给其他节点
    private int votedFor = -1;
    //这里存放着所有，最新提交的日志信息
    private Map<Short, LastRaftMessage> lastRaftMessageMap = new HashMap<Short, LastRaftMessage>();

    // 最后被应用到状态机的日志条目索引值（初始化为 0）
    private long lastAppliedIndex = 0;


    //一致性hash
    private ConsistentHash consistentHash = null;

    //snaphot 文件路径
    private String raftSnaphotPath = QCacheConfiguration.getRaftSnaphotPath();

    //raftlog 文件路径
    private String raftLogPath = QCacheConfiguration.getRaftLogsPath();

    //记录上次leader 访问的时间，以此判断集群内是否有leader
    private volatile long leaderConnectTime = 0;

    //该节点信息
    private Node myNode;

    //负责维护连接信息
    private HashMap<Node, Channel> channels = new HashMap<Node, Channel>();

    //存储leader 所有未提交的日志（force ）
    private List<RaftLogUnCommit> unCommitLogs = new LinkedList<RaftLogUnCommit>();

    //集群信息
    private List<Node> nodes;

    private QLinkedBlockingQueue<String> asyncAppendFileQueue = new QLinkedBlockingQueue<String>();

    private Lock lock = new ReentrantLock();
    private HashMap<String, CacheData> cache = new HashMap<String, CacheData>();
    private ExecutorService executorService;
    private ScheduledExecutorService scheduledExecutorService;
    private ScheduledFuture electionScheduledFuture;
    private ScheduledFuture heartbeatScheduledFuture;

    private EventLoopGroup clientEventLoopGroupBoss = new NioEventLoopGroup(1);

    //缓存是否可写
    private volatile boolean writeAble = true;
    //缓存是否可读
    private volatile boolean readAble = true;

    //LeaderId
    //private volatile Short leaderId = -1;

    public void init() {
        //load nodes
        log.info("load nodes");
        nodes = QCacheConfiguration.getNodeList();
        myNode = QCacheConfiguration.getMyNode();

        //init ConsistentHash
        consistentHash = new ConsistentHash(CacheOptions.numberOfReplicas);


        //load snapshot
        log.info("load sanpshot");
        installRaftSnaphot();

        // load raft logs
        log.info("load raft logs");
        appendEntries();

        //load cache
        loadCache();

        //init thread pool
        executorService = new ThreadPoolExecutor(
                RaftOptions.coreThreadNum,
                RaftOptions.maxThreadNum,
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>());
        //启动监听其它节点的线程
        listenNode();

        scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
            public void run() {

                if (haveLeader()) {
                    //log.info("server {} have leader on term {}",myNode.getNodeId(),currentTerm);
                    printStatus();
                } else {
                    //log.info("server {} do not have leader on term",myNode.getNodeId(),currentTerm);
                    resetElectionTimer();
                }
            }
        }, 0, RaftOptions.electionTimeoutMilliseconds, TimeUnit.MILLISECONDS);

        //init listenClient
        log.info("listen client");
        initListenClient();

        log.info("initAsyncAppendLogThread");
        initAsyncAppendLogThread();

    }

    private void resetElectionTimer() {
        //中断未完成的选举
        if (electionScheduledFuture != null && !electionScheduledFuture.isDone()) {
            electionScheduledFuture.cancel(true);
        }
        electionScheduledFuture = scheduledExecutorService.schedule(new Runnable() {
            public void run() {
                startElection();
            }
        }, getRandomElection(), TimeUnit.MILLISECONDS);
    }

    /**
     * 开始选举，选举前先预选举，防止网络的异常的节点，导致全局term 变大
     */
    private void startElection() {
        if (preVote()) {
            log.info("server {} start vote", myNode.getNodeId());
            startVote();
        } else {
            log.info("server {} pre vote error", myNode.getNodeId());
            resetElectionTimer();
        }
    }

    /**
     * nodes->leader,nodes->pre_candidate,nodes->candidate
     * leader,pre_candidate,candidate ,向其他节点发送消息的时候，收到回复消息
     * 根据这个回复消息节点状态自动降级为follower.
     *
     * @param message 收到的消息内容
     */
    //in lock
    private void stepDown(RaftMessageProto.RaftMessage message) {
        long newTerm = message.getCurrentTerm();
        lastRaftMessageMap.put((short) message.getId(), new LastRaftMessage((short) message.getId(),
                message.getLastAppliedIndex()));
        if (currentTerm > newTerm) {
            return;
        } else if (currentTerm < newTerm) {
            currentTerm = newTerm;
            votedFor = -1;
            state = RaftState.FOLLOWER;

            // stop heartbeat
            if (heartbeatScheduledFuture != null && !heartbeatScheduledFuture.isDone()) {
                heartbeatScheduledFuture.cancel(true);
            }

            // stop election
            if (electionScheduledFuture != null && !electionScheduledFuture.isDone()) {
                electionScheduledFuture.cancel(true);
            }
            if (message.getLeader()) {
                leaderConnectTime = new Date().getTime();
            }
        }
        if (message.getLeader()) {
            leaderConnectTime = new Date().getTime();
            state = RaftState.FOLLOWER;
            // stop heartbeat
            if (heartbeatScheduledFuture != null && !heartbeatScheduledFuture.isDone()) {
                heartbeatScheduledFuture.cancel(true);
            }
            // stop election
            if (electionScheduledFuture != null && !electionScheduledFuture.isDone()) {
                electionScheduledFuture.cancel(true);
            }
        }
    }

    /**
     * 获取与对应node的连接
     *
     * @param node 要连接的节点
     * @return channel
     */
    private Channel getChannel(Node node) {
        Channel channel = channels.get(node);
        if (channel != null && channel.isActive()) {
            return channel;
        }
        ChannelFuture channelFuture = null;
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(clientEventLoopGroupBoss)
                    .channel(NioSocketChannel.class)
                    .handler(new SocketClientInitializer());
            channelFuture = bootstrap.connect(node.getIp(), node.getListenHeartbeatPort()).sync();
        } catch (InterruptedException ex) {
            log.debug(ex.toString());
        } finally {
            if (channelFuture != null && channelFuture.isSuccess()) {
                log.info("server {} create channel with server {} ", myNode.getNodeId(), node.getNodeId());
                channel = channelFuture.channel();
                channels.put(node, channel);
            } else {
                log.info("server {} lost", node.getNodeId());
                channel = null;
            }
            return channel;
        }
    }

    /**
     * 预先投票防止该节点自身网络异常导致全局term变大.
     *
     * @return pre vote 是否成功
     */
    private boolean preVote() {
        lock.lock();
        try {
            state = RaftState.PRE_CANDIDATE;
            log.info("server {} start preVote on term {}", myNode.getNodeId(), currentTerm);
            preVotes.clear();
        } finally {
            lock.unlock();
        }
        for (final Node node : nodes) {
            if (!node.equals(myNode)) {
                executorService.execute(new Runnable() {
                    public void run() {
                        RaftMessageProto.RaftMessage raftMessage;
                        lock.lock();
                        try {
                            if (state != RaftState.PRE_CANDIDATE || preVotes.size() + 1 > nodes.size() / 2) {
                                return;
                            }
                            raftMessage = RaftMessageProto.RaftMessage.newBuilder()
                                    .setMessageType(RaftMessageProto.MessageType.PRE_VOTE)
                                    .setId((int) myNode.getNodeId())
                                    .setLastAppliedIndex(lastAppliedIndex)
                                    .setCurrentTerm(currentTerm)
                                    .setLeader(state == RaftState.LEADER)
                                    .build();
                        } finally {
                            lock.unlock();
                        }
                        Channel channel = getChannel(node);
                        if (channel != null && channel.isActive()) {
                            channel.writeAndFlush(raftMessage);
                        }

                    }
                });
            }
        }

        if (preVotes.size() + 1 <= nodes.size() / 2) {
            synchronized (preVotes) {
                if (preVotes.size() + 1 <= nodes.size() / 2) {
                    try {
                        preVotes.wait((nodes.size() / 2 + 1) * RaftOptions.maxWaitTime);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return preVotes.size() + 1 > nodes.size() / 2;
    }

    /**
     * 开始投票
     */
    //in lock
    private void startVote() {
        lock.lock();
        try {
            state = RaftState.CANDIDATE;
            votedFor = (int) myNode.getNodeId();
            currentTerm++;
            log.info("server {} start Vote on term {}", myNode.getNodeId(), currentTerm);
            votes.clear();
        } finally {
            lock.unlock();
        }
        for (final Node node : nodes) {
            if (!node.equals(myNode)) {
                executorService.execute(new Runnable() {
                    public void run() {
                        RaftMessageProto.RaftMessage raftMessage;
                        lock.lock();
                        try {
                            if (state != RaftState.CANDIDATE || votes.size() + 1 > nodes.size() / 2) {
                                return;
                            }
                            raftMessage = RaftMessageProto.RaftMessage.newBuilder()
                                    .setMessageType(RaftMessageProto.MessageType.VOTE)
                                    .setId((int) myNode.getNodeId())
                                    .setLastAppliedIndex(lastAppliedIndex)
                                    .setCurrentTerm(currentTerm)
                                    .setLeader(false)
                                    .build();
                        } finally {
                            lock.unlock();
                        }
                        if (raftMessage != null) {
                            Channel channel = getChannel(node);
                            channel.writeAndFlush(raftMessage);
                        }
                    }
                });
            }
        }

        if (votes.size() + 1 <= nodes.size() / 2) {
            synchronized (votes) {
                if (votes.size() + 1 <= nodes.size() / 2) {
                    //不足半数以上投票继续等待
                    try {
                        votes.wait((nodes.size() / 2 + 1) * RaftOptions.maxWaitTime);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        boolean flag = (votes.size() + 1 > nodes.size() / 2);
        if (flag) {
            //become leader
            becomeLeader();
        } else {
            resetElectionTimer();
        }
    }

    /**
     * 当选为leader
     */
    private void becomeLeader() {
        lock.lock();
        try {
            state = RaftState.LEADER;
            votedFor = myNode.getNodeId();
            unCommitLogs.clear();
        } finally {
            lock.unlock();
        }
        //维护需要发送给其他节点的消息，只有leader需要
        for (Node node : nodes) {
            lastRaftMessageMap.put(node.getNodeId(),
                    new LastRaftMessage(node.getNodeId(), -1)
            );
        }
        // stop vote timer
        if (electionScheduledFuture != null && !electionScheduledFuture.isDone()) {
            electionScheduledFuture.cancel(true);
        }
        // start heartbeat timer
        log.info("server {} become leader on term {}", myNode.getNodeId(), currentTerm);
        startNewHeartbeat();
    }

    private void resetHeartbeatTimer() {
        log.info("server {} resetHeartbeatTimer", myNode.getNodeId());
        lock.lock();
        try {
            if (state != RaftState.LEADER) {
                if (heartbeatScheduledFuture != null && !heartbeatScheduledFuture.isDone()) {
                    heartbeatScheduledFuture.cancel(true);
                }
                return;
            }
        } finally {
            lock.unlock();
        }
        if (heartbeatScheduledFuture != null && !heartbeatScheduledFuture.isDone()) {
            heartbeatScheduledFuture.cancel(true);
        }
        heartbeatScheduledFuture = scheduledExecutorService.schedule(new Runnable() {
            public void run() {
                startNewHeartbeat();
            }
        }, RaftOptions.heartbeatPeriodMilliseconds, TimeUnit.MILLISECONDS);
    }

    /**
     * 当选leader,向所有节点发送心跳包
     * 优先发送同步消息
     * 然后才是未提交
     * 最后是普通心跳包
     */
    private void startNewHeartbeat() {
        for (final Node node : nodes) {
            if (!node.equals(myNode)) {
                executorService.execute(new Runnable() {
                    public void run() {
                        RaftMessageProto.RaftMessage raftMessage;
                        lock.lock();
                        try {
                            if (state != RaftState.LEADER) {
                                return;
                            }
                            long tempIndex = lastRaftMessageMap.get(node.getNodeId()).getCommitIndex();
                            if ((tempIndex == 0 && consistentHash.getSize() > 0)
                                    || lastAppliedIndex < tempIndex) {
                                //同步所有节点消息

                                String data = consistentHash.getNodesStr();
                                log.info("server {} send NodeMessage to server {} data {}", myNode.getNodeId(), node.getNodeId(), data);
                                raftMessage = RaftMessageProto.RaftMessage
                                        .newBuilder()
                                        .setMessageType(RaftMessageProto.MessageType.NODE)
                                        .setId((int) myNode.getNodeId())
                                        .setCurrentTerm(currentTerm)
                                        .setLastAppliedIndex(lastAppliedIndex)
                                        .setLeader(true)
                                        .setNodeMessage(
                                                RaftMessageProto.NodeMessage.newBuilder()
                                                        .setNodeMessage(data)
                                                        .build()
                                        )
                                        .build();
                            } else if (tempIndex > 0 && tempIndex < lastAppliedIndex) {
                                //同步日志
                                RaftLogParse logParse = new RaftLogParse(raftLogPath);
                                String logsStr = logParse.getLogsStr(logParse.getNewRaftLogs(tempIndex));
                                log.info("server {} send CommitMessage to server {} data {}",
                                        myNode.getNodeId(), node.getNodeId(), logsStr);
                                raftMessage = RaftMessageProto.RaftMessage
                                        .newBuilder()
                                        .setMessageType(RaftMessageProto.MessageType.COMMIT)
                                        .setId((int) myNode.getNodeId())
                                        .setCurrentTerm(currentTerm)
                                        .setLastAppliedIndex(lastAppliedIndex)
                                        .setLeader(true)
                                        .setCommitMessage(
                                                RaftMessageProto.CommitMessage.newBuilder()
                                                        .setCommitMessage(logsStr)
                                                        .build()
                                        )
                                        .build();

                            } else if (unCommitLogs.size() > 0
                                    && !unCommitLogs.get(0).getAck().contains(node.getNodeId())) {
                                log.info("server {} send AckMessage to server {}", myNode.getNodeId(), node.getNodeId());
                                raftMessage = RaftMessageProto.RaftMessage
                                        .newBuilder()
                                        .setMessageType(RaftMessageProto.MessageType.ACK)
                                        .setId((int) myNode.getNodeId())
                                        .setCurrentTerm(currentTerm)
                                        .setLastAppliedIndex(lastAppliedIndex)
                                        .setLeader(true)
                                        .setAckMessage(
                                                RaftMessageProto.AckMessage
                                                        .newBuilder()
                                                        .setAckIndex(unCommitLogs.get(0).getIndex())
                                                        .build()
                                        )
                                        .build();
                                //
                            } else {
                                //发送普通的心跳包
                                log.info("server {} send HeartMessage to server {}", myNode.getNodeId(), node.getNodeId());
                                raftMessage = RaftMessageProto.RaftMessage
                                        .newBuilder()
                                        .setMessageType(RaftMessageProto.MessageType.HEART)
                                        .setId((int) myNode.getNodeId())
                                        .setCurrentTerm(currentTerm)
                                        .setLastAppliedIndex(lastAppliedIndex)
                                        .setLeader(true)
                                        .setHeartMessage(RaftMessageProto.HeartMessage.newBuilder().build())
                                        .build();
                            }
                        } finally {
                            lock.unlock();
                        }
                        Channel channel = getChannel(node);
                        if (channel == null || !channel.isActive()) {
                            if (consistentHash.hashNode(node)) {
                                generate(node, false);
                            }
                        } else {
                            if (!consistentHash.hashNode(node)) {
                                generate(node, true);
                            }
                        }
                        if (raftMessage != null && channel != null) {
                            channel.writeAndFlush(raftMessage);
                        }
                    }
                });
            } else {
                if (!consistentHash.hashNode(myNode)) {
                    generate(myNode, true); //内部会加锁
                }
            }
        }
        resetHeartbeatTimer();
    }

    /**
     * 产生新提交日志信息。
     *
     * @param node 节点
     * @param tags 日志类型
     */
    private void generate(Node node, boolean tags) {
        lock.lock();
        try {
            RaftLogUnCommit unCommitLog = new RaftLogUnCommit();
            long index = unCommitLogs.size() > 0 ? unCommitLogs.get(unCommitLogs.size() - 1).getIndex() + 1 : lastAppliedIndex + 1;
            unCommitLog.setIndex(index);
            unCommitLog.setTimestamp(new Date().getTime() / 1000);
            String nodeStr = String.format(
                    "%d:%s:%d:%d",
                    node.getNodeId(),
                    node.getIp(),
                    node.getListenHeartbeatPort(),
                    node.getListenClientPort()
            );
            if (tags) {
                unCommitLog.setCommand("add " + nodeStr);
            } else {
                unCommitLog.setCommand("remove " + nodeStr);
            }
            if (unCommitLogs.size() > 0) {
                boolean tag = true;
                for (RaftLogUnCommit unLog : unCommitLogs) {
                    if (unLog.getCommand().equals(unCommitLog.getCommand())) {
                        tag = false;
                        break;
                    }
                }
                if (tag) {
                    unCommitLogs.add(unCommitLog);
                }
                log.info("server {} generate uncommit log {}", myNode.getNodeId(), unCommitLogs);
            } else {
                unCommitLogs.add(unCommitLog);
            }
        } finally {
            lock.unlock();
        }
    }


    /**
     * 监听其他节点的消息
     */
    private void listenNode() {
        //创建一个线程组,接收连接
        EventLoopGroup boss = new NioEventLoopGroup(1);
        //创建一个线程组,处理连接
        EventLoopGroup worker = new NioEventLoopGroup(1);
        try {
            //启动一个服务端
            ServerBootstrap serverBootstrap = new ServerBootstrap();
            serverBootstrap.group(boss, worker)
                    .channel(NioServerSocketChannel.class)  //通过反射的机制
                    .childHandler(new ListenNodeInitializer());
            //端口绑定
            ChannelFuture channelFuture = serverBootstrap.bind(myNode.getListenHeartbeatPort()).sync();
            channelFuture.channel().closeFuture();
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * 处理投票信息
     *
     * @param ctx ChannelHandlerContext
     * @param msg 收到的消息
     */
    private void doVoteMessage(ChannelHandlerContext ctx, RaftMessageProto.RaftMessage msg) {
        //candidate->nodes
        Long term = msg.getCurrentTerm();
        RaftMessageProto.VoteMessage voteMessage = null;
        RaftMessageProto.RaftMessage raftMessage;
        lock.lock();
        try {
            if (state == RaftState.LEADER) {
                if (currentTerm >= term) { //拒绝
                    voteMessage = RaftMessageProto.VoteMessage.newBuilder()
                            .setVoteFor(false)
                            .build();
                } else { //接收,同时降为follower
                    voteMessage = RaftMessageProto.VoteMessage.newBuilder()
                            .setVoteFor(true)
                            .build();
                    votedFor = msg.getId();
                    state = RaftState.FOLLOWER;
                    currentTerm = term;
                    //既然把票投了别人就当做收到leader 消息，使得自己不会发起新的投票
                    leaderConnectTime = new Date().getTime();
                    // stop heartbeat
                    if (heartbeatScheduledFuture != null && !heartbeatScheduledFuture.isDone()) {
                        heartbeatScheduledFuture.cancel(true);
                    }
                }
            } else if (state == RaftState.FOLLOWER) {
                if (currentTerm > term) { //拒绝
                    voteMessage = RaftMessageProto.VoteMessage.newBuilder()
                            .setVoteFor(false)
                            .build();

                } else if (currentTerm == term) {
                    if (canVoteFor(msg)) {
                        voteMessage = RaftMessageProto.VoteMessage.newBuilder()
                                .setVoteFor(true)
                                .build();
                        votedFor = msg.getId();
                        //既然把票投了别人就当做收到leader 消息，使得自己不会发起新的投票
                        leaderConnectTime = new Date().getTime();
                    } else {
                        voteMessage = RaftMessageProto.VoteMessage.newBuilder()
                                .setVoteFor(false)
                                .build();
                    }
                } else {
                    voteMessage = RaftMessageProto.VoteMessage.newBuilder()
                            .setVoteFor(true)
                            .build();
                    //既然把票投了别人就当做收到leader 消息，使得自己不会发起新的投票
                    leaderConnectTime = new Date().getTime();
                    votedFor = msg.getId();
                    currentTerm = term;
                }
            } else if (state == RaftState.PRE_CANDIDATE) {
                if (currentTerm > term) { //拒绝
                    voteMessage = RaftMessageProto.VoteMessage.newBuilder()
                            .setVoteFor(false)
                            .build();
                } else if (currentTerm < term) {  //接收并降级为follower
                    voteMessage = RaftMessageProto.VoteMessage.newBuilder()
                            .setVoteFor(true)
                            .build();
                    //既然把票投了别人就当做收到leader 消息，使得自己不会发起新的投票
                    leaderConnectTime = new Date().getTime();
                    currentTerm = term;
                    state = RaftState.FOLLOWER;
                    votedFor = msg.getId();
                    if (electionScheduledFuture != null && !electionScheduledFuture.isDone()) {
                        electionScheduledFuture.cancel(true);
                    }
                } else {
                    if (canVoteFor(msg)) { //接收并降级为follower
                        votedFor = msg.getId();
                        currentTerm = term;
                        state = RaftState.FOLLOWER;
                        voteMessage = RaftMessageProto.VoteMessage.newBuilder()
                                .setVoteFor(true)
                                .build();
                        //既然把票投了别人就当做收到leader 消息，使得自己不会发起新的投票
                        leaderConnectTime = new Date().getTime();
                        if (electionScheduledFuture != null && !electionScheduledFuture.isDone()) {
                            electionScheduledFuture.cancel(true);
                        }
                    } else {
                        voteMessage = RaftMessageProto.VoteMessage.newBuilder()
                                .setVoteFor(false)
                                .build();
                    }
                }
            } else if (state == RaftState.CANDIDATE) {
                if (currentTerm >= term) { //拒绝
                    voteMessage = RaftMessageProto.VoteMessage.newBuilder()
                            .setVoteFor(false)
                            .build();
                } else {
                    voteMessage = RaftMessageProto.VoteMessage.newBuilder()
                            .setVoteFor(true)
                            .build();
                    //既然把票投了别人就当做收到leader 消息，使得自己不会发起新的投票
                    leaderConnectTime = new Date().getTime();
                    votedFor = msg.getId();
                    currentTerm = term;
                    state = RaftState.FOLLOWER;
                    if (electionScheduledFuture != null && !electionScheduledFuture.isDone()) {
                        electionScheduledFuture.cancel(true);
                    }
                }
            }
            raftMessage = RaftMessageProto.RaftMessage.newBuilder()
                    .setMessageType(RaftMessageProto.MessageType.VOTE)
                    .setId((int) myNode.getNodeId())
                    .setCurrentTerm(currentTerm)
                    .setLastAppliedIndex(lastAppliedIndex)
                    .setLeader(state == RaftState.LEADER)
                    .setVoteMessage(voteMessage)
                    .build();
        } finally {
            lock.unlock();
        }
        ctx.writeAndFlush(raftMessage);
    }

    /**
     * 处理预投票信息.
     *
     * @param ctx ChannelHandlerContext
     * @param msg 收到的消息
     * @throws IOException 网络io 异常
     */
    private void doPreVoteMessage(ChannelHandlerContext ctx, RaftMessageProto.RaftMessage msg) throws IOException {
        RaftMessageProto.PreVoteMessage preVoteMessage;
        RaftMessageProto.RaftMessage message;
        lock.lock();
        try {
            if (canPreVoteFor(msg)) {
                preVoteMessage = RaftMessageProto.PreVoteMessage.newBuilder()
                        .setVoteFor(true)
                        .build();
                log.info("server {} pre vote for server{}", myNode.getNodeId(), msg.getId());
            } else {
                preVoteMessage = RaftMessageProto.PreVoteMessage.newBuilder()
                        .setVoteFor(false)
                        .build();
                log.info("server {} do not pre vote for server{}", myNode.getNodeId(), msg.getId());
            }
            message = RaftMessageProto.RaftMessage.newBuilder()
                    .setMessageType(RaftMessageProto.MessageType.PRE_VOTE)
                    .setId((int) myNode.getNodeId())
                    .setLastAppliedIndex(lastAppliedIndex)
                    .setCurrentTerm(currentTerm)
                    .setLeader(state == RaftState.LEADER)
                    .setPreVoteMessage(preVoteMessage)
                    .build();

        } finally {
            lock.unlock();
        }
        //发送消息
        if (!ctx.channel().isActive()) {
            throw new IOException("网络连接异常");
        }
        ctx.writeAndFlush(message);
    }

    /**
     * 未提交日志等待其他节点确认.
     *
     * @param ctx ChannelHandlerContext
     * @param msg 消息
     */
    private void doAckMessage(ChannelHandlerContext ctx, RaftMessageProto.RaftMessage msg) {
        long term = msg.getCurrentTerm();
        RaftMessageProto.AckMessage ackMessage = null;
        RaftMessageProto.RaftMessage raftMessage;
        lock.lock();
        try {
            if (state == RaftState.LEADER) {
                if (currentTerm > term) { //不接受
                    ackMessage = RaftMessageProto.AckMessage.newBuilder()
                            .setAckIndex(-1)
                            .build();
                } else if (currentTerm == term) {
                    //这种情况不存在
                    log.debug("servers have two leaders on same term {}", currentTerm);
                } else {  //接收,同时降级为follower
                    currentTerm = term;
                    votedFor = -1;
                    ackMessage = RaftMessageProto.AckMessage.newBuilder()
                            .setAckIndex(msg.getAckMessage().getAckIndex())
                            .build();
                    leaderConnectTime = new Date().getTime();
                    state = RaftState.FOLLOWER;
                    // stop heartbeat
                    if (heartbeatScheduledFuture != null && !heartbeatScheduledFuture.isDone()) {
                        heartbeatScheduledFuture.cancel(true);
                    }
                }
            } else if (state == RaftState.FOLLOWER) {
                if (currentTerm > term) { //不接受
                    ackMessage = RaftMessageProto.AckMessage.newBuilder()
                            .setAckIndex(-1)
                            .build();
                } else if (currentTerm == term) { //接收
                    ackMessage = RaftMessageProto.AckMessage.newBuilder()
                            .setAckIndex(msg.getAckMessage().getAckIndex())
                            .build();
                    leaderConnectTime = new Date().getTime();
                } else {  //接收
                    currentTerm = term;
                    votedFor = -1;
                    ackMessage = RaftMessageProto.AckMessage.newBuilder()
                            .setAckIndex(msg.getAckMessage().getAckIndex())
                            .build();
                    leaderConnectTime = new Date().getTime();
                }
            } else if (state == RaftState.PRE_CANDIDATE || state == RaftState.CANDIDATE) {
                if (currentTerm > term) { //不接受
                    ackMessage = RaftMessageProto.AckMessage.newBuilder()
                            .setAckIndex(-1)
                            .build();
                } else { //接收,降级为follower

                    if (currentTerm < term) {
                        votedFor = -1;
                    }
                    currentTerm = term;
                    state = RaftState.FOLLOWER;
                    leaderConnectTime = new Date().getTime();
                    ackMessage = RaftMessageProto.AckMessage.newBuilder()
                            .setAckIndex(msg.getAckMessage().getAckIndex())
                            .build();
                    // stop election
                    if (electionScheduledFuture != null && !electionScheduledFuture.isDone()) {
                        electionScheduledFuture.cancel(true);
                    }
                }
            }
            raftMessage = RaftMessageProto.RaftMessage.newBuilder()
                    .setMessageType(RaftMessageProto.MessageType.ACK)
                    .setId((int) myNode.getNodeId())
                    .setLastAppliedIndex(lastAppliedIndex)
                    .setCurrentTerm(currentTerm)
                    .setLeader(state == RaftState.LEADER)
                    .setAckMessage(ackMessage)
                    .build();
        } finally {
            lock.unlock();
        }
        ctx.writeAndFlush(raftMessage);
    }

    /**
     * 心跳包处理的 .
     *
     * @param ctx ChannelHandlerContext
     * @param msg RaftMessageProto.RaftMessage
     */
    private void doHeartMessage(ChannelHandlerContext ctx, RaftMessageProto.RaftMessage msg) {
        //leader-> nodes
        long term = msg.getCurrentTerm();
        RaftMessageProto.HeartMessage heartMessage = RaftMessageProto.HeartMessage.newBuilder().build();
        RaftMessageProto.RaftMessage raftMessage;
        lock.lock();
        try {
            if (state == RaftState.LEADER) {
                if (term > currentTerm) { //leader降级为follower
                    state = RaftState.FOLLOWER;
                    currentTerm = term;
                    votedFor = -1;
                    leaderConnectTime = new Date().getTime();
                    // stop heartbeat
                    if (heartbeatScheduledFuture != null && !heartbeatScheduledFuture.isDone()) {
                        heartbeatScheduledFuture.cancel(true);
                    }
                }
            } else if (state == RaftState.FOLLOWER) {
                if (term > currentTerm) {
                    currentTerm = term;
                    votedFor = -1;
                    leaderConnectTime = new Date().getTime();
                } else if (term == currentTerm) {
                    leaderConnectTime = new Date().getTime();
                }
            } else if (state == RaftState.PRE_CANDIDATE || state == RaftState.CANDIDATE) {
                if (term >= currentTerm) { //降级为follower
                    state = RaftState.FOLLOWER;
                    currentTerm = term;
                    votedFor = -1;
                    leaderConnectTime = new Date().getTime();
                    // stop election
                    if (electionScheduledFuture != null && !electionScheduledFuture.isDone()) {
                        electionScheduledFuture.cancel(true);
                    }
                }
            }
            raftMessage = RaftMessageProto.RaftMessage.newBuilder()
                    .setMessageType(RaftMessageProto.MessageType.HEART)
                    .setId((int) myNode.getNodeId())
                    .setLastAppliedIndex(lastAppliedIndex)
                    .setCurrentTerm(currentTerm)
                    .setLeader(state == RaftState.LEADER)
                    .setHeartMessage(heartMessage)
                    .build();
        } finally {
            lock.unlock();
        }
        ctx.channel().writeAndFlush(raftMessage);

    }

    /**
     * 同步已经提交的日志
     *
     * @param ctx ChannelHandlerContext
     * @param msg RaftMessageProto.RaftMessage
     */
    private void doCommitMessage(ChannelHandlerContext ctx, RaftMessageProto.RaftMessage msg) {
        //leader->nodes
        long term = msg.getCurrentTerm();
        RaftMessageProto.RaftMessage raftMessage;
        //是否将接受这个同步日志
        boolean tags = false;
        lock.lock();
        try {
            if (state == RaftState.LEADER) {
                if (term > currentTerm) { //leader降级为follower，同时接收这个日志同步
                    state = RaftState.FOLLOWER;
                    currentTerm = term;
                    votedFor = -1;
                    // stop heartbeat
                    if (heartbeatScheduledFuture != null && !heartbeatScheduledFuture.isDone()) {
                        heartbeatScheduledFuture.cancel(true);
                    }
                    tags = true;
                }
            } else if (state == RaftState.FOLLOWER) {
                if (term > currentTerm) {
                    currentTerm = term;
                    votedFor = -1;
                    tags = true;
                } else if (term == currentTerm) {
                    //将同步日志写入本地日志，同时将日志应用于本地state machine
                    tags = true;
                }
            } else if (state == RaftState.PRE_CANDIDATE || state == RaftState.CANDIDATE) {
                if (term > currentTerm) { //降级为follower,同时接收这个日志同步
                    state = RaftState.FOLLOWER;
                    currentTerm = term;
                    votedFor = -1;
                    // stop election
                    if (electionScheduledFuture != null && !electionScheduledFuture.isDone()) {
                        electionScheduledFuture.cancel(true);
                    }
                    tags = true;
                } else if (term == currentTerm) {
                    state = RaftState.FOLLOWER;
                    // stop election
                    if (electionScheduledFuture != null && !electionScheduledFuture.isDone()) {
                        electionScheduledFuture.cancel(true);
                    }
                    tags = true;
                }
            }
            if (tags) {
                leaderConnectTime = new Date().getTime();
                lastAppliedIndex = msg.getLastAppliedIndex();

            }
        } finally {

            lock.unlock();
        }
        if (tags) {
            //将同步日志写入本地日志，同时将日志应用于本地state machine
            RaftLogParse logParse = new RaftLogParse(raftLogPath);
            List<RaftLog> logs = logParse.getLogsFromStr(msg.getCommitMessage().getCommitMessage());
            log.info("server {} get logs {}", myNode.getNodeId(), logs);
            appendEntries(logs, true);
        }
        lock.lock();
        try {
            raftMessage = RaftMessageProto.RaftMessage.newBuilder()
                    .setMessageType(RaftMessageProto.MessageType.COMMIT)
                    .setId((int) myNode.getNodeId())
                    .setCurrentTerm(currentTerm)
                    .setLastAppliedIndex(lastAppliedIndex)
                    .setLeader(state == RaftState.LEADER)
                    .setCommitMessage(RaftMessageProto.CommitMessage.newBuilder().build())
                    .build();
        } finally {
            lock.unlock();
        }
        ctx.channel().writeAndFlush(raftMessage);
    }

    /**
     * 对于刚启动节点直接同步集群状态.
     *
     * @param ctx ChannelHandlerContext
     * @param msg RaftMessageProto.RaftMessage
     * @throws IOException 网路io 异常
     */
    private void doSnapHotMessage(ChannelHandlerContext ctx, RaftMessageProto.RaftMessage msg) throws IOException {
        //leader->nodes
        long term = msg.getCurrentTerm();
        boolean tag = false;
        RaftMessageProto.RaftMessage raftMessage;
        lock.lock();
        try {
            if (state == RaftState.LEADER) {
                if (currentTerm < term) {        //接收并降级为follower
                    currentTerm = term;
                    votedFor = -1;
                    log.info("清空已经无效的日志信息");
                    tag = true;
                    state = RaftState.FOLLOWER;
                    // stop heartbeat
                    if (heartbeatScheduledFuture != null && !heartbeatScheduledFuture.isDone()) {
                        heartbeatScheduledFuture.cancel(true);
                    }
                }
            } else if (state == RaftState.FOLLOWER) {
                if (currentTerm == term) {
                    log.info("清空已经无效的日志信息");
                    tag = true;
                } else if (currentTerm < term) {
                    currentTerm = term;
                    votedFor = -1;
                    log.info("清空已经无效的日志信息");
                    tag = true;
                }
            } else if (state == RaftState.PRE_CANDIDATE || state == RaftState.CANDIDATE) {
                if (currentTerm == term) { //降级为follower
                    state = RaftState.FOLLOWER;
                    log.info("清空已经无效的日志信息");
                    tag = true;
                    // stop election
                    if (electionScheduledFuture != null && !electionScheduledFuture.isDone()) {
                        electionScheduledFuture.cancel(true);
                    }
                } else if (currentTerm < term) {                        //降级为follower
                    state = RaftState.FOLLOWER;
                    currentTerm = term;
                    votedFor = -1;
                    log.info("清空已经无效的日志信息");
                    tag = true;
                    // stop election
                    if (electionScheduledFuture != null && !electionScheduledFuture.isDone()) {
                        electionScheduledFuture.cancel(true);
                    }
                }
            }
            if (tag) {
                leaderConnectTime = new Date().getTime();
                lastAppliedIndex = msg.getLastAppliedIndex();
            }
            raftMessage = RaftMessageProto.RaftMessage.newBuilder()
                    .setMessageType(RaftMessageProto.MessageType.NODE)
                    .setId((int) myNode.getNodeId())
                    .setLastAppliedIndex(lastAppliedIndex)
                    .setLeader(state == RaftState.LEADER)
                    .setCurrentTerm(currentTerm)
                    .setNodeMessage(RaftMessageProto.NodeMessage.newBuilder().build())
                    .build();
        } finally {
            lock.unlock();
        }

        if (tag) {
            syncStateMachine(msg.getNodeMessage().getNodeMessage());
            log.info("清空已经无效的文件");
            RaftSnaphot raftSnaphot = new RaftSnaphot(raftSnaphotPath);
            raftSnaphot.raftSnaphotStore(consistentHash.getCircle());

            RaftLogParse logParse = new RaftLogParse(raftLogPath);
            logParse.clearFile();
        }
        if (!ctx.channel().isActive()) {
            throw new IOException("网络io异常");
        } else {
            ctx.writeAndFlush(raftMessage);
        }

    }

    /**
     * 是否把投票给他
     *
     * @param message 收到的消息
     * @return 是否投票给他
     */
    //in lock
    private boolean canVoteFor(RaftMessageProto.RaftMessage message) {
        //1.term > currentTerm
        //2.term == currentTerm && 日志不比当前节点旧 && 该节点没有把票投其他节点
        if (currentTerm < message.getCurrentTerm()) {
            return true;
        }
        if (currentTerm == message.getCurrentTerm() && votedFor == -1) {
            return message.getLastAppliedIndex() >= lastAppliedIndex && !haveLeader();
        }
        return false;
    }

    /**
     * 对于这个预投票，是否把票投给它
     *
     * @param message 收到的消息
     * @return 是否投票给他
     */
    //in lock
    private boolean canPreVoteFor(RaftMessageProto.RaftMessage message) {
        //当前节点没有term 小于那个节点 || (或者等于&&该节点也没有leader)
        if (currentTerm < message.getCurrentTerm()) {
            return true;
        } else if (currentTerm == message.getCurrentTerm()) {
            return !haveLeader();
        }
        return false;
    }

    /**
     * 处理同步过来的节点数据.
     *
     * @param nodesStr id:ip:port1:port2#id:ip:port1:port2
     */
    private void syncStateMachine(String nodesStr) {
        if (nodesStr == null || nodesStr.length() < 4) {
            return;
        }
        String[] temp = nodesStr.split("#");
        for (String nodeStr : temp) {
            String[] nodeStrTemp = nodeStr.split(":");
            int id = Integer.valueOf(nodeStrTemp[0]);
            String ip = nodeStrTemp[1];
            int listenHeartPort = Integer.valueOf(nodeStrTemp[2]);
            int listenClientPort = Integer.valueOf(nodeStrTemp[3]);
            Node node = new Node.Builder()
                    .setNodeId((short) id)
                    .setIp(ip)
                    .setListenHeartbeatPort(listenHeartPort)
                    .setListenClientPort(listenClientPort)
                    .build();
            consistentHash.add(node);
        }
    }

    /**
     * 节点启动的时候将RaftSnaphot 应用于state machine
     */
    private void installRaftSnaphot() {
        RaftSnaphot raftSnaphot = new RaftSnaphot(raftSnaphotPath);
        //这个文件不存在是size
        Map<Short, Node> map = raftSnaphot.getRaftSnaphot();
        Set<Map.Entry<Short, Node>> sets = map.entrySet();
        for (Map.Entry<Short, Node> data : sets) {
            consistentHash.add(data.getValue());
        }
    }

    private HashMap<Short, Node> getRaftSnaphot() {
        RaftSnaphot raftSnaphot = new RaftSnaphot(raftSnaphotPath);
        return raftSnaphot.getRaftSnaphot();
    }

    /**
     * 节点启动的时候将已经提交的日志应用于state machine.
     */
    private void appendEntries() {
        List<RaftLog> logs = new RaftLogParse(QCacheConfiguration.getRaftLogsPath()).getRaftLogs();
        appendEntries(logs, false);
    }

    /**
     * 解析日志同步到state Machine
     *
     * @param logs     日志
     * @param writeLog 是否写入到日志文件中
     */
    private void appendEntries(List<RaftLog> logs, boolean writeLog) {
        if (logs != null && logs.size() > 0) {
            lock.lock();
            try {
                lastAppliedIndex = logs.get(logs.size() - 1).getIndex();
            } finally {
                lock.unlock();
            }
        } else {
            return;
        }
        for (RaftLog raftLog : logs) {
            String command = raftLog.getCommand();
            appendEntry(command);
            //追加日志到文件
            if (writeLog) {
                RaftLogParse logParse = new RaftLogParse(raftLogPath);
                File file = new File(raftLogPath);
                if (file.exists() && file.isFile() && file.length() > RaftOptions.maxLogSize) {
                    logParse.clearFile();
                    RaftSnaphot raftSnaphot = new RaftSnaphot(raftSnaphotPath);
                    raftSnaphot.raftSnaphotStore(getRaftSnaphot());
                }
                logParse.insertLog(raftLog);
            }
        }
    }

    /**
     * 解析命令，并将他应用于state machine
     *
     * @param command command
     */
    private void appendEntry(String command) {
        log.info("server {} parse command {}", myNode.getNodeId(), command);
        String[] temp = command.split("\\s+");
        String methodStr = temp[0];
        String nodeStr = temp[1];
        String[] temp2 = nodeStr.split(":");
        short id = Short.valueOf(temp2[0]);
        String ip = temp2[1];
        int heartPort = Integer.valueOf(temp2[2]);
        int clientPort = Integer.valueOf(temp2[3]);
        Node node = new Node.Builder()
                .setNodeId(id)
                .setIp(ip)
                .setListenHeartbeatPort(heartPort)
                .setListenClientPort(clientPort)
                .build();
        Class<?> consistentHashClass = ConsistentHash.class;
        try {
            Method method = consistentHashClass.getMethod(methodStr, Node.class);
            if (method != null) {
                method.invoke(consistentHash, node);
            }
        } catch (NoSuchMethodException ex) {
            ex.printStackTrace();
        } catch (IllegalAccessException ex) {
            ex.printStackTrace();
        } catch (InvocationTargetException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * 为了避免在同时发起选举，而拿不到半数以上的投票，
     * 在150-300ms 这个随机时间内，发起新一轮选举
     * (Raft 论文上说150-300ms这个随机时间，不过好像把时差弄大点，效果更好)
     *
     * @return 一个随机的时间(ms)
     */
    private int getRandomElection() {
        int randTime = new Random().nextInt(500) + 150;
        log.info("server {} start election after {} ms", myNode.getNodeId(), randTime);
        return randTime;
    }

    /**
     * 集群内是否有leader
     *
     * @return 集群内受有leader
     */
    private boolean haveLeader() {

        if (state == RaftState.LEADER
                || new Date().getTime() - leaderConnectTime <= 2 * RaftOptions.electionTimeoutMilliseconds) {
            log.info("server {} have leader on term {}", myNode.getNodeId(), currentTerm);
            return true;
        }
        log.info("server {} do not have leader on term {}", myNode.getNodeId(), currentTerm);
        return false;
    }

    /**
     * 打印server状态.
     */
    private void printStatus() {
        log.info("server {} currentTerm = {} state =  {},lastAppliedIndex = {},circle = {}",
                myNode.getNodeId(), currentTerm, state, lastAppliedIndex, consistentHash.getCircle());
    }

    /**
     * 从本地加载缓存
     */
    private void loadCache() {
        //load RDB
        BackUpRdb backUpRdb = BackUpRdb.getInstance();
        backUpRdb.loadData(cache);
        //load AOF file
        BackUpAof backUpAof = BackUpAof.getInstance();
        backUpAof.loadData(cache);

    }

    /**
     * 客户端发送HeartMessage后对收到的消息处理.
     *
     * @param message 收到的消息
     */
    private void doClientHeartMessage(RaftMessageProto.RaftMessage message) {
        doClientCommitMessage(message);
    }

    /**
     * 客户端发送NodeMessage后对收到的消息处理.
     *
     * @param message message
     */
    private void doClientNodeMessage(RaftMessageProto.RaftMessage message) {
        doClientCommitMessage(message);
    }

    /**
     * 客户端发送CommitMessage后对收到的消息处理.
     *
     * @param message 收到的消息
     */
    private void doClientCommitMessage(RaftMessageProto.RaftMessage message) {
        lock.lock();
        try {
            stepDown(message);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 处理客户端发送UN_COMMIT_MESSAGE 后收到的回复消息.
     *
     * @param message 收到的消息
     */
    private void doClientAckMessage(RaftMessageProto.RaftMessage message) {
        lock.lock();
        try {
            stepDown(message);
            if (message.getAckMessage().getAckIndex() > 0
                    && unCommitLogs.size() > 0) {
                RaftLogUnCommit uncommit = unCommitLogs.get(0);

                Set<Short> temp = uncommit.getAck();
                temp.add((short) message.getId());
                unCommitLogs.remove(0);
                if (temp.size() + 1 <= nodes.size() / 2) {
                    uncommit.setAck(temp);
                    unCommitLogs.add(0, uncommit);
                }

                if (uncommit.getAck().size() + 1 > nodes.size() / 2) {
                    //
                    RaftLogParse raftLogParse = new RaftLogParse(raftLogPath);
                    raftLogParse.insertLog(uncommit);
                    appendEntry(uncommit.getCommand());
                    lastAppliedIndex = uncommit.getIndex();
                }
            }
        } finally {
            lock.unlock();
        }
    }

    //启动监听客户端请求的线程
    private void initListenClient() {
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                ListenClientServer clientServer = new ListenClientServer(UserMessageProto.UserMessage.getDefaultInstance(),
                        myNode.getListenClientPort()
                );
                clientServer.init();
            }
        });
        thread.setName("Listen-Client-Thread");
        thread.start();

    }

    private void initAsyncAppendLogThread() {
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                BackUpAof backUpAof = BackUpAof.getInstance();
                LinkedList<String> buffer = new LinkedList<String>();
                while (true) {
                    String log;
                    int size = asyncAppendFileQueue.size();
                    if (size == 0) {
                        log = asyncAppendFileQueue.poll();
                        if (log != null && log.length() > 0) {
                            backUpAof.appendAofLog(log, cache);
                        }
                    } else {
                        buffer.clear();
                        for (int i = 0;i<size;i++) {
                            log = asyncAppendFileQueue.poll();
                            if (log != null && log.length() > 0) {
                                buffer.add(log);
                            }

                        }

                        backUpAof.appendAofLogs(buffer,cache);

                    }



                }

            }
        });
        thread.setName("Async-AppendLog-Thread");
        thread.start();
    }

    /**
     * 处理set请求.
     * 对于写操作是需要写日志的.
     *
     * @param ctx ctx
     * @param msg msg
     */
    private void doSet(NioChannel ctx, UserMessageProto.UserMessage msg) {
        String key = msg.getSetMessage().getKey();
        if (switchNode(key)) {
            doSwitchNode(ctx, key);
            return;
        }
        String val = msg.getSetMessage().getVal();
        long last = msg.getSetMessage().getLastTime();
        UserMessageProto.UserMessage userMessage;
        UserMessageProto.ResponseMessage responseMessage;
        if (!writeAble) {
            responseMessage = UserMessageProto.ResponseMessage
                    .newBuilder()
                    .setResponseType(UserMessageProto.ResponseType.NOT_WRITE)
                    .setVal("不可写")
                    .build();
            userMessage = UserMessageProto.UserMessage
                    .newBuilder()
                    .setMessageType(UserMessageProto.MessageType.RESPONSE)
                    .setResponseMessage(responseMessage)
                    .build();
            ctx.write(userMessage);
            return;
        }

        //不要用异常做流程控制
        try {
            int data = Integer.valueOf(val);
            CacheData cacheData = new CacheDataInt(data, new Date().getTime(), last);
            cache.put(key, cacheData);
        } catch (Exception ex) {
            CacheData cacheData = new CacheDataString(val, new Date().getTime(), last);
            cache.put(key, cacheData);
        }

        //set 操作数据写日志
        //由于val 里面可能会有空格 解析的时候可以找两边的空格,再拆分
        String logLine = "set " + key + " " + val + " " + last;



        asyncAppendFileQueue.put(logLine);



        /*BackUpAof backUpAof = BackUpAof.getInstance();
        backUpAof.appendAofLog(logLine, cache);*/

        responseMessage = UserMessageProto.ResponseMessage
                .newBuilder()
                .setResponseType(UserMessageProto.ResponseType.SUCCESS)
                .setVal(val)
                .build();
        userMessage = UserMessageProto.UserMessage
                .newBuilder()
                .setMessageType(UserMessageProto.MessageType.RESPONSE)
                .setResponseMessage(responseMessage)
                .build();
        ctx.write(userMessage);
    }

    /**
     * 处理del请求.
     * 对于写操作是需要写日志的.
     *
     * @param ctx ctx
     * @param msg msg
     */
    private void doDel(NioChannel ctx, UserMessageProto.UserMessage msg) {
        String key = msg.getDelMessage().getKey();
        if (switchNode(key)) {
            doSwitchNode(ctx, key);
            return;
        }
        UserMessageProto.UserMessage userMessage;
        UserMessageProto.ResponseMessage responseMessage;
        if (!writeAble) {
            responseMessage = UserMessageProto.ResponseMessage
                    .newBuilder()
                    .setResponseType(UserMessageProto.ResponseType.NOT_WRITE)
                    .setVal("不可写")
                    .build();
            userMessage = UserMessageProto.UserMessage
                    .newBuilder()
                    .setMessageType(UserMessageProto.MessageType.RESPONSE)
                    .setResponseMessage(responseMessage)
                    .build();
            ctx.write(userMessage);
            return;
        }

        if (cache.containsKey(key)) {
            cache.remove(key);
            responseMessage = UserMessageProto.ResponseMessage
                    .newBuilder()
                    .setResponseType(UserMessageProto.ResponseType.SUCCESS)
                    .setVal("del success")
                    .build();
            //del 操作数据写日志
            String logLine = "del " + key;
            /*BackUpAof backUpAof = BackUpAof.getInstance();
            backUpAof.appendAofLog(logLine, cache);*/
            asyncAppendFileQueue.put(logLine);

        } else {
            responseMessage = UserMessageProto.ResponseMessage
                    .newBuilder()
                    .setResponseType(UserMessageProto.ResponseType.NIL)
                    .setVal("key not exist")
                    .build();
        }
        userMessage = UserMessageProto.UserMessage
                .newBuilder()
                .setMessageType(UserMessageProto.MessageType.RESPONSE)
                .setResponseMessage(responseMessage)
                .build();
        ctx.write(userMessage);

    }

    /**
     * 当前key 是否在本机.
     *
     * @param key key
     * @return bool
     */
    private boolean switchNode(String key) {

        return !myNode.equals(consistentHash.get(key));
    }

    private void doSwitchNode(NioChannel ctx, String key) {
        Node node = consistentHash.get(key);
        UserMessageProto.UserMessage userMessage;
        UserMessageProto.ResponseMessage responseMessage;
        responseMessage = UserMessageProto.ResponseMessage
                .newBuilder()
                .setResponseType(UserMessageProto.ResponseType.SWITCH)
                .setVal(node.toString())
                .build();
        userMessage = UserMessageProto.UserMessage
                .newBuilder()
                .setMessageType(UserMessageProto.MessageType.RESPONSE)
                .setResponseMessage(responseMessage)
                .build();
        ctx.write(userMessage);

    }

    /**
     * 处理查看集群状态请求.
     *
     * @param ctx ctx
     */
    private void doStatus(NioChannel ctx) {
        String resData = getStatus();
        UserMessageProto.UserMessage userMessage = UserMessageProto.UserMessage
                .newBuilder()
                .setMessageType(UserMessageProto.MessageType.RESPONSE)
                .setResponseMessage(UserMessageProto.ResponseMessage
                        .newBuilder()
                        .setResponseType(UserMessageProto.ResponseType.SUCCESS)
                        .setVal(resData)
                        .build()
                )
                .build();
        ctx.write(userMessage);

    }

    /**
     * 返回服务器集群状态
     *
     * @return string
     */
    private String getStatus() {
        String res = "------------------------------------------" + "\n" +
                "State:" + state + "\n" +
                "Term:" + currentTerm + "\n" +
                "Id:" + myNode.getNodeId() + "\n" +
                "Ip:" + myNode.getIp() + "\n" +
                "Port:" + myNode.getListenClientPort() + "\n" +
                "Alive Servers:" + "\n";
        StringBuilder builder = new StringBuilder();
        for (Node node : getAliveNodes()) {
            String temp = "server." +
                    node.getNodeId() +
                    "=" + node.getIp() +
                    ":" + node.getListenClientPort() + "\n";
            builder.append(temp);
        }
        builder.append("------------------------------------------" + "\n");
        return res + builder.toString();
    }

    /**
     * 获取集群中所有活跃节点
     *
     * @return 活跃节点集合
     */
    private Set<Node> getAliveNodes() {
        Map<Short, Node> circle = consistentHash.getCircle();
        Set<Node> resNodes = new TreeSet<Node>();
        for (short key : circle.keySet()) {
            resNodes.add(circle.get(key));
        }
        return resNodes;
    }

    /**
     * 处理get请求
     */
    private void doGet(NioChannel ctx, UserMessageProto.UserMessage msg) {
        UserMessageProto.UserMessage userMessage;
        UserMessageProto.ResponseMessage responseMessage;
        String key = msg.getGetMessage().getKey();
        //切换节点
        if (switchNode(key)) {
            doSwitchNode(ctx, key);
            return;
        }
        if (!readAble) {
            responseMessage = UserMessageProto.ResponseMessage
                    .newBuilder()
                    .setResponseType(UserMessageProto.ResponseType.NOT_READ)
                    .setVal("不可读")
                    .build();
            userMessage = UserMessageProto.UserMessage
                    .newBuilder()
                    .setMessageType(UserMessageProto.MessageType.RESPONSE)
                    .setResponseMessage(responseMessage)
                    .build();
            ctx.write(userMessage);
            return;
        }
        CacheData cacheData = cache.get(key);
        if (cacheData != null && !cacheData.isOutDate()) {
            if (cacheData instanceof CacheDataInt) {
                responseMessage = UserMessageProto.ResponseMessage
                        .newBuilder()
                        .setResponseType(UserMessageProto.ResponseType.SUCCESS)
                        .setVal(((CacheDataInt) cacheData).val + "")
                        .build();
                userMessage = UserMessageProto.UserMessage
                        .newBuilder()
                        .setMessageType(UserMessageProto.MessageType.RESPONSE)
                        .setResponseMessage(responseMessage)
                        .build();
                ctx.write(userMessage);
            } else if (cacheData instanceof CacheDataString) {
                responseMessage = UserMessageProto.ResponseMessage
                        .newBuilder()
                        .setResponseType(UserMessageProto.ResponseType.SUCCESS)
                        .setVal(((CacheDataString) cacheData).val)
                        .build();
                userMessage = UserMessageProto.UserMessage
                        .newBuilder()
                        .setMessageType(UserMessageProto.MessageType.RESPONSE)
                        .setResponseMessage(responseMessage)
                        .build();
                ctx.write(userMessage);
            } else {
                responseMessage = UserMessageProto.ResponseMessage
                        .newBuilder()
                        .setResponseType(UserMessageProto.ResponseType.ERROR)
                        .setVal("Error")
                        .build();
                userMessage = UserMessageProto.UserMessage
                        .newBuilder()
                        .setMessageType(UserMessageProto.MessageType.RESPONSE)
                        .setResponseMessage(responseMessage)
                        .build();
                ctx.write(userMessage);
            }
        } else {
            responseMessage = UserMessageProto.ResponseMessage
                    .newBuilder()
                    .setResponseType(UserMessageProto.ResponseType.NIL)
                    .setVal("key not exist")
                    .build();
            userMessage = UserMessageProto.UserMessage
                    .newBuilder()
                    .setMessageType(UserMessageProto.MessageType.RESPONSE)
                    .setResponseMessage(responseMessage)
                    .build();
            ctx.write(userMessage);
        }
    }

    /**
     * Initializer
     */
    private class ListenNodeInitializer extends ChannelInitializer<io.netty.channel.socket.SocketChannel> {
        @Override
        protected void initChannel(io.netty.channel.socket.SocketChannel ch) {
            ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast(new ProtobufVarint32FrameDecoder());
            pipeline.addLast(new ProtobufDecoder(RaftMessageProto.RaftMessage.getDefaultInstance()));
            pipeline.addLast(new ProtobufVarint32LengthFieldPrepender());
            pipeline.addLast(new ProtobufEncoder());
            pipeline.addLast(new ListenNodeHandler());
        }
    }

    /**
     * ListenNodeHandler
     */
    private class ListenNodeHandler extends SimpleChannelInboundHandler<RaftMessageProto.RaftMessage> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RaftMessageProto.RaftMessage msg) throws Exception {
            if (msg.getMessageType() == RaftMessageProto.MessageType.PRE_VOTE) {
                //预先投票信息
                doPreVoteMessage(ctx, msg);
            } else if (msg.getMessageType() == RaftMessageProto.MessageType.VOTE) {
                //投票信息
                doVoteMessage(ctx, msg);
            } else if (msg.getMessageType() == RaftMessageProto.MessageType.HEART) {
                //心跳包
                doHeartMessage(ctx, msg);
            } else if (msg.getMessageType() == RaftMessageProto.MessageType.ACK) {
                //未提交日志确认
                doAckMessage(ctx, msg);
            } else if (msg.getMessageType() == RaftMessageProto.MessageType.COMMIT) {
                //同步已提交日志
                doCommitMessage(ctx, msg);
            } else if (msg.getMessageType() == RaftMessageProto.MessageType.NODE) {
                //同步数据
                doSnapHotMessage(ctx, msg);
            }
        }
    }

    //Initializer
    private class SocketClientInitializer extends ChannelInitializer<SocketChannel> {
        @Override
        protected void initChannel(SocketChannel ch) {
            ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast(new ProtobufVarint32FrameDecoder());
            pipeline.addLast(new ProtobufDecoder(RaftMessageProto.RaftMessage.getDefaultInstance()));
            pipeline.addLast(new ProtobufVarint32LengthFieldPrepender());
            pipeline.addLast(new ProtobufEncoder());
            pipeline.addLast("SocketServerHandler", new SocketClientHandler());
            //自己别的处理器
        }
    }

    //Handler
    private class SocketClientHandler extends SimpleChannelInboundHandler<RaftMessageProto.RaftMessage> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RaftMessageProto.RaftMessage msg) {
            if (msg.getMessageType() == RaftMessageProto.MessageType.PRE_VOTE) {
                //预先投票信息
                lock.lock();
                try {
                    stepDown(msg);
                } finally {
                    lock.unlock();
                }
                if (msg.getPreVoteMessage().getVoteFor()) {
                    synchronized (preVotes) {
                        if (!preVotes.contains((short) msg.getId())) {
                            preVotes.add((short) msg.getId());
                            if (preVotes.size() + 1 > nodes.size() / 2) {
                                preVotes.notifyAll();
                            }
                        }
                    }
                }

            } else if (msg.getMessageType() == RaftMessageProto.MessageType.VOTE) {
                //投票信息
                lock.lock();
                try {
                    stepDown(msg);
                } finally {
                    lock.unlock();
                }
                if (msg.getVoteMessage().getVoteFor()) {
                    synchronized (votes) {
                        if (!votes.contains((short) msg.getId())) {
                            votes.add((short) msg.getId());
                            if (votes.size() + 1 > nodes.size() / 2) {
                                votes.notifyAll();
                            }
                        }
                    }
                }
            } else if (msg.getMessageType() == RaftMessageProto.MessageType.HEART) {
                //心跳包
                doClientHeartMessage(msg);
            } else if (msg.getMessageType() == RaftMessageProto.MessageType.ACK) {
                doClientAckMessage(msg);
            } else if (msg.getMessageType() == RaftMessageProto.MessageType.COMMIT) {
                doClientCommitMessage(msg);
            } else if (msg.getMessageType() == RaftMessageProto.MessageType.NODE) {
                doClientNodeMessage(msg);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            log.debug(cause.toString());
            ctx.close();
        }
    }

    /**
     * 处理客户端请求
     */
    private class ListenClientServer extends NioServer {
        public ListenClientServer(MessageLite messageLite, int port) {
            super(messageLite, port);
        }

        @Override
        protected void processReadKey(SelectionKey selectionKey) {
            NioChannel nioChannel = nioChannelGroup.findChannel(selectionKey);
            if (nioChannel == null) {
                log.info("Channel already closed!");
            } else {
                Object obj = nioChannel.read();
                if (obj == null) {
                    return;
                }
                if (obj instanceof UserMessageProto.UserMessage) {
                    UserMessageProto.UserMessage msg = (UserMessageProto.UserMessage) obj;
                    if (msg.getMessageType() == UserMessageProto.MessageType.GET) {
                        doGet(nioChannel, msg);
                    } else if (msg.getMessageType() == UserMessageProto.MessageType.SET) {
                        doSet(nioChannel, msg);
                    } else if (msg.getMessageType() == UserMessageProto.MessageType.DEL) {
                        doDel(nioChannel, msg);
                    } else if (msg.getMessageType() == UserMessageProto.MessageType.STATUS) {
                        doStatus(nioChannel);
                    }

                }
            }
        }
    }
}

