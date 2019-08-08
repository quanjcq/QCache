package raft;

import common.Node;
import common.UtilAll;
import constant.RaftOptions;
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
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.message.RaftMessageProto;
import store.RaftLogMessageService;
import store.RaftStateMachineService;
import store.checkpoint.CheckPoint;
import store.message.RaftLogMessage;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class RaftServer implements Runnable {
    private static Logger log = LoggerFactory.getLogger(RaftServer.class);

    //该节点为candidate时候获取的选票信息
    private final Set<Short> votes = new TreeSet<Short>();

    //该节点为pre_candidate时候获取的选票信息
    private final Set<Short> preVotes = new TreeSet<Short>();

    //一个节点当选为leader最初,是不知道其他节点的lastAppliedIndex,认为他们的默认值.
    private final long DEFAULT_REMOTE_INDEX = Long.MIN_VALUE;

    //状态，初始的时候为follower
    private volatile RaftState state = RaftState.FOLLOWER;

    // 服务器任期号(term)（初始化为 0，持续递增）
    private long currentTerm = 0;
    // 在当前获得选票的候选人的Id，若这个值为负数，说明他没有把票投给其他节点
    private int votedFor = -1;
    //这里存放着,所有其他日志提交情况
    private Map<Short, LastRaftMessage> lastRaftMessageMap = new HashMap<Short, LastRaftMessage>();
    // 最后被应用到状态机的日志条目索引值.
    private long lastAppliedIndex = -1;

    //记录上次leader 访问的时间，以此判断集群内是否有leader
    private volatile long leaderConnectTime = 0;


    //负责维护连接信息
    private HashMap<Node, Channel> channels = new HashMap<Node, Channel>();

    //存储leader 所有未提交的日志（force ）
    private LinkedList<RaftUnCommitLog> unCommitLogs = new LinkedList<RaftUnCommitLog>();

    private Lock lock = new ReentrantLock();

    private ScheduledExecutorService scheduledExecutorService;
    private ScheduledFuture electionScheduledFuture;
    private ScheduledFuture heartbeatScheduledFuture;

    //Netty client eventLoopGroup
    private EventLoopGroup clientEventLoopGroupBoss = new NioEventLoopGroup(1);

    //Netty server eventLoopGroup for connect
    private EventLoopGroup boss = new NioEventLoopGroup(1);

    //Netty server eventLoopGroup for deal connect
    private EventLoopGroup worker = new NioEventLoopGroup(1);

    private AtomicBoolean isShutDown = new AtomicBoolean(true);

    //下面的变量都要通过参数传过来
    //集群信息
    private List<Node> nodes;

    //该节点信息
    private Node myNode;

    //一致性hash
    private ConsistentHash consistentHash = null;

    //负责RaftLog读写的
    private RaftLogMessageService raftLogMessageService;

    //负责RaftStateMachine读写
    private RaftStateMachineService raftStateMachineService;

    //负责checkPoint 读写的.
    private CheckPoint checkPoint;

    @Override
    public void run() {
        init();
    }

    /**
     * 运行RaftServer
     */
    public void start() {
        if (!isShutDown()) {
            return;
        }
        UtilAll.getThreadPool().execute(this);
        isShutDown.compareAndSet(true,false);
    }

    private void init() {
        if (!validate()) {
            log.warn("parameter not set");
            return;
        }

        //load StateMachine
        raftStateMachineService.loadRaftStateFile();

        //load RaftLog
        raftLogMessageService.loadMessageInStateMachine(consistentHash);

        //load term
        long tempTerm = checkPoint.getTerm();
        if (tempTerm > 0) {
            currentTerm = tempTerm;
        }

        //load lastAppliedIndex
        lastAppliedIndex = checkPoint.getLastAppliedIndex();


        //监听其他节点的消息
        listenNodes();

        //定时执行检测服务器中是否含有Leader,没有则尝试发起新的选举.
        scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                printStatus();
                if (!haveLeader()) {
                    resetElectionTimer();
                }
            }
        }, 0, RaftOptions.electionTimeoutMilliseconds, TimeUnit.MILLISECONDS);
    }

    /**
     * 监听其他节点的消息
     */
    private void listenNodes() {
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
            log.warn(ex.toString());
        }
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
     * 开始选举，选举前先预选举，防止自身网络异常的节点,发起选举,导致全局term 变大
     */
    private void startElection() {
        if (preVote()) {
            //log.info("server {} start vote", myNode.getNodeId());
            startVote();
        } else {
            //log.info("server {} pre vote error", myNode.getNodeId());
            resetElectionTimer();
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
            //log.info("server {} start preVote on term {}", myNode.getNodeId(), currentTerm);
            preVotes.clear();
        } finally {
            lock.unlock();
        }
        //单一节点的情况
        if (nodes.size() == 1) {
            return true;
        }
        for (final Node node : nodes) {
            if (!node.equals(myNode)) {
                UtilAll.getThreadPool().execute(new Runnable() {
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
                        } else {
                            log.info("server {} can not connect server {}", myNode.getNodeId(), node.getNodeId());
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
                        log.warn(e.toString());
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
            checkPoint.setTerm(currentTerm);

            //log.info("server {} start Vote on term {}", myNode.getNodeId(), currentTerm);
            votes.clear();
        } finally {
            lock.unlock();
        }
        //单一节点的情况
        if (nodes.size() == 1) {
            return;
        }
        for (final Node node : nodes) {
            if (!node.equals(myNode)) {
                UtilAll.getThreadPool().execute(new Runnable() {
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
                                    .setLastAppliedIndex(getMaxIndex())
                                    .setCurrentTerm(currentTerm)
                                    .setLeader(false)
                                    .build();
                        } finally {
                            lock.unlock();
                        }
                        if (raftMessage != null) {
                            Channel channel = getChannel(node);
                            if (channel != null && channel.isActive()) {
                                channel.writeAndFlush(raftMessage);
                            } else {
                                log.info("server {} can not connect server {}", myNode.getNodeId(), node.getNodeId());
                            }


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
                        log.info(e.toString());
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
        } finally {
            lock.unlock();
        }
        //维护需要发送给其他节点的消息，只有leader需要
        for (Node node : nodes) {
            lastRaftMessageMap.put(node.getNodeId(),
                    new LastRaftMessage(node.getNodeId(), DEFAULT_REMOTE_INDEX)
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

    /**
     * 当选leader,向所有节点发送心跳包
     * 优先发送同步数据消息[根据是发送日志,还是StatMachine 更快决定发那个]
     * 然后才是未提交
     * 最后是普通心跳包
     */
    private void startNewHeartbeat() {
        for (final Node node : nodes) {
            if (!node.equals(myNode)) {
                UtilAll.getThreadPool().execute(new HeartService(node));
            } else {
                //节点自身
                if (!consistentHash.hashNode(myNode)) {
                    generate(myNode, (byte) 0); //内部会加锁
                }
            }
        }
        resetHeartbeatTimer();
    }

    //生成heartMessage
    //in lock
    private RaftMessageProto.RaftMessage getHeartMessage(Node node) {
        log.info("server {} send HeartMessage to server {}", myNode.getNodeId(), node.getNodeId());
        return RaftMessageProto.RaftMessage
                .newBuilder()
                .setMessageType(RaftMessageProto.MessageType.HEART)
                .setId((int) myNode.getNodeId())
                .setCurrentTerm(currentTerm)
                .setLastAppliedIndex(lastAppliedIndex)
                .setLeader(true)
                .setHeartMessage(RaftMessageProto.HeartMessage.newBuilder().build())
                .build();
    }

    //生成commit_log message
    private RaftMessageProto.RaftMessage getCommitLogMessage(Node node) {
        log.info("server {} send commit log  to server {}", myNode.getNodeId(), node.getNodeId());
        long tempIndex = lastRaftMessageMap.get(node.getNodeId()).getLastAppliedIndex();
        ByteBuffer byteBuffer = raftLogMessageService.queryRaftLogMessage(tempIndex);
        byte[] logs = new byte[byteBuffer.limit() - byteBuffer.position()];
        byteBuffer.get(logs);
        String logStr = UtilAll.byte2String(logs,"ISO-8859-1");
        return RaftMessageProto.RaftMessage
                .newBuilder()
                .setMessageType(RaftMessageProto.MessageType.COMMIT)
                .setId((int) myNode.getNodeId())
                .setCurrentTerm(currentTerm)
                .setLastAppliedIndex(lastAppliedIndex)
                .setLeader(true)
                .setCommitMessage(
                        RaftMessageProto.CommitMessage.newBuilder()
                                .setCommitMessage(logStr)
                                .build()
                )
                .build();
    }

    //生成sync state message
    private RaftMessageProto.RaftMessage getStateMessage(Node node) {
        String data = consistentHash.getSerializedByteString();
        log.info("server {} send State Message to server {} data {}", myNode.getNodeId(), node.getNodeId(),consistentHash.getNodesStr());
        System.out.println("send state Message data = " + consistentHash.getNodesStr());
        return RaftMessageProto.RaftMessage
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
    }

    //生成UnCommit message
    private RaftMessageProto.RaftMessage getUnCommitMessage(Node node) {
        RaftUnCommitLog unCommitLog = unCommitLogs.getFirst();
        RaftLogMessage raftLogMessage = RaftLogMessage.getInstance(unCommitLog.getNode(),
                unCommitLog.getIndex(),
                unCommitLog.getType()
        );

        String data = UtilAll.byte2String(raftLogMessage.encode(),"ISO-8859-1");
        log.info("server {} send unCommit Message to server {} data {}", myNode.getNodeId(), node.getNodeId(), unCommitLog);

        return RaftMessageProto.RaftMessage
                .newBuilder()
                .setMessageType(RaftMessageProto.MessageType.UN_COMMIT)
                .setId((int) myNode.getNodeId())
                .setCurrentTerm(currentTerm)
                .setLastAppliedIndex(lastAppliedIndex)
                .setLeader(true)
                .setUnCommitMessage(
                        RaftMessageProto.UnCommitMessage.newBuilder()
                                .setUnCommitMessage(data)
                                .build()
                )
                .build();
    }

    //获取同步消息类型
    private SendMessageType getSendMessageType(Node node) {
        long tempIndex = lastRaftMessageMap.get(node.getNodeId()).getLastAppliedIndex();
        if (tempIndex == DEFAULT_REMOTE_INDEX) {
            return SendMessageType.HEART;
        }
        if (tempIndex < lastAppliedIndex) {
            //计算是该发送日志还是状态
            ByteBuffer buffer = raftLogMessageService.queryRaftLogMessage(tempIndex);
            if (buffer == null) {
                log.info("Raft log file is empty! sync by StateMachine");
                return SendMessageType.SYNC_STATE;
            }
            int logSize = (buffer.limit() - buffer.position()) / RaftLogMessage.RAFT_LOG_MESSAGE_SIZE;
            if (logSize < lastAppliedIndex - tempIndex) {
                log.info("Raft log file have no enough log! sync by StateMachine");
                return SendMessageType.SYNC_STATE;
            }
            int logLength = buffer.limit() - buffer.position();
            if (consistentHash.getSerializedSize() <= logLength) {
                log.info("sync by StateMachine");
                return SendMessageType.SYNC_STATE;
            } else {
                log.info("sync by log");
                return SendMessageType.SYNC_LOG;
            }

        }
        //发送未提交日志
        if (tempIndex == lastAppliedIndex && !unCommitLogs.isEmpty()) {
            log.info("send unCommit log");
            return SendMessageType.UN_COMMIT_LOG;
        }

        return SendMessageType.HEART;
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
     * 产生新提交日志信息。
     *
     * @param node 节点
     * @param tags 日志类型
     */
    private void generate(Node node, byte tags) {
        lock.lock();
        try {
            RaftUnCommitLog unCommitLog = new RaftUnCommitLog();
            long index = getMaxIndex() + 1;
            unCommitLog.setIndex(index);
            unCommitLog.setNode(node);
            unCommitLog.setType(tags);

            if (unCommitLogs.size() > 0) {
                boolean tag = true;

                //查看这条日志是否存在
                for (RaftUnCommitLog unLog : unCommitLogs) {
                    if (unLog.equals(unCommitLog)) {
                        tag = false;
                        break;
                    }
                }
                if (tag) {
                    unCommitLogs.add(unCommitLog);
                }
                log.info("server {} generate unCommit log {}", myNode.getNodeId(), unCommitLogs);
            } else {
                unCommitLogs.add(unCommitLog);
            }
        } finally {
            lock.unlock();
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
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(clientEventLoopGroupBoss)
                .channel(NioSocketChannel.class)
                .handler(new SocketClientInitializer());
        ChannelFuture channelFuture = bootstrap.connect(node.getIp(), node.getListenHeartbeatPort());
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        channelFuture.addListener(new GenericFutureListener<Future<? super Void>>() {
            @Override
            public void operationComplete(Future<? super Void> future){
                if (future.isSuccess()) {
                    countDownLatch.countDown();
                }
            }
        });
        try {
            countDownLatch.await(RaftOptions.maxWaitTime,TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.info("server {} connect server {} time out!" + e);
        }
        if (channelFuture.isSuccess()) {
            log.info("server {} create channel with server {} ", myNode.getNodeId(), node.getNodeId());
            channel = channelFuture.channel();
            channels.put(node, channel);
        } else {
            log.info("server.SingleServer {} create channel with server.SingleServer {} Fail!",myNode.getNodeId(),node.getNodeId());
            channel = null;
        }
        return channel;
    }

    /**
     * 处理预投票信息.
     *
     * @param ctx ChannelHandlerContext
     * @param msg 收到的消息
     */
    private void doPreVoteMessage(ChannelHandlerContext ctx, RaftMessageProto.RaftMessage msg){
        RaftMessageProto.PreVoteMessage preVoteMessage;
        RaftMessageProto.RaftMessage message;
        lock.lock();
        try {
            if (canPreVoteFor(msg)) {
                preVoteMessage = RaftMessageProto.PreVoteMessage.newBuilder()
                        .setVoteFor(true)
                        .build();
            } else {
                preVoteMessage = RaftMessageProto.PreVoteMessage.newBuilder()
                        .setVoteFor(false)
                        .build();
                //log.info("server {} do not pre vote for server{}", myNode.getNodeId(), msg.getId());
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
            log.info("connect lost!");
        }
        ctx.writeAndFlush(message);
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
                    checkPoint.setTerm(currentTerm);
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
                    checkPoint.setTerm(currentTerm);
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
                    checkPoint.setTerm(currentTerm);
                    state = RaftState.FOLLOWER;
                    votedFor = msg.getId();
                    if (electionScheduledFuture != null && !electionScheduledFuture.isDone()) {
                        electionScheduledFuture.cancel(true);
                    }
                } else {
                    if (canVoteFor(msg)) { //接收并降级为follower
                        votedFor = msg.getId();
                        currentTerm = term;
                        checkPoint.setTerm(currentTerm);
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
                    checkPoint.setTerm(currentTerm);
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
                    checkPoint.setTerm(currentTerm);
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
                    checkPoint.setTerm(currentTerm);
                    votedFor = -1;
                    leaderConnectTime = new Date().getTime();
                } else if (term == currentTerm) {
                    leaderConnectTime = new Date().getTime();
                }
            } else if (state == RaftState.PRE_CANDIDATE || state == RaftState.CANDIDATE) {
                if (term >= currentTerm) { //降级为follower
                    state = RaftState.FOLLOWER;
                    currentTerm = term;
                    checkPoint.setTerm(currentTerm);
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
     * 未提交日志等待其他节点确认.
     *
     * @param ctx ChannelHandlerContext
     * @param msg 消息
     */
    private void doUnCommitMessage(ChannelHandlerContext ctx, RaftMessageProto.RaftMessage msg) {

        long term = msg.getCurrentTerm();
        String data = msg.getUnCommitMessage().getUnCommitMessage();
        byte[] byteData = UtilAll.string2Byte(data,"ISO-8859-1");
        if (byteData == null) {
            log.info("unCommit log data destroy!");
            return;

        }
        RaftLogMessage raftLogMessage = RaftLogMessage.getInstance(ByteBuffer.wrap(byteData));
        if (raftLogMessage == null) {
            log.info("unCommit log data destroy!");
            return;

        }
        RaftUnCommitLog unCommitLog = new RaftUnCommitLog();
        unCommitLog.setIndex(raftLogMessage.getLastAppliedIndex());
        unCommitLog.setType(raftLogMessage.getRaftMessageType());
        unCommitLog.setNode(raftLogMessage.getNode());

        log.info("server {} get UnCommitMessage form server {},data {} ",myNode.getNodeId(),msg.getId(),unCommitLog);

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
                    checkPoint.setTerm(currentTerm);
                    votedFor = -1;
                    ackMessage = RaftMessageProto.AckMessage.newBuilder()
                            .setAckIndex(raftLogMessage.getLastAppliedIndex())
                            .build();
                    leaderConnectTime = new Date().getTime();
                    state = RaftState.FOLLOWER;
                    // stop heartbeat
                    if (heartbeatScheduledFuture != null && !heartbeatScheduledFuture.isDone()) {
                        heartbeatScheduledFuture.cancel(true);
                    }
                    //是否把该uncommitLog保存本地
                    if (!unCommitLogs.contains(unCommitLog)) {
                        unCommitLogs.add(unCommitLog);
                    }
                }
            } else if (state == RaftState.FOLLOWER) {
                if (currentTerm > term) { //不接受
                    ackMessage = RaftMessageProto.AckMessage.newBuilder()
                            .setAckIndex(-1)
                            .build();
                } else if (currentTerm == term) { //接收
                    ackMessage = RaftMessageProto.AckMessage.newBuilder()
                            .setAckIndex(raftLogMessage.getLastAppliedIndex())
                            .build();
                    leaderConnectTime = new Date().getTime();
                    //是否把该uncommitLog保存本地
                    if (!unCommitLogs.contains(unCommitLog)) {
                        unCommitLogs.add(unCommitLog);
                    }
                } else {  //接收
                    currentTerm = term;
                    checkPoint.setTerm(currentTerm);
                    votedFor = -1;
                    ackMessage = RaftMessageProto.AckMessage.newBuilder()
                            .setAckIndex(raftLogMessage.getLastAppliedIndex())
                            .build();
                    leaderConnectTime = new Date().getTime();
                    //是否把该uncommitLog保存本地
                    if (!unCommitLogs.contains(unCommitLog)) {
                        unCommitLogs.add(unCommitLog);
                    }
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
                    checkPoint.setTerm(currentTerm);
                    state = RaftState.FOLLOWER;
                    leaderConnectTime = new Date().getTime();
                    ackMessage = RaftMessageProto.AckMessage.newBuilder()
                            .setAckIndex(raftLogMessage.getLastAppliedIndex())
                            .build();
                    // stop election
                    if (electionScheduledFuture != null && !electionScheduledFuture.isDone()) {
                        electionScheduledFuture.cancel(true);
                    }
                    //是否把该uncommitLog保存本地
                    if (!unCommitLogs.contains(unCommitLog)) {
                        unCommitLogs.add(unCommitLog);
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
                    checkPoint.setTerm(currentTerm);
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
                    checkPoint.setTerm(currentTerm);
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
                    checkPoint.setTerm(currentTerm);
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
            ////lastAppliedIndex < msg.getLastAppliedIndex(),防止之前同步过这个消息
            if (tags && lastAppliedIndex < msg.getLastAppliedIndex()) {
                Iterator<RaftUnCommitLog> itr = unCommitLogs.iterator();
                while (itr.hasNext()) {
                    if (itr.next().getIndex() <= msg.getLastAppliedIndex()) {
                        itr.remove();
                    }
                }
                leaderConnectTime = new Date().getTime();

                String data = msg.getCommitMessage().getCommitMessage();
                byte[] byteData = UtilAll.string2Byte(data,"ISO-8859-1");
                //解析数据
                if (byteData == null || byteData.length == 0) {
                    log.info("commit log data destroy!");
                    return;
                }
                ByteBuffer byteBuffer = ByteBuffer.wrap(byteData);
                byteBuffer.limit(byteBuffer.capacity());
                while (byteBuffer.limit() - byteBuffer.position() >= RaftLogMessage.RAFT_LOG_MESSAGE_SIZE) {
                    RaftLogMessage message = RaftLogMessage.getInstance(byteBuffer);
                    log.info("server {} get commitLog from server{},data {}",myNode.getNodeId(),msg.getId(),message);
                    if (message == null) {
                        log.info("RaftLogMessage was destroy!");
                        continue;
                    }
                    lastAppliedIndex = message.getLastAppliedIndex();
                    raftLogMessageService.appendMessage(message, false);
                    if (message.getRaftMessageType() == (byte) 0) {
                        consistentHash.add(message.getNode());
                    } else {
                        consistentHash.remove(message.getNode());
                    }
                }
                raftLogMessageService.flush();
                checkPoint.setLastAppliedIndex(lastAppliedIndex);
                raftMessage = RaftMessageProto.RaftMessage.newBuilder()
                        .setMessageType(RaftMessageProto.MessageType.COMMIT)
                        .setId((int) myNode.getNodeId())
                        .setCurrentTerm(currentTerm)
                        .setLastAppliedIndex(lastAppliedIndex)
                        .setLeader(state == RaftState.LEADER)
                        .setCommitMessage(RaftMessageProto.CommitMessage.newBuilder().build())
                        .build();
                ctx.channel().writeAndFlush(raftMessage);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 直接同步集群状态.
     *
     * @param ctx ChannelHandlerContext
     * @param msg RaftMessageProto.RaftMessage
     */
    private void doStateMessage(ChannelHandlerContext ctx, RaftMessageProto.RaftMessage msg) {
        //leader->nodes
        long term = msg.getCurrentTerm();
        boolean tag = false;
        RaftMessageProto.RaftMessage raftMessage;
        lock.lock();
        try {
            if (state == RaftState.LEADER) {
                if (currentTerm < term) {        //接收并降级为follower
                    currentTerm = term;
                    checkPoint.setTerm(currentTerm);
                    votedFor = -1;
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
                    checkPoint.setTerm(currentTerm);
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
                    checkPoint.setTerm(currentTerm);
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

                String data = msg.getNodeMessage().getNodeMessage();
                byte[] byteData = UtilAll.string2Byte(data,"ISO-8859-1");
                if (byteData == null) {
                    log.info("sync state data error!");
                    return;
                }

                ConsistentHash newConsistentHash = new ConsistentHash(1);
                boolean flag = newConsistentHash.add(ByteBuffer.wrap(byteData));
                if (flag) {
                    //清空日志文件
                    consistentHash.clear();
                    consistentHash.addAll(newConsistentHash);
                    raftLogMessageService.clear();
                    System.out.println("state store" + consistentHash.getNodesStr());

                    raftStateMachineService.raftStateMachineStore();

                    lastAppliedIndex = msg.getLastAppliedIndex();
                    checkPoint.setLastAppliedIndex(lastAppliedIndex);
                    Iterator<RaftUnCommitLog> itr = unCommitLogs.iterator();
                    while (itr.hasNext()) {
                        if (itr.next().getIndex() <= msg.getLastAppliedIndex()) {
                            itr.remove();
                        }
                    }
                }else {
                    log.info("server {} sync StateMachine error!",myNode.getNodeId());
                }
            }
            raftMessage = RaftMessageProto.RaftMessage.newBuilder()
                    .setMessageType(RaftMessageProto.MessageType.NODE)
                    .setId((int) myNode.getNodeId())
                    .setLastAppliedIndex(lastAppliedIndex)
                    .setLeader(state == RaftState.LEADER)
                    .setCurrentTerm(currentTerm)
                    .setNodeMessage(RaftMessageProto.NodeMessage.newBuilder().build())
                    .build();
            ctx.writeAndFlush(raftMessage);
        } finally {
            lock.unlock();
        }

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
        log.info("server {} currentTerm = {} state =  {},lastAppliedIndex = {},circle = {},unCommitLogs= {},nodes = {}",
                myNode.getNodeId(), currentTerm, state, lastAppliedIndex, consistentHash.getCircle(),unCommitLogs,consistentHash.getNodesStr());
    }

    /**
     * 为了避免在同时发起选举，而拿不到半数以上的投票，
     * 在150-300ms 这个随机时间内，发起新一轮选举
     * (Raft 论文上说150-300ms这个随机时间，不过好像把时差弄大点，效果更好)
     *
     * @return 一个随机的时间(ms)
     */
    private int getRandomElection() {
        int randTime = new Random().nextInt(500);
        log.info("server {} start election after {} ms", myNode.getNodeId(), randTime);
        return randTime;
    }

    /**
     * @return bool参数是否设置
     */
    private boolean validate() {
        return  (nodes != null
                && myNode != null
                && consistentHash != null
                && raftLogMessageService != null
                && raftStateMachineService != null
                && checkPoint != null);
    }

    private boolean isShutDown() {
        return isShutDown.get();
    }

    /**
     * 关闭RaftServer
     */
    public void shutdown() {
        if (isShutDown()) {
            return;
        }
        isShutDown.getAndSet(false);
        //关闭其他资源
        boss.shutdownGracefully();
        worker.shutdownGracefully();
        clientEventLoopGroupBoss.shutdownGracefully();

        raftLogMessageService.flush();
        scheduledExecutorService.shutdown();

        //关闭所有连接
        for (Channel channel:channels.values()) {
            if (channel.isActive()) {
                channel.close();
            }
        }
        consistentHash = null;

    }

    public RaftState getState(){
        return state;
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
            return message.getLastAppliedIndex() >= getMaxIndex() && !haveLeader();
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

    //in lock
    private long getMaxIndex() {
        return unCommitLogs.size() > 0 ? unCommitLogs.getLast().getIndex() : lastAppliedIndex;
    }

    /**
     * 客户端发送Vote后对收到的消息处理.
     *
     * @param msg 收到的消息
     */
    private void doClientVoteMessage(RaftMessageProto.RaftMessage msg) {
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
    }

    /**
     * 客户端发送PreVote后对收到的消息处理.
     *
     * @param msg 收到的消息
     */
    private void doClientPreVoteMessage(RaftMessageProto.RaftMessage msg) {
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
        log.info("server {} get ack message {}",myNode.getNodeId(),message.getAckMessage().getAckIndex());
        lock.lock();
        try {
            stepDown(message);
            if (message.getAckMessage().getAckIndex() >= 0
                    && unCommitLogs.size() > 0) {
                RaftUnCommitLog unCommit = unCommitLogs.getFirst();
                if (unCommit.getIndex() == message.getAckMessage().getAckIndex()) {
                    unCommit.getAck().add((short) message.getId());
                    if (unCommit.getAck().size() + 1 > nodes.size() / 2) {
                        //收到半数以上确认了,在本地提交
                        unCommitLogs.removeFirst();
                        lastAppliedIndex = unCommit.getIndex();
                        checkPoint.setLastAppliedIndex(lastAppliedIndex);
                        RaftLogMessage raftLogMessage = RaftLogMessage.getInstance(unCommit.getNode(),
                                unCommit.getIndex(),
                                unCommit.getType());
                        //日志追加到本地
                        raftLogMessageService.appendMessage(raftLogMessage, true);
                        //日志应用于stateMachine
                        if (unCommit.getType() == (byte) 0) {
                            consistentHash.add(unCommit.getNode());
                        } else {
                            consistentHash.remove(unCommit.getNode());
                        }

                    }
                }


            }
        } finally {
            lock.unlock();
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
        //lastRaftMessageMap.get((short)message.getId()).setLastAppliedIndex(message.getLastAppliedIndex());
        lastRaftMessageMap.put((short) message.getId(), new LastRaftMessage((short) message.getId(),
                message.getLastAppliedIndex()));
        if (currentTerm > newTerm) {
            return;
        } else if (currentTerm < newTerm) {
            currentTerm = newTerm;
            checkPoint.setTerm(currentTerm);
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

    public List<Node> getNodes() {
        return nodes;
    }

    public void setNodes(List<Node> nodes) {
        this.nodes = nodes;
    }

    public Node getMyNode() {
        return myNode;
    }

    public void setMyNode(Node myNode) {
        this.myNode = myNode;
    }

    public ConsistentHash getConsistentHash() {
        return consistentHash;
    }

    public void setConsistentHash(ConsistentHash consistentHash) {
        this.consistentHash = consistentHash;
    }

    public RaftLogMessageService getRaftLogMessageService() {
        return raftLogMessageService;
    }

    public void setRaftLogMessageService(RaftLogMessageService raftLogMessageService) {
        this.raftLogMessageService = raftLogMessageService;
    }

    public RaftStateMachineService getRaftStateMachineService() {
        return raftStateMachineService;
    }

    public void setRaftStateMachineService(RaftStateMachineService raftStateMachineService) {
        this.raftStateMachineService = raftStateMachineService;
    }

    public CheckPoint getCheckPoint() {
        return checkPoint;
    }

    public void setCheckPoint(CheckPoint checkPoint) {
        this.checkPoint = checkPoint;
    }

    private class HeartService implements Runnable {
        private Node node;

        private HeartService(Node node) {
            this.node = node;
        }

        @Override
        public void run() {
            RaftMessageProto.RaftMessage raftMessage = null;
            lock.lock();
            try {
                if (state != RaftState.LEADER) {
                    return;
                }
                SendMessageType sendMessageType = getSendMessageType(node);
                switch (sendMessageType) {
                    case HEART:
                        raftMessage = getHeartMessage(node);
                        break;
                    case SYNC_LOG:
                        raftMessage = getCommitLogMessage(node);
                        break;
                    case SYNC_STATE:
                        raftMessage = getStateMessage(node);
                        break;
                    case UN_COMMIT_LOG:
                        raftMessage = getUnCommitMessage(node);
                        break;


                }
            } finally {
                lock.unlock();
            }
            Channel channel = getChannel(node);
            if (channel == null || !channel.isActive()) {
                if (consistentHash.hashNode(node)) {
                    generate(node, (byte) 1);
                }
            } else {
                if (!consistentHash.hashNode(node)) {
                    generate(node, (byte) 0);
                }
            }
            if (raftMessage != null && channel != null && channel.isActive()) {
                channel.writeAndFlush(raftMessage);
            }
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
            switch (msg.getMessageType()) {
                case PRE_VOTE:
                    doPreVoteMessage(ctx, msg);
                    break;
                case VOTE:
                    doVoteMessage(ctx, msg);
                    break;
                case HEART:
                    doHeartMessage(ctx, msg);
                    break;
                case UN_COMMIT:
                    doUnCommitMessage(ctx, msg);
                    break;
                case COMMIT:
                    doCommitMessage(ctx, msg);
                    break;
                case NODE:
                    doStateMessage(ctx, msg);
                    break;


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
            pipeline.addLast("SocketClientHandler", new SocketClientHandler());
        }
    }

    //Handler
    private class SocketClientHandler extends SimpleChannelInboundHandler<RaftMessageProto.RaftMessage> {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RaftMessageProto.RaftMessage msg) {
            switch (msg.getMessageType()) {
                case PRE_VOTE:
                    doClientPreVoteMessage(msg);
                    break;
                case VOTE:
                    doClientVoteMessage(msg);
                    break;
                case HEART:
                    doClientHeartMessage(msg);
                    break;
                case ACK:
                    doClientAckMessage(msg);
                    break;
                case COMMIT:
                    doClientCommitMessage(msg);
                    break;
                case NODE:
                    doClientNodeMessage(msg);
                    break;
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            log.debug(cause.toString());
            ctx.close();
        }
    }

    public long getCurrentTerm() {
        return currentTerm;
    }

}
