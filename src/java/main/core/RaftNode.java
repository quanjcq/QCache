package core;
import common.ConsistentHash;
import common.Node;
import common.RaftSnaphot;
import common.Tools;
import configuration.QCacheConfiguration;
import constant.RaftOptions;
import constant.RaftState;
import core.cache.*;
import core.cache.backup.BackUpAof;
import core.message.*;
import log.LogParse;
import log.RaftLog;
import log.RaftLogUncommit;
import org.omg.CORBA.CODESET_INCOMPATIBLE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by quan on 2019/4/24.
 * 该类是负责的功能有：
 *  选举
 *  监听其他node(pre_candidate,candidate,leader)消息
 *  处理客户端请求
 *
 *
 */
public class RaftNode{
    private static final Logger log = LoggerFactory.getLogger(RaftNode.class);
    //状态，初始的时候为follower
    private volatile RaftState state = RaftState.FOLLOWER;

    // 服务器任期号(term)（初始化为 0，持续递增）
    private volatile long currentTerm = 0;

    // 在当前获得选票的候选人的Id，若这个值为负数，说明他没有把票投给其他节点
    private volatile int votedFor = -1;

    //该节点为candidate 时候获取的选票信息
    private volatile  Set<Integer> votes = new TreeSet<Integer>();

    //这里存放着所有，最新提交的日志信息
    private Map<Integer, LastRaftMessage>  lastRaftMessageMap = new HashMap<Integer, LastRaftMessage>();

    // 最后被应用到状态机的日志条目索引值（初始化为 -1）
    private volatile long lastAppliedIndex = -1 ;

    // 最后被应用到状态机的日志条目term（初始化为 -1持续递增）
    private volatile long lastAppliedTerm = -1 ;

    //一致性hash
    private ConsistentHash consistentHash=null;

    //snaphot 文件路径
    private String raftSnaphotPath = QCacheConfiguration.getRaftSnaphotPath();

    //记录上次leader 访问的时间，以此判断集群内是否有leader
    private volatile long leaderConnectTime = 0;

    //该节点信息
    private Node myNode;

    //负责维护连接信息
    private HashMap<Node,Socket> sockets = new HashMap<Node, Socket>();

    //存储leader 所有未提交的日志（force ）
    private List<RaftLogUncommit> unCommitLogs = new LinkedList<RaftLogUncommit>();

    //集群信息
    private List<Node> nodes;
    private Lock lock = new ReentrantLock();
    private ConcurrentHashMap<String, CacheDataI>  cache = new ConcurrentHashMap<String,CacheDataI>();
    private ExecutorService executorService;
    private ScheduledExecutorService scheduledExecutorService;
    private ScheduledFuture electionScheduledFuture;
    private ScheduledFuture heartbeatScheduledFuture;
    public void init(){


        //load nodes
        log.info("load nodes");
        nodes = QCacheConfiguration.getNodeList();
        myNode = QCacheConfiguration.getMyNode();
        consistentHash = new ConsistentHash(20);

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
        scheduledExecutorService = Executors.newScheduledThreadPool(3);
        scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
            public void run() {

                if(haveLeader()){
                    //log.info("server {} have leader on term {}",myNode.getNodeId(),currentTerm);
                    printStatus();
                }else {
                    //log.info("server {} do not have leader on term",myNode.getNodeId(),currentTerm);
                    resetElectionTimer();
                }
            }
        },0,RaftOptions.electionTimeoutMilliseconds ,TimeUnit.MILLISECONDS);

        //init listenClientThread
        initListenClientThread();
        log.info("listen client");
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
        }, getRandomElection() + RaftOptions.heartbeatPeriodMilliseconds, TimeUnit.MILLISECONDS);
    }

    /**
     * 开始选举，选举前先预选举，防止网络的异常的节点，导致全局term 变大
     */
    private void startElection(){
        if(preVote()){
            log.info("server {} start vote",myNode.getNodeId());
            startVote();
        }else{
            log.info("server {} pre vote error",myNode.getNodeId());
            resetElectionTimer();
        }
    }

    /**
     * nodes->leader,nodes->pre_candidate,nodes->candidate
     * leader,pre_candidate,candidate ,向其他节点发送消息的时候，收到回复消息
     * 根据这个回复消息节点状态自动降级为follower
     * @param message
     */
    //in lock
    private void stepDown(RaftMessage message) {
        long newTerm = message.getCurrentTerm();
        if(currentTerm > newTerm)
            return;
        else if(currentTerm < newTerm){
            currentTerm = newTerm;
            votedFor = -1;
            votes.clear();
            state = RaftState.FOLLOWER;
            // stop heartbeat
            if (heartbeatScheduledFuture != null && !heartbeatScheduledFuture.isDone()) {
                heartbeatScheduledFuture.cancel(true);
            }

            // stop election
            if (electionScheduledFuture != null && !electionScheduledFuture.isDone()) {
                electionScheduledFuture.cancel(true);
            }
            if(message.isLeader()){
                leaderConnectTime = new Date().getTime();
            }
        }else{
            //term == currentTerm
        }
        if(message.isLeader()){
            leaderConnectTime = new Date().getTime();
            state = RaftState.FOLLOWER;
            votes.clear();
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
     * 预先投票防止该节点自身网络异常导致全局term变大
     * @return
     */
    private boolean preVote(){
        lock.lock();
        try {
            state = RaftState.PRE_CANDIDATE;
            log.info("server {} start preVote on term {}",myNode.getNodeId(),currentTerm);
            votes.clear();
            votes.add(myNode.getNodeId());
        }finally {
            lock.unlock();
        }
        final CountDownLatch countDownLatch = new CountDownLatch(nodes.size()-1);
        for (final Node node:nodes){
            if(!node.equals(myNode)) {
               executorService.execute(new Runnable() {
                    public void run() {
                        lock.lock();
                        try{
                            if (state != RaftState.PRE_CANDIDATE) {
                                countDownLatch.countDown();
                                return;
                            }
                        }finally {
                            lock.unlock();
                        }
                        ObjectOutputStream objectOutputStream = null;
                        ObjectInputStream objectInputStream = null;
                        try {
                            Socket socket = getSocket(node);
                            objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                            RaftPreVoteMessage message = new RaftPreVoteMessage();
                            lock.lock();
                            try {
                                message.setCurrentTerm(currentTerm);
                                message.setId(myNode.getNodeId());
                                message.setLastAppendedIndex(lastAppliedIndex);
                                message.setLastAppendedTerm(lastAppliedTerm);
                                message.setLeader(false);
                            }finally {
                                lock.unlock();
                            }
                            log.info("server {} send pre vote to server {} message {}",
                                    myNode.getNodeId(),
                                    node.getNodeId(),
                                    message);
                            objectOutputStream.writeObject(message);
                            objectInputStream = new ObjectInputStream(socket.getInputStream());
                            RaftPreVoteMessage messageIn = (RaftPreVoteMessage) objectInputStream.readObject();
                            log.info("server {} get pre voter message {}",myNode.getNodeId(),messageIn);
                            lock.lock();
                            try {
                                stepDown(messageIn);
                                if (messageIn.isVoteFor()) {
                                    votes.add(messageIn.getId());
                                    log.info("server {} get pre vote from {} total votes = {} on term {}",
                                            myNode.getNodeId(), messageIn.getId(), votes.size(),currentTerm);
                                } else {
                                    log.info("server {} do not get pre vote from {}",
                                            myNode.getNodeId(), messageIn.getId());
                                }
                            }finally {
                                lock.unlock();
                            }
                        } catch (IOException ex) {
                            ex.printStackTrace();
                            log.info(ex.toString());
                        } catch (ClassNotFoundException ex) {
                            ex.printStackTrace();
                            log.info(ex.toString());
                        } finally {
                            try {
                                objectOutputStream.close();
                                objectInputStream.close();
                            }catch (IOException ex){
                                log.debug(ex.toString());
                            }
                            countDownLatch.countDown();
                        }
                    }
               });
            }
        }
        try {
            countDownLatch.await((nodes.size()-1)*RaftOptions.heartbeatPeriodMilliseconds,TimeUnit.MILLISECONDS);
        }catch (InterruptedException ex){
            ex.printStackTrace();
        }
        boolean res = false;
        lock.lock();
        try {
            res = votes.size() > nodes.size()/2;
        }finally {
            lock.unlock();
        }
        return  res ;
    }

    /**
     * 开始投票
     */
    //in lock
    private void startVote(){
        lock.lock();
        try {
            state = RaftState.CANDIDATE;
            currentTerm++;
            votedFor = myNode.getNodeId();
            log.info("server {} start Vote",myNode.getNodeId());
            votes.clear();
            votes.add(myNode.getNodeId());
        }finally {
            lock.unlock();
        }
        final CountDownLatch countDownLatch = new CountDownLatch(nodes.size()-1);
        for (final Node node:nodes){
            if(!node.equals(myNode)) {
                executorService.execute(new Runnable() {
                    public void run() {
                        ObjectOutputStream objectOutputStream = null;
                        ObjectInputStream objectInputStream = null;
                        try {
                            lock.lock();
                            try {
                                if(state != RaftState.CANDIDATE){
                                    countDownLatch.countDown();
                                    return;
                                }
                            }finally {
                                lock.unlock();
                            }
                            log.info("server {} start vote to {} on port{}",
                                    myNode,
                                    node.getNodeId(),
                                    node.getListenHeartbeatPort());
                            Socket socket = getSocket(node);
                            objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                            RaftVoteMessage message = new RaftVoteMessage();
                            lock.lock();
                            try {
                                message.setCurrentTerm(currentTerm);
                                message.setId(myNode.getNodeId());
                                message.setLastAppendedIndex(lastAppliedIndex);
                                message.setLastAppendedTerm(lastAppliedTerm);
                                message.setLeader(false);
                            }finally {
                                lock.unlock();
                            }
                            objectOutputStream.writeObject(message);
                            objectInputStream = new ObjectInputStream(socket.getInputStream());
                            RaftVoteMessage messageIn = (RaftVoteMessage)objectInputStream.readObject();
                            stepDown(messageIn);
                            lock.lock();
                            try {
                                if(messageIn.isVoteFor()) {
                                    votes.add(messageIn.getId());
                                    log.info("server {} get vote from {} total votes = {} on term {}",
                                            myNode.getNodeId(),messageIn.getId(),votes.size(),currentTerm);
                                }else{
                                    log.info("server {} do not get vote from {} total votes={} on term {}",
                                            myNode.getNodeId(),messageIn.getId(),votes.size(),currentTerm);
                                }
                            }finally {
                                lock.unlock();
                            }
                        }catch (IOException ex){
                            log.debug(ex.toString());
                        }catch (ClassNotFoundException ex){
                            log.debug(ex.toString());
                        }finally {
                            try {
                                objectOutputStream.close();
                                objectInputStream.close();
                            }catch (IOException ex){
                                log.debug(ex.toString());
                            }
                            countDownLatch.countDown();
                        }

                    }
                });
            }
        }
        try {
            countDownLatch.await((nodes.size()-1)*RaftOptions.heartbeatPeriodMilliseconds,TimeUnit.MILLISECONDS);
        }catch (InterruptedException ex){
            ex.printStackTrace();
        }

        boolean tag = false;
        lock.lock();
        try{
            tag = (votes.size() > nodes.size() /2);
        }finally {
            lock.unlock();
        }
        if(tag){
            //become leader
            becomeLeader();

        }else{
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
            votes.clear();
        }finally {
            lock.unlock();
        }
        //维护需要发送给其他节点的消息，只有leader需要
        for (Node node:nodes){
            lastRaftMessageMap.put(node.getNodeId(),
                    new LastRaftMessage(node.getNodeId(),-1,-1)
            );
        }
        // stop vote timer
        if (electionScheduledFuture != null && !electionScheduledFuture.isDone()) {
            electionScheduledFuture.cancel(true);
        }
        // start heartbeat timer
        log.info("server {} become leader on term {}",myNode.getNodeId(),currentTerm);
        startNewHeartbeat();
    }


    private void resetHeartbeatTimer() {
        log.info("server {} resetHeartbeatTimer",myNode.getNodeId());
        lock.lock();
        try {
            if(state != RaftState.LEADER){
                if (heartbeatScheduledFuture != null && !heartbeatScheduledFuture.isDone()) {
                    heartbeatScheduledFuture.cancel(true);
                }
                return;
            }
        }finally {
            lock.unlock();
        }
        if (heartbeatScheduledFuture != null && !heartbeatScheduledFuture.isDone()) {
            heartbeatScheduledFuture.cancel(true);
        }
        heartbeatScheduledFuture = scheduledExecutorService.schedule(new Runnable() {
            public void run() {
                startNewHeartbeat();
            }
        },RaftOptions.heartbeatPeriodMilliseconds, TimeUnit.MILLISECONDS);
    }


    /**
     * 当选leader,向所有节点发送心跳包
     *
     */
    private void startNewHeartbeat(){
        final CountDownLatch countDownLatch = new CountDownLatch(nodes.size()-1);
        for (final Node node:nodes){
            if(!node.equals(myNode)){
                executorService.execute(new Runnable() {
                    public void run() {
                        ObjectOutputStream objectOutputStream = null;
                        try {
                            lock.lock();
                            try {
                                if (state != RaftState.LEADER) {
                                    countDownLatch.countDown();
                                    return;
                                }
                            }finally {
                                lock.unlock();
                            }
                            log.info("server {} state={} send heart to server {}",myNode.getNodeId(),state,node.getNodeId());
                            Socket socket = getSocket(node);
                            objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                            TreeMap<Long,Node> circle = getRaftSnaphot();
                            long term = lastRaftMessageMap.get(node.getNodeId()).getCommitTerm();
                            long index = lastRaftMessageMap.get(node.getNodeId()).getCommitIndex();
                            List<RaftLog> logs = LogParse.getNewRaftLogs(term,index,lastAppliedTerm,lastAppliedIndex);
                            boolean response = false;
                            if(term == -1 && circle != null){
                                //1.对于刚启动的节点，直接发送snaphot
                                log.info("server {} send snap {}",circle);
                                RaftSnapHotMessage snapHotMessage = new RaftSnapHotMessage();
                                snapHotMessage.setCircle(circle);
                                snapHotMessage.setId(myNode.getNodeId());
                                lock.lock();
                                try {
                                    snapHotMessage.setCurrentTerm(currentTerm);
                                    snapHotMessage.setLastAppendedIndex(lastAppliedIndex);
                                    snapHotMessage.setLastAppendedTerm(lastAppliedTerm);
                                    snapHotMessage.setLeader(true);
                                }finally {
                                    lock.unlock();
                                }
                                objectOutputStream.writeObject(snapHotMessage);
                                response = true;
                            }else if(logs.size() >0) {
                                //同步最新的日志
                                RaftCommitMessage raftCommitMessage = new RaftCommitMessage();
                                raftCommitMessage.setId(myNode.getNodeId());
                                lock.lock();
                                try {
                                    raftCommitMessage.setCurrentTerm(currentTerm);
                                    raftCommitMessage.setLastAppendedIndex(lastAppliedIndex);
                                    raftCommitMessage.setLastAppendedTerm(lastAppliedTerm);
                                } finally {
                                    lock.unlock();
                                }
                                raftCommitMessage.setRaftLogs(logs);
                                raftCommitMessage.setLeader(true);
                                objectOutputStream.writeObject(raftCommitMessage);
                                log.info("server {} send commitLog log{}",raftCommitMessage);
                                response = true;
                            }else if(unCommitLogs.size() > 0){
                                //处理未提交日志
                                //将未提交日志，收到半数以上确认的日志提交，不够半数，继续发送
                                boolean tagLog = false;
                                boolean tagUnLog = false;
                                RaftLogUncommit uncommitLog = null;
                                RaftUnCommitMessage raftUnCommitMessage = null;
                                lock.lock();
                                try {
                                    if(unCommitLogs.size() > 0){
                                        uncommitLog = unCommitLogs.get(0);
                                        Set<Integer> ack = uncommitLog.getAck();
                                        if(ack.size() + 1 > nodes.size() /2){
                                            unCommitLogs.remove(0);
                                            //写入到提交日志，同时将该日志应用于state machine
                                            lastAppliedTerm = uncommitLog.getTerm();
                                            lastAppliedIndex = uncommitLog.getIndex();
                                            tagLog = true;
                                        }else{
                                            if(!ack.contains(node.getNodeId())){
                                                raftUnCommitMessage = new RaftUnCommitMessage();
                                                raftUnCommitMessage.setId(myNode.getNodeId());
                                                raftUnCommitMessage.setRaftLog(uncommitLog);
                                                raftUnCommitMessage.setLeader(true);
                                                raftUnCommitMessage.setLastAppendedIndex(lastAppliedIndex);
                                                raftUnCommitMessage.setLastAppendedTerm(lastAppliedTerm);
                                                raftUnCommitMessage.setCurrentTerm(currentTerm);
                                                tagUnLog = true;
                                                response = true;
                                                log.info("server {} send ack to server {} message {}",
                                                        myNode.getNodeId(),node.getNodeId(),raftUnCommitMessage);
                                            }
                                        }
                                    }
                                }finally {
                                    lock.unlock();
                                }
                                if(tagLog){
                                    LogParse.insertLog(uncommitLog);
                                    appendEntry(uncommitLog.getCommand());
                                }
                                if(tagUnLog){
                                    objectOutputStream.writeObject(raftUnCommitMessage);

                                }
                            }
                            if(response){
                                //接收回复的消息
                                resetLastAppendIndex(socket);//内部加锁
                            }else{
                                //没有其他消息，发送普通的心跳消息
                                RaftHeartMessage heartMessage = new RaftHeartMessage();
                                heartMessage.setId(myNode.getNodeId());
                                lock.lock();
                                try {
                                    heartMessage.setCurrentTerm(currentTerm);
                                    heartMessage.setLastAppendedIndex(lastAppliedIndex);
                                    heartMessage.setLastAppendedTerm(lastAppliedTerm);
                                }finally {
                                    lock.unlock();
                                }
                                heartMessage.setLeader(true);
                                objectOutputStream.writeObject(heartMessage);
                                resetLastAppendIndex(socket); //内部会加锁
                            }
                            //更新节点日志状态
                            if(!consistentHash.hashNode(node)){
                                log.info("server {} 检测到 server {} 接入",myNode.getNodeId(),node.getNodeId());
                                generate(node,true);//内部会加锁
                            }
                        }catch (IOException ex){
                            log.debug("Server: {} Exception {}",ex);
                            if(consistentHash.hashNode(node)){
                                generate(node,false);
                                log.info("server {} lost on term {}",node.getNodeId(),currentTerm);
                            }
                        }finally {
                            try {
                                objectOutputStream.close();
                            }catch (IOException ex){
                                log.debug(ex.toString());
                            }
                            countDownLatch.countDown();
                        }
                    }
                });
            }else{
                if(!consistentHash.hashNode(myNode)){
                    generate(myNode,true);//内部会加锁
                }
            }
        }
        try{
            countDownLatch.await((nodes.size()-1)*RaftOptions.heartbeatPeriodMilliseconds,TimeUnit.MILLISECONDS);
        }catch (InterruptedException ex){
            log.info(ex.toString());
        }
        resetHeartbeatTimer();
    }

    /**
     * 产生新提交日志信息。
     * @param node
     * @param tags
     */
    private void generate(Node node,boolean tags){
        lock.lock();
        try{
            RaftLogUncommit unCommitLog = new RaftLogUncommit();
            long index = unCommitLogs.size() > 0?unCommitLogs.get(unCommitLogs.size()-1).getIndex() + 1:lastAppliedIndex + 1;
            unCommitLog.setIndex(index);
            unCommitLog.setTerm(currentTerm);
            unCommitLog.setTimestamp(new Date().getTime()/1000);
            if(tags){
                unCommitLog.setCommand("add "+ node.getNodeId());
            }else{
                unCommitLog.setCommand("remove "+ node.getNodeId());
            }
            if(unCommitLogs.size() > 0){
                boolean tag= true;
                for(RaftLogUncommit unLog:unCommitLogs){
                    if(unLog.getCommand().equals(unCommitLog.getCommand())){
                        tag = false;
                    }
                }
                if(tag)
                    unCommitLogs.add(unCommitLog);

                log.info("server {} uncommit log {}",myNode.getNodeId(),unCommitLogs);
            }else {
                unCommitLogs.add(unCommitLog);
            }
        }finally {
            lock.unlock();
        }
    }

    /**
     * 更新 append_index and term
     */
    private void resetLastAppendIndex(Socket socket)throws IOException{
        ObjectInputStream in = null;
        try{
            in = new ObjectInputStream(socket.getInputStream());
            RaftMessage message =(RaftMessage) in.readObject();
            lock.lock();
            try {

                if(message instanceof  RaftSnapHotMessage){
                    int id = message.getId();
                    lastRaftMessageMap.put(id,
                            new LastRaftMessage(id,0,0)
                    );
                }else if(message instanceof RaftCommitMessage) {
                    int id = message.getId();
                    lastRaftMessageMap.put(id,
                            new LastRaftMessage(id,
                                    message.getLastAppendedIndex(),
                                    message.getLastAppendedTerm())
                    );
                }else if(message instanceof RaftAckMessage){
                    RaftAckMessage ackMessage = (RaftAckMessage)message;
                    int id = ackMessage.getId();
                    long index = ackMessage.getAckIndex();
                    long term = ackMessage.getAckTerm();
                    if(unCommitLogs.size() > 0){
                        RaftLogUncommit raftLogUncommit = unCommitLogs.get(0);
                        if(index == raftLogUncommit.getIndex() && term == raftLogUncommit.getTerm()){
                            Set<Integer> ack = raftLogUncommit.getAck();
                            ack.add(id);
                            raftLogUncommit.setAck(ack);
                            unCommitLogs.remove(0);
                            unCommitLogs.add(0,raftLogUncommit);
                        }

                    }
                }
                stepDown(message);
            }finally {
                lock.unlock();
            }
        }catch (ClassNotFoundException ex){
            ex.printStackTrace();
        }finally {
            in.close();
        }
    }
    /**
     * 监听其他节点的消息
     */
    private void  listenNode(){
        Thread thread = new Thread() {
            public void run() {
                ServerSocket server = null;
                try {
                    server = new ServerSocket(myNode.getListenHeartbeatPort(),50, InetAddress.getByName(myNode.getIp()+""));
                    log.info("Server {} listen port on {}",myNode.getNodeId(),myNode.getListenHeartbeatPort());
                    while (true){
                        Socket socket = server.accept();
                        executorService.execute(new ListenNodeThread(socket));
                    }
                }catch (IOException ex){
                    ex.printStackTrace();
                }

            }
        };
        thread.start();
    }
    private class ListenNodeThread extends Thread{
        private  Socket socket;
        public ListenNodeThread(Socket socket) {
            this.socket = socket;
        }
        @Override
        public void run() {
            ObjectInputStream objectInputStream = null;
            try {
                while (socketAlive(socket)){
                    objectInputStream = new ObjectInputStream(
                            socket.getInputStream());
                    RaftMessage message =(RaftMessage) objectInputStream.readObject();
                    log.info("server {} get message {},",myNode.getNodeId(),message.toString());
                    doMessage(message,socket);
                }
            }catch (IOException ex){
                log.debug(ex.toString());
            }catch (ClassNotFoundException ex){
                log.debug(ex.toString());
            }finally {
                try {
                    socket.close();
                    objectInputStream.close();
                }catch (IOException ex){
                    log.debug(ex.toString());
                }
            }
        }
    }

    /**
     * 处理,监听到的消息
     * @param message
     * @param socket
     */
    //in lock
    private void doMessage(RaftMessage message,Socket socket)throws IOException {
        //一共有7 种消息类型，ACK 只可能出现nodes->leader 不可能出现在这里
        //这里的消息只有candidate->nodes,pre_candidate->nodes,leader->nodes
        if (message instanceof RaftHeartMessage) {
            //leader->nodes
            doHeartMessage((RaftHeartMessage) message, socket);
            return;
        }
        //同步已提交日志
        if (message instanceof RaftCommitMessage) {
            //leader->nodes
            doCommitMessage((RaftCommitMessage) message, socket);
            return;
        }
        //同步snaphot
        if (message instanceof RaftSnapHotMessage) {
            //leader->nodes
            doSnapHotMessage((RaftSnapHotMessage) message, socket);
            return;
        }
        //未提交日志等待确认消息
        if (message instanceof RaftUnCommitMessage) {
            //leader->nodes
            doUnCommitMessage((RaftUnCommitMessage) message, socket);
            return;
        }
        //为了防止自身网络异常，不断发起新的投票
        if (message instanceof RaftPreVoteMessage) {
            //pre_candidate->nodes
            doPreVoteMessage((RaftPreVoteMessage) message, socket);
            return;
        }
        //处理投票消息
        if (message instanceof RaftVoteMessage) {
            //candidate->nodes
            doVoteMessage((RaftVoteMessage) message, socket);
            return;
        }
    }

    /**
     * 处理投票选举消息
     * @param message
     * @param socket
     */
    private void  doVoteMessage(RaftVoteMessage message,Socket socket)throws IOException{
        //candidate->nodes
        RaftVoteMessage voteMessage = new RaftVoteMessage();
        long term = message.getCurrentTerm();
        voteMessage.setId(myNode.getNodeId());
        lock.lock();
        try{
            if(state == RaftState.LEADER){
                if(currentTerm >= term ){//拒绝
                    voteMessage.setVoteFor(false);
                    voteMessage.setLeader(true);
                }else{ //接收,同时降为follower
                    voteMessage.setVoteFor(true);
                    votedFor = message.getId();
                    state = RaftState.FOLLOWER;
                    currentTerm = term;

                    // stop heartbeat
                    if (heartbeatScheduledFuture != null && !heartbeatScheduledFuture.isDone()) {
                        heartbeatScheduledFuture.cancel(true);
                    }
                }
            }else if(state == RaftState.FOLLOWER){
                if(currentTerm > term){ //拒绝
                    voteMessage.setVoteFor(false);
                }else if(currentTerm == term){
                    if(canVoteFor(message)){
                        voteMessage.setVoteFor(true);
                        votedFor = message.getId();
                    }else{
                        voteMessage.setVoteFor(false);
                    }
                }else if(currentTerm < term){
                    voteMessage.setVoteFor(true);
                    votedFor = message.getId();
                    currentTerm = term;
                }
            }else if(state == RaftState.PRE_CANDIDATE){
                if(currentTerm > term){//拒绝
                    voteMessage.setVoteFor(false);
                }else if(currentTerm < term){  //接收并降级为follower
                    voteMessage.setVoteFor(true);
                    currentTerm = term;
                    state = RaftState.FOLLOWER;
                    votedFor = message.getId();
                    if (electionScheduledFuture != null && !electionScheduledFuture.isDone()) {
                        electionScheduledFuture.cancel(true);
                    }
                    votedFor = message.getId();
                    currentTerm = term;
                    state = RaftState.FOLLOWER;
                }else{
                    if(canVoteFor(message)){ //接收并降级为follower

                        votedFor = message.getId();
                        currentTerm = term;
                        state = RaftState.FOLLOWER;
                        voteMessage.setVoteFor(true);
                        if (electionScheduledFuture != null && !electionScheduledFuture.isDone()) {
                            electionScheduledFuture.cancel(true);
                        }
                    }else {
                        voteMessage.setVoteFor(false);
                    }
                }
            }else if(state == RaftState.CANDIDATE){
                if(currentTerm >= term){//拒绝
                    voteMessage.setVoteFor(false);
                }else {
                    voteMessage.setVoteFor(true);

                    votedFor = message.getId();
                    currentTerm = term;
                    state = RaftState.FOLLOWER;
                    if (electionScheduledFuture != null && !electionScheduledFuture.isDone()) {
                        electionScheduledFuture.cancel(true);
                    }
                }
            }
            voteMessage.setCurrentTerm(currentTerm);
            voteMessage.setLastAppendedIndex(lastAppliedIndex);
            voteMessage.setLastAppendedTerm(lastAppliedTerm);
        }finally {
            lock.unlock();
        }
        new ObjectOutputStream(socket.getOutputStream()).writeObject(voteMessage);
    }
    /**
     * 是否把投票给他
     * @param message
     * @return
     */
    //in lock
    private boolean canVoteFor(RaftMessage message){
        //1.term > currentTerm
        //2.term == currentTerm && 日志不比当前节点旧 && 该节点没有票投其他节点
        if(currentTerm < message.getCurrentTerm()){
            return true;
        }
        if(currentTerm == message.getCurrentTerm()
                && LogParse.compare(message.getLastAppendedTerm(),message.getLastAppendedIndex(),lastAppliedTerm,lastAppliedIndex) >=0
                && votedFor == -1){
            return true;
        }
        return false;
    }
    /**
     * 对于这个预投票，是否把票投给它
     * @param message
     * @return
     */
    //in lock
    private boolean canPreVoteFor(RaftMessage message){
        //当前节点没有term 小于那个节点 || (或者等于&&该节点也没有leader)
        if(currentTerm < message.getCurrentTerm()
                ||(currentTerm == message.getCurrentTerm() && !haveLeader()) ){
            return true;
        }
        return false;
    }
    /**
     * 处理预先投票选举消息
     * @param message
     * @param socket
     */
    private void doPreVoteMessage(RaftPreVoteMessage message,Socket socket)throws IOException{
        RaftPreVoteMessage raftPreVoteMessage = new RaftPreVoteMessage();
        lock.lock();
        try {
            raftPreVoteMessage.setId(myNode.getNodeId());
            raftPreVoteMessage.setCurrentTerm(currentTerm);
            raftPreVoteMessage.setLastAppendedTerm(lastAppliedTerm);
            raftPreVoteMessage.setLastAppendedIndex(lastAppliedIndex);
            if (canPreVoteFor(message)) {
                raftPreVoteMessage.setVoteFor(true);
                log.info("server {} pre vote for server{}", myNode.getNodeId(), message.getId());
            } else {
                raftPreVoteMessage.setVoteFor(false);
                log.info("server {} do not pre vote for server{}", myNode.getNodeId(), message.getId());
            }
        }finally {
            lock.unlock();
        }
        new ObjectOutputStream(socket.getOutputStream()).writeObject(raftPreVoteMessage);
    }
    /**
     * 未提交日志等待其他server确认
     * @param message
     */
    private void doUnCommitMessage(RaftUnCommitMessage message,Socket socket)throws IOException{
        long  term = message.getCurrentTerm();
        RaftAckMessage ack = new RaftAckMessage();
        lock.lock();
        try {
            if (state == RaftState.LEADER) {
                if (currentTerm > term) { //不接受
                    ack.setAckTerm(-1);
                    ack.setAckIndex(-1);
                    ack.setLeader(true);
                } else if (currentTerm == term) {
                    //这种情况不存在
                    log.debug("servers have two leaders on same term {}",currentTerm);
                } else {  //接收,同时降级为follower
                    currentTerm = term;
                    votedFor = -1;
                    ack.setAckTerm(message.getRaftLog().getTerm());
                    ack.setAckIndex(message.getRaftLog().getIndex());
                    leaderConnectTime = new Date().getTime();
                    state = RaftState.FOLLOWER;
                    // stop heartbeat
                    if (heartbeatScheduledFuture != null && !heartbeatScheduledFuture.isDone()) {
                        heartbeatScheduledFuture.cancel(true);
                    }
                }
            } else if (state == RaftState.FOLLOWER) {
                if (currentTerm > term) { //不接受
                    ack.setAckTerm(-1);
                    ack.setAckIndex(-1);
                    ack.setLeader(false);
                } else if (currentTerm == term) {//接收
                    currentTerm = term;
                    ack.setAckTerm(message.getRaftLog().getTerm());
                    ack.setAckIndex(message.getRaftLog().getIndex());
                    leaderConnectTime = new Date().getTime();
                } else {  //接收
                    currentTerm = term;
                    votedFor = -1;
                    ack.setAckTerm(message.getRaftLog().getTerm());
                    ack.setAckIndex(message.getRaftLog().getIndex());
                    leaderConnectTime = new Date().getTime();
                }
            } else if (state == RaftState.PRE_CANDIDATE || state == RaftState.CANDIDATE) {
                if (currentTerm > term) { //不接受
                    ack.setAckTerm(-1);
                    ack.setAckIndex(-1);
                    ack.setLeader(false);
                } else if (currentTerm == term || currentTerm < term) {//接收,降级为follower

                    if (currentTerm < term)
                        votedFor = -1;
                    currentTerm = term;
                    state = RaftState.FOLLOWER;
                    leaderConnectTime = new Date().getTime();
                    ack.setAckTerm(message.getRaftLog().getTerm());
                    ack.setAckIndex(message.getRaftLog().getIndex());
                    // stop election
                    if (electionScheduledFuture != null && !electionScheduledFuture.isDone()) {
                        electionScheduledFuture.cancel(true);
                    }
                }
            }
            ack.setLastAppendedIndex(lastAppliedIndex);
            ack.setLastAppendedTerm(lastAppliedTerm);
            ack.setId(myNode.getNodeId());
            ack.setCurrentTerm(currentTerm);
        }finally {
            lock.unlock();
        }
        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
        out.writeObject(ack);
    }
    /**
     * 普通的心跳消息
     * @param message
     * @param socket
     * @throws IOException
     */
    //in lock
    private void doHeartMessage(RaftHeartMessage message,Socket socket)throws IOException{
        //leader-> nodes
        long term = message.getCurrentTerm();
        RaftHeartMessage raftHeartMessage = new RaftHeartMessage();
        raftHeartMessage.setId(myNode.getNodeId());
        lock.lock();
        try {
            if (state == RaftState.LEADER) {
                if (term > currentTerm) {//leader降级为follower
                    state = RaftState.FOLLOWER;
                    currentTerm = term;
                    votedFor = -1;
                    leaderConnectTime = new Date().getTime();
                    // stop heartbeat
                    if (heartbeatScheduledFuture != null && !heartbeatScheduledFuture.isDone()) {
                        heartbeatScheduledFuture.cancel(true);
                    }
                } else if(currentTerm > term){
                    raftHeartMessage.setLeader(true);
                }else{
                    //不存在
                }
            } else if (state == RaftState.FOLLOWER) {
                if (term > currentTerm) {
                    currentTerm = term;
                    votedFor = -1;
                    leaderConnectTime = new Date().getTime();
                }else if(term == currentTerm){
                    leaderConnectTime = new Date().getTime();
                }else {
                    //假的leader
                }
            } else if (state == RaftState.PRE_CANDIDATE || state == RaftState.CANDIDATE) {
                if (term >= currentTerm) {//降级为follower
                    state = RaftState.FOLLOWER;
                    currentTerm = term;
                    votedFor = -1;
                    leaderConnectTime = new Date().getTime();
                    // stop election
                    if (electionScheduledFuture != null && !electionScheduledFuture.isDone()) {
                        electionScheduledFuture.cancel(true);
                    }
                }else {

                }
            }
            raftHeartMessage.setCurrentTerm(currentTerm);
            raftHeartMessage.setLastAppendedTerm(lastAppliedTerm);
            raftHeartMessage.setLastAppendedIndex(lastAppliedIndex);
        }finally {
            lock.unlock();
        }
        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
        out.writeObject(raftHeartMessage);
        out.close();
    }

    /**
     * 同步以提交的日志
     * @param message
     * @param socket
     */
    //in lock
    private void doCommitMessage(RaftCommitMessage message,Socket socket)throws IOException{
        //leader->nodes
        long term = message.getCurrentTerm();
        RaftCommitMessage raftCommitMessage = new RaftCommitMessage();
        raftCommitMessage.setId(myNode.getNodeId());
        //是否将接受这个同步日志
        boolean tags = false;
        lock.lock();
        try {
            if (state == RaftState.LEADER) {
                if (term > currentTerm) {//leader降级为follower，同时接收这个日志同步
                    state = RaftState.FOLLOWER;
                    currentTerm = term;
                    votedFor = -1;
                    // stop heartbeat
                    if (heartbeatScheduledFuture != null && !heartbeatScheduledFuture.isDone()) {
                        heartbeatScheduledFuture.cancel(true);
                    }
                    tags = true;
                } else {
                    raftCommitMessage.setLeader(true);
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
                if (term > currentTerm) {//降级为follower,同时接收这个日志同步
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
        }finally {
            if(tags)
                leaderConnectTime = new  Date().getTime();
            lock.unlock();
        }
        if(tags) {
            //将同步日志写入本地日志，同时将日志应用于本地state machine
            List<RaftLog> logs = message.getRaftLogs();
            appendEntries(logs, true);
        }
        lock.lock();
        try {
            raftCommitMessage.setCurrentTerm(currentTerm);
            raftCommitMessage.setLastAppendedTerm(lastAppliedTerm);
            raftCommitMessage.setLastAppendedIndex(lastAppliedIndex);
        }finally {
            lock.unlock();
        }
        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
        out.writeObject(raftCommitMessage);
        out.close();
    }

    /**
     * leader->nodes
     * @param message
     * @param socket
     * @throws IOException
     */
    //in lock
    private void doSnapHotMessage(RaftSnapHotMessage message,Socket socket)throws IOException{
        //leader->nodes
        long term = message.getCurrentTerm();
        RaftSnapHotMessage raftSnapHotMessage = new RaftSnapHotMessage();
        raftSnapHotMessage.setId(myNode.getNodeId());
        //是否清空已经无效的日志信息
        boolean tag  =false;
        lock.lock();
        try {
            if (state == RaftState.LEADER) {
                if (currentTerm > term) {//不接受这个snaphot
                    raftSnapHotMessage.setLeader(true);
                }else if(currentTerm == term){
                    //正常情况下不会出现这种情况
                    log.debug("servers have two leader on same term",currentTerm);
                }else {        //接收并降级为follower
                    currentTerm = term;
                    votedFor = -1;
                    consistentHash.setCircle(message.getCircle());
                    log.info("清空已经无效的日志信息");
                    tag = true;
                    state = RaftState.FOLLOWER;
                    // stop heartbeat
                    if (heartbeatScheduledFuture != null && !heartbeatScheduledFuture.isDone()) {
                        heartbeatScheduledFuture.cancel(true);
                    }
                }
            } else if (state == RaftState.FOLLOWER) {
                if (currentTerm > term) { //不接受

                } else if (currentTerm == term) {
                    consistentHash.setCircle(message.getCircle());
                    log.info("清空已经无效的日志信息");
                    tag = true;
                } else {
                    currentTerm = term;
                    votedFor = -1;
                    consistentHash.setCircle(message.getCircle());
                    log.info("清空已经无效的日志信息");
                    tag = true;
                }
            } else if (state == RaftState.PRE_CANDIDATE || state == RaftState.CANDIDATE) {
                if (currentTerm > term) { //不接受

                } else if (currentTerm == term) { //降级为follower
                    consistentHash.setCircle(message.getCircle());
                    state = RaftState.FOLLOWER;
                    log.info("清空已经无效的日志信息");
                    tag = true;
                    // stop election
                    if (electionScheduledFuture != null && !electionScheduledFuture.isDone()) {
                        electionScheduledFuture.cancel(true);
                    }
                } else {                        //降级为follower
                    state = RaftState.FOLLOWER;
                    currentTerm = term;
                    votedFor = -1;
                    consistentHash.setCircle(message.getCircle());
                    log.info("清空已经无效的日志信息");
                    tag = true;
                    // stop election
                    if (electionScheduledFuture != null && !electionScheduledFuture.isDone()) {
                        electionScheduledFuture.cancel(true);
                    }
                }
            }
            if(tag){
                leaderConnectTime = new  Date().getTime();
                lastAppliedTerm = message.getLastAppendedTerm();
                lastAppliedIndex = message.getLastAppendedIndex();
            }
            raftSnapHotMessage.setCurrentTerm(currentTerm);
            raftSnapHotMessage.setLastAppendedTerm(lastAppliedTerm);
            raftSnapHotMessage.setLastAppendedIndex(lastAppliedIndex);
        }finally {
            lock.unlock();
        }
        if(tag){
            log.info("清空已经无效的文件");
            LogParse.clearLogs();
            LogParse.clearSnaphotFile();
        }
        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
        out.writeObject(raftSnapHotMessage);
    }
    /**
     * 节点启动的时候将RaftSnaphot 应用于state machine
     */
    private void installRaftSnaphot(){
        RaftSnaphot raftSnaphot = new RaftSnaphot(raftSnaphotPath);
        //这个文件不存在是返回null
        TreeMap<Long, Node> circle = raftSnaphot.getRaftSnaphot();
        if(circle != null)
            consistentHash.setCircle(circle);
    }

    private TreeMap<Long, Node> getRaftSnaphot(){
        RaftSnaphot raftSnaphot = new RaftSnaphot(raftSnaphotPath);
        TreeMap<Long, Node> circle = raftSnaphot.getRaftSnaphot();
        return circle;
    }
    /**
     * 节点启动的时候将已经提交的日志应用于state machine
     */
    private void appendEntries(){
        List<RaftLog> logs = LogParse.getRaftLogs();
        appendEntries(logs,false);
    }
    /**
     * 同步日志应用于state machine，并将该日志写入log文件
     * @param logs
     */
    private void appendEntries(List<RaftLog> logs,boolean writeLog){
        lock.lock();
        try{
            if (logs.size() == 0) {
                lastAppliedIndex = 0;
                lastAppliedTerm = 0;
            } else {
                lastAppliedIndex = logs.get(logs.size() - 1).getIndex();
                lastAppliedTerm = logs.get(logs.size() - 1).getTerm();
                currentTerm = Math.max(lastAppliedTerm,currentTerm);
            }
        }finally {
            lock.unlock();
        }
        for (RaftLog raftLog : logs) {
            String command = raftLog.getCommand();
            appendEntry(command);
            //追加日志到文件
            if (writeLog)
                LogParse.insertLog(raftLog);
        }
    }

    /**
     * 根据nodeId 查询节点信息，Nodes 这个数不会太大，直接遍历
     * @param id
     * @return
     */
    private Node findNode(int id){
        for (Node node:nodes){
            if(node.getNodeId() == id)
                return node;
        }
        return  null;
    }
    /**
     * 解析命令，并将他应用于state machine
     * @param command
     */
    private void appendEntry(String command){
        String methodStr = command.split(" ")[0];
        int id = Integer.valueOf(command.split(" ")[1]);
        Node node = findNode(id);
        Class consistentHashClass = ConsistentHash.class;
        try {
            Method method = consistentHashClass.getMethod(methodStr,Node.class);
            method.invoke(consistentHash,node);
        }catch (NoSuchMethodException ex){
            ex.printStackTrace();
        }catch (IllegalAccessException ex){
            ex.printStackTrace();
        }catch (InvocationTargetException ex){
            ex.printStackTrace();
        }
    }

    /**
     * 为了避免在同时发起选举，而拿不到半数以上的投票，
     * 在150-300ms 这个随机时间内，发起新一轮选举
     * (Raft 论文上说150-300ms这个随机时间，不过好像把时差弄大点，效果更好)
     * @return
     */
    private  int getRandomElection(){
        int randTime =  new Random().nextInt(500) + 150;
        log.info("server {} start election after {} ms",myNode.getNodeId(),randTime);
        return randTime;
    }
    /**
     * 根据node 信息获取socket连接
     * @param node
     * @return
     */
    private Socket getSocket(Node node)throws IOException{
        Socket socket;
        if(sockets.get(node) == null
                || sockets.get(node).isClosed()
                || !sockets.get(node).isConnected()){
            socket = new Socket(node.getIp(),
                    node.getListenHeartbeatPort());
            sockets.put(node,socket);
        }
        return sockets.get(node);
    }
    /**
     * socket 是否可用
     * @param socket
     * @return
     */
    private boolean socketAlive(Socket socket){
        return socket.isConnected() && !socket.isClosed();
    }
    /**
     * 集群内是否有leader
     * @return
     */
    private boolean haveLeader(){
        lock.lock();
        try{
            if(state == RaftState.LEADER
                    ||new Date().getTime() - leaderConnectTime <= RaftOptions.electionTimeoutMilliseconds){
                log.info("server {} have leader on term {}",myNode.getNodeId(),currentTerm);
                return true;
            }
        }finally {
            lock.unlock();
        }
        log.info("server {} do not have leader on term {}",myNode.getNodeId(),currentTerm);
        return  false;
    }
    /**
     * 打印server状态
     */
    private void printStatus(){
        log.info("server {} currentTerm = {} state =  {},lastindex = {},lastTerm = {},circle = {}",
                myNode.getNodeId(),currentTerm,state,lastAppliedIndex,lastAppliedTerm,consistentHash.getCircle());
    }

    /**
     * 启动初始化监听客户端线程
     */
    private void initListenClientThread(){
        Thread listenClientThread = new Thread(new Runnable() {
            public void run() {
                CacheServer server = new CacheServer(myNode.getListenClientPort());
                server.init();
            }
        });
        listenClientThread.start();
    }
    /**
     * 加载磁盘中的缓存数据到内存中
     */
    private void loadCache(){
        //load RDB
       File rdbFile = new File(QCacheConfiguration.getCacheRdbPath());
       if(rdbFile.exists() && rdbFile.length() > 0){
           try {
               ObjectInputStream obj = new ObjectInputStream(new FileInputStream(rdbFile));
               cache = (ConcurrentHashMap<String, CacheDataI>)obj.readObject();
           } catch (IOException e) {
               log.debug(e.toString());
           } catch (ClassNotFoundException e){
                log.debug(e.toString());
           }
       }
       //load AOF file
        File aofFile = new File(QCacheConfiguration.getCacheAofPath());
       if(aofFile.exists() && aofFile.length() >0){
           List<String> list = BackUpAof.getCommands();
           for(String line:list) {
               String temp[] = line.split("\\s+");
               String command = temp[0];
               if (command.equalsIgnoreCase(core.cache.Method.del)) {
                   doDel(line);
               } else if (command.equalsIgnoreCase(core.cache.Method.set)) {
                   doSet(line);

               }
           }
       }
    }
    //处理删除数据
    private void doDel(String line){
        String temp[] = line.split("\\s+");
        String key = temp[0];
        cache.remove(key);
    }

    //处理添加数据
    private void  doSet(String line){
        List<String> setStrs = Tools.split(line);
        String  key = setStrs.get(1);
        String  val = setStrs.get(2);
        if(setStrs.size() == 4){
            //含有过期时间
            int last = Integer.valueOf(setStrs.get(3));
            doSet(key,val,last);
        }else{
            //没有过期时间,表示永久有效
            doSet(key,val,-1);
        }
    }
    //处理添加数据
    private void doSet(String key,String val,int last){
        try {
            int intVal = Integer.valueOf(val);
            CacheDataInt cacheDataInt = new CacheDataInt(intVal,new Date().getTime(),last);
            cache.put(key,cacheDataInt);
        }catch (Exception ex){
            CacheDataString cacheDataStr = new CacheDataString(val,new Date().getTime(),last);
            cache.put(key,cacheDataStr);
        }
    }

    /*******************************上面的基本是Raft算法实现下面部分是缓存相关的功能*******************************/
    private class CacheServer {
        ServerSocketChannel serverChannel;
        ServerSocket serverSocket;
        public final int port;
        private Selector selector;
        private int bufferSize = 1024;
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        public CacheServer(final int port) {
            this.port = port;
        }
        /**
         * 启动NIO ServerSocket 监听客户端的连接
         */
        private void init(){
            try {
                serverChannel = ServerSocketChannel.open();
                serverSocket = serverChannel.socket();
                serverSocket.bind(new InetSocketAddress(port));
                serverChannel.configureBlocking(false);
                selector = Selector.open();
                serverChannel.register(selector, SelectionKey.OP_ACCEPT);
                go();
            }catch (IOException ex){
                log.info(ex.toString());
            }
        }
        private void go()throws IOException {
            while (true) {
                int num = selector.select();
                if (num <= 0)
                    continue;
                Iterator<SelectionKey> keyIter = selector.selectedKeys().iterator();
                while (keyIter.hasNext()) {
                    final SelectionKey key = keyIter.next();
                    if (key.isAcceptable()) {
                        SocketChannel clientChannel = serverChannel.accept();
                        if (clientChannel != null) {
                            clientChannel.configureBlocking(false);
                            clientChannel.register(selector, SelectionKey.OP_READ);
                        }
                    }else if (key.isReadable()) {
                        String requestContent = read(key);
                        String responseContent = doCommand(requestContent);
                        key.selector().wakeup();
                        write(key,responseContent);
                    }
                    keyIter.remove();
                }
            }
        }
        /**
         * 读客户端数据
         * @param key
         * @return
         */
        private String read(SelectionKey key){
            SocketChannel socketChannel = (SocketChannel) key.channel();
            buffer.clear();
            int len;
            StringBuffer str=new StringBuffer();
            try {
                while ((len = socketChannel.read(buffer)) > 0) {
                    byte[] bs = buffer.array();
                    buffer.flip();
                    str.append(new String(bs, 0, len));
                    buffer.clear();
                }
                if(len == -1){
                    key.cancel();
                }
            }catch (IOException ex){
               log.error(ex.toString());
                key.cancel();
                try {
                    socketChannel.close();
                } catch (IOException e) {
                    log.error(e.toString());
                }

            }
            return str.toString();
        }
        /**
         * 向客户端写入数据
         * @param key
         * @param str
         */
        private void write(SelectionKey key, String str){
            SocketChannel socketChannel = (SocketChannel) key.channel();
            byte[] buf = str.getBytes();
            int len = buf.length;
            int index = 0;
            int num  = len -index;
            try {
                while (num > 0 && index < len) {
                    if (num >= bufferSize) {
                        buffer.clear();
                        buffer.put(buf, index, bufferSize);
                        buffer.flip();
                        socketChannel.write(buffer);
                        index = index + bufferSize;
                        num -= bufferSize;
                    } else {
                        buffer.clear();
                        buffer.put(buf, index, num);
                        buffer.flip();
                        index = index + num;
                        socketChannel.write(buffer);
                        num = 0;
                    }
                }
                socketChannel.close();
            }catch (IOException ex){
                key.cancel();
                if(socketChannel != null){
                    try {
                        socketChannel.close();
                    } catch (IOException e) {
                        log.error(e.toString());
                    }
                }
            }
        }

        /**
         *
         * @param line
         * @return
         */
        private String  doCommand(String line){
            String temp[] = line.split("\\s+");
            String command = temp[0];
            if(command.equalsIgnoreCase(core.cache.Method.status)){
                return doStatus();
            }else if(command.equalsIgnoreCase(core.cache.Method.get)){
                return doGet(line);
            }else if(command.equalsIgnoreCase(core.cache.Method.del)){

                return doDel(line);
            }else if(command.equalsIgnoreCase(core.cache.Method.set)){

                return doSet(line);

            }
            return new JsonMessage(CodeNum.COMMAND_NOT_EXIST,"Unknown Command").toString();
        }

        /**
         * 删除数据
         * @param line
         * @return
         */
        private String doDel(String line ){
            String[] temp = line.split("\\s+");
            String key = temp[1];
            Node node = consistentHash.get(key);
            if (node.equals(myNode)) {
                if (cache.get(key) != null) {
                   cache.remove(key);
                    BackUpAof.appendAofLog(line,cache);
                   return new JsonMessage(CodeNum.SUCCESS,"delete success").toString();
                } else {
                    return new JsonMessage(CodeNum.NIL, "key not exist").toString();
                }
            } else {
                return dealAnotherServer(node, line);
            }
        }
        /**
         * 处理get 请求
         * @param line
         * @return
         */
        private String doGet(String line) {
            String[] temp = line.split("\\s+");
            String key = temp[1];
            Node node = consistentHash.get(key);
            if (node.equals(myNode)) {
                if (cache.get(key) != null) {
                    CacheDataI cacheDataI = cache.get(key);
                    if (cacheDataI instanceof CacheDataInt) {
                        CacheDataInt cacheDataInt = (CacheDataInt) cacheDataI;
                        if (cacheDataInt.last == -1
                                || new Date().getTime() <= cacheDataInt.last + cacheDataInt.lastVisit) {
                            cacheDataInt.lastVisit = new Date().getTime();
                            cache.put(key, cacheDataInt);
                            return new JsonMessage(CodeNum.SUCCESS, cacheDataInt.val + "").toString();
                        } else {
                            return new JsonMessage(CodeNum.NIL, "key not exist").toString();
                        }
                    } else if (cacheDataI instanceof CacheDataString) {
                        CacheDataString cacheDataString = (CacheDataString) cacheDataI;
                        if (cacheDataString.last == -1
                                || new Date().getTime() <= cacheDataString.lastVisit + cacheDataString.last) {
                            cacheDataString.lastVisit = new Date().getTime();
                            cache.put(key, cacheDataString);
                            return new JsonMessage(CodeNum.SUCCESS, cacheDataString.val).toString();
                        } else {
                            return new JsonMessage(CodeNum.NIL, "key not exist").toString();
                        }
                    }
                } else {
                    return new JsonMessage(CodeNum.NIL, "key not exist").toString();
                }
            } else {
                return dealAnotherServer(node, line);
            }
            return new JsonMessage(CodeNum.NIL, "key not exist").toString();
        }
        /**
         * 处理set
         * @param line
         * @return
         */
        private String doSet(String line){
            List<String> setStrs = Tools.split(line);
            String  key = setStrs.get(1);
            String  val = setStrs.get(2);
            Node node = consistentHash.get(key);
            if(node.equals(myNode)){
                if(setStrs.size() == 4){
                    //含有过期时间
                    int last = Integer.valueOf(setStrs.get(3));
                    BackUpAof.appendAofLog(line,cache);
                    return doSet(key,val,last);
                }else{
                    //没有过期时间,表示永久有效
                    BackUpAof.appendAofLog(line,cache);
                    return doSet(key,val,-1);
                }
            }else {
                return dealAnotherServer(node,line);
            }
        }

        /**
         * set key val [time]
         * @param key
         * @param val
         * @param last
         * @return 这个字符串是个json格式的
         */
        private String doSet(String key,String val,int last){
            try {
                int intVal = Integer.valueOf(val);
                CacheDataInt cacheDataInt = new CacheDataInt(intVal,new Date().getTime(),last);
                cache.put(key,cacheDataInt);
            }catch (Exception ex){
                CacheDataString cacheDataStr = new CacheDataString(val,new Date().getTime(),last);
                cache.put(key,cacheDataStr);
            }
            log.info("server {} set {} {}", myNode.getNodeId(),key,val);
            return new JsonMessage(CodeNum.SUCCESS,"OK").toString();
        }
        /**
         * 该数据不归本节点处理交给第三方节点处理
         * @return
         */
        private String dealAnotherServer(Node node,String line){
            Socket socket = null;
            OutputStream outputStream = null;
            InputStream inputStream = null;
            try {
                socket = new Socket(node.getIp(), node.getListenClientPort());
                outputStream = socket.getOutputStream();
                inputStream = socket.getInputStream();
                outputStream.write(line.getBytes());
                byte[] buffer = new byte[1024];
                int n = 0;
                StringBuilder stringBuilder = new StringBuilder();
                while ((n = inputStream.read(buffer)) > 0) {
                    stringBuilder.append(new String(buffer, 0, n));
                }
                return stringBuilder.toString();
            }catch (IOException ex){
                log.debug(ex.toString());
            }finally {
                try {
                    socket.close();
                    outputStream.close();
                    inputStream.close();
                }catch (IOException ex){
                    log.debug(ex.toString());
                }
            }
            return new JsonMessage(CodeNum.ERROR,"ERROR").toString();
        }
        /**
         * 返回服务器状态
         * @return
         */
        private String doStatus() {
            String res = "------------------------------------------" +"\n"+
                         "State:" + state + "\n" +
                         "Term:" + currentTerm + "\n" +
                         "Id:" + myNode.getNodeId() + "\n" +
                         "Ip:" + myNode.getIp() + "\n" +
                         "Port:" + myNode.getListenClientPort() + "\n"+
                         "Alive Servers:"+"\n";
            StringBuilder builder = new StringBuilder();
            for (Node node:getAliveNodes()){
                String temp = "server." + node.getNodeId()+"=" + node.getIp() + ":" + node.getListenClientPort() + "\n";
                builder.append(temp);
            }
            builder.append("------------------------------------------"+"\n");
            log.info(builder.toString());
            return new JsonMessage(CodeNum.SUCCESS,res + builder.toString()).toString();
        }
        /**
         * 获取集群中所有活跃节点
         * @return
         */
        private Set<Node> getAliveNodes(){
            TreeMap<Long,Node> circle =(TreeMap)consistentHash.getCircle();
            Set<Node> nodes = new TreeSet<Node>();
            for (long key:circle.keySet()){
                nodes.add(circle.get(key));
            }
            return nodes;
        }
    }
}
