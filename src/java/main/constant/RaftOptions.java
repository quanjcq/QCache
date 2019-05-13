package constant;

/**
 * Created by quan on 2019/4/24
 * Raft 运行的的配置
 */
public class RaftOptions {
    /**
     * follower 在一定时间内（electionTimeoutMilliseconds）没有接收到 来自leader 的心跳包，就会变成candidate,
     * 在随机时间（150~300 ms）内发起一轮选举
     */
    public static final int electionTimeoutMilliseconds = 3000;

    /**
     * leader 会每隔（heartbeatPeriodMilliseconds）向所有follower 发送心跳包，即使没有数据
     * 心跳包里携带leader 已经提交的日志信息，保证编号在这之前的日志都是已经提交的
      */
    public static final int heartbeatPeriodMilliseconds = 600;

    /**
     *只有在log 文件大小到达指定大小(snapshotMaxnLogSize)才会出现snapshot
     */
    public static final int snapshotPeriodSeconds = 3600;
    /**
     * 日志最大大小，这个为已经提交的日志，未提交的日志在内存中，达到这个大小会清空这个日志同时snapshot
     */
    public static final int maxLogSize = 1 * 1024 * 1024; //1m

    /**
     * core thread 数量
     */
    public static final int coreThreadNum = 10;

    /**
     * max thread
     */
    public static final int maxThreadNum = 30;
}
