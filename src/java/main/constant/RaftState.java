package constant;

/**
 * Created by quan on 2019/4/25
 * Raft 节点状态
 */
public enum RaftState {
    FOLLOWER,
    CANDIDATE,
    //为了避免candidate 自身断网，发起新选举，使得全局的term一直增大，
    //发起选举前会测试是否能连接到绝大多数server
    PRE_CANDIDATE,
    LEADER
}
