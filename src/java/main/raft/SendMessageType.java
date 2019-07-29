package raft;

/**
 * leader 向其他节点发送消息的类型
 */
public enum SendMessageType {
    //普通心跳包[没有任何消息需要同步,或者不知道远程节点消息状况]
    HEART,
    //同步日志[达到服务器状态一致性,通过发送日志网络带宽更小]
    SYNC_LOG,
    //同步服务状态[达到服务器的状态一致性,通过发送state网络带宽更小]
    SYNC_STATE,
    //发送为提交日志[远程主机,日志都同步好了,leader含有未提交日志]
    UN_COMMIT_LOG

}
