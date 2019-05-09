package core.cache.backup;

/**
 * 负责数据的备份工作
 * 结合RDB 和 AOF 两种形式
 * AOF 通过追加(写)操作日志
 * AOF 追加的日志文件大小到达一个阀值
 * RDB 拷贝cache内存快照同时清空AOF文件
 * AOF + RDB  = 整个服务器的缓存数据
 * 系统重启的时候 加载RDB 和解析 AOF文件恢复到最新状态
 */
public interface BackUpI {
    int aofMaxSize = 256; //1m，这个数据小点不然不好测试

}
