package core.cache.backup;

import core.cache.CacheData;

import java.util.HashMap;

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
    /**
     * 加载缓存数据
     * @param cache HashMap
     */
    void loadData(HashMap<String, CacheData> cache);
}
