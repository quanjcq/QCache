package constant;

public class CacheOptions {

    private CacheOptions() {

    }

    /**
     * 缓存AOF日志
     */
    public static final int maxAofLogSize = 1 * 1024 * 1024;

    /**
     * 虚拟节点个数
     */
    public static final int numberOfReplicas = 200;
}
