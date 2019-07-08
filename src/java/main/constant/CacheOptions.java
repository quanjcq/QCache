package constant;

public class CacheOptions {

    private CacheOptions() {

    }

    /**
     * 缓存AOF日志
     */
    public static final int maxAofLogSize = 10 * 1024 * 1024;//10m

    /**
     * 虚拟节点个数
     */
    public static final int numberOfReplicas = 200;
}
