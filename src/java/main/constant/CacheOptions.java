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

    /**
     * 连接最多被保持多久.
     */
    public static final long maxKeepTime = 1000 * 60 * 2;
    /**
     * Key 最大长度[使得可以单字节表示长度]
     */
    public static final int maxKeySize = 255;
}
