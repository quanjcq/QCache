package recycle;

/**
 * 过期缓存回收,都是先标记[状态位置1],然后在合适的时候回收这个空间.
 * 这个过程是不存在线程安全的问题.
 */
public interface Mark {
    /**
     * 标记,返回本机标记待回收的数量.
     * @return int.标记的数量
     */
    int mark();
}
