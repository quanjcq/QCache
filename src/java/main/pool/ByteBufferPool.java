package pool;

/**
 * 对于每个客户单连接会分配写缓冲区,为了减少分配空间的开销,复用之前的空间
 * 保证线程安全
 */
public interface ByteBufferPool {

}
