package common;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.SortedMap;
import java.util.TreeMap;


/**
 * 一致性hash算法实现.
 */
public class ConsistentHash {
    private final String salt = "quanjcq";
    /**
     * 复制的节点个数.
     */
    private final int numberOfReplicas;
    /**
     * 一致性Hash环.
     */
    private  SortedMap<Long,Node> circle = new TreeMap<Long,Node>();

    /**
     * @param number 复制的节点个数，增加每个节点的复制节点有利于负载均衡
     */
    public ConsistentHash(final int number) {
        this.numberOfReplicas = number;
    }

    /**
     * @param numberOfReplicas 复制的节点个数，增加每个节点的复制节点有利于负载均衡
     * @param nodes  节点对象
     */
    public ConsistentHash(final int numberOfReplicas, final Collection<Node> nodes) {
        this.numberOfReplicas = numberOfReplicas;

        //初始化节点
        for (Node node : nodes) {
            add(node);
        }
    }

    /**
     * hash 算法.
     * @param key 关键字.
     * @return  返回key的hash值
     */
    private Long hash(final Object key) {
        //return (long)key.hashCode();
        return md5(key.toString());
    }

    /**
     * 增加节点
     * 每增加一个节点，就会在闭环上增加给定复制节点数
     * 例如复制节点数是2，则每调用此方法一次，增加两个虚拟节点，这两个节点指向同一Node
     * 由于hash算法会调用node的toString方法，故按照toString去重.
     *
     * @param node 节点对象
     */
    public synchronized void add(final Node node) {
        for (int i = 0; i < numberOfReplicas; i++) {
            circle.put(hash(node.toString() + salt + i), node);
        }
    }

    /**
     * 获取服务器个数.
     * @return 返回服务器的个数
     */
    public synchronized int getSize() {
        return circle.size() / numberOfReplicas;
    }

    /**
     * 移除节点的同时移除相应的虚拟节点.
     * @param node 节点对象
     */
    public synchronized void remove(final Node node) {
        for (int i = 0; i < numberOfReplicas; i++) {
            circle.remove(hash(node.toString() + salt + i));
        }
    }

    /**
     * 获得一个最近的顺时针节点.
     * @param key 为给定键取Hash，取得顺时针方向上最近的一个虚拟节点对应的实际节点
     * @return 节点对象
     */
    public synchronized Node get(final Object key) {
        if (circle.isEmpty()) {
            return null;
        }
        long hashStr = hash(key);
        if (!circle.containsKey(hashStr)) {
            //返回此映射的部分视图，其键大于等于 hash
            SortedMap<Long,Node> tailMap = circle.tailMap(hashStr);
            if (tailMap.isEmpty()) {
                hashStr = circle.firstKey();
            } else {
                hashStr = tailMap.firstKey();
            }
        }
        //正好命中
        return circle.get(hashStr);
    }

    /**
     * MD5加密算法.
     * @param key 关键字
     * @return 返回md5加密后的字符串
     */
    private static long md5(final String key) {
        MessageDigest md5 = null;
        try {
            md5 = MessageDigest.getInstance("MD5");
            md5.reset();
            md5.update(key.getBytes());
            byte[] bKey = md5.digest();
            final long and = 0xFF;
            final short offset1 = 24;
            final short offset2 = 16;
            final short offset3 = 8;
            final short index = 3;
            long digit1 = (bKey[index] & and) << offset1;
            long digit2 = (bKey[2] & and) << offset2;
            long digit3 = (bKey[1] & and) << offset3;
            long digit4 = (bKey[0] & and);
            long res = digit1 | digit2 | digit3 | digit4;
            return res;
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return 0L;
    }

    /**
     * 获取circle,用于持久化到磁盘中
     * @return
     */
    public synchronized SortedMap<Long,Node> getCircle(){
        return circle;
    }

    /**
     * 获取数据
     * @param circle
     */
    public synchronized void setCircle(SortedMap<Long,Node> circle){
        this.circle = circle;
    }

    /**
     * 该节点是否存在
     * @param node
     * @return
     */
    public synchronized boolean hashNode(Node node){
        long hashStr = hash(node.toString() + salt + 0);
        return circle.containsKey(hashStr);
    }


}
