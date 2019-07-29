package raft;

import common.Node;
import common.UtilAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;


/**
 * 一致性hash算法实现.
 */
//线程安全.
public class ConsistentHash implements StateMachine{
    private static Logger logger = LoggerFactory.getLogger(ConsistentHash.class);
    private final String salt = "quanjcq";

    /**
     * 复制的节点个数.
     */
    private int numberOfReplicas;

    /**
     * 由于一致性hash 虚拟节点的个数比较多,所以Hash 环中不在存Node而是存索引节点
     */
    private Map<Short, Node> nodesMap = new HashMap<Short, Node>();

    /**
     * 一致性Hash环.
     */
    private SortedMap<Long, Short> circle = new TreeMap<Long, Short>();




    /**
     * @param number 复制的节点个数，增加每个节点的复制节点有利于负载均衡
     */
    public ConsistentHash(final int number) {
        this.numberOfReplicas = number;
    }

    /**
     * @param numberOfReplicas 复制的节点个数，增加每个节点的复制节点有利于负载均衡
     * @param nodes            节点对象
     */
    public ConsistentHash(final int numberOfReplicas, final Collection<Node> nodes) {
        this.numberOfReplicas = numberOfReplicas;
        //初始化节点
        for (Node node : nodes) {
            add(node);
            if (nodesMap.get(node.getNodeId()) == null) {
                nodesMap.put(node.getNodeId(), node);
            }
        }
    }

    /**
     * MD5
     *
     * @param key 关键字
     * @return 返回md5加密后的字符串
     */
    private static long md5(final String key) {
        MessageDigest md5;
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
            return digit1 | digit2 | digit3 | digit4;
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return 0L;
    }

    /**
     * hash 算法.
     *
     * @param key 关键字.
     * @return 返回key的hash值
     */
    private Long hash(final Object key) {
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
        if (nodesMap.get(node.getNodeId()) == null) {
            nodesMap.put(node.getNodeId(), node);
            for (int i = 0; i < numberOfReplicas; i++) {
                circle.put(hash(node.toString() + salt + i), node.getNodeId());
            }
        }
    }

    /**
     * 获取服务器个数.
     *
     * @return 返回服务器的个数.
     */
    public synchronized int getSize() {
        return nodesMap.size();
    }

    /**
     * 移除节点的同时移除相应的虚拟节点.
     *
     * @param node 节点对象
     */
    public synchronized void remove(final Node node) {
        if (nodesMap.containsKey(node.getNodeId())) {
            nodesMap.remove(node.getNodeId());
            for (int i = 0; i < numberOfReplicas; i++) {
                circle.remove(hash(node.toString() + salt + i));
            }
        }
    }

    /**
     * 获得一个最近的顺时针节点.
     *
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
            SortedMap<Long, Short> tailMap = circle.tailMap(hashStr);
            if (tailMap.isEmpty()) {
                hashStr = circle.firstKey();
            } else {
                hashStr = tailMap.firstKey();
            }
        }
        //正好命中
        return nodesMap.get(circle.get(hashStr));
    }

    /**
     * 随机获取一个节点.
     *
     * @return Node
     */
    public synchronized Node getRandomNode() {
        Short[] keys = nodesMap.keySet().toArray(new Short[0]);
        Random random = new Random();
        short randomKey = keys[random.nextInt(keys.length)];
        return nodesMap.get(randomKey);
    }

    /**
     * 获取circle,用于持久化到磁盘中
     *
     * @return map
     */
    public synchronized Map<Short, Node> getCircle() {
        return nodesMap;
    }

    /**
     * 指定节点是否存在
     *
     * @param node 节点
     * @return 节点是否存在
     */
    public synchronized boolean hashNode(Node node) {
        return nodesMap.get(node.getNodeId()) != null;
    }

    /**
     * 序列化成字符串.
     * @return
     */
    public synchronized String getNodesStr() {
        StringBuilder builder = new StringBuilder();
        Set<Short> nodeIdSet = nodesMap.keySet();
        int count = 0;
        for (short nodeId : nodeIdSet) {
            Node node = nodesMap.get(nodeId);
            if (count == 0) {
                builder.append(String.format(
                        "%d:%s:%d",
                        node.getNodeId(),
                        node.getIp(),
                        node.getListenClientPort())
                );
            } else {
                builder.append(String.format(
                        "#%d:%s:%d",
                        node.getNodeId(),
                        node.getIp(),
                        node.getListenClientPort())
                );
            }
            count++;
        }
        return builder.toString();
    }

    /**
     * 将hash中的节点的数据依次序列化,对于每个Node序列化的数据:id ip[4] port1 crc32 共2 + 4 + 4 + 4 = 14B
     * @return 一致性hash序列化后的byteBuffer, 错误将会返回null.
     */
    public synchronized ByteBuffer serializedConsistentHash(){
        Set<Short> nodeIdSet = nodesMap.keySet();
        if (nodeIdSet.size() == 0) {
            logger.info("ConsistentHash was empty!");
            return null;
        }
        int totalSize = getSerializedSize();
        ByteBuffer serializedConsistentHash = ByteBuffer.allocate(totalSize);
        for (short nodeId : nodeIdSet) {
            Node node = nodesMap.get(nodeId);
            byte[] ipByte = UtilAll.ipToByte(node.getIp());
            if (ipByte == null) {
                logger.warn("serialize ConsistentHash error");
                return null;
            }
            int position = serializedConsistentHash.position();
            //id
            serializedConsistentHash.putShort(node.getNodeId());
            //ip
            serializedConsistentHash.put(ipByte);
            //port1
            serializedConsistentHash.putInt(node.getListenClientPort());

            //crc32
            int crc32Code = UtilAll.crc32(serializedConsistentHash.array(),position,10);
            serializedConsistentHash.putInt(crc32Code);

        }
        return serializedConsistentHash;
    }

    /**
     * 由于发送数据使用的是ProtoBuf 无法直接发送字节数组,需要将字节数组转换成String
     * 另外一端收到这个String,再转换成字节数组.使用ISO-8859-1编码.
     * 注:比如ASCII 低七位表示字符,会导致高位丢失,再转换回来的就错乱了.
     * @return 序列化的字节数组转化后的字符串.
     */
    public synchronized String getSerializedByteString(){
        ByteBuffer data = serializedConsistentHash();
        //只能用这个ISO-8859-1 形式的变啊,单字节的,其他编码转换回来会有异常
        return new String(data.array(), Charset.forName("ISO-8859-1"));
    }
    /**
     * 序列化ConsistentHash 长度.
     * @return 长度
     */
    public synchronized int getSerializedSize(){
        return nodesMap.size() * 14;
    }

    /**
     * 将序列化的后字节数组,应用到该ConsistentHash
     * @param byteBuffer byteBuffer.
     */
    public synchronized boolean add(ByteBuffer byteBuffer) {
        ByteBuffer bodyBuf = ByteBuffer.allocate(Node.NODE_SERIALIZED_SIZE + 4);
        if (byteBuffer.limit() - byteBuffer.position() < 14) {
            System.out.println("数据太少了");
        }
        while (byteBuffer.limit() - byteBuffer.position() >= Node.NODE_SERIALIZED_SIZE + 4) {
            bodyBuf.clear();
            byteBuffer.get(bodyBuf.array());

            bodyBuf.position(0);
            bodyBuf.limit(bodyBuf.capacity());

            //id
            short id = bodyBuf.getShort();

            //ip
            byte[] ip = new byte[4];
            bodyBuf.get(ip);

            //port
            int listenClientPort = bodyBuf.getInt();

            //crc32 code
            int crc32Code = bodyBuf.getInt();

            //根据数据计算出的crc32 code
            int crc32CodeCaculate = UtilAll.crc32(bodyBuf.array(),0,bodyBuf.capacity() -4);
            if (crc32Code != crc32CodeCaculate) {
                logger.info("RaftStateMachine file destroy!");
                System.out.println("RaftStateMachine file destroy code = " + crc32Code +"caculate = " + crc32CodeCaculate);
                return false;
            }
            Node node = new Node.Builder()
                    .setNodeId(id)
                    .setIp(UtilAll.ipToString(ip))
                    .setListenClientPort(listenClientPort)
                    .setListenHeartbeatPort(listenClientPort - Node.CLIENT_TO_HEART)
                    .build();
            //对于同一个node,重复被添加是没有任何影响的
            logger.info("add node" + node);
            //System.out.println("add node " + node);
            this.add(node);

        }
        return true;
    }

    /**
     * 清除,ConsistentHash.
     */
    public synchronized void clear(){
        circle.clear();
        nodesMap.clear();
    }

    /**
     * addAll
     * @param consistentHash consistent.
     */
    public synchronized void addAll(ConsistentHash consistentHash){
        for(Node node:consistentHash.getCircle().values()) {
            this.add(node);
        }
    }

    public List<Node> getAliveNodes() {
        List<Node> result = new LinkedList<Node>();
        for (Node node:nodesMap.values()) {
            result.add(node);
        }
        return result;
    }

}
