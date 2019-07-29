package client;

import raft.ConsistentHash;
import common.Node;
import common.UtilAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import remote.message.Message;
import remote.message.RemoteMessage;
import remote.message.RemoteMessageType;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public final class CacheClient {
    private static Logger log = LoggerFactory.getLogger(CacheClient.class);
    //维护连接
    private HashMap<Node, ClientSocket> sockets = new HashMap<Node, ClientSocket>();
    private ConsistentHash consistentHash;
    private RemoteMessage remoteMessage = RemoteMessage.getInstance(true);

    private CacheClient(newBuilder builder) {
        if (builder.getNodes().isEmpty()) {
            throw new IllegalArgumentException("必须提供服务器集群信息");
        }

        if (builder.getNumberOfReplicas() <= 0) {
            throw new IllegalArgumentException("必须提供正确的虚拟节点个数");
        }
        consistentHash = new ConsistentHash(builder.getNumberOfReplicas(), builder.getNodes());
    }

    /**
     * 添加一个缓存,没有过期时间.
     *
     * @param key key
     * @param val val
     * @return bool
     */
    public boolean put(String key, String val) {
        return put(key, val, -1);
    }

    /**
     * 添加一个缓存.
     *
     * @param key  key
     * @param val  val
     * @param timeOut 过期时间
     * @return bool
     */
    public boolean put(String key, String val, int timeOut) {
        RemoteMessage response = doPut(key,val,timeOut);
        //log.info(response.toString());
        return response.getMessageType() == RemoteMessage.getTypeByte(RemoteMessageType.OK);
    }

    private RemoteMessage doPut(String key, String val, int timeOut) {
        Node node = consistentHash.get(key);
        ClientSocket channel = getChannel(node);
        if (channel == null || !channel.isActive()) {
            remoteMessage.setMessageType(RemoteMessage.getTypeByte(RemoteMessageType.SERVICE_LOST));
            return remoteMessage;
        }
        remoteMessage.setMessageType(RemoteMessage.getTypeByte(RemoteMessageType.PUT));
        remoteMessage.setVal(val);
        remoteMessage.setKey(key);
        remoteMessage.setTimeOut(timeOut);

        Message obj = channel.write(remoteMessage);
        if (obj == null) {
            log.info("get {} timeOut",key);
            remoteMessage.setMessageType(RemoteMessage.getTypeByte(RemoteMessageType.TIMEOUT));
            return remoteMessage;
        }
        RemoteMessage response =  (RemoteMessage) obj;
        if (response.getMessageType() == RemoteMessage.getTypeByte(RemoteMessageType.SWITCH_NODE)) {
            log.info("key not in this node,and switch node");

            byte[] data = response.getResponse();
            if (data.length == 0) {
                response.setMessageType(RemoteMessage.getTypeByte(RemoteMessageType.SERVICE_LOST));
                return response;
            }
            consistentHash.clear();
            consistentHash.add(ByteBuffer.wrap(data));
            //System.out.println(consistentHash.getNodesStr());
            //尝试发送第二次请求
            node = consistentHash.get(key);
            channel = getChannel(node);
            if (channel == null || !channel.isActive()) {
                remoteMessage.setMessageType(RemoteMessage.getTypeByte(RemoteMessageType.SERVICE_LOST));
                return remoteMessage;
            }

            //重新发送
            obj = channel.write(remoteMessage);

            if (obj == null) {
                log.info("get {} timeOut",key);
                remoteMessage.setMessageType(RemoteMessage.getTypeByte(RemoteMessageType.TIMEOUT));
                return remoteMessage;
            }
            return (RemoteMessage) obj;
        } else {
            return  (RemoteMessage) obj;
        }
    }



    /**
     * 获取一个缓存,失败返回null.
     *
     * @param key key
     * @return string
     */
    public String get(String key) {
        RemoteMessage response = doGet(key);
        log.info(response.toString());
        if (response.getMessageType() == RemoteMessage.getTypeByte(RemoteMessageType.GET_R)) {
            byte[] data = response.getResponse();
            return UtilAll.byte2String(data,"UTF-8");
        }
        return null;
    }

    /**
     * 获取一个缓存
     *
     * @param key key
     * @return ResponseMessage
     */
    private RemoteMessage doGet(String key) {
        Node node = consistentHash.get(key);
        ClientSocket channel = getChannel(node);
        if (channel == null || !channel.isActive()) {
            remoteMessage.setMessageType(RemoteMessage.getTypeByte(RemoteMessageType.SERVICE_LOST));
            return remoteMessage;
        }
        remoteMessage.setMessageType(RemoteMessage.getTypeByte(RemoteMessageType.GET));
        remoteMessage.setKey(key);
        Message obj = channel.write(remoteMessage);
        if (obj == null) {
            log.info("get {} timeOut",key);
            remoteMessage.setMessageType(RemoteMessage.getTypeByte(RemoteMessageType.TIMEOUT));
            return remoteMessage;
        }
        RemoteMessage response =  (RemoteMessage) obj;
        if (response.getMessageType() == RemoteMessage.getTypeByte(RemoteMessageType.SWITCH_NODE)) {
            log.info("key not in this node,and switch node");
            byte[] data = response.getResponse();
            if (data.length == 0) {
                response.setMessageType(RemoteMessage.getTypeByte(RemoteMessageType.SERVICE_LOST));
                return response;
            }
            consistentHash.clear();
            consistentHash.add(ByteBuffer.wrap(data));
            System.out.println(consistentHash.getNodesStr());
            //尝试发送第二次请求
            node = consistentHash.get(key);
            channel = getChannel(node);
            if (channel == null || !channel.isActive()) {
                remoteMessage.setMessageType(RemoteMessage.getTypeByte(RemoteMessageType.SERVICE_LOST));
                return remoteMessage;
            }
            remoteMessage.setMessageType(RemoteMessage.getTypeByte(RemoteMessageType.GET));
            remoteMessage.setKey(key);
            obj = channel.write(remoteMessage);
            if (obj == null) {
                log.info("get {} timeOut",key);
                remoteMessage.setMessageType(RemoteMessage.getTypeByte(RemoteMessageType.TIMEOUT));
                return remoteMessage;
            }
            return (RemoteMessage) obj;
        } else {
            return  (RemoteMessage) obj;
        }


    }

    /**
     * 返回集群状态.
     *
     * @return string
     */
    public String status(String nodeStr) {
        Node node;
        if (nodeStr == null || nodeStr.length() == 0) {
            node = consistentHash.getRandomNode();
        } else {
            node = new CacheClient.newBuilder().getNode(nodeStr);
            if (node == null) {
                //随机选一台主机
                node = consistentHash.getRandomNode();
            }
        }
        ClientSocket channel = getChannel(node);
        if (channel == null || !channel.isActive()) {
            return "service lost";
        }
        remoteMessage.setMessageType(RemoteMessage.getTypeByte(RemoteMessageType.STATUS));
        Message obj = channel.write(remoteMessage);

        if (obj == null) {
            log.info("timeOut");
            return null;
        }
        RemoteMessage response = (RemoteMessage) obj;
        if (response.getMessageType() != RemoteMessage.getTypeByte(RemoteMessageType.STATUS_R)) {
            log.info("message error {}",response);
            return "操作错误";
        } else {
            byte[] data = response.getResponse();
            return UtilAll.byte2String(data,"UTF-8");
        }
    }

    /**
     * 删除一个缓存
     *
     * @param key 需要删除数据的kev
     * @return bool
     */
    public boolean del(String key) {
        RemoteMessage responseMessage = doDel(key);
        log.info(responseMessage.toString());
        return responseMessage.getMessageType() == RemoteMessage.getTypeByte(RemoteMessageType.OK);
    }

    /**
     * 删除一个缓存
     *
     * @param key key
     * @return ResponseMessage
     */
    private RemoteMessage doDel(String key) {
        Node node = consistentHash.get(key);
        ClientSocket channel = getChannel(node);
        if (channel == null || !channel.isActive()) {
            remoteMessage.setMessageType(RemoteMessage.getTypeByte(RemoteMessageType.SERVICE_LOST));
            return remoteMessage;
        }
        remoteMessage.setMessageType(RemoteMessage.getTypeByte(RemoteMessageType.DEL));
        remoteMessage.setKey(key);
        Message obj = channel.write(remoteMessage);
        if (obj == null) {
            log.info("get {} timeOut",key);
            remoteMessage.setMessageType(RemoteMessage.getTypeByte(RemoteMessageType.TIMEOUT));
            return remoteMessage;
        }
        RemoteMessage response =  (RemoteMessage) obj;
        if (response.getMessageType() == RemoteMessage.getTypeByte(RemoteMessageType.SWITCH_NODE)) {
            log.info("key not in this node,and switch node");
            byte[] data = response.getResponse();
            if (data.length == 0) {
                response.setMessageType(RemoteMessage.getTypeByte(RemoteMessageType.SERVICE_LOST));
                return response;
            }
            consistentHash.clear();
            consistentHash.add(ByteBuffer.wrap(data));
            System.out.println(consistentHash.getNodesStr());
            //尝试发送第二次请求
            node = consistentHash.get(key);
            channel = getChannel(node);
            if (channel == null || !channel.isActive()) {
                remoteMessage.setMessageType(RemoteMessage.getTypeByte(RemoteMessageType.SERVICE_LOST));
                return remoteMessage;
            }
            //重新发送
            obj = channel.write(remoteMessage);
            if (obj == null) {
                log.info("get {} timeOut",key);
                remoteMessage.setMessageType(RemoteMessage.getTypeByte(RemoteMessageType.TIMEOUT));
                return remoteMessage;
            }
            return (RemoteMessage) obj;
        } else {
            return  (RemoteMessage) obj;
        }
    }

    /**
     * 获取与对应node的连接ClientSocket.
     *
     * @param node 要连接的节点.
     * @return ClientSocket
     */
    private ClientSocket getChannel(Node node) {
        ClientSocket clientSocket = sockets.get(node);
        if (clientSocket == null || !clientSocket.isActive()) {
            Socket socket = null;
            try {
                socket = new Socket(node.getIp(), node.getListenClientPort());
                socket.setTcpNoDelay(true);
            } catch (IOException e) {
                log.error(e.toString());
            }
            if (socket != null) {
                clientSocket = new ClientSocket.NewBuilder()
                        .setSocket(socket)
                        .setSocketInHandler(new SocketInHandler())
                        .setSocketOutHandler(new SocketOutHandler())
                        .build();
                sockets.put(node, clientSocket);
            }

        }
        return clientSocket;
    }

    /**
     * 关闭连接
     */
    public void close() {
        Set<Node> keys = sockets.keySet();
        for (Node key : keys) {
            sockets.get(key).close();
        }
    }

    public static class newBuilder {
        private Set<Node> nodes = new TreeSet<Node>();
        private int numberOfReplicas;

        public newBuilder setNewNode(String line) {
            nodes.add(getNode(line));
            return this;
        }

        public CacheClient build() {
            return new CacheClient(this);
        }

        private Set<Node> getNodes() {
            return nodes;
        }

        private int getNumberOfReplicas() {
            return numberOfReplicas;
        }

        public newBuilder setNumberOfReplicas(int num) {
            this.numberOfReplicas = num;
            return this;
        }

        private Node getNode(String line) throws IllegalArgumentException {
            if (line == null || line.length() == 0) {
                throw new IllegalArgumentException(line + "节点参数错误");
            }
            //id:ip:port1
            String[] temp = line.split(":");
            if (temp.length != 3) {
                throw new IllegalArgumentException(line + "节点参数错误");
            }
            Short id;
            if (illegalId(temp[0])) {
                id = Short.valueOf(temp[0]);
            } else {
                throw new IllegalArgumentException(line + "节点参数错误");
            }

            String ip = "";
            if (isIpAddress(temp[1])) {
                ip = temp[1];
            } else {
                throw new IllegalArgumentException(ip + "节点ip错误");
            }
            int heartPort;
            int listenClient;

            if (isPort(temp[2])) {
                listenClient = Integer.valueOf(temp[2]);
                heartPort = listenClient -1000;
            } else {
                throw new IllegalArgumentException(line + "端口设置错误ip错误");
            }

            return new Node.Builder()
                    .setNodeId(id)
                    .setIp(ip)
                    .setListenHeartbeatPort(heartPort)
                    .setListenClientPort(listenClient)
                    .build();
        }

        /**
         * id 是否合法
         *
         * @param id node id是否合法
         * @return bool
         */
        private boolean illegalId(String id) {
            if (id == null || id.length() == 0 || id.length() > 5) {
                return false;
            }
            int res = 0;
            for (int i = 0, len = id.length(); i < len; i++) {
                if (id.charAt(i) <= '9' && id.charAt(i) >= '0') {
                    res = res * 10 + id.charAt(i) - '0';
                } else {
                    return false;
                }
            }
            return res <= Short.MAX_VALUE;
        }

        /**
         * ip 是否合法
         *
         * @param ip ip地址
         * @return bool
         */
        private boolean isIpAddress(String ip) {
            String regex = "(((2[0-4]\\d)|(25[0-5]))|(1\\d{2})|([1-9]\\d)|(\\d))[.](((2[0-4]\\d)|(25[0-5]))|(1\\d{2})|([1-9]\\d)|(\\d))[.](((2[0-4]\\d)|(25[0-5]))|(1\\d{2})|([1-9]\\d)|(\\d))[.](((2[0-4]\\d)|(25[0-5]))|(1\\d{2})|([1-9]\\d)|(\\d))";
            Pattern p = Pattern.compile(regex);
            Matcher m = p.matcher(ip);
            return m.matches();
        }

        /**
         * 端口是否合法
         *
         * @return bool
         */
        private boolean isPort(String port) {
            if (port == null || port.length() == 0 || port.length() > 5) {
                return false;
            }
            int res = 0;
            for (int i = 0, len = port.length(); i < len; i++) {
                if (port.charAt(i) <= '9' && port.charAt(i) >= '0') {
                    res = res * 10 + port.charAt(i) - '0';
                } else {
                    return false;
                }
            }
            return res <= 0x0000FFFF;
        }
    }

    private class SocketInHandler extends SocketInProtoBufHandler<RemoteMessage> {

        @Override
        public RemoteMessage read(Socket socket) {
            return super.read(socket);
        }
    }

    private class SocketOutHandler extends SocketOutProtoBufHandler<RemoteMessage> {
        @Override
        public void write(Socket socket, RemoteMessage msg) {
            super.write(socket, msg);
        }
    }

}
