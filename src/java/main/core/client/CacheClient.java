package core.client;

import com.google.protobuf.MessageLite;
import common.ConsistentHash;
import common.Node;
import core.message.UserMessageProto;

import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public final class CacheClient {
    //维护连接
    private HashMap<Node, ClientSocket> sockets = new HashMap<Node, ClientSocket>();
    private ConsistentHash consistentHash;

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
     * 添加一个缓存.
     *
     * @param key  key
     * @param val  val
     * @param time 过期时间
     * @return bool
     */
    public boolean set(String key, String val, int time) {
        UserMessageProto.ResponseMessage responseMessage = doSet(key, val, time);
        if (responseMessage != null) {
            return responseMessage.getResponseType() == UserMessageProto.ResponseType.SUCCESS;
        }
        return false;
    }

    public UserMessageProto.ResponseMessage doSet(String key, String val, int time) {
        UserMessageProto.ResponseMessage responseMessage;
        if (key == null || key.length() == 0) {
            responseMessage = UserMessageProto.ResponseMessage
                    .newBuilder()
                    .setResponseType(UserMessageProto.ResponseType.ERROR)
                    .setVal("key 不能为空")
                    .build();
            return responseMessage;
        }

        if (val == null || val.length() == 0) {
            responseMessage = UserMessageProto.ResponseMessage
                    .newBuilder()
                    .setResponseType(UserMessageProto.ResponseType.ERROR)
                    .setVal("val 不能为空")
                    .build();
            return responseMessage;
        }

        Node node = consistentHash.get(key);
        ClientSocket channel = getChannel(node);
        if (channel == null || !channel.isActive()) {
            responseMessage = UserMessageProto.ResponseMessage
                    .newBuilder()
                    .setResponseType(UserMessageProto.ResponseType.ERROR)
                    .setVal("网络异常")
                    .build();
            return responseMessage;
        }
        UserMessageProto.UserMessage userMessage = UserMessageProto.UserMessage
                .newBuilder()
                .setMessageType(UserMessageProto.MessageType.SET)
                .setSetMessage(UserMessageProto.SetMessage
                        .newBuilder()
                        .setKey(key)
                        .setVal(val)
                        .setLastTime(time)
                        .build()
                )
                .build();
        Object msg = channel.write(userMessage);
        if (msg instanceof UserMessageProto.UserMessage) {
            UserMessageProto.UserMessage message = (UserMessageProto.UserMessage) msg;
            if (message.getResponseMessage().getResponseType() == UserMessageProto.ResponseType.SWITCH) {
                //System.out.println("重新连接新节点");
                //System.out.println(new CacheClient.newBuilder().getNode(message.getResponseMessage().getVal()));
                Node newNode = new CacheClient.newBuilder().getNode(message.getResponseMessage().getVal());
                consistentHash.add(newNode);
                channel = getChannel(newNode);
                if (channel == null || !channel.isActive()) {
                    responseMessage = UserMessageProto.ResponseMessage
                            .newBuilder()
                            .setResponseType(UserMessageProto.ResponseType.ERROR)
                            .setVal("网络异常")
                            .build();
                    return responseMessage;
                }
                msg = channel.write(userMessage);
                if (msg instanceof UserMessageProto.UserMessage) {
                    message = (UserMessageProto.UserMessage) msg;
                    return message.getResponseMessage();
                } else {
                    responseMessage = UserMessageProto.ResponseMessage
                            .newBuilder()
                            .setResponseType(UserMessageProto.ResponseType.ERROR)
                            .setVal("数据异常")
                            .build();
                    return responseMessage;
                }
            } else {
                return message.getResponseMessage();
            }
        }
        responseMessage = UserMessageProto.ResponseMessage
                .newBuilder()
                .setResponseType(UserMessageProto.ResponseType.ERROR)
                .setVal("数据异常")
                .build();
        return responseMessage;
    }

    /**
     * 添加一个缓存,没有过期时间.
     *
     * @param key key
     * @param val val
     * @return bool
     */
    public boolean set(String key, String val) {
        return set(key, val, -1);
    }

    /**
     * 获取一个缓存
     *
     * @param key key
     * @return string
     */
    public String get(String key) {
        UserMessageProto.ResponseMessage responseMessage = doGet(key);
        if (responseMessage == null) {
            return "";
        }
        UserMessageProto.ResponseType type = responseMessage.getResponseType();
        if (type == UserMessageProto.ResponseType.NIL) {
            //key 不存在
            return "";
        } else if (type == UserMessageProto.ResponseType.NOT_READ) {
            //不可读
            return "";
        } else if (type == UserMessageProto.ResponseType.ERROR) {
            //错误,一般都是网络错误
            return "";

        } else if (type == UserMessageProto.ResponseType.SUCCESS) {
            return responseMessage.getVal();
        }
        return "";
    }

    /**
     * 获取一个缓存
     *
     * @param key key
     * @return ResponseMessage
     */
    public UserMessageProto.ResponseMessage doGet(String key) {
        UserMessageProto.ResponseMessage responseMessage = UserMessageProto.ResponseMessage
                .newBuilder()
                .setResponseType(UserMessageProto.ResponseType.ERROR)
                .setVal("Error")
                .build();
        if (key == null || key.length() == 0) {
            responseMessage = UserMessageProto.ResponseMessage
                    .newBuilder()
                    .setResponseType(UserMessageProto.ResponseType.ERROR)
                    .setVal("key 不能为空")
                    .build();
            return responseMessage;
        }

        Node node = consistentHash.get(key);
        //System.out.println(node);
        ClientSocket channel = getChannel(node);
        if (channel == null || !channel.isActive()) {
            responseMessage = UserMessageProto.ResponseMessage
                    .newBuilder()
                    .setResponseType(UserMessageProto.ResponseType.ERROR)
                    .setVal("网络异常")
                    .build();
            return responseMessage;
        }
        UserMessageProto.UserMessage userMessage = UserMessageProto.UserMessage
                .newBuilder()
                .setMessageType(UserMessageProto.MessageType.GET)
                .setGetMessage(UserMessageProto.GetMessage
                        .newBuilder()
                        .setKey(key)
                        .build()
                )
                .build();

        Object msg = channel.write(userMessage);

        if (msg == null) {
            responseMessage = UserMessageProto.ResponseMessage
                    .newBuilder()
                    .setResponseType(UserMessageProto.ResponseType.ERROR)
                    .setVal("网络超时")
                    .build();
            return responseMessage;
        } else if (msg instanceof UserMessageProto.UserMessage) {
            UserMessageProto.UserMessage message = (UserMessageProto.UserMessage) msg;
            //System.out.println("msg = " + msg);
            if (message.getResponseMessage().getResponseType() == UserMessageProto.ResponseType.SWITCH) {
                //重现连接新节点
                Node newNode = new CacheClient.newBuilder().getNode(message.getResponseMessage().getVal());
                consistentHash.add(newNode);
                channel = getChannel(newNode);
                if (channel == null || !channel.isActive()) {
                    responseMessage = UserMessageProto.ResponseMessage
                            .newBuilder()
                            .setResponseType(UserMessageProto.ResponseType.ERROR)
                            .setVal("网络异常")
                            .build();
                    return responseMessage;
                }
                msg = channel.write(userMessage);
                if (msg == null) {
                    responseMessage = UserMessageProto.ResponseMessage
                            .newBuilder()
                            .setResponseType(UserMessageProto.ResponseType.ERROR)
                            .setVal("网络超时")
                            .build();
                    return responseMessage;
                } else if (msg instanceof UserMessageProto.UserMessage) {
                    message = (UserMessageProto.UserMessage) msg;
                    return message.getResponseMessage();
                }
            } else {
                return message.getResponseMessage();
            }
            return message.getResponseMessage();
        }
        return responseMessage;
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
        UserMessageProto.UserMessage userMessage = UserMessageProto.UserMessage
                .newBuilder()
                .setMessageType(UserMessageProto.MessageType.STATUS)
                .setStatusMessage(UserMessageProto.StatusMessage.newBuilder().build())
                .build();
        Object msg = channel.write(userMessage);
        if (msg instanceof UserMessageProto.UserMessage) {
            UserMessageProto.UserMessage message = (UserMessageProto.UserMessage) msg;
            return message.getResponseMessage().getVal();
        } else {
            return "网络超时";
        }
    }

    /**
     * 删除一个缓存
     *
     * @param key 需要删除数据的kev
     * @return bool
     */
    public boolean del(String key) {
        UserMessageProto.ResponseMessage responseMessage = doDel(key);
        return responseMessage.getResponseType() == UserMessageProto.ResponseType.SUCCESS;
    }

    /**
     * 删除一个缓存
     *
     * @param key key
     * @return ResponseMessage
     */
    public UserMessageProto.ResponseMessage doDel(String key) {
        UserMessageProto.ResponseMessage responseMessage = UserMessageProto.ResponseMessage
                .newBuilder()
                .setResponseType(UserMessageProto.ResponseType.ERROR)
                .setVal("Error")
                .build();
        if (key == null || key.length() == 0) {
            responseMessage = UserMessageProto.ResponseMessage
                    .newBuilder()
                    .setResponseType(UserMessageProto.ResponseType.ERROR)
                    .setVal("key 不能为空")
                    .build();
            return responseMessage;
        }

        Node node = consistentHash.get(key);
        ClientSocket channel = getChannel(node);
        if (channel == null || !channel.isActive()) {
            responseMessage = UserMessageProto.ResponseMessage
                    .newBuilder()
                    .setResponseType(UserMessageProto.ResponseType.ERROR)
                    .setVal("网络异常")
                    .build();
            return responseMessage;
        }
        UserMessageProto.UserMessage userMessage = UserMessageProto.UserMessage
                .newBuilder()
                .setMessageType(UserMessageProto.MessageType.DEL)
                .setDelMessage(UserMessageProto.DelMessage
                        .newBuilder()
                        .setKey(key)
                        .build()
                )
                .build();
        Object msg = channel.write(userMessage);
        if (msg == null) {
            responseMessage = UserMessageProto.ResponseMessage
                    .newBuilder()
                    .setResponseType(UserMessageProto.ResponseType.ERROR)
                    .setVal("网络超时")
                    .build();
            return responseMessage;
        } else if (msg instanceof UserMessageProto.UserMessage) {
            UserMessageProto.UserMessage message = (UserMessageProto.UserMessage) msg;
            if (message.getResponseMessage().getResponseType() == UserMessageProto.ResponseType.SWITCH) {
                Node newNode = new CacheClient.newBuilder().getNode(message.getResponseMessage().getVal());
                consistentHash.add(newNode);
                channel = getChannel(newNode);
                if (channel == null || !channel.isActive()) {
                    responseMessage = UserMessageProto.ResponseMessage
                            .newBuilder()
                            .setResponseType(UserMessageProto.ResponseType.ERROR)
                            .setVal("网络异常")
                            .build();
                    return responseMessage;
                }
                msg = channel.write(userMessage);
                if (msg == null) {
                    responseMessage = UserMessageProto.ResponseMessage
                            .newBuilder()
                            .setResponseType(UserMessageProto.ResponseType.ERROR)
                            .setVal("网络超时")
                            .build();
                    return responseMessage;
                } else if (msg instanceof UserMessageProto.UserMessage) {
                    message = (UserMessageProto.UserMessage) msg;
                    return message.getResponseMessage();
                }
            } else {
                return message.getResponseMessage();
            }
        }
        return responseMessage;
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
                //System.out.println("创建新的连接" + (count++));
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (socket != null) {
                clientSocket = new ClientSocket.newBuilder()
                        .setSocket(socket)
                        .setSocketInHandler(new SocketInHandler(UserMessageProto.UserMessage.getDefaultInstance()))
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
            //id:ip:port1:port2
            String[] temp = line.split(":");
            if (temp.length != 4) {
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

            if (isPort(temp[2]) && isPort(temp[3])) {
                heartPort = Integer.valueOf(temp[2]);
                listenClient = Integer.valueOf(temp[3]);
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

    private class SocketInHandler extends SocketInProtoBufHandler<UserMessageProto.UserMessage> {
        private SocketInHandler(MessageLite messageLite) {
            super(messageLite);
        }

        @Override
        public Object read(Socket socket) {
            return super.read(socket);
        }
    }

    private class SocketOutHandler extends SocketOutProtoBufHandler<UserMessageProto.UserMessage> {
        @Override
        public void write(Socket socket, UserMessageProto.UserMessage msg) {
            super.write(socket, msg);
        }
    }

}
