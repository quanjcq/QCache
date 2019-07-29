import cache.CacheServer;
import client.CacheClient;
import common.Node;
import common.QCacheConfiguration;
import common.Tools;
import constant.CacheOptions;
import raft.ConsistentHash;
import recycle.MarkExpire;
import recycle.RecycleService;
import store.AofLogService;
import store.CacheFileGroup;
import store.RaftLogMessageService;
import store.RaftStateMachineService;
import store.checkpoint.CheckPoint;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.List;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        if (args[0].equals("runServer")) {
            runServer();
            return;
        }
        if (args[0].equals("runClient")) {
            runClient();
        }
    }

    /**
     * 获取进程id.
     *
     * @return pid
     */
    private static int getProcessID() {
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        return Integer.valueOf(runtimeMXBean.getName().split("@")[0]);
    }

    private static void runServer() {
        //把进程id 用于关闭程序
        int pid = getProcessID();
        String path = QCacheConfiguration.getPidFilePath();
        File file = new File(path);
        if (!file.isFile() || !file.exists()) {
            try {
                boolean flag = file.createNewFile();
                if (!flag) {
                    System.out.println("create pid file error");
                    System.exit(1);
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        } else if (file.exists()) {
            System.exit(1);
        }
        FileOutputStream out = null;
        try {
            out = new FileOutputStream(file);
            out.write(String.valueOf(pid).getBytes());
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            try {
                if (out != null) {
                    out.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        List<Node> nodes = QCacheConfiguration.getNodeList();
        Node myNode = QCacheConfiguration.getMyNode();
        ConsistentHash consistentHash = new ConsistentHash(CacheOptions.numberOfReplicas);

        //checkPoint
        CheckPoint checkPoint = new CheckPoint(QCacheConfiguration.getCheckPointPath());

        //raftLog
        int raftLogFileSize = 1024 * 1024; //1m
        RaftLogMessageService raftLogMessageService = new RaftLogMessageService(QCacheConfiguration.getRaftLogsPath(),raftLogFileSize);

        //state machine
        int raftStateFileSize = 1024 * 1024;//1m
        RaftStateMachineService raftStateMachineService = new RaftStateMachineService(raftStateFileSize,QCacheConfiguration.getRaftSnaphotPath(),consistentHash);
        raftLogMessageService.setRaftStateMachineService(raftStateMachineService);

        CacheServer server = new CacheServer();

        //缓存文件
        int cacheFileSize = 1024 * 1024 * 1024;
        CacheFileGroup cacheFileGroup = new CacheFileGroup(QCacheConfiguration.getCacheFilesPath(),cacheFileSize);

        //recycle
        RecycleService recycleService = new RecycleService(new MarkExpire(cacheFileGroup),
                cacheFileGroup,
                server.getCanWrite(),
                server.getCanRead());

        int cacheAofLogSize = 1024 * 1204;
        AofLogService aofLogService = new AofLogService(QCacheConfiguration.getCacheAofPath(),cacheAofLogSize);


        //自身节点
        server.setMyNode(myNode);
        server.setNodes(nodes);
        //一致性hash
        server.setConsistentHash(consistentHash);
        server.setCacheFileGroup(cacheFileGroup);
        server.setCheckPoint(checkPoint);
        server.setRecycleService(recycleService);
        server.setAofLogService(aofLogService);
        server.setRaftLogMessageService(raftLogMessageService);
        server.setRaftStateMachineService(raftStateMachineService);

        //启动server
        server.start();


    }

    private static void runClient() {
        Scanner scanner = new Scanner(System.in);
        CacheClient.newBuilder cacheBuilder = new CacheClient.newBuilder();
        cacheBuilder = cacheBuilder.setNumberOfReplicas(CacheOptions.numberOfReplicas);
        List<Node> nodes = QCacheConfiguration.getNodeList();
        for (Node node : nodes) {
            cacheBuilder = cacheBuilder.setNewNode(node.getNodeId() +":"+ node.getIp() + ":" + node.getListenClientPort());
        }
        CacheClient cacheClient = cacheBuilder.build();
        while (true) {
            System.out.print("QCache>");
            String temp = scanner.nextLine();
            String line = temp.trim();
            if (line.equals("") || line.equals("\n"))
                continue;
            if (line.equals("exit") || line.equals("q")) {
                System.out.println("bye");
                cacheClient.close();
                System.exit(0);
            }
            if (checkCommand(line)) {
                doClient(cacheClient, line);
            } else {
                printUsage();
            }
        }
    }

    private static void doClient(CacheClient client, String line) {
        List<String> list = Tools.split(line);
        if (list == null || list.size() == 0) {
            System.out.println("SYNTAX_ERROR");
            return;
        }
        String command = list.get(0);
        if (command.equalsIgnoreCase(Method.status)) {
            if (list.size() >= 2) {
                System.out.println(client.status(list.get(1)));
            } else {
                System.out.println(client.status(null));
            }
        } else if (command.equalsIgnoreCase(Method.get)) {
            String val = client.get(list.get(1));
            if (val == null) {
                System.out.println("(NIL) key not exist");
            } else {
                System.out.println(val);
            }
        } else if (command.equalsIgnoreCase(Method.put)) {
            boolean res;
            if (list.size() == 3) {
                res = client.put(list.get(1), list.get(2), -1);
            } else {
                res = client.put(list.get(1), list.get(2), Integer.valueOf(list.get(3)));
            }
            System.out.println(res ? "OK" : "error!");
        } else if (command.equalsIgnoreCase(Method.del)) {
            boolean res = client.del(list.get(1));
            System.out.println(res ? "OK" : "error!");
        } else {
            System.out.println("SYNTAX_ERROR");
        }
    }

    /**
     * 验证命令的合法性
     *
     * @return bool
     */
    private static boolean checkCommand(String line) {
        String temp[] = line.split("\\s+");
        String command = temp[0];
        if (command.equalsIgnoreCase(Method.status)) {
            return true;
        } else if (command.equalsIgnoreCase(Method.get) || command.equalsIgnoreCase(Method.del)) {
            return temp.length == 2;
        } else if (command.equalsIgnoreCase(Method.put)) {
            List<String> setStr = Tools.split(line);
            if (setStr == null)
                return false;
            if (setStr.size() < 3) {
                return false;
            } else if (setStr.size() == 4) {
                //最后一个是数字，表示过期时间
                //不要用异常做流程控制
                try {
                    Integer.valueOf(setStr.get(setStr.size() - 1));
                } catch (Exception ex) {
                    return false;
                }
                return true;

            } else {
                return setStr.size() == 3;
            }
        } else {
            return false;
        }
    }

    //usage info
    private static void printUsage() {
        String info = "------------------Usage-------------------" + "\n" +
                "status [id:ip:port1:port2]-查看服务器信息" + "\n" +
                "put key val [time]添加缓存" + "\n" +
                "get key---获取缓存数据" + "\n" +
                "del key---删除缓存数据" + "\n" +
                "exit or q---退出！" + "\n" +
                "------------------------------------------";
        System.out.println(info);
    }
    private static class Method{
        public static String status = "status";
        public static String get = "get";
        public static String put = "put";
        public static String del ="del";
    }
}
