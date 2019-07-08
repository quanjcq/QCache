import common.Node;
import common.QCacheConfiguration;
import common.Tools;
import constant.CacheOptions;
import core.RaftNode;
import core.cache.Method;
import core.client.CacheClient;
import core.message.UserMessageProto;

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
                    System.out.println("创建文件失败");
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
        RaftNode raftNode = new RaftNode();
        raftNode.init();
    }

    private static void runClient() {
        Scanner scanner = new Scanner(System.in);
        CacheClient.newBuilder cacheBuilder = new CacheClient.newBuilder();
        cacheBuilder = cacheBuilder.setNumberOfReplicas(CacheOptions.numberOfReplicas);
        List<Node> nodes = QCacheConfiguration.getNodeList();
        for (Node node : nodes) {
            cacheBuilder = cacheBuilder.setNewNode(node.toString());
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
            UserMessageProto.ResponseMessage responseMessage = client.doGet(list.get(1));
            System.out.println("(" + responseMessage.getResponseType() + ") " + responseMessage.getVal());
        } else if (command.equalsIgnoreCase(Method.set)) {
            UserMessageProto.ResponseMessage responseMessage;
            if (list.size() == 3) {
                responseMessage = client.doSet(list.get(1), list.get(2), -1);
            } else {
                responseMessage = client.doSet(list.get(1), list.get(2), Integer.valueOf(list.get(3)));
            }
            System.out.println("(" + responseMessage.getResponseType() + ") " + responseMessage.getVal());
        } else if (command.equalsIgnoreCase(Method.del)) {
            UserMessageProto.ResponseMessage responseMessage;
            responseMessage = client.doDel(list.get(1));
            System.out.println("(" + responseMessage.getResponseType() + ") " + responseMessage.getVal());
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
        } else if (command.equalsIgnoreCase(Method.set)) {
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
                "set key val [time]添加缓存" + "\n" +
                "get key---获取缓存数据" + "\n" +
                "del key---删除缓存数据" + "\n" +
                "exit or q---退出！" + "\n" +
                "------------------------------------------";
        System.out.println(info);
    }
}
