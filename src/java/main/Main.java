import common.Node;
import common.Tools;
import configuration.QCacheConfiguration;
import core.RaftNode;
import core.cache.CodeNum;
import core.cache.JsonMessage;
import core.cache.Method;
import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.Socket;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
public class Main {
    public static void main(String[] args){
        if (args[0].equals("runServer")){
            runServer();
            return;
        }
        if(args[0].equals("runClient")){
            if(args.length <2)
                runClient("978788");
            else
                runClient(args[1]);
            return;
        }
    }
    /**
     * 获取进程id
     * @return
     */
    private static  int getProcessID() {
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        return Integer.valueOf(runtimeMXBean.getName().split("@")[0])
                .intValue();
    }

    private static void runServer(){
        //把进程id 用于关闭程序
        int pid = getProcessID();
        String path = QCacheConfiguration.getPidFilePath();
        File file = new File(path);
        if(!file.isFile() || !file.exists()){
            try {
                file.createNewFile();
            }catch (IOException ex){
                ex.printStackTrace();
            }
        }else if(file.exists()){
            System.exit(1);
        }
        FileOutputStream out = null;
        try {
            out = new FileOutputStream(file);
            out.write(String.valueOf(pid).getBytes());
        }catch (IOException ex){
            ex.printStackTrace();
        }finally {
            try {
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        RaftNode raftNode = new RaftNode();
        raftNode.init();
    }

    private static void runClient(String args){
        Scanner scanner = new Scanner(System.in);
        while (true){
            System.out.print("QCache>");
            String temp = scanner.nextLine();
            String line  = temp.trim();
            if(line.equals("") || line.equals("\n"))
                continue;
            if(line.equals("exit") || line.equals("q")){
                System.out.println("bye");
                System.exit(0);
            }
            if(checkCommand(line)) {
                doClient(line,args);
            }else{
                printUsage();
            }
        }
    }

    private static void doClient(String command,String args){
        String ip;
        int port;
        try {
            ip = args.split(":")[0];
            port = Integer.valueOf(args.split(":")[1]);
        }catch (Exception ex){
            List<Node> nodes = QCacheConfiguration.getNodeList();
            int index = new Random().nextInt(nodes.size());
            ip = nodes.get(index).getIp();
            port = nodes.get(index).getListenClientPort();
        }
        try {
            Socket socket = new Socket(ip, port);
            OutputStream outputStream = socket.getOutputStream();
            InputStream inputStream = socket.getInputStream();
            outputStream.write(command.getBytes());
            byte [] buffer = new byte[1024 * 2];
            int n = 0;
            StringBuilder stringBuilder = new StringBuilder();
            while ((n = inputStream.read(buffer)) >0){
                stringBuilder.append(new String(buffer,0,n));
            }
            //System.out.println(stringBuilder.toString());
            JsonMessage jsonMessage = JsonMessage.getJsonMessage(stringBuilder.toString());
            int code = jsonMessage.getCode();
            String message = jsonMessage.getData();
            if(code == CodeNum.SUCCESS){
                System.out.println(message);
            }else if(code == CodeNum.ERROR){
                System.out.println("(error)"+ message);
            }else if(code == CodeNum.NIL){
                System.out.println("(nil)" + message);
            }
            socket.close();
            outputStream.close();
            inputStream.close();
    }catch (IOException ex){
            ex.printStackTrace();
        }finally {

        }
    }

    /**
     * 验证命令的合法性
     * @return
     */
    private static boolean checkCommand(String line){
        String temp[] = line.split("\\s+");
        String command = temp[0];
        if(command.equalsIgnoreCase(Method.status)){
            return true;
        }else if(command.equalsIgnoreCase(Method.get) || command.equalsIgnoreCase(Method.del)){
            if(temp.length != 2){
                return false;
            }else
                return true;
        }else if(command.equalsIgnoreCase(Method.set)){
            List<String> setStr = Tools.split(line);
            if(setStr == null)
                return false;
            if(setStr.size() < 3){
                return false;
            }else if(setStr.size() == 4){
                //最后一个是数字，表示过期时间
                try {
                    int num = Integer.valueOf(setStr.get(setStr.size()-1));
                }catch (Exception ex){
                    return false;
                }
                return true;

            }else if(setStr.size() == 3){
                return true;
            }else {
                return  false;
            }
        }else {
            return  false;
        }
    }


    public static void printUsage(){
        String info = "------------------Usage-------------------"+"\n"+
                      "status---查看连接的服务器信息"+"\n"+
                      "set key val [time]添加缓存"+"\n"+
                      "get key---获取缓存数据"+"\n"+
                      "del key---删除缓存数据"+"\n"+
                      "exit or q---退出！"+"\n"+
                      "------------------------------------------";
        System.out.println(info);



    }
}
