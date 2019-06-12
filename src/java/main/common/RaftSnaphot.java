package common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by quan on 2019/4/25
 * 持久化和恢复
 * Single Thread 上运行不用加锁
 */
public class RaftSnaphot {
    private static Logger log = LoggerFactory.getLogger(RaftSnaphot.class);
    /**
     * 存放RaftSnaphot 文件的路径
     */
    private String path;

    public RaftSnaphot(String path) {
        this.path = path;
    }

    /**
     * 获取snaphost.
     *
     * @return 返回一个以nodeId 为key的 Node 值未val的map. 不存在的时候返回一个空的map,而不是null
     */
    public HashMap<Short, Node> getRaftSnaphot() {
        HashMap<Short, Node> res = new HashMap<Short, Node>();
        FileInputStream fileInputStream = null;
        ObjectInputStream objectInputStream = null;
        try {
            File file = new File(path);
            if (file.isDirectory() || !file.exists()) {
                boolean flag = file.createNewFile();
                if (!flag) {
                    System.exit(1);
                }
            }
            fileInputStream = new FileInputStream(new File(path));
            objectInputStream = new ObjectInputStream(fileInputStream);
            Object temp = objectInputStream.readObject();
            if (file.length() > 0 && temp == null) {
                file.delete();
            }
            if (temp instanceof HashMap) {
                res = (HashMap<Short, Node>) temp;
            }
        } catch (IOException ex) {
            log.debug(ex.toString());
        } catch (ClassNotFoundException ex) {
            log.debug(ex.toString());
        } finally {
            try {
                if (fileInputStream != null) {
                    fileInputStream.close();
                }
                if (objectInputStream != null) {
                    objectInputStream.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return res;
    }

    /**
     * 持久化数据到磁盘中
     */
    public void raftSnaphotStore(Map<Short, Node> map) {
        File file = new File(path);
        if (!file.exists() || file.isDirectory()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        ObjectOutputStream objectOutputStream = null;
        try {
            objectOutputStream = new ObjectOutputStream(
                    new FileOutputStream(file));
            objectOutputStream.writeObject(map);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (objectOutputStream != null) {
                try {
                    objectOutputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
