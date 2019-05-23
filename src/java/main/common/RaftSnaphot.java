package common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.TreeMap;

/**
 * Created by quan on 2019/4/25
 * 持久化和恢复
 */
public class RaftSnaphot {
    private static final Logger log = LoggerFactory.getLogger(RaftSnaphot.class);
    /**
     * 存放RaftSnaphot 文件的路径
     */
    private String path;

    public RaftSnaphot(String path) {
        this.path = path;
    }

    /**
     * 获取snaphost
     *
     * @return
     */
    public synchronized TreeMap<Long, Node> getRaftSnaphot() {
        try {
            File file = new File(path);
            if (file.isDirectory() || !file.exists()) {
                file.createNewFile();
                return null;
            }
            if (file.length() == 0) {
                return null;
            }
            FileInputStream fileInputStream = new FileInputStream(new File(path));
            ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
            TreeMap<Long, Node> treeMap = (TreeMap<Long, Node>) objectInputStream.readObject();
            return treeMap;
        } catch (IOException ex) {
            ex.printStackTrace();
            log.debug(ex.toString());
        } catch (ClassNotFoundException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    /**
     * 持久化对象到磁盘
     *
     * @param circle
     */
    public synchronized void store(TreeMap<Long, Node> circle) throws IOException {
        File file = new File(path);
        if (file.isDirectory() || !file.exists()) {
            file.createNewFile();
        }
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(
                new FileOutputStream(file));
        objectOutputStream.writeObject(circle);
    }
}
