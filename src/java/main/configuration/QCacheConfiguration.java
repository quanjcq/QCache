package configuration;

import common.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

/**
 * Created by quan on 2019/4/26
 * 获取集群的配置信息
 */
public class QCacheConfiguration {

    private static final Logger log = LoggerFactory.getLogger(QCacheConfiguration.class);
    private static final String basePath = getBasePath();
    public static final String path = getConfigPath();

    /**
     * 获取集群信息（ip,id,port1,port2）。
     *
     * @return
     */
    public static List<Node> getNodeList() {
        List<Node> list = new ArrayList<Node>();
        try {
            Properties properties = new Properties();
            FileInputStream fileInputStream = new FileInputStream(new File(path));
            properties.load(fileInputStream);
            Enumeration enumeration = properties.propertyNames();
            while (enumeration.hasMoreElements()) {
                String key = (String) enumeration.nextElement();
                if (key.startsWith("server")) {
                    String value = properties.getProperty(key);
                    int myid = Integer.valueOf(key.substring(7));
                    String ip = value.substring(0, value.indexOf(":"));
                    String port = value.substring(value.indexOf(":") + 1);
                    int listenHeartbeartPort = Integer.valueOf(port.split(":")[0]);
                    int listenClientPort = Integer.valueOf(port.split(":")[1]);
                    list.add(new Node(myid,
                            ip,
                            listenHeartbeartPort,
                            listenClientPort
                    ));
                }
            }
            fileInputStream.close();
        } catch (FileNotFoundException ex) {
            log.debug(ex.toString());
            ex.printStackTrace();
        } catch (IOException ex) {
            log.debug(ex.toString());
            ex.printStackTrace();
        }
        return list;
    }

    /**
     * 本节点信息（ip,id,port1,port2）
     *
     * @return
     */
    public static Node getMyNode() {
        try {
            Properties properties = new Properties();
            FileInputStream fileInputStream = new FileInputStream(new File(path));
            properties.load(fileInputStream);
            Enumeration enumeration = properties.propertyNames();
            while (enumeration.hasMoreElements()) {
                String key = (String) enumeration.nextElement();
                if (key.equals("myid")) {
                    int value = Integer.valueOf(properties.getProperty(key));
                    for (Node node : getNodeList()) {
                        if (node.getNodeId() == value) {
                            return node;
                        }
                    }
                }
            }
            fileInputStream.close();
        } catch (FileNotFoundException ex) {
            log.debug(ex.toString());
            ex.printStackTrace();
        } catch (IOException ex) {
            log.debug(ex.toString());
            ex.printStackTrace();
        }
        return null;
    }

    /**
     * RaftLogs 文件路径
     *
     * @return
     */
    public static String getRaftLogsPath() {

        return basePath + "/logs/commit_logs.log";
    }

    /**
     * RaftSnaphot 文件路径
     */
    public static String getRaftSnaphotPath() {
        return basePath + "/snaphot/snaphot";
    }

    /**
     * 配置文件的地址
     */
    public static String getConfigPath() {
        return basePath + "/conf/q.cfg";
    }

    public static String getBasePath() {
        String base = System.getProperty("java.class.path");
        File file = new File(base);
        return file.getParentFile().getParent();

    }

    /**
     * 进程id的文件
     *
     * @return
     */
    public static String getPidFilePath() {
        return basePath + "/logs/pid";
    }

    /**
     * aop 备份的文件路径
     *
     * @return
     */
    public static String getCacheAofPath() {
        return basePath + "/logs/aop.log";
    }

    /**
     * RDB 备份的文件路径
     *
     * @return
     */
    public static String getCacheRdbPath() {
        return basePath + "/snaphot/rdb";
    }

}
