package core.cache.backup;

import common.QCacheConfiguration;
import constant.CacheOptions;
import core.cache.CacheData;
import core.cache.CacheDataInt;
import core.cache.CacheDataString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;


public class BackUpAof implements BackUpI {
    private static Logger log = LoggerFactory.getLogger(BackUpAof.class);
    private static volatile BackUpAof backUpAof;
    private String path = QCacheConfiguration.getCacheAofPath();

    private BackUpAof() {

    }

    /**
     * 获取实例.
     *
     * @return backUpAof
     */
    public static BackUpAof getInstance() {
        if (backUpAof == null) {
            synchronized (BackUpAof.class) {
                if (backUpAof == null) {
                    backUpAof = new BackUpAof();
                }
            }
        }
        return backUpAof;
    }

    /**
     * 向aof文件中追加多条日志.
     * @param logs 多条日志
     * @param cache 数据
     */
    public synchronized void appendAofLogs(List<String> logs, HashMap<String,CacheData> cache) {
        long time = new Date().getTime();
        //String line = time + ";" + command;
        File file = new File(path);

        if (!file.exists() || file.isDirectory()) {
            try {
                boolean flag = file.createNewFile();
                if (!flag) {
                    throw new RuntimeException("没有写文件权限");
                }
            } catch (IOException e) {
                log.error(e.toString());
            }
        }
        long len = file.length();
        if (len >= CacheOptions.maxAofLogSize) {
            File rdbFile = new File(QCacheConfiguration.getCacheRdbPath());

            if (!rdbFile.exists() || rdbFile.isDirectory()) {
                try {
                    boolean flag = rdbFile.createNewFile();
                    if (!flag) {
                        throw new RuntimeException("没有创建文件权限");
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            ObjectOutputStream out = null;
            try {
                out = new ObjectOutputStream(new FileOutputStream(rdbFile));
                out.writeObject(cache);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (out != null) {
                        out.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            //清空日志文件
            clearAofFile();

        }
        len = file.length();
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rwd");
            randomAccessFile.seek(len);
            for(String line:logs) {
                randomAccessFile.writeBytes(line + "\n");
            }

            randomAccessFile.close();
        } catch (IOException ex) {
            log.debug(ex.toString());
        }
    }

    /**
     * 向aof 文件中添加一条日志.
     *
     * @param command 日志内容
     * @param cache   数据
     */
    public synchronized void appendAofLog(String command, HashMap<String, CacheData> cache) {
        long time = new Date().getTime();
        String line = time + ";" + command;
        File file = new File(path);

        if (!file.exists() || file.isDirectory()) {
            try {
                boolean flag = file.createNewFile();
                if (!flag) {
                    throw new RuntimeException("没有写文件权限");
                }
            } catch (IOException e) {
                log.error(e.toString());
            }
        }
        long len = file.length();
        if (len >= CacheOptions.maxAofLogSize) {
            File rdbFile = new File(QCacheConfiguration.getCacheRdbPath());

            if (!rdbFile.exists() || rdbFile.isDirectory()) {
                try {
                    boolean flag = rdbFile.createNewFile();
                    if (!flag) {
                        throw new RuntimeException("没有创建文件权限");
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            ObjectOutputStream out = null;
            try {
                out = new ObjectOutputStream(new FileOutputStream(rdbFile));
                out.writeObject(cache);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (out != null) {
                        out.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            //清空日志文件
            clearAofFile();

        }
        len = file.length();
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rwd");
            randomAccessFile.seek(len);
            randomAccessFile.writeBytes(line + "\n");
            randomAccessFile.close();
        } catch (IOException ex) {
            log.debug(ex.toString());
        }

    }

    /**
     * 系统启动的时候,解析出日志文件,并应用于cache.
     */
    @Override
    public synchronized void loadData(HashMap<String, CacheData> cache) {
        File aofFile = new File(path);
        if (!aofFile.exists() || aofFile.isDirectory()) {
            try {
                boolean flag = aofFile.createNewFile();
                if (!flag) {
                    throw new RuntimeException("没有写权限");
                }
            } catch (IOException ex) {
                System.exit(1);
            }
        } else {
            try {
                FileReader fileReader = new FileReader(aofFile);
                BufferedReader bufferedReader = new BufferedReader(fileReader);
                String logLine;
                while ((logLine = bufferedReader.readLine()) != null) {
                    String command = getCommand(logLine);
                    int index1 = command.indexOf(' ');
                    int index2 = command.lastIndexOf(' ');
                    if (index1 == index2) {
                        //del
                        String key = command.substring(index1 + 1);
                        cache.remove(key);
                    } else {
                        //set
                        String temp = command.substring(index1 + 1, index2);
                        int index3 = temp.indexOf(' ');
                        String key = temp.substring(0, index3);
                        String val = temp.substring(index3 + 1);
                        long last = Long.valueOf(command.substring(index2 + 1));
                        //不要用异常控制程序流程
                        try {
                            int data = Integer.valueOf(val);
                            CacheData cacheData = new CacheDataInt(data, new Date().getTime(), last);

                            cache.put(key, cacheData);
                        } catch (Exception ex) {
                            CacheData cacheData = new CacheDataString(val, new Date().getTime(), last);
                            cache.put(key, cacheData);
                        }
                    }
                }
                fileReader.close();
                bufferedReader.close();
            } catch (IOException ex) {
                ex.printStackTrace();
                System.exit(1);
            }
        }
    }

    private String getCommand(String line) {
        return line.substring(line.indexOf(";") + 1);
    }

    /**
     * 清空日志文件
     */
    private synchronized void clearAofFile() {
        File file = new File(path);
        try {
            if (!file.exists()) {
                boolean flag = file.createNewFile();
                if (!flag) {
                    throw new RuntimeException("没有写文件权限");
                }
            }
            FileWriter fileWriter = new FileWriter(file);
            fileWriter.write("");
            fileWriter.flush();
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
