package core.cache.backup;

import configuration.QCacheConfiguration;
import core.cache.CacheDataI;

import java.io.*;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class BackUpAof implements BackUpI {
    private static String path = QCacheConfiguration.getCacheAofPath();

    public synchronized static void appendAofLog(String command, ConcurrentHashMap<String, CacheDataI> cache) {
        long time = new Date().getTime();
        String line = time + ";" + command;
        File file = new File(path);

        if (!file.exists() || file.isDirectory()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        Long len = file.length();
        if (len >= aofMaxSize) {
            clearAofFile();
            File rdbFile = new File(QCacheConfiguration.getCacheRdbPath());

            if (!rdbFile.exists() || rdbFile.isDirectory()) {
                try {
                    rdbFile.createNewFile();
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
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
        len = file.length();
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rwd");
            randomAccessFile.seek(len);
            randomAccessFile.writeBytes(line + "\n");
            randomAccessFile.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }

    }

    /**
     * 解析出日志文件的所有操作数据
     *
     * @return
     */
    public synchronized static List<String> getCommands() {
        List<String> res = new LinkedList<String>();
        File aofFile = new File(path);
        if (!aofFile.exists() || aofFile.isDirectory()) {
            try {
                aofFile.createNewFile();
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
                    res.add(command);
                }
                fileReader.close();
                bufferedReader.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        return res;
    }

    private static String getCommand(String line) {
        return line.substring(line.indexOf(";") + 1);
    }

    //清空文件
    private static synchronized void clearAofFile() {
        File file = new File(path);
        try {
            if (!file.exists()) {
                file.createNewFile();
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
