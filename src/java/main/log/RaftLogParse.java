package log;

import java.io.*;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by quan on 2019/4/25
 * 负责解析raft日志信息
 * 日子存储格式：一行一条日志的形式
 * Timestamp=112121212;index=12;command=add id:ip:listHeartPort:listClientPort
 */
public class RaftLogParse {
    private String path;

    public RaftLogParse(String path) {
        this.path = path;
    }

    /**
     * 解析一条日志.
     *
     * @param log 一条日志
     * @return 日志对象
     */
    public RaftLog parseOneLog(String log) {
        if (log == null) {
            return null;
        }
        String temp[] = log.split(";");
        if (temp.length < 3) {
            return null;
        }
        long timestamp = Long.valueOf(temp[0].split("=")[1]);
        long index = Long.valueOf(temp[1].split("=")[1]);
        String command = temp[2].split("=")[1];
        RaftLog raftLog = new RaftLog();
        raftLog.setCommand(command);
        raftLog.setIndex(index);
        raftLog.setTimestamp(timestamp);
        return raftLog;
    }

    /**
     * 字符串中解析log,每条log是用'#' 这个字符分开的.
     *
     * @param str str
     * @return list
     */
    public List<RaftLog> getLogsFromStr(String str) {
        List<RaftLog> res = new LinkedList<RaftLog>();
        if (str == null || str.length() == 0) {
            return res;
        }
        String[] temp = str.split("#");
        for (String logLine : temp) {
            RaftLog raftLog = this.parseOneLog(logLine);
            res.add(raftLog);
        }
        return res;
    }

    public String getLogsStr(List<RaftLog> logs) {
        StringBuilder builder = new StringBuilder();
        int count = 0;
        for (RaftLog log : logs) {
            if (count == 0) {
                builder.append(String.format(
                        "Timestamp=%d;index=%d;command=%s",
                        log.getTimestamp(),
                        log.getIndex(),
                        log.getCommand())
                );
            } else {
                builder.append(String.format(
                        "#Timestamp=%d;index=%d;command=%s",
                        log.getTimestamp(),
                        log.getIndex(),
                        log.getCommand())
                );
            }
            count++;
        }
        return builder.toString();
    }

    /**
     * 读取日志中所有日志.
     *
     * @return list
     */
    public List<RaftLog> getRaftLogs() {
        return getNewRaftLogs(-1);
    }

    /**
     * 读取比这新的日志条目
     *
     * @param index index
     * @return 返回日志列表
     */
    public List<RaftLog> getNewRaftLogs(long index) {
        List<RaftLog> listLogs = new LinkedList<RaftLog>();
        File LogFile = new File(path);
        if (!LogFile.exists() || LogFile.isDirectory()) {
            try {
                boolean flag = LogFile.createNewFile();
                if (!flag) {
                    throw new RuntimeException("没有写文件权限");
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        } else {
            try {
                FileReader fileReader = new FileReader(LogFile);
                BufferedReader bufferedReader = new BufferedReader(fileReader);
                String logLine;
                while ((logLine = bufferedReader.readLine()) != null) {
                    RaftLog raftLog = this.parseOneLog(logLine);
                    if (raftLog.getIndex() > index) {
                        listLogs.add(raftLog);
                    }
                }
                fileReader.close();
                bufferedReader.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        return listLogs;
    }

    /**
     * 清空日志文件.
     */
    public void clearFile() {
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

    /**
     * 找到文件的最后一行.
     *
     * @param file file
     * @return long
     */
    private long getLastLinePos(File file) {
        long lastLinePos = 0L;
        RandomAccessFile randomAccessFile;
        if (file.length() == 0) {
            return 0;
        }
        try {
            randomAccessFile = new RandomAccessFile(file, "r");
            long len = randomAccessFile.length();
            if (len > 0L) {
                long pos = len - 1;
                while (pos > 0) {
                    pos--;
                    randomAccessFile.seek(pos);
                    if (randomAccessFile.readByte() == '\n') {
                        lastLinePos = pos;
                        break;
                    }
                }
                if (pos == 0)
                    return 0;
            }
            randomAccessFile.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ++lastLinePos;
    }

    /**
     * 向文件末尾插入一行数据.
     *
     * @param line str
     */
    private void insertLog(String line) {
        File file = new File(path);
        long position = file.length();
        if (!file.exists() || file.isDirectory()) {
            try {
                boolean flag = file.createNewFile();
                if (!flag) {
                    throw new RuntimeException("没有写文件权限");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rwd");
            randomAccessFile.seek(position);
            randomAccessFile.writeBytes(line + "\n");
            randomAccessFile.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * 追加日志.
     *
     * @param raftLog log
     */
    public void insertLog(RaftLog raftLog) {
        String line = "Timestamp=" + raftLog.getTimestamp() +
                ";index=" + raftLog.getIndex() +
                ";command=" + raftLog.getCommand();
        this.insertLog(line);
    }

    /**
     * 获取日志文件中的最后一行日志
     *
     * @return raftLog
     */
    public RaftLog getLastLog() {
        RaftLog raftLog = null;
        File file = new File(path);
        if (!file.exists() || file.isDirectory()) {
            return null;
        }
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
            randomAccessFile.seek(getLastLinePos(file));
            String line = randomAccessFile.readLine();
            if (line == null) {
                return null;
            }
            raftLog = parseOneLog(line);
            randomAccessFile.close();

        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return raftLog;
    }
}
