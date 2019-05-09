package log;

import java.io.*;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import configuration.QCacheConfiguration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by quan on 2019/4/25
 * 负责解析raft日志信息
 * 日子存储格式：一行一条日志的形式
 * Timestamp=112121212;index=12;term=12;command=
 */
public class LogParse {
    private static Logger log = LoggerFactory.getLogger(LogParse.class);
    private static final String path = QCacheConfiguration.getRaftLogsPath();
    /**
     * 解析一行日志
     * @param LogLine
     * @return
     */
    public static RaftLog parseOneLog(String LogLine){
        String temp[] = LogLine.split(";");
        long timestamp = Integer.valueOf(temp[0].split("=")[1]);
        long index = Integer.valueOf(temp[1].split("=")[1]);
        long term = Integer.valueOf(temp[2].split("=")[1]);
        String command = temp[3].split("=")[1];
        RaftLog raftLog = new RaftLog();
        raftLog.setCommand(command);
        raftLog.setIndex(index);
        raftLog.setTerm(term);
        raftLog.setTimestamp(timestamp);
        return raftLog;
    }

    /**
     * 读取logs中所有已经提交的日志
     * @return
     */
    public static synchronized List<RaftLog> getRaftLogs(){
        return getNewRaftLogs(-1,-1);
    }

    /**
     * 获取提交日志中的最后一条日志
     * @return
     */
    public static synchronized RaftLog getLastLog(){
        RaftLog raftLog = null;
        File file  = new File(path);
        if(!file.exists() || file.isDirectory()){
            log.debug("file {} is not exist",path);
            return null;
        }
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
            randomAccessFile.seek(getLastLinePos(file));
            String line = randomAccessFile.readLine();
            if(line == null){
                log.debug("file {} is empty",path);
                return null;
            }
            raftLog = parseOneLog(line);
            randomAccessFile.close();

        } catch (FileNotFoundException ex) {
            log.debug(ex.toString());
            ex.printStackTrace();
        } catch (IOException ex){
            ex.printStackTrace();
            log.debug(ex.toString());
        }
        return raftLog;
    }

    /**
     * 找到日志文件的最后一行position
     * 注意文件为空或者文件只有一行的时候
     * @param file
     * @return
     */
    private static synchronized long getLastLinePos(File file) {
        long lastLinePos = 0L;
        RandomAccessFile randomAccessFile;
        if(file.length() == 0){
            log.debug("log {} is empty",file.getName());
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
            log.debug(e.toString());
            e.printStackTrace();
        }
        return ++lastLinePos;
    }

    /**
     *
     * @param line
     */
    public static synchronized void  insertLog(String line){
        File file = new File(path);
        Long position = file.length();
        if(!file.exists() || file.isDirectory()){
            log.debug("file {} is not exist",path);
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file,"rwd");
            randomAccessFile.seek(position);
            randomAccessFile.writeBytes(line+"\n");
            randomAccessFile.close();
        }catch (IOException ex){
            log.debug(ex.toString());
            ex.printStackTrace();
        }
    }

    /**
     * 追加这一条日志
     * @param raftLog
     */
    public static synchronized void  insertLog(RaftLog raftLog){
        String line = "Timestamp=" + raftLog.getTimestamp()+
                ";index=" + raftLog.getIndex()+
                ";term=" + raftLog.getTerm() +
                ";command=" + raftLog.getCommand();
        insertLog(line);
    }
    /**
     * 获取比这新的日志
     * @param index
     * @param term
     * @return
     */
    public static synchronized List<RaftLog> getNewRaftLogs(long term,long index){
        List<RaftLog> listLogs = new LinkedList<RaftLog>();
        File LogFile = new File(path);
        if(!LogFile.exists() || LogFile.isDirectory()) {
            log.info("file {} not exist ",path);
            try {
                LogFile.createNewFile();
            }catch (IOException ex){
                log.debug(ex.toString());
                System.exit(1);
            }
        }else{
            try {
                FileReader fileReader = new FileReader(LogFile);
                BufferedReader bufferedReader = new BufferedReader(fileReader);
                String logLine;
                while ((logLine = bufferedReader.readLine()) != null){
                    RaftLog raftLog = LogParse.parseOneLog(logLine);
                    if(raftLog.getTerm() > term
                            || (raftLog.getIndex() > index && raftLog.getTerm() == term))
                        listLogs.add(LogParse.parseOneLog(logLine));
                }
                fileReader.close();
                bufferedReader.close();
            }catch (IOException ex){
                log.debug(ex.toString());
                ex.printStackTrace();
            }
        }
        return listLogs;
    }

    public static synchronized List<RaftLog> getNewRaftLogs(long term,long index,long lastAppliedTerm,long lastAppliedIndex){
        if(term > lastAppliedTerm
                ||(term == lastAppliedTerm && index >= lastAppliedIndex))
            return new LinkedList<RaftLog>();
        else
            return getNewRaftLogs(term,index);
    }

    /**
     * 清空日志文件
     */
    public static synchronized void clearLogs(){
        clearFile(path);
    }

    /**
     * 清空snaphot 文件
     */
    public static synchronized void clearSnaphotFile(){
        clearFile(QCacheConfiguration.getRaftSnaphotPath());
    }

    /**
     * 清空文件内容
     * @param path
     */
    private static synchronized void clearFile(String path){
        File file =new File(path);
        try {
            if(!file.exists()) {
                file.createNewFile();
            }
            FileWriter fileWriter =new FileWriter(file);
            fileWriter.write("");
            fileWriter.flush();
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 基于 两个日志的index,term 比较那个日志更新
     * @return
     */
    public static int compare(long termA,long indexA,long termB,long indexB){
        if(termA == termB && indexA == indexB){
            return 0;
        }else if(termA > termB
                ||(termA == termB && indexA > indexB) ){
            return 1;
        }else{
            return -1;
        }
    }

}
