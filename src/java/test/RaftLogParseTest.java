import log.RaftLog;
import log.RaftLogParse;
import org.junit.Test;

public class RaftLogParseTest {
    @Test
    public void parseOneLogTest(){
        String logStr = "Timestamp=112121212;index=12;command=add 2:127.0.0.1:8080:9090";
        RaftLogParse logParse = new RaftLogParse("");
        RaftLog raftLog = logParse.parseOneLog(logStr);
        System.out.println(raftLog);
    }
    @Test
    public void getRaftLogsTest() {
        String path = "/home/jcq/IdeaProjects/Cache/src/log";
        RaftLogParse logParse = new RaftLogParse(path);
        System.out.println(logParse.getRaftLogs());

    }
    @Test
    public void getLastLogTest() {
        String path = "/home/jcq/IdeaProjects/Cache/src/log";
        RaftLogParse logParse = new RaftLogParse(path);
        System.out.println(logParse.getLastLog());
    }
    @Test
    public void getNewLogsTest() {
        String path = "/home/jcq/IdeaProjects/Cache/src/log";
        RaftLogParse logParse = new RaftLogParse(path);
        System.out.println(logParse.getNewRaftLogs(3));
    }
    @Test
    public void insertLogTest() {
        String path = "/home/jcq/IdeaProjects/Cache/src/log";
        RaftLogParse logParse = new RaftLogParse(path);
        String raftLog = "Timestamp=112121212;index=7;command=add 7:127.0.0.1:8080:9090";
        //logParse.insertLog(raftLog);
        logParse.insertLog(logParse.parseOneLog(raftLog));
    }

}
