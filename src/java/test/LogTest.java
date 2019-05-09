import constant.RaftOptions;
import core.message.RaftHeartMessage;
import core.message.RaftMessage;
import core.message.RaftVoteMessage;
import log.LogParse;
import log.RaftLog;
import org.junit.Test;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class LogTest {
    @Test
    public void LogParseTest(){

        RaftLog raftLog  = LogParse.parseOneLog("Timestamp=112121212;index=12;term=12;command=set key value");
        System.out.println(raftLog.toString());
    }
    @Test
    public void getLastLogTest(){
        System.out.println(LogParse.getLastLog());
    }

    @Test
    public void socketTest(){

        try {
            RaftHeartMessage raftMessage = new RaftHeartMessage();
            raftMessage.setId(7);
            raftMessage.setCurrentTerm(0);
            raftMessage.setLastAppendedTerm(-1);
            raftMessage.setLastAppendedIndex(-1);
            Socket socket = new Socket("127.0.0.1", 10000);
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.writeObject(raftMessage);
            System.out.println(12233);
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
            RaftMessage message = (RaftMessage) in.readObject();
            System.out.println(message.getId());
            System.out.println(message);
        }catch (IOException ex){
            ex.printStackTrace();
        }catch (ClassNotFoundException ex){
            ex.printStackTrace();
        }




    }

    @Test
    public void finallyTest(){
        //测试下 线程被中断,finally 会不会执行
        try{
            Thread.currentThread().interrupt();
            //System.exit(0);
        }finally {
            System.out.println("12121");
        }


    }

}
