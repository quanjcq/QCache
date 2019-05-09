import core.message.RaftHeartMessage;
import core.message.RaftMessage;
import org.junit.Test;

import java.io.*;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.util.Scanner;
public class NioTest {
    @Test
    public void CacheServerTest(){
        try {
            Socket socket = new Socket("127.0.0.1", 9091);
            OutputStream outputStream = socket.getOutputStream();
            InputStream inputStream = socket.getInputStream();
            String temp = "hello";
            System.out.println(temp);
            outputStream.write(temp.getBytes());
            byte [] buffer = new byte[1024 * 2];
            int n = 0;
            StringBuilder stringBuilder = new StringBuilder();
            while ((n = inputStream.read(buffer)) >0){
                System.out.println(new String(buffer,0,n));
                stringBuilder.append(new String(buffer,0,n));
            }
            System.out.println(stringBuilder.toString());
            socket.close();
            outputStream.close();
            inputStream.close();
        }catch (IOException ex){
            ex.printStackTrace();
        }finally {

        }
    }
}
