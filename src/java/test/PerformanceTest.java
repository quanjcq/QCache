import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class PerformanceTest {
    @Test
    public void getTest() {
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            try {
                Socket socket = new Socket("127.0.0.1", 9092);
                OutputStream outputStream = socket.getOutputStream();
                InputStream inputStream = socket.getInputStream();
                String temp = "get test";
                byte[] buffer = new byte[1024];
                int n = 0;
                StringBuilder stringBuilder = new StringBuilder();
                while ((n = inputStream.read(buffer)) > 0) {
                    stringBuilder.append(new String(buffer, 0, n));
                }
                socket.close();
                outputStream.close();
                inputStream.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }

        long end = System.currentTimeMillis();
        System.out.println(10000 * 1000 / (end - start));
    }
}
