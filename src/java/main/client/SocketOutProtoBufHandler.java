package client;

import remote.message.Message;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

public abstract class SocketOutProtoBufHandler<T extends Message> implements SocketOutHandler<T> {
    @Override
    public void write(Socket socket, T msg) {
        try {
            OutputStream outputStream = socket.getOutputStream();
            int bodyLen = msg.getSerializedSize();
            if (bodyLen > Short.MAX_VALUE) {
                throw new IllegalArgumentException("msg length not bigger than  " + Short.MAX_VALUE);
            }
            if (bodyLen <= 0) {
                throw  new RuntimeException("data serialized error");
            }
            byte[] body = msg.encode();

            outputStream.write(body);
            outputStream.flush();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

}
