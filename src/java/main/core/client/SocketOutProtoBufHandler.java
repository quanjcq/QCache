package core.client;

import com.google.protobuf.MessageLite;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

public abstract class SocketOutProtoBufHandler<T extends MessageLite> implements SocketOutHandler<T> {
    @Override
    public void write(Socket socket, T msg) {
        try {
            OutputStream outputStream = socket.getOutputStream();
            int bodyLen = msg.getSerializedSize();
            if (bodyLen > Short.MAX_VALUE) {
                throw new IllegalArgumentException("msg length not bigger than  " + Short.MAX_VALUE);
            }


            byte[] head = (shortToBytes((short) bodyLen));
            byte[] body = msg.toByteArray();
            byte[] content = new byte[head.length + body.length];
            System.arraycopy(head, 0, content, 0, head.length);
            System.arraycopy(body, 0, content, 2, bodyLen);
            outputStream.write(content);
            outputStream.flush();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private byte[] shortToBytes(short val) {
        byte[] bytes = new byte[2];
        bytes[0] = (byte) (val >> 8);
        bytes[1] = (byte) val;
        return bytes;
    }
}
