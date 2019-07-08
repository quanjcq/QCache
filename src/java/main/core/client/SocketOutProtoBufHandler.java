package core.client;

import com.google.protobuf.MessageLite;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

public abstract class SocketOutProtoBufHandler<T extends MessageLite> implements SocketOutHandler<T> {
    private int bufferSize = 1024;
    private byte[] buffer = new byte[bufferSize];
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
            int contentLen = bodyLen + 2;
            if (contentLen > bufferSize) {
                bufferSize = contentLen;
                buffer = new byte[bufferSize];
            }
            System.arraycopy(head, 0, buffer, 0, 2);
            System.arraycopy(body, 0, buffer, 2, bodyLen);
            outputStream.write(buffer,0,contentLen);
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
