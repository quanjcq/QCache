import org.junit.Test;
import remote.message.RemoteMessage;
import remote.message.RemoteMessageType;

import java.nio.ByteBuffer;

public class RemoteMessageTest {
    @Test
    public void remoteMessageTest(){
        RemoteMessage remoteMessage = RemoteMessage.getInstance(false);
        remoteMessage.setMessageType(RemoteMessage.getTypeByte(RemoteMessageType.PUT));
        remoteMessage.setTimeOut(-1);
        remoteMessage.setKey("name");
        remoteMessage.setVal("quan");
        System.out.println(remoteMessage);
        //序列化
        byte[] body = remoteMessage.encode();

        //反序列化
        System.out.println(RemoteMessage.decode(ByteBuffer.wrap(body),false));
    }
}
