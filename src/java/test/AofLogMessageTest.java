import org.junit.Test;
import store.message.AofLogMessage;

import java.nio.ByteBuffer;

public class AofLogMessageTest {
    @Test
    public void encodeDecodeTest(){
        AofLogMessage aofLogMessage = AofLogMessage.getPutAofInstance("name","quan",-1);
        System.out.println(aofLogMessage);
        byte[] data = aofLogMessage.encode();

        AofLogMessage aofLogMessage1 = AofLogMessage.getInstance().decode(ByteBuffer.wrap(data));
        System.out.println(aofLogMessage1);
    }
}
