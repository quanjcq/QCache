import org.junit.Test;
import store.AofLogService;
import store.message.AofLogMessage;

public class AofLogServiceTest {
    @Test
    public void aofLogTest(){
        String filePath = "/home/jcq/store/001";
        int fileSize = 1024 * 1024;
        AofLogService aofLogService = new AofLogService(filePath,fileSize);
        AofLogMessage aofLogMessage = AofLogMessage.getPutAofInstance("quan2","basds",-1);
        aofLogMessage = AofLogMessage.getDelAofInstance("quan2");
        aofLogService.appendMessage(aofLogMessage);
        int position = 0;
        while (aofLogService.getAofMessage(aofLogService.getMappedFile().getMappedByteBuffer(),position) != null) {
            System.out.println(aofLogMessage);
            position += aofLogMessage.getSerializedSize();
        }
    }
}
