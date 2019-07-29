import common.UtilAll;
import org.junit.Test;
import remote.message.RemoteMessage;
public class UtilAllTest {

    /**
     * 测试ip跟byte数组相互转换是否正常.
     */
    @Test
    public void ipToByteTest() {
        String ip = "127.255.0.254";
        byte[] bytes = UtilAll.ipToByte(ip);
        assert ip.equals(UtilAll.ipToString(bytes));

    }
    @Test
    public void int2ByteTest(){
        for (int i = 0;i <= 255;i++) {
            assert UtilAll.byteToInt(UtilAll.intToByte(i)) == i;
        }
    }

    @Test
    public void testAll(){
        StringBuilder builder = new StringBuilder();
        for (int i = 0;i< 29;i++) {
            builder.append("0123456789abcdefghijklmnopqrstuvwxyz");
        }
        System.out.println(UtilAll.byte2String(UtilAll.string2Byte(builder.toString(),"UTF-8"),"UTF-8"));
    }

}
