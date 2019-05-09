import core.cache.JsonMessage;
import org.junit.Test;

public class JsonMessageTest {
    @Test
    public void  toStringTest(){
        System.out.println(new JsonMessage(12,"val"));
    }

    @Test
    public void constructTest(){
        System.out.println(JsonMessage.getJsonMessage(new JsonMessage(12,"val").toString()));
    }
}
