import configuration.QCacheConfiguration;
import org.junit.Test;

public class QCacheConfigurationTest {
    @Test
    public void getNodeListTest() {
        System.out.println(QCacheConfiguration.getNodeList());
    }

    @Test
    public void getMyNodeTest() {
        System.out.println(QCacheConfiguration.getMyNode());
    }
}
