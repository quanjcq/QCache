import common.Node;
import common.RaftSnaphot;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class RaftSnaphotTest {

    @Test
    public void getRaftSnaphotTest() {
        //测试文件不存在,为空,文件被破坏是否能正确返回
        String path = "/home/jcq/IdeaProjects/Cache/src/snaphot";
        RaftSnaphot raftSnaphot = new RaftSnaphot(path);
        assert  raftSnaphot.getRaftSnaphot().size() == 0;
    }

    @Test
    public void raftSnaphotStoreTest() {
        //写测试
        String path = "/home/jcq/IdeaProjects/Cache/src/snaphot";
        HashMap<Short, Node> map = new HashMap<Short, Node>();
        map.put((short)1,new Node.Builder()
                .setNodeId((short)1)
                .setIp("127.0.0.1")
                .setListenClientPort(1024)
                .setListenHeartbeatPort(1025)
                .build()
        );
        RaftSnaphot raftSnaphot = new RaftSnaphot(path);
        raftSnaphot.raftSnaphotStore(map);


    }
}
