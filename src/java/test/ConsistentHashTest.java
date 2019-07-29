import common.Node;
import common.UtilAll;
import org.junit.Test;
import raft.ConsistentHash;

import java.nio.ByteBuffer;

public class ConsistentHashTest {
    @Test
    public void serializedConsistentHash(){
        ConsistentHash consistentHash = new ConsistentHash(1);
        for (int i = 0;i<3;i++) {
            Node node = new Node.Builder()
                    .setNodeId((short)(i + 1))
                    .setIp("127.0.0.1")
                    .setListenClientPort(9000 + i)
                    .setListenHeartbeatPort(8000 + i)
                    .build();
            consistentHash.add(node);
        }
        String data = consistentHash.getSerializedByteString();

        consistentHash.clear();
        System.out.println(consistentHash.getNodesStr());
        consistentHash.add(ByteBuffer.wrap(UtilAll.string2Byte(data,"ISO-8859-1")));
        System.out.println(consistentHash.getNodesStr());
    }
}
