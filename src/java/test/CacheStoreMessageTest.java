import org.junit.Test;
import store.message.CacheStoreMessage;

import java.nio.ByteBuffer;

public class CacheStoreMessageTest {
    @Test
    public void cacheStoreTest(){
        String key = "name";
        String val = "quan";
        int storeTime = 111;
        int timeOut = 222;
        boolean createNew = true;
        CacheStoreMessage cacheStoreMessage = CacheStoreMessage.getInstance(key,val,storeTime,timeOut,createNew);

        byte[] serializedData = cacheStoreMessage.encode();
        CacheStoreMessage cacheStoreMessage1 = cacheStoreMessage.decode(ByteBuffer.wrap(serializedData));
        cacheStoreMessage1.setVisit((short) 12);
        System.out.println(cacheStoreMessage);
        System.out.println(cacheStoreMessage1);
        assert cacheStoreMessage != cacheStoreMessage1;

    }

    @Test
    public void cacheStoreSingleTest(){
        String key = "name";
        String val = "quan";
        int storeTime = 111;
        int timeOut = 222;
        boolean createNew = false;
        CacheStoreMessage cacheStoreMessage = CacheStoreMessage.getInstance(key,val,storeTime,timeOut,false);
        byte[] serializedData = cacheStoreMessage.encode();
        System.out.println(cacheStoreMessage);
        CacheStoreMessage cacheStoreMessage1 = cacheStoreMessage.decode(ByteBuffer.wrap(serializedData));
        cacheStoreMessage1.setVisit((short) 12);
        System.out.println(cacheStoreMessage1);


        CacheStoreMessage cacheStoreMessage3 = CacheStoreMessage.getInstance(key,val,storeTime,timeOut,false);
        System.out.println(cacheStoreMessage3);
        assert cacheStoreMessage == cacheStoreMessage1;

    }

}
