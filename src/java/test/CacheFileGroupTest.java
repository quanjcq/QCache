import org.junit.Test;
import store.CacheFileGroup;

public class CacheFileGroupTest {
    @Test
    public void cacheTest(){
        String dirPath = "/home/jcq/store/cache/";
        //单文件1G
        int fileSize = 1024 * 1024 * 10;
        CacheFileGroup cacheFileGroup = new CacheFileGroup(dirPath,fileSize);
        StringBuilder builder = new StringBuilder();
        for (int i = 0;i< 29;i++) {
            builder.append("0123456789abcdefghijklmnopqrstuvwxyz");
        }
        long start = System.currentTimeMillis();
        //put name
        /*for (int i = 0;i<1000;i++) {
            cacheFileGroup.put("name" + i,builder.toString(),-1);
        }*/

        for (int i = 0;i< 1000;i++) {
            String val = cacheFileGroup.get("name" + i);
            //System.out.println(val);
            if (val == null) {
                System.out.println(i);
            }
        }
        System.out.println(cacheFileGroup.get("name0"));
        System.out.println("total cost:  " + (System.currentTimeMillis() - start) +" (ms)");
    }
    @Test
    public void cacheDelTest(){
        String dirPath = "/home/jcq/store/cache/";
        int fileSize = 1024 * 1024 * 10;
        CacheFileGroup cacheFileGroup = new CacheFileGroup(dirPath,fileSize);
        //put name quan
        cacheFileGroup.put("name","quan",- 1);
        cacheFileGroup.del("name");

        assert  cacheFileGroup.get("name") == null;
    }

    @Test
    public void cacheRebulidTest() {
        String dirPath = "/home/jcq/store/cache/";
        int fileSize = 1024 * 1024 * 10;
        CacheFileGroup cacheFileGroup = new CacheFileGroup(dirPath,fileSize);
        StringBuilder builder = new StringBuilder();
        for (int i = 0;i< 29;i++) {
            builder.append("0123456789abcdefghijklmnopqrstuvwxyz");
        }

        for (int i = 0;i<10000;i++) {
            cacheFileGroup.put("name" + i,builder.toString(),-1);
        }

        //重建,可以修复之前删除的
        cacheFileGroup.rebulid();
        cacheFileGroup.put("quan","quan",-1);
        System.out.println(cacheFileGroup.get("name0"));
        cacheFileGroup.del("name0");
        cacheFileGroup.setRebulidEffective();

        cacheFileGroup.put("sex","男",-1);
        System.out.println(cacheFileGroup.get("sex"));
        System.out.println(cacheFileGroup.get("quan"));
        System.out.println(cacheFileGroup.get("name0"));

    }

    @Test
    public void cacheRebulidTest2() {
        String dirPath = "/home/jcq/store/cache/";
        int fileSize = 1024 * 1024 * 10;
        CacheFileGroup cacheFileGroup = new CacheFileGroup(dirPath,fileSize);
        for (int i = 0;i<10000;i++) {
            assert cacheFileGroup.get("name" + i) != null;
        }
        System.out.println(cacheFileGroup.get("name9999"));
        System.out.println(cacheFileGroup.getWriteSize());
    }
}
