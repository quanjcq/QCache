package core.cache.backup;

import common.QCacheConfiguration;
import core.cache.CacheData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashMap;

public class BackUpRdb implements BackUpI {
    private static Logger log = LoggerFactory.getLogger(BackUpAof.class);
    private static volatile BackUpRdb backUpRdb;
    private String path = QCacheConfiguration.getCacheAofPath();

    private BackUpRdb() {

    }

    /**
     * 获取实例.
     *
     * @return BackUpRdb
     */
    public static BackUpRdb getInstance() {
        if (backUpRdb == null) {
            synchronized (BackUpRdb.class) {
                if (backUpRdb == null) {
                    backUpRdb = new BackUpRdb();
                }
            }
        }
        return backUpRdb;
    }

    /**
     * 加载本地数据到cache
     *
     * @param cache cache
     */
    @Override
    public void loadData(HashMap<String, CacheData> cache) {
        File rdbFile = new File(path);
        if (rdbFile.exists() && rdbFile.length() > 0) {
            try {
                ObjectInputStream obj = new ObjectInputStream(new FileInputStream(rdbFile));
                HashMap<String, CacheData> cacheTemp = (HashMap<String, CacheData>) obj.readObject();
                if (cacheTemp != null) {
                    cache = cacheTemp;
                    cacheTemp = null;
                }
            } catch (IOException e) {
                log.debug(e.toString());
            } catch (ClassNotFoundException e) {
                log.debug(e.toString());
            }
        }
    }


}
