package core.cache;

import java.io.Serializable;
import java.util.LinkedList;

public class CacheDataList extends CacheData implements Serializable {
    public LinkedList<CacheData> val = new LinkedList<CacheData>();


}
