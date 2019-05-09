package core.cache;

/**
 * 目前只处理这些方法吧
 * status:查看服务器状态
 * set key value [time]数据存储 time 为缓存过期时间(ms),没有的话不过期的,有空格的字符串加双引号
 * get key  获取数据
 * del key  删除数据
 */
public class Method {
    public static final  String status = "status";
    public static final  String get = "get";
    public static final  String set = "set";
    public static final  String del = "del";

    public static String[] getMethods(){
        String res[] = {"status","set","get","del"};
        return res;
    }
}
