package recycle;

/**
 * 之前没想到,没有提供访问的记录,目前没能实现
 */
public class MarkLRU implements Mark {
    @Override
    public int mark() {
        return 0;
    }
}
