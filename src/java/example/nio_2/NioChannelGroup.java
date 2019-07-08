package nio_2;


import java.nio.channels.SelectionKey;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * 管理所有Channel
 */
public class NioChannelGroup {
    private Map<SelectionKey, NioChannel> channels = new HashMap<SelectionKey, NioChannel>();

    public NioChannelGroup() {

    }

    /**
     * 根据selectionKey 查找NioChannel.
     *
     * @param selectionKey key
     * @return nioChannel
     */
    public NioChannel findChannel(SelectionKey selectionKey) {
        return channels.get(selectionKey);
    }

    /**
     * 关闭所有channel
     */
    public void closeAll() {
        Set<SelectionKey> keys = channels.keySet();
        for (SelectionKey selectionKey : keys) {
            channels.get(selectionKey).close();
            channels.remove(selectionKey);
        }
    }

    /**
     * put.
     *
     * @param selectionKey key.
     * @param nioChannel   channel.
     */
    public void put(SelectionKey selectionKey, NioChannel nioChannel) {
        channels.put(selectionKey, nioChannel);
    }

    /**
     * put.
     *
     * @param nioChannel channel.
     */
    public void put(NioChannel nioChannel) {
        this.put(nioChannel.selectionKey(), nioChannel);
    }

    /**
     * remove.
     *
     * @param selectionKey selectionKey
     */
    public void remove(SelectionKey selectionKey) {
        channels.remove(selectionKey);
    }

    /**
     * remove.
     *
     * @param channel channel
     */
    public void remove(NioChannel channel) {
        this.remove(channel.selectionKey());
    }

    /**
     * contain.
     *
     * @param selectionKey selectionKey.
     * @return boolean.
     */
    public boolean contain(SelectionKey selectionKey) {
        return findChannel(selectionKey) != null;
    }

    /**
     * contain.
     *
     * @param channel nioChannel
     * @return boolean
     */
    public boolean contain(NioChannel channel) {
        return this.contain(channel.selectionKey());
    }
}
