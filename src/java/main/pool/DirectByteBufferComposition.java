package pool;

//非线程安全
public class DirectByteBufferComposition {

    //每个ByteBuffer长度
    private int size = 512;

    private int position = 0;

    private int limit = 0;

    private int cap = 0;

    /**
     * 写入byte
     * @param data data.
     */
    public void putByte(byte data){

    }

    /**
     * 写入short.
     * @param data data.
     */
    public void putShort(short data){

    }

    /**
     * 写入int.
     * @param data data.
     */
    public void putInt(int data){

    }

    /**
     * 写入long
     * @param data data
     */
    public void putLong(long data){

    }

    /**
     * 写入bytes
     * @param bytes bytes
     */
    public void putBytes(byte[] bytes){

    }


    /**
     * 将所有空间放回池中
     */
    public void free() {

    }

    /**
     * 扩容.
     * @param size size
     */
    public void expansion(int size){

    }
}
