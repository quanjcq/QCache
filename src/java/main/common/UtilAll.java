package common;

import constant.RaftOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.nio.ByteBuffer;
import java.util.concurrent.*;
import java.util.zip.CRC32;

public class UtilAll {
    private static Logger logger = LoggerFactory.getLogger(UtilAll.class);
    /**
     * 线程池
     */
    private static volatile ThreadPoolExecutor threadPoolExecutor = null;

    /**
     * 执行定时任务的线程.
     */
    private static volatile ScheduledExecutorService scheduledExecutorService = null;

    /**
     * 获取一个线程池实例.(单例)
     *
     * @return 线程池对象实例.
     */
    public static ThreadPoolExecutor getThreadPool() {
        if (threadPoolExecutor == null) {
            synchronized (UtilAll.class) {
                if (threadPoolExecutor == null) {
                    threadPoolExecutor = new ThreadPoolExecutor(RaftOptions.coreThreadNum,
                            RaftOptions.maxThreadNum,
                            60,
                            TimeUnit.MILLISECONDS,
                            new LinkedBlockingQueue<Runnable>()
                    );
                }
            }
        }

        return threadPoolExecutor;
    }

    /**
     * get执行定时任务的线程.
     * @return ScheduledExecutorService
     */
    public static ScheduledExecutorService getScheduledExecutorService() {
        if (scheduledExecutorService == null) {
            synchronized (UtilAll.class) {
                if (scheduledExecutorService == null) {
                    scheduledExecutorService = Executors.newScheduledThreadPool(1);
                }
            }
        }
        return scheduledExecutorService;
    }

    /**
     * 获取cpu 核心数.
     *
     * @return cpu核心数.
     */
    public static int getProcessNum() {
        return Runtime.getRuntime().availableProcessors();
    }

    /**
     * 获取总内存大小.
     *
     * @return int.
     */
    public static long getTotalMemorySize() {
        long physicalTotal = 0L;
        OperatingSystemMXBean osmxb = ManagementFactory.getOperatingSystemMXBean();
        if (osmxb instanceof com.sun.management.OperatingSystemMXBean) {
            physicalTotal = ((com.sun.management.OperatingSystemMXBean) osmxb).getTotalPhysicalMemorySize();
        }

        return physicalTotal;
    }

    /**
     * 将127.0.0.1字符串形式的ip转换成长度为4的字节数组
     *
     * @param ip ip
     * @return byte[4], 若是错误字符串返回null.
     */
    public static byte[] ipToByte(String ip) {
        String[] temp = ip.split("\\.");
        if (temp.length != 4) {
            return null;
        }
        ByteBuffer result = ByteBuffer.allocate(4);
        int ip1 = Integer.valueOf(temp[0]);
        if (ip1 > 255 || ip1 < 0) {
            return null;
        }
        int ip2 = Integer.valueOf(temp[1]);
        if (ip2 > 255 || ip2 < 0) {
            return null;
        }
        int ip3 = Integer.valueOf(temp[2]);
        if (ip3 > 255 || ip3 < 0) {
            return null;
        }
        int ip4 = Integer.valueOf(temp[3]);
        if (ip4 > 255 || ip4 < 0) {
            return null;
        }
        int ipInt = ip1 << 24 | ip2 << 16 | ip3 << 8 | ip4;
        result.putInt(ipInt);
        return result.array();
    }

    /**
     * 将长度为4的字节数组转换成String 类型的ip地址
     *
     * @param ip ip
     * @return 返回字符串形式的ip地址127.0.0.1, 若参数错误将返回null
     */
    public static String ipToString(byte[] ip) {
        if (ip.length < 4) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 4; i++) {
            if (i != 0) {
                sb.append(".");
            }
            sb.append(byteToInt(ip[i]));

        }
        return sb.toString();
    }

    //将无符号byte 转int
    public static int byteToInt(byte a) {
        return 0XFF & a;
    }

    /**
     * int 转byte.
     *
     * @param a int a<=255 && a >= 0
     * @return byte
     */
    public static byte intToByte(int a) {
        if (a > 255 || a < 0) {
            throw new IllegalArgumentException("a must 0 ~ 255");
        }
        return (byte) a;
    }


    /**
     * 数据放在磁盘上可能会损坏,要确保文件上记录都是真是有效的
     * 所以把磁盘的数据都需要校验.
     *
     * @param array arr
     * @return int.
     */
    public static int crc32(byte[] array) {
        if (array != null) {
            return crc32(array, 0, array.length);
        }

        return 0;
    }

    public static int crc32(byte[] array, int offset, int length) {
        CRC32 crc32 = new CRC32();
        crc32.update(array, offset, length);
        return (int) (crc32.getValue() & 0x7FFFFFFF);
    }

    public static boolean crc32Verify(byte[] array, int crc32Code) {
        return crc32Verify(array, 0, array.length, crc32Code);
    }

    public static boolean crc32Verify(byte[] array, int offset, int length, int crc32Code) {
        int code = crc32(array, offset, length);
        return code == crc32Code;
    }

    public static String byte2String(byte[] data,String charsetName) {

        try {
            return new String(data, charsetName);
        } catch (UnsupportedEncodingException e) {
            logger.warn(e.toString());
        }
        return null;
    }

    public static byte[] string2Byte(String data,String charsetName) {
        try {
            return data.getBytes(charsetName);
        } catch (UnsupportedEncodingException e) {
            logger.warn(e.toString());
        }
        return null;
    }

    /**
     * 字符串转byte长度.
     *
     * @param str 字符串.
     * @return int
     */
    public static int getStrLen(String str,String charsetName) {
        int result = 0;
        try {
            result = str.getBytes(charsetName).length;
        } catch (UnsupportedEncodingException e) {
            logger.warn(e.toString());
        }
        return result;
    }

    /**
     * 获取进程id.
     *
     * @return pid
     */
    private static int getProcessID() {
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        return Integer.valueOf(runtimeMXBean.getName().split("@")[0]);
    }

    public static void closeThreadPool(){
        if (threadPoolExecutor != null) {
            threadPoolExecutor.shutdown();
            threadPoolExecutor = null;
        }
    }

}
