package client.async;

import client.CacheClient;

import java.util.concurrent.Callable;

/**
 * 只是简单的把同步调用简单封装了下,并没有完全发挥异步编程的优势.
 * 有空再写了.
 * Usage see  AsyncClientDemo
 */
public class AsyncClient {
    private CacheClient client;
    private final Object sync = new Object();
    public AsyncClient(CacheClient client) {
        this.client = client;
    }

    public Future put(String key,String val) {
        return put(key,val,-1);
    }

    public Future put(String key,String val,int timeOut) {
        return put(key,val,timeOut,null);
    }

    public Future put(String key, String val,FutureListener listener) {

        return put(key,val,-1,listener);
    }

    /**
     * put key val
     * @param key key
     * @param val val
     * @param timeOut timeOut -1 为不会过时
     * @param listener 异步任务执行完毕后的回调.
     * @return future.
     */
    public Future put(final String key, final String val, final int timeOut, FutureListener listener) {
        Future result = new FutureResult<Boolean>(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                synchronized (sync) {
                    return client.put(key,val,timeOut);
                }
            }
        });
        result.addListener(listener);
        result.start();
        return result;
    }

    public Future get(final String key){
        return get(key,null);
    }

    /**
     * get key
     * @param key key
     * @param listener 结果返回后的回调函数
     * @return future
     */
    public Future get(final String key,FutureListener listener) {
        Future result = new FutureResult<String>(new Callable<String>() {
            @Override
            public String call() throws Exception {
                synchronized (sync) {
                    return client.get(key);
                }
            }
        });
        result.addListener(listener);
        result.start();
        return result;
    }

    public Future del(final String key) {
        return del(key,null);
    }

    /**
     * del key
     * @param key key
     * @param listener 结果返回后的回调函数.
     * @return future
     */
    public Future del(final  String key,FutureListener listener) {
        Future result = new FutureResult<Boolean>(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                synchronized (sync) {
                    return client.del(key);
                }
            }
        });
        result.addListener(listener);
        result.start();
        return result;
    }

    public Future status(final  String node) {
        return status(node,null);
    }

    /**
     * 返回集群状态
     * @return node
     */
    public Future status(final String node, FutureListener listener) {
        Future result = new FutureResult<String>(new Callable<String>() {
            @Override
            public String call() throws Exception {
                synchronized (sync) {
                    return client.status(node);
                }
            }
        });
        result.addListener(listener);
        result.start();
        return result;
    }




}
