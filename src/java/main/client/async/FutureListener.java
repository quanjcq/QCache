package client.async;

public interface FutureListener {
    /**
     * 当异步任务执行成功会调用这个方法.
     * @param obj 异步任务返回的结果.
     */
    void onSuccess(Object obj);

    /**
     * 异步任务出现异常的时候会调用
     */
    void onException(Throwable throwable);
}
