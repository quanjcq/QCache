package client.async;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public interface Future  extends java.util.concurrent.Future {
    @Override
    boolean cancel(boolean mayInterruptIfRunning);

    @Override
    boolean isCancelled();

    @Override
    boolean isDone();

    @Override
    Object get() throws InterruptedException, ExecutionException;

    @Override
    Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException;


    /**
     * 立刻返回不管有没有结果
     * @return  object.
     */
    Object getNow();

    void addListener(FutureListener listerner);

    void start();
}
