package client.async;

import common.UtilAll;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;

public class FutureResult<T> implements Future,Runnable {

    private FutureListener futureListener = null;

    //运行状态
    private volatile int state;
    private static final int NEW          = 0;
    private static final int RUNNING      = 1;
    private static final int COMPLETING   = 2;
    private static final int CANCELLED    = 3;
    private static final int INTERRUPTED  = 4;
    private static final int EXCEPTIONAL  = 5;

    private Callable<T> callable = null;
    //等待结果的线程
    private volatile WaitThread waitors = null;

    //运行该任务的线程
    private volatile Thread runner = null;
    //运行结果
    private volatile  Object result = null;

    public FutureResult(Callable<T> callable,FutureListener listener) {
        this.callable = callable;
        this.futureListener = listener;
        state = NEW;

    }
    public FutureResult(Callable<T> callable) {
        this.callable = callable;
        state = NEW;

    }
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        if (state  >   COMPLETING) {
            return false;
        }
        if (runner.isInterrupted()) {
            state = INTERRUPTED;
            return true;
        }
        if (mayInterruptIfRunning && runner != null) {
            runner.interrupt();
            runner = null;
            state = INTERRUPTED;
        }
        return true;
    }

    @Override
    public boolean isCancelled() {
        return state == CANCELLED;
    }

    @Override
    public boolean isDone() {
        return state == COMPLETING;
    }

    @Override
    public Object get() throws InterruptedException, ExecutionException {
        if (state == INTERRUPTED){
            throw new InterruptedException("already interrupted!");
        }
        if (result instanceof Exception) {
            throw  new ExecutionException((Throwable)result);
        }
        if (result != null && state == COMPLETING) {
            return result;
        }
        addWaitor(-1);
        return result;
    }

    @Override
    public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        if (timeout < 0 || unit == null) {
            throw  new TimeoutException("timeOut error!");
        }
        long nanos = unit.toNanos(timeout);
        if (state == INTERRUPTED){
            throw new InterruptedException("already interrupted!");
        }
        if (result instanceof Exception) {
            throw  new ExecutionException((Throwable)result);
        }
        if (result != null && state == COMPLETING) {
            return result;
        }
        addWaitor(nanos);
        return result;
    }

    @Override
    public Object getNow() {
        return result;
    }

    @Override
    public void addListener(FutureListener listener) {
        this.futureListener = listener;
    }


    @Override
    public void run() {
        if (!UNSAFE.compareAndSwapInt(this,stateOffset,NEW,RUNNING)) {
            return;
        }
        if (!UNSAFE.compareAndSwapObject(this,runnerOffset,null,Thread.currentThread())){
            return;
        }

        if (callable != null) {
            try {
                result = callable.call();
            } catch (Exception e) {
                result = e;
                state = EXCEPTIONAL;
                if (futureListener != null) {
                    futureListener.onException(e);
                }
            }
        }

        //
        state = COMPLETING;
        if (futureListener != null) {
            futureListener.onSuccess(result);
        }
        notifyWaitThread();
    }

    @Override
    public void start(){
        if (state != NEW) {
            return;
        }
        UtilAll.getThreadPool().execute(this);
    }
    /**
     * 唤醒所有等待的Thread
     */
    private void notifyWaitThread() {
        for (WaitThread q; (q = waitors) != null;) {
            if (UNSAFE.compareAndSwapObject(this, waitersOffset, q, null)) {
                for (; ; ) {
                    Thread t = q.thread;
                    if (t != null) {
                        q.thread = null;
                        LockSupport.unpark(t);
                    }
                    WaitThread next = q.next;
                    if (next == null)
                        break;
                    q.next = null;
                    q = next;
                }
                break;
            }
        }
    }

    private void addWaitor(long nanos) {
        WaitThread waitThread = new WaitThread();
        while (true) {
            if (state > RUNNING) {
                break;
            }
            WaitThread waitorTemp = waitors;
            waitThread.next = waitorTemp;
            if (UNSAFE.compareAndSwapObject(this,waitersOffset,waitorTemp,waitThread)){
                if (nanos < 0){
                    LockSupport.park(this);
                } else {
                    LockSupport.parkNanos(this,nanos);

                }
                return;
            }
        }
    }

    private class WaitThread {
        volatile Thread thread;
        volatile WaitThread next;
        public WaitThread() {
            thread = Thread.currentThread();
            next = null;
        }
    }
    public static Unsafe getUnsafe() {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            return (Unsafe)field.get(null);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    // Unsafe mechanics
    private static final sun.misc.Unsafe UNSAFE;
    private static final long stateOffset;
    private static final long waitersOffset;
    private static final long runnerOffset;
    static {
        try {
            UNSAFE = getUnsafe();
            Class<?> cl = FutureResult.class;
            stateOffset = UNSAFE.objectFieldOffset(cl.getDeclaredField("state"));
            waitersOffset = UNSAFE.objectFieldOffset(cl.getDeclaredField("waitors"));
            runnerOffset = UNSAFE.objectFieldOffset(cl.getDeclaredField("runner"));
        } catch (Exception e) {
            throw new Error(e);
        }
    }


}
