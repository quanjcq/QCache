package common;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * 阻塞队列,入队出队都是无锁状态,只实现线程的等待通知模型,相比{@link java.util.concurrent.LinkedBlockingQueue} 快很多.
 * 注: 适合单线程读,单线程写的情况,在多写多读会出现线程安全问题,只适合该场景.
 * @param <E>
 */
public class QLinkedBlockingQueue<E> {

    /**
     * 等待读的线程
     */
    private volatile Thread waitReadThread = null;
    /**
     * 等待写的线程
     */
    private volatile Thread waitWriteThread = null;
    private volatile transient Node<E> head;
    private volatile transient Node<E> last;
    private int capacity;
    private AtomicInteger count = new AtomicInteger(0);

    public QLinkedBlockingQueue() {
        this(Integer.MAX_VALUE - 5);
    }

    public QLinkedBlockingQueue(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("队列容量需要大于零");
        }
        this.capacity = capacity;
        head = last = new Node<E>(null);
    }

    /**
     * read lock，当读线程发现没有可读的内容的时候调用它,自锁（只有读线程会调用该方法）
     */
    private void readLock() {
        waitReadThread = Thread.currentThread();
        LockSupport.park();
    }

    /**
     * signal ReadWait,在写线程发现之前队列为空，会尝试唤醒等待的读线程.（只有写线程会调用）
     */
    private void signalReadWait() {
        if (waitReadThread != null) {
            LockSupport.unpark(waitReadThread);
            waitReadThread = null;
        }
    }

    /**
     * write lock,在写线程发现容器满了，会调用这个方法自锁(只有写线程才会调用这个方法)
     */
    private void writeLock() {
        waitWriteThread = Thread.currentThread();
        LockSupport.park();
    }

    /**
     * signal write wait, 读线程，发现容器是满的，会尝试唤醒等待读的线程，（只有读线程才会调用这个方法）
     */
    private void signalWriteWait() {
        if (waitWriteThread != null) {
            LockSupport.unpark(waitReadThread);
            waitReadThread = null;
        }
    }

    /**
     * 入队.
     *
     * @param node 节点
     */
    private void enqueue(Node<E> node) {
        last.next = node;
        last = node;
    }

    /**
     * Removes a node from head of queue.
     *
     * @return the node
     */
    private E dequeue() {
        // assert takeLock.isHeldByCurrentThread();
        // assert head.item == null;
        Node<E> h = head;
        Node<E> first = h.next;
        h.next = h; // help GC
        head = first;
        E x = first.item;
        first.item = null;
        return x;
    }

    /**
     * 入队.
     *
     * @param e 元素
     */
    public void put(E e) {
        while (count.get() == capacity) {
            //队列满了
            writeLock();
        }

        Node<E> node = new Node<E>(e);
        enqueue(node);

        int c = count.getAndIncrement();
        //c == 1
        if (c == 0) {
            signalReadWait();
        }

    }

    /**
     * 出队.
     *
     * @return element
     */
    public E poll() {
        while (count.get() == 0) {
            //为空
            readLock();
        }
        E res = dequeue();
        int c = count.getAndDecrement();
        if (c == capacity) {
            signalWriteWait();
        }
        return res;
    }

    public int size() {
        return count.get();
    }

    static class Node<E> {
        E item;
        Node<E> next;

        Node(E x) {
            item = x;
        }
    }

}
