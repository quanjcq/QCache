package common;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 写操作的时候,需要写日志,虽然采用了异步写日志,最后导致了写操作比读操作每秒处理数量少了2w
 * 之前异步处理缓冲区是用{@code LinkedBlockingQueue}由于这个需要同步控制导致性能差了很多
 * 现在的场景是往队列放数据/取数的都是单线程的,写的速度远大于取数据的速度.
 * 注: 在多写多读会出现线程安全问题,只适合该场景.
 * 注: 适合单线程读,单线程写的情况
 *
 * @param <E>
 */
public class QLinkedBlockingQueue<E> {
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();
    private Node<E> head;
    private Node<E> last;
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
     * wait
     */
    private void selfLock() {
        lock.lock();
        try {
            try {
                condition.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * signal
     */
    private void signal() {
        lock.lock();
        try {
            condition.signal();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 入队.
     *
     * @param node 节点
     */
    private void enqueue(Node<E> node) {
        last = last.next = node;
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
     * @return bool
     */
    public void put(E e) {
        while (count.get() == capacity) {
            //队列满了
            selfLock();
        }
        int c;
        c = count.get();
        Node<E> node = new Node<E>(e);
        enqueue(node);
        count.getAndIncrement();

        if (c == 0) {
            signal();
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
            selfLock();
        }
        int c;
        c = count.get();
        count.getAndDecrement();
        E res = dequeue();

        if (c == capacity) {
            signal();
        }

        return res;
    }

    public int size () {
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
