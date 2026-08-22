/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks.mailbox;

import org.apache.flink.annotation.VisibleForTesting;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox.State.CLOSED;
import static org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox.State.OPEN;
import static org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox.State.QUIESCED;

/**
 * Implementation of {@link TaskMailbox} in a {@link java.util.concurrent.BlockingQueue} fashion and
 * tailored towards our use case with multiple writers and single reader.
 */
//是 TaskMailbox 接口的核心实现类。它本质上是一个高度优化、支持优先级、具备批处理能力且线程安全的阻塞双端队列
@ThreadSafe
public class TaskMailboxImpl implements TaskMailbox {
    /** Lock for all concurrent ops. */
    private final ReentrantLock lock = new ReentrantLock();

    /** Internal queue of mails. */
    //作用：全局核心邮件队列。存放所有其他线程（如 JobManager 发送 Checkpoint 命令的 RPC 线程、定时器触发的线程）投递过来的控制消息。
    //设计：使用带有 @GuardedBy("lock") 显式锁保护的 ArrayDeque。这意味着任何生产者线程向此队列添加 Mail，
    //或者 Mailbox 线程从中取 Mail 时，都必须先获取 lock 锁，确保了多线程并发环境下的数据一致性
    @GuardedBy("lock")
    private final Deque<Mail> queue = new ArrayDeque<>();

    /** Condition that is triggered when the mailbox is no longer empty. */
    //作用：条件等待/唤醒机制。设计：当 Mailbox 线程发现当前没有任何任务可做（队列和缓冲区都为空），且默认行为被挂起时，它会调用 notEmpty.await() 进入阻塞挂起状态，释放 CPU 资源。
    //一旦其他外部线程通过 put() 投递了新邮件，就会调用 notEmpty.signal() 唤醒 Mailbox 线程起来干活
    @GuardedBy("lock")
    private final Condition notEmpty = lock.newCondition();

    /** The state of the mailbox in the lifecycle of open, quiesced, and closed. */
    //作用：邮箱生命周期状态机。设计：通常包含 OPEN（正常接收和处理）、QUIESCED（静默状态，不再接受新邮件，但会把队列里剩余的邮件处理完）、CLOSED（彻底关闭，不再处理任何邮件）。
    //它同样受 lock 保护，防止在 Task 退出或取消时，外部线程仍在盲目投递邮件导致内存泄漏
    @GuardedBy("lock")
    private State state = OPEN;

    /** Reference to the thread that executes the mailbox mails. */
    //作用：单线程模型守卫（引用记录）。设计：它记录了哪个具体线程才是被允许执行这些邮件的“正统 Mailbox 线程”（通常是 StreamTask 运行的那个主线程）。
    //在 TaskMailboxImpl 的很多方法中（如判断当前是否在 Mailbox 线程中），
    //会通过 Thread.currentThread == taskMailboxThread 进行校验。这防止了非主线程恶意或误调用消费逻辑，保障了单线程无锁化设计的初衷
    @Nonnull private final Thread taskMailboxThread;

    /**
     * The current batch of mails. A new batch can be created with {@link #tryBuildBatch()} and
     * consumed with {@link #tryTakeFromBatch()}.
     */
    //作用：线程私有的“批处理缓冲区”。设计（极关键）：这是极其惊艳的设计。如果 Mailbox 线程每消费一个 Mail 都要去拿一次 lock 锁，那么当并发很高时，锁竞争会严重拖慢主线程的数据处理。
    //因此，Mailbox 线程在消费时，会通过 tryBuildBatch() 一次性把 queue 里的所有 Mail 转移（Move）到这个本地的 batch 队列中。
    //无锁化消费：因为 batch 队列只有 Mailbox 线程自己可见，不需要加锁。之后主线程从 batch 里一条条拿邮件（tryTakeFromBatch()）时，完全是无锁（Lock-Free）的高效操作
    private final Deque<Mail> batch = new ArrayDeque<>();

    /**
     * Performance optimization where hasNewMail == !queue.isEmpty(). Will not reflect the state of
     * {@link #batch}.
     */
    //作用：无锁快速状态检查（标志位）。设计：它映射的是 !queue.isEmpty() 的状态，通过 volatile 保证可见性。在 Flink 的主循环中，
    //如果每次都要去调用 queue.isEmpty() 检查有没有新邮件，就必须频繁加锁。现在只需要无锁读取这个 volatile 变量即可。如果是 false，主线程可以极其丝滑地直接去处理常规数据流，没有任何锁开销。
    private volatile boolean hasNewMail = false;

    /**
     * Performance optimization where there is new urgent mail. When there is no urgent mail in the
     * batch, it should be checked every time mail is taken, including taking mail from batch queue.
     */
    //作用：紧急邮件（如 Task 取消、严重异常）的最高优先级插队检查。设计：Flink 的 Mail 区分普通和紧急（Urgent）。
    //如果 batch 缓冲区里积压了一大堆普通控制邮件（正在一条条无锁消费中），此时突然来了一个“紧急邮件”（放到了全局 queue 里），主线程如果只管消费本地 batch，就会导致延迟。通过 hasNewUrgentMail 标志位，即使主线程在消费私有的 batch 队列，
    //它每拿一封信前都会无锁看一眼这个标志位，一旦为 true，就会立刻打破当前的 batch 消费循环，重新去全局队列里把紧急邮件捞出来先执行
    private volatile boolean hasNewUrgentMail = false;

    public TaskMailboxImpl(@Nonnull final Thread taskMailboxThread) {
        this.taskMailboxThread = taskMailboxThread;
    }

    @VisibleForTesting
    public TaskMailboxImpl() {
        this(Thread.currentThread());
    }

    @Override
    public boolean isMailboxThread() {
        // taskMailboxThread = Source: Socket Stream -> Flat Map -> Map (1/1)#0
        return Thread.currentThread() == taskMailboxThread;
    }

    @Override
    public boolean hasMail() {
        checkIsMailboxThread();
        return !batch.isEmpty() || hasNewMail;
    }

    @Override
    public int size() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            return batch.size() + queue.size();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Optional<Mail> tryTake(int priority) {
        //
        return tryTake(priority, false);
    }

    @Override
    public Optional<Mail> tryTakeIgnoringDeferrable(int priority) {
        return tryTake(priority, true);
    }

    private Optional<Mail> tryTake(int priority, boolean ignoreDeferrable) {
        checkIsMailboxThread();
        checkTakeStateConditions();

        moveUrgentMailsToBatchIfNeeded(true);
        //batch 有数据就直接取 直至取完 （无锁获取）
        Mail head = takeOrNull(batch, priority, ignoreDeferrable);
        if (head != null) {
            return Optional.of(head);
        }
        if (!hasNewMail) {
            return Optional.empty();
        }
        //加锁从queue获取
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            final Mail value = takeOrNull(queue, priority, ignoreDeferrable);
            if (value == null) {
                return Optional.empty();
            }
            updateNewMailFlags();
            return Optional.ofNullable(value);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public @Nonnull Mail take(int priority) throws InterruptedException, IllegalStateException {
        checkIsMailboxThread();
        checkTakeStateConditions();

        moveUrgentMailsToBatchIfNeeded(true);
        //从batch获取邮件
        Mail head = takeOrNull(batch, priority, false);
        if (head != null) {
            return head;
        }
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            Mail headMail;
            while ((headMail = takeOrNull(queue, priority, false)) == null) {
                //【重点】数据为空则一直等待 直到被唤醒
                // 该地方做了变动 老版本是  notEmpty.await();
                // to ease debugging
                notEmpty.await(1, TimeUnit.SECONDS);
            }
            updateNewMailFlags();
            return headMail;
        } finally {
            lock.unlock();
        }
    }

    private void updateNewMailFlags() {
        Mail peek = queue.peek();
        if (peek != null) {
            hasNewMail = true;
            hasNewUrgentMail = peek.getMailOptions().isUrgent();
        } else {
            hasNewMail = false;
            hasNewUrgentMail = false;
        }
    }

    // ------------------------------------------------------------------------------------------------------------------

    @Override
    public boolean createBatch() {
        moveUrgentMailsToBatchIfNeeded(false);
        return !batch.isEmpty();
    }

    /**
     * Try to create a batch of mails that can be taken with {@link #tryTakeFromBatch()}. The batch
     * does not affect {@link #tryTake(int)} and {@link #take(int)}; that is, they return the same
     * mails even if no batch had been created.
     *
     * <p>If a batch is not completely consumed by {@link #tryTakeFromBatch()}, its elements are
     * carried over to the new batch.
     *
     * <p>Must be called from the mailbox thread ({@link #isMailboxThread()}.
     *
     * <p>To ensure that urgent mails are executed in FIFO order, moving mails from queue to batch
     * only happens when batch doesn't have urgent mails. In addition, in order to reduce the
     * frequent acquisition of locks caused by moving, moving is not required if batch is not empty
     * and no new urgent mails.
     *
     * <pre>{@code
     * All received mails (from head to tail):
     *   Urgent mails:     MailA -> MailB -> MailC -> MailD -> MailE -> MailF
     *   Non-urgent mails: Mail1 -> Mail2 -> Mail3 -> Mail4 -> Mail5 -> Mail6 -> Mail7
     *
     * All received mails (The actual storage structure):
     *   queue: UrgentMails and NonUrgentMails (UrgentMails is in reverse order)
     *      MailF - MailE - MailD - Mail6 - Mail7 (the second batch)
     *   batch: UrgentMails and NonUrgentMails (Both are FIFO)
     *      MailA - MailB - MailC - Mail1 - Mail2 - Mail3 - Mail4 - Mail5 (the first batch)
     *
     * To ensure that urgent mails are executed in FIFO order, moving mails from queue to batch
     * only happens when batch doesn't have urgent mails. For examples, the moving will happen when
     * MailA, MailB and MailC are taken from batch:
     *
     * Step1 : moving all urgent mails into the head of batch:
     *   queue: Mail6 - Mail7 - Mail8
     *   batch: MailE - MailF - MailG - Mail1 - Mail2 - Mail3 - Mail4
     * Step2 : moving all non-urgent mails into the tail of batch:
     *   queue:
     *   batch: MailE - MailF - MailG - Mail1 - Mail2 - Mail3 - Mail4 - Mail5 - Mail6 - Mail7
     * }</pre>
     */
    //在尽量减少加锁开销的前提下，将全局队列 queue 中的紧急邮件（Urgent Mail）或普通邮件，
    //智能地转移到线程私有的缓冲区 batch 中，同时死守“紧急邮件优先”和“先进先出（FIFO）”两个硬性原则
    private void moveUrgentMailsToBatchIfNeeded(boolean onlyMoveUrgentMails) {
        checkIsMailboxThread();
        Mail peek = batch.peek();
        if (peek != null) {
            if (peek.getMailOptions().isUrgent()) {
                // To ensure that urgent mails are executed in FIFO order, moving mails from queue
                // to batch only happens when batch doesn't have urgent mails.
                return;
            } else if (!hasNewUrgentMail) {
                // In order to reduce the frequent lock acquisition caused by movement, if the batch
                // is not empty and there are no new urgent emails, there is no need to move.
                return;
            }
        } else {
            if (onlyMoveUrgentMails && !hasNewUrgentMail) {
                return;
            }
            if (!hasNewMail) {
                // Both batch and queue are empty
                return;
            }
        }

        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            Mail mail;
            while ((mail = queue.pollFirst()) != null) {
                if (mail.getMailOptions().isUrgent()) {
                    batch.addFirst(mail);
                } else {
                    if (onlyMoveUrgentMails) {
                        // Put non-urgent mail back into the queue, and stop the loop
                        queue.addFirst(mail);
                        break;
                    } else {
                        batch.addLast(mail);
                    }
                }
            }
            hasNewUrgentMail = false;
            hasNewMail = !queue.isEmpty();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Optional<Mail> tryTakeFromBatch() {
        checkIsMailboxThread();
        checkTakeStateConditions();
        moveUrgentMailsToBatchIfNeeded(true);
        //从队列弹出第一个元素
        return Optional.ofNullable(batch.pollFirst());
    }

    // ------------------------------------------------------------------------------------------------------------------

    @Override
    public void put(@Nonnull Mail mail) {
        //判断是否是紧急邮件 是入队首 不是入队尾
        if (mail.getMailOptions().isUrgent()) {
            //紧急邮箱放队首
            putFirst(mail);
        } else {
            //非紧急邮箱放队尾 有唤醒操作
            putLast(mail);
        }
    }

    /** Adds the given action to the tail of the mailbox. */
    private void putLast(@Nonnull Mail mail) {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            checkPutStateConditions();
            queue.addLast(mail);
            hasNewMail = true;
            //todo 唤醒
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    /** Adds the given action to the head of the mailbox. */
    private void putFirst(@Nonnull Mail mail) {
        Mail peek = batch.peek();
        // isMailboxThread() 判断是否为邮箱主线程
        if (isMailboxThread()
                && peek != null
                && !peek.getMailOptions().isUrgent()
                && !hasNewUrgentMail) {
            // To ensure that urgent mails are executed in FIFO order, the urgent mail can be put in
            // batch directly if there is no any urgent mail in mailbox
            checkPutStateConditions();
            batch.addFirst(mail);
        } else {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                checkPutStateConditions();
                queue.addFirst(mail);
                hasNewMail = true;
                hasNewUrgentMail = true;
                notEmpty.signal();
            } finally {
                lock.unlock();
            }
        }
    }

    // ------------------------------------------------------------------------------------------------------------------

    @Nullable
    private Mail takeOrNull(Deque<Mail> queue, int priority, boolean ignoreDeferrable) {
        if (queue.isEmpty()) {
            return null;
        }

        Iterator<Mail> iterator = queue.iterator();
        while (iterator.hasNext()) {
            Mail mail = iterator.next();
            int mailPriority =
                    ignoreDeferrable ? mail.getPriorityIgnoringDeferrable() : mail.getPriority();
            if (mailPriority >= priority) {
                iterator.remove();
                return mail;
            }
        }
        return null;
    }

    @Override
    public List<Mail> drain() {
        List<Mail> drainedMails = new ArrayList<>(batch);
        batch.clear();
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            drainedMails.addAll(queue);
            queue.clear();
            hasNewUrgentMail = false;
            hasNewMail = false;
            return drainedMails;
        } finally {
            lock.unlock();
        }
    }

    private void checkIsMailboxThread() {
        if (!isMailboxThread()) {
            throw new IllegalStateException(
                    "Illegal thread detected. This method must be called from inside the mailbox thread!");
        }
    }

    private void checkPutStateConditions() {
        if (state != OPEN) {
            throw new MailboxClosedException(
                    "Mailbox is in state "
                            + state
                            + ", but is required to be in state "
                            + OPEN
                            + " for put operations.");
        }
    }

    private void checkTakeStateConditions() {
        if (state == CLOSED) {
            throw new MailboxClosedException(
                    "Mailbox is in state "
                            + state
                            + ", but is required to be in state "
                            + OPEN
                            + " or "
                            + QUIESCED
                            + " for take operations.");
        }
    }

    @Override
    public void quiesce() {
        checkIsMailboxThread();
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (state == OPEN) {
                state = QUIESCED;
            }
        } finally {
            this.lock.unlock();
        }
    }

    @Nonnull
    @Override
    public List<Mail> close() {
        checkIsMailboxThread();
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (state == CLOSED) {
                return Collections.emptyList();
            }
            List<Mail> droppedMails = drain();
            state = CLOSED;
            // to unblock all
            notEmpty.signalAll();
            return droppedMails;
        } finally {
            lock.unlock();
        }
    }

    @Nonnull
    @Override
    public State getState() {
        if (isMailboxThread()) {
            return state;
        }
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            return state;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void runExclusively(Runnable runnable) {
        lock.lock();
        try {
            runnable.run();
        } finally {
            lock.unlock();
        }
    }
}
