/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumerWithPartialRecordLength;
import org.apache.flink.runtime.io.network.logger.NetworkActionsLogger;

import org.apache.flink.shaded.guava33.com.google.common.collect.Iterators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A pipelined in-memory only subpartition, which can be consumed once.
 *
 * <p>Whenever {@link ResultSubpartition#add(BufferConsumer)} adds a finished {@link BufferConsumer}
 * or a second {@link BufferConsumer} (in which case we will assume the first one finished), we will
 * {@link PipelinedSubpartitionView#notifyDataAvailable() notify} a read view created via {@link
 * ResultSubpartition#createReadView(BufferAvailabilityListener)} of new data availability. Except
 * by calling {@link #flush()} explicitly, we always only notify when the first finished buffer
 * turns up and then, the reader has to drain the buffers via {@link #pollBuffer()} until its return
 * value shows no more buffers being available. This results in a buffer queue which is either empty
 * or has an unfinished {@link BufferConsumer} left from which the notifications will eventually
 * start again.
 *
 * <p>Explicit calls to {@link #flush()} will force this {@link
 * PipelinedSubpartitionView#notifyDataAvailable() notification} for any {@link BufferConsumer}
 * present in the queue.
 */
//它负责缓存一个上游 Task 发往某一个特定下游 Task 的所有网络数据块
public class PipelinedSubpartition extends ResultSubpartition implements ChannelStateHolder {

    private static final Logger LOG = LoggerFactory.getLogger(PipelinedSubpartition.class);

    private static final int DEFAULT_PRIORITY_SEQUENCE_NUMBER = -1;

    // ------------------------------------------------------------------------

    /** Number of exclusive credits per input channel at the downstream tasks. */
    //下游 Task 针对这个特定的 Channel，在它那端分配的专属保底 Credit（独占缓冲区数）
    //通过 Netty 发送数据前，必须知道下游还有多少“空闲低保卡槽”。这个属性就是一个常驻的参考底标，用来参与基于 Credit 的动态流控算法
    private final int receiverExclusiveBuffersPerChannel;

    /** All buffers of this subpartition. Access to the buffers is synchronized on this object. */
    //PrioritizedDeque（带优先级的双端队列）。为什么要带优先级？因为在分布式快照时，特殊的控制事件（如 CheckpointBarrier）必须享有“插队”特权。
    //当 Barrier 到达时，它会被赋予最高优先级，直接插入到队列的最前面（Overtaking），从而超越普通数据缓冲区，实现超低延迟的快照传递
    final PrioritizedDeque<BufferConsumerWithPartialRecordLength> buffers = new PrioritizedDeque<>();

    /** The number of non-event buffers currently in this subpartition. */
    @GuardedBy("buffers")
    //这个值就是大名鼎鼎的 Backlog（积压值）。当上游向发送队列塞入一个数据块时，该值加 1。Flink 底层的 Netty 线程在顺着网线向下游发送数据时，
    // 会把这个 Backlog 值顺便捎带发给下游。下游一看到这个值变大，就知道上游产生了积压，从而触发下游去申请流动缓冲区（Floating Buffers）来接盘
    private int buffersInBacklog;

    /** The read view to consume this subpartition. */
    //PipelinedSubpartition 负责接收上游写的动作（add()）；
    // 而底层的 Netty 传输线程不会直接去操作这个子通道，而是创建并持有一个 readView。Netty 顺着这个视图去异步地、单向地读取队列并吐向网络，实现了写线程与网络读线程的解耦
    PipelinedSubpartitionView readView;

    /** Flag indicating whether the subpartition has been finished. */
    //上游算子已经宣布数据全部产出完毕（比如遇到了有界流的 EndOfPartitionEvent），此后不再接收新数据
    private boolean isFinished;

    //底层作用：标记当前通道是否收到了 flush()（强制刷写） 的请求。
    // 物理意义：流处理强求低延迟。如果数据量很小，没达到 bufferSize，但系统触发了定时 Flush，这个标记会变为 true，
    // 通知 Netty 线程：“别等了，虽然没写满，赶紧把当前的半块数据也发走！
    @GuardedBy("buffers")
    private boolean flushRequested;

    /** Flag indicating whether the subpartition has been released. */
    //该通道已经彻底被销毁释放，内部所有的 BufferConsumer 对应的内存引用计数全部扣减并归还给内存池（防止内存泄漏）
    volatile boolean isReleased;

    /** The total number of buffers (both data and event buffers). */
    //该通道自启动以来，累计处理过的总 Buffer 数量
    private long totalNumberOfBuffers;

    /** The total number of bytes (both data and event buffers). */
    //该通道自启动以来，累计处理过的总 总字节数
    private long totalNumberOfBytes;

    /** Writes in-flight data. */
    //底层作用：负责把传输中（In-Flight）的数据写入状态后端的物理编织器。物理意义：在非对齐检查点（Unaligned Checkpoint）机制下，当 Barrier 到达时，Flink 不需要等待队列里的数据全部发完。
    //channelStateWriter 会直接把当前 buffers 队列里还没来得及发走、积压在管道里的那些纯数据块，一股脑全部当成“通道状态（Channel State）”直接持久化存储到 HDFS/S3
    private ChannelStateWriter channelStateWriter;
    //当前通道的动态自适应缓冲区尺寸
    private int bufferSize;

    /** The channelState Future of unaligned checkpoint. */
    //底层作用：异步保存管道中数据的未来对象和其对应的 Checkpoint ID。物理意义：
    // Flink 2.2 极大优化了快照的异步化吞吐。为了不卡住计算线程，当需要截取当前管道里的数据作为状态时，Flink 会生成一个 channelStateFuture。
    // 当底层的持久化线程真正把这批 Buffer 写完并安全落盘后，该 Future 才会 complete。
    // 这组参数精准控制了当前正在进行的快照版本（checkpointId），防止分布式环境下的版本错乱
    @GuardedBy("buffers")
    private CompletableFuture<List<Buffer>> channelStateFuture;

    /**
     * It is the checkpointId corresponding to channelStateFuture. And It should be always update
     * with {@link #channelStateFuture}.
     */
    @GuardedBy("buffers")
    private long channelStateCheckpointId;

    /**
     * Whether this subpartition is blocked (e.g. by exactly once checkpoint) and is waiting for
     * resumption.
     */
    @GuardedBy("buffers")
    //标记当前通道是否被强行阻塞
    boolean isBlocked = false;
    //当前通道发出的物理数据块的自增序列号
    //每成功向下游 Netty 发送一个 Buffer，这个 sequenceNumber 就会加 1。
    // 下游的 TaskManager 在接收数据时，会严格校验序列号是否连续（如 0, 1, 2, 3...）。
    // 如果因为网络抖动、Netty 重连导致序列号断层（比如收到 3 之后直接收到了 5），
    // Flink 会立刻判定网络数据损坏，触发全量 Failover 报错，以此确保分布式环境下数据传输的绝对不丢、不重、不乱序
    int sequenceNumber = 0;

    // ------------------------------------------------------------------------

    PipelinedSubpartition(
            int index,
            int receiverExclusiveBuffersPerChannel,
            int startingBufferSize,
            ResultPartition parent) {
        super(index, parent);

        checkArgument(
                receiverExclusiveBuffersPerChannel >= 0,
                "Buffers per channel must be non-negative.");
        this.receiverExclusiveBuffersPerChannel = receiverExclusiveBuffersPerChannel;
        this.bufferSize = startingBufferSize;
    }

    @Override
    public void setChannelStateWriter(ChannelStateWriter channelStateWriter) {
        checkState(this.channelStateWriter == null, "Already initialized");
        this.channelStateWriter = checkNotNull(channelStateWriter);
    }

    @Override
    public int add(BufferConsumer bufferConsumer, int partialRecordLength) {//
        return add(bufferConsumer, partialRecordLength, false);//
    }

    public boolean isSupportChannelStateRecover() {
        return true;
    }

    @Override
    public int finish() throws IOException {
        BufferConsumer eventBufferConsumer =
                EventSerializer.toBufferConsumer(EndOfPartitionEvent.INSTANCE, false);
        add(eventBufferConsumer, 0, true);
        LOG.debug("{}: Finished {}.", parent.getOwningTaskName(), this);
        return eventBufferConsumer.getWrittenBytes();
    }

    private int add(BufferConsumer bufferConsumer, int partialRecordLength, boolean finish) {
        checkNotNull(bufferConsumer);
        final boolean notifyDataAvailable;
        int prioritySequenceNumber = DEFAULT_PRIORITY_SEQUENCE_NUMBER;
        int newBufferSize;
        //因为 Netty 线程会异步地从这个队列里读数据，而计算线程在疯狂地往里写数据，所以核心动作必须包裹在 synchronized (buffers) 锁内部执行
        synchronized (buffers) {
            //安全兜底。如果这个通道由于作业正在关闭、取消（isReleased）或者已经接收到了有界流的结束信号（isFinished），
            // 那么新进来的数据块会被就地当场直接销毁（调用 close() 扣减引用计数，归还给内存池），防止产生内存泄漏
            if (isFinished || isReleased) {
                bufferConsumer.close();
                return ADD_BUFFER_ERROR_CODE;
            }

            // Add the bufferConsumer and update the stats
            //调用 addBuffer 把 bufferConsumer 塞进刚才提到的 PrioritizedDeque 物理队列
            if (addBuffer(bufferConsumer, partialRecordLength)) {//
                prioritySequenceNumber = sequenceNumber;
            }
            //读取这个 bufferConsumer 的大小，瞬间累加到前面提到的 totalNumberOfBuffers 和 totalNumberOfBytes 计数器中，为 Web UI 和 Metrics 提供最实时的发送吞吐量监控
            updateStatistics(bufferConsumer);
            //如果当前塞进来的是普通的业务数据块（而不是控制事件），它会让前面剖析的 buffersInBacklog（积压值）自增 1。
            // 这个不断上涨的数字将作为信使，通过网络告诉下游：“我这里堆积了更多的数据，你快多准备点 Buffer 来接盘！”
            increaseBuffersInBacklog(bufferConsumer);
            notifyDataAvailable = finish || shouldNotifyDataAvailable();

            isFinished |= finish;
            newBufferSize = bufferSize;
        }

        notifyPriorityEvent(prioritySequenceNumber);
        if (notifyDataAvailable) {//false
            notifyDataAvailable();
        }

        return newBufferSize;
    }

    @GuardedBy("buffers")
    private boolean addBuffer(BufferConsumer bufferConsumer, int partialRecordLength) {//
        assert Thread.holdsLock(buffers);
        if (bufferConsumer.getDataType().hasPriority()) {//false
            return processPriorityBuffer(bufferConsumer, partialRecordLength);
        } else if (Buffer.DataType.TIMEOUTABLE_ALIGNED_CHECKPOINT_BARRIER == bufferConsumer.getDataType()) {
            processTimeoutableCheckpointBarrier(bufferConsumer);
        }
        //
        buffers.add(new BufferConsumerWithPartialRecordLength(bufferConsumer, partialRecordLength));
        return false;
    }

    @GuardedBy("buffers")
    private boolean processPriorityBuffer(BufferConsumer bufferConsumer, int partialRecordLength) {
        buffers.addPriorityElement(
                new BufferConsumerWithPartialRecordLength(bufferConsumer, partialRecordLength));
        final int numPriorityElements = buffers.getNumPriorityElements();

        CheckpointBarrier barrier = parseCheckpointBarrier(bufferConsumer);
        if (barrier != null) {
            checkState(
                    barrier.getCheckpointOptions().isUnalignedCheckpoint(),
                    "Only unaligned checkpoints should be priority events");
            final Iterator<BufferConsumerWithPartialRecordLength> iterator = buffers.iterator();
            Iterators.advance(iterator, numPriorityElements);
            List<Buffer> inflightBuffers = new ArrayList<>();
            while (iterator.hasNext()) {
                BufferConsumer buffer = iterator.next().getBufferConsumer();

                if (buffer.isBuffer()) {
                    try (BufferConsumer bc = buffer.copy()) {
                        inflightBuffers.add(bc.build());
                    }
                }
            }
            if (!inflightBuffers.isEmpty()) {
                channelStateWriter.addOutputData(
                        barrier.getId(),
                        subpartitionInfo,
                        ChannelStateWriter.SEQUENCE_NUMBER_UNKNOWN,
                        inflightBuffers.toArray(new Buffer[0]));
            }
        }
        return needNotifyPriorityEvent();
    }

    // It is just called after add priorityEvent.
    @GuardedBy("buffers")
    private boolean needNotifyPriorityEvent() {
        assert Thread.holdsLock(buffers);
        // if subpartition is blocked then downstream doesn't expect any notifications
        return buffers.getNumPriorityElements() == 1 && !isBlocked;
    }

    @GuardedBy("buffers")
    private void processTimeoutableCheckpointBarrier(BufferConsumer bufferConsumer) {
        CheckpointBarrier barrier = parseAndCheckTimeoutableCheckpointBarrier(bufferConsumer);
        channelStateWriter.addOutputDataFuture(
                barrier.getId(),
                subpartitionInfo,
                ChannelStateWriter.SEQUENCE_NUMBER_UNKNOWN,
                createChannelStateFuture(barrier.getId()));
    }

    @GuardedBy("buffers")
    private CompletableFuture<List<Buffer>> createChannelStateFuture(long checkpointId) {
        assert Thread.holdsLock(buffers);
        if (channelStateFuture != null) {
            completeChannelStateFuture(
                    null,
                    new IllegalStateException(
                            String.format(
                                    "%s has uncompleted channelStateFuture of checkpointId=%s, but it received "
                                            + "a new timeoutable checkpoint barrier of checkpointId=%s, it maybe "
                                            + "a bug due to currently not supported concurrent unaligned checkpoint.",
                                    this, channelStateCheckpointId, checkpointId)));
        }
        channelStateFuture = new CompletableFuture<>();
        channelStateCheckpointId = checkpointId;
        return channelStateFuture;
    }

    @GuardedBy("buffers")
    private void completeChannelStateFuture(List<Buffer> channelResult, Throwable e) {
        assert Thread.holdsLock(buffers);
        if (e != null) {
            channelStateFuture.completeExceptionally(e);
        } else {
            channelStateFuture.complete(channelResult);
        }
        channelStateFuture = null;
    }

    @GuardedBy("buffers")
    private boolean isChannelStateFutureAvailable(long checkpointId) {
        assert Thread.holdsLock(buffers);
        return channelStateFuture != null && channelStateCheckpointId == checkpointId;
    }

    private CheckpointBarrier parseAndCheckTimeoutableCheckpointBarrier(
            BufferConsumer bufferConsumer) {
        CheckpointBarrier barrier = parseCheckpointBarrier(bufferConsumer);
        checkArgument(barrier != null, "Parse the timeoutable Checkpoint Barrier failed.");
        checkState(
                barrier.getCheckpointOptions().isTimeoutable()
                        && Buffer.DataType.TIMEOUTABLE_ALIGNED_CHECKPOINT_BARRIER
                                == bufferConsumer.getDataType());
        return barrier;
    }

    @Override
    public void alignedBarrierTimeout(long checkpointId) throws IOException {
        int prioritySequenceNumber = DEFAULT_PRIORITY_SEQUENCE_NUMBER;
        synchronized (buffers) {
            // The checkpoint barrier has sent to downstream, so nothing to do.
            if (!isChannelStateFutureAvailable(checkpointId)) {
                return;
            }

            // 1. find inflightBuffers and timeout the aligned barrier to unaligned barrier
            List<Buffer> inflightBuffers = new ArrayList<>();
            try {
                if (findInflightBuffersAndMakeBarrierToPriority(checkpointId, inflightBuffers)) {
                    prioritySequenceNumber = sequenceNumber;
                }
            } catch (IOException e) {
                inflightBuffers.forEach(Buffer::recycleBuffer);
                completeChannelStateFuture(null, e);
                throw e;
            }

            // 2. complete the channelStateFuture
            completeChannelStateFuture(inflightBuffers, null);
        }

        // 3. notify downstream read barrier, it must be called outside the buffers_lock to avoid
        // the deadlock.
        notifyPriorityEvent(prioritySequenceNumber);
    }

    @Override
    public void abortCheckpoint(long checkpointId, CheckpointException cause) {
        synchronized (buffers) {
            if (isChannelStateFutureAvailable(checkpointId)) {
                completeChannelStateFuture(null, cause);
            }
        }
    }

    @GuardedBy("buffers")
    private boolean findInflightBuffersAndMakeBarrierToPriority(
            long checkpointId, List<Buffer> inflightBuffers) throws IOException {
        // 1. record the buffers before barrier as inflightBuffers
        final int numPriorityElements = buffers.getNumPriorityElements();
        final Iterator<BufferConsumerWithPartialRecordLength> iterator = buffers.iterator();
        Iterators.advance(iterator, numPriorityElements);

        BufferConsumerWithPartialRecordLength element = null;
        CheckpointBarrier barrier = null;
        while (iterator.hasNext()) {
            BufferConsumerWithPartialRecordLength next = iterator.next();
            BufferConsumer bufferConsumer = next.getBufferConsumer();

            if (Buffer.DataType.TIMEOUTABLE_ALIGNED_CHECKPOINT_BARRIER
                    == bufferConsumer.getDataType()) {
                barrier = parseAndCheckTimeoutableCheckpointBarrier(bufferConsumer);
                // It may be an aborted barrier
                if (barrier.getId() != checkpointId) {
                    continue;
                }
                element = next;
                break;
            } else if (bufferConsumer.isBuffer()) {
                try (BufferConsumer bc = bufferConsumer.copy()) {
                    inflightBuffers.add(bc.build());
                }
            }
        }

        // 2. Make the barrier to be priority
        checkNotNull(
                element, "The checkpoint barrier=%d don't find in %s.", checkpointId, toString());
        makeBarrierToPriority(element, barrier);

        return needNotifyPriorityEvent();
    }

    private void makeBarrierToPriority(
            BufferConsumerWithPartialRecordLength oldElement, CheckpointBarrier barrier)
            throws IOException {
        buffers.getAndRemove(oldElement::equals);
        buffers.addPriorityElement(
                new BufferConsumerWithPartialRecordLength(
                        EventSerializer.toBufferConsumer(barrier.asUnaligned(), true), 0));
    }

    @Nullable
    private CheckpointBarrier parseCheckpointBarrier(BufferConsumer bufferConsumer) {
        CheckpointBarrier barrier;
        try (BufferConsumer bc = bufferConsumer.copy()) {
            Buffer buffer = bc.build();
            try {
                final AbstractEvent event =
                        EventSerializer.fromBuffer(buffer, getClass().getClassLoader());
                barrier = event instanceof CheckpointBarrier ? (CheckpointBarrier) event : null;
            } catch (IOException e) {
                throw new IllegalStateException(
                        "Should always be able to deserialize in-memory event", e);
            } finally {
                buffer.recycleBuffer();
            }
        }
        return barrier;
    }

    @Override
    public void release() {
        // view reference accessible outside the lock, but assigned inside the locked scope
        final PipelinedSubpartitionView view;

        synchronized (buffers) {
            if (isReleased) {
                return;
            }

            // Release all available buffers
            for (BufferConsumerWithPartialRecordLength buffer : buffers) {
                buffer.getBufferConsumer().close();
            }
            buffers.clear();

            if (channelStateFuture != null) {
                IllegalStateException exception =
                        new IllegalStateException("The PipelinedSubpartition is released");
                completeChannelStateFuture(null, exception);
            }

            view = readView;
            readView = null;

            // Make sure that no further buffers are added to the subpartition
            isReleased = true;
        }

        LOG.debug("{}: Released {}.", parent.getOwningTaskName(), this);

        if (view != null) {
            view.releaseAllResources();
        }
    }

    //【重点】从内部的 buffers 队列中消费 BufferConsumer，将其切片/转化为可读的 Buffer，并计算当前的积压量（Backlog）和后续数据状态，最后打包返回
    @Nullable
    BufferAndBacklog pollBuffer() {//
        //确保了多线程环境（通常是 Task 线程写入，Netty 线程读取）下的线程安全
        synchronized (buffers) {
            if (isBlocked) {//false
                //如果当前子分区处于被阻塞状态（例如触发了某种反压或特定的对齐 Barrier 机制），则直接拒绝拉取，返回 null
                return null;
            }

            Buffer buffer = null;

            if (buffers.isEmpty()) {
                //buffers 队列为空
                flushRequested = false;
            }

            while (!buffers.isEmpty()) {
                //buffer 队列不为空进来
                //获取队列第一个（队头 Head）
                BufferConsumerWithPartialRecordLength bufferConsumerWithPartialRecordLength = buffers.peek();
                //获取BufferConsumer
                BufferConsumer bufferConsumer = bufferConsumerWithPartialRecordLength.getBufferConsumer();
                //判断bufferConsumer 数据类型 bufferConsumer.getDataType() = DATA_BUFFER
                if (Buffer.DataType.TIMEOUTABLE_ALIGNED_CHECKPOINT_BARRIER == bufferConsumer.getDataType()) {
                    //Flink 的 Checkpoint 机制依赖 Barrier。如果遇到了支持超时的对齐 Barrier（Timeoutable Aligned Checkpoint Barrier），
                    // 这里会立即触发其状态转换（例如尝试将其转化为非对齐或触发超时倒计时），确保分布式快照的正确性
                    completeTimeoutableCheckpointBarrier(bufferConsumer);
                }
                //这里通过切片（Slice）技术将其转化为一个只读的 Buffer（消费者视角），无内存拷贝，极度高效
                //buffer = ReadOnlySlicedNetworkBuffer
                buffer = buildSliceBuffer(bufferConsumerWithPartialRecordLength);//

                //Flink 保证只有队列中的最后一个 Buffer 可以处于“未写满/未结束（Unfinished）”状态。
                // 如果队列里有多个 Buffer，排在头部的 Buffer 必须是已经写满且 Finished 的。如果违反，说明写入和读取的拓扑顺序发生了严重 Bug
                checkState(
                        //BufferConsumer.isFinished() 判断当前这个缓冲区（Buffer）是否已经“写结束”，即上游的 Task 线程是否已经停止向这个 Buffer 写入数据。
                        bufferConsumer.isFinished() || buffers.size() == 1,
                        "When there are multiple buffers, an unfinished bufferConsumer can not be at the head of the buffers queue.");

                if (buffers.size() == 1) {
                    // turn off flushRequested flag if we drained all the available data
                    flushRequested = false;
                }

                if (bufferConsumer.isFinished()) {
                    //如果头部的 BufferConsumer 已经完全读完（Finished），将其从队列中弹出（poll），并调用 close() 释放其对应的引用计数（或内存）。
                    // 同时，减少 Backlog（积压数）。Backlog 的大小直接决定了 Flink Credit-based 流控机制中下游给上游发放多少信用额度（Credit）
                    requireNonNull(buffers.poll()).getBufferConsumer().close();
                    // 减少buffersInBacklog 的数量
                    decreaseBuffersInBacklogUnsafe(bufferConsumer.isBuffer());
                }

                // if we have an empty finished buffer and the exclusive credit is 0, we just return
                // the empty buffer so that the downstream task can release the allocated credit for
                // this empty buffer, this happens in two main scenarios currently:
                // 1. all data of a buffer builder has been read and after that the buffer builder
                // is finished
                // 2. in approximate recovery mode, a partial record takes a whole buffer builder
                if (receiverExclusiveBuffersPerChannel == 0 && bufferConsumer.isFinished()) {
                    //如果下游分配给当前通道的专属信用额度（Exclusive Credit）为 0，
                    // 且当前 Buffer 刚好写完。即使这个 Buffer 内部没有实际的业务数据（是一个空 Buffer），Flink 也会坚持把这个空 Buffer 返回
                    //让下游 Task 能够感知到并释放为这个空 Buffer 预留的内存资源，防止在极端的反压或近似恢复（Approximate Recovery）模式下导致内存死锁
                    break;
                }
                if (buffer.readableBytes() > 0) {
                    // 有新数据，正常跳出循环并把切片数据返回发走
                    break;
                }
                // 没新数据，把这次创建的空切片回收掉
                buffer.recycleBuffer();
                buffer = null;
                if (!bufferConsumer.isFinished()) {
                    // 因为 buffer 没写满，绝对不能执行 buffers.poll()！它必须留在队列里等上游继续写
                    break;
                }
            }

            if (buffer == null) {
                return null;
            }

            if (buffer.getDataType().isBlockingUpstream()) {
                // 关键动作：将当前子分区标记为阻塞状态！
                // 检查当前拉取出来的这个 Buffer 里的数据类型，是否需要立即“阻塞/反压”上游的写入线程，停止让上游继续生产数据
                isBlocked = true;
            }
            //修改指标信息
            updateStatistics(buffer);
            // Do not report last remaining buffer on buffers as available to read (assuming it's
            // unfinished).
            // It will be reported for reading either on flush or when the number of buffers in the
            // queue
            // will be 2 or more.
            NetworkActionsLogger.traceOutput(
                    "PipelinedSubpartition#pollBuffer",
                    buffer,
                    parent.getOwningTaskName(),
                    subpartitionInfo);
            return new BufferAndBacklog(
                    buffer,
                    //告诉下游：“我这里还堆积了多少个 Buffer 没发”。
                    // 下游 Netty 接收端收到这个值后，会根据这个数值向本地的 LocalBufferPool 申请对应数量的 Floating Credits（浮动额度）并回传给上游
                    getBuffersInBacklogUnsafe(),//0
                    isDataAvailableUnsafe() ?
                            //提前告诉下游下一个 Buffer 是什么类型  DATA_BUFFER 还是 EVENT_BUFFER
                            getNextBufferTypeUnsafe() : Buffer.DataType.NONE,
                    //序号自增。用于下游检测网络传输是否丢包或乱序
                    sequenceNumber++);
        }
    }

    @GuardedBy("buffers")
    private void completeTimeoutableCheckpointBarrier(BufferConsumer bufferConsumer) {
        CheckpointBarrier barrier = parseAndCheckTimeoutableCheckpointBarrier(bufferConsumer);
        if (!isChannelStateFutureAvailable(barrier.getId())) {
            // It happens on a previously aborted checkpoint.
            return;
        }
        completeChannelStateFuture(Collections.emptyList(), null);
    }

    void resumeConsumption() {
        synchronized (buffers) {
            checkState(isBlocked, "Should be blocked by checkpoint.");

            isBlocked = false;
        }
    }

    public void acknowledgeAllDataProcessed() {
        parent.onSubpartitionAllDataProcessed(subpartitionInfo.getSubPartitionIdx());
    }

    @Override
    public boolean isReleased() {
        return isReleased;
    }

    @Override
    public PipelinedSubpartitionView createReadView(BufferAvailabilityListener availabilityListener) {
        synchronized (buffers) {
            checkState(!isReleased);
            checkState(
                    readView == null,
                    "Subpartition %s of is being (or already has been) consumed, "
                            + "but pipelined subpartitions can only be consumed once.",
                    getSubPartitionIndex(),
                    parent.getPartitionId());

            LOG.debug(
                    "{}: Creating read view for subpartition {} of partition {}.",
                    parent.getOwningTaskName(),
                    getSubPartitionIndex(),
                    parent.getPartitionId());

            readView = new PipelinedSubpartitionView(this, availabilityListener);//
        }

        return readView;
    }

    public ResultSubpartitionView.AvailabilityWithBacklog getAvailabilityAndBacklog(boolean isCreditAvailable) {
        synchronized (buffers) {
            boolean isAvailable;
            if (isCreditAvailable) {
                //
                isAvailable = isDataAvailableUnsafe();
            } else {
                isAvailable = getNextBufferTypeUnsafe().isEvent();
            }
            return new ResultSubpartitionView.AvailabilityWithBacklog(isAvailable, getBuffersInBacklogUnsafe());
        }
    }

    @GuardedBy("buffers")
    private boolean isDataAvailableUnsafe() {
        assert Thread.holdsLock(buffers);

        return !isBlocked && (flushRequested || getNumberOfFinishedBuffers() > 0);
    }

    private Buffer.DataType getNextBufferTypeUnsafe() {
        assert Thread.holdsLock(buffers);

        final BufferConsumerWithPartialRecordLength first = buffers.peek();
        return first != null ? first.getBufferConsumer().getDataType() : Buffer.DataType.NONE;
    }

    // ------------------------------------------------------------------------

    @Override
    public int getNumberOfQueuedBuffers() {
        synchronized (buffers) {
            return buffers.size();
        }
    }

    @Override
    public void bufferSize(int desirableNewBufferSize) {
        if (desirableNewBufferSize < 0) {
            throw new IllegalArgumentException("New buffer size can not be less than zero");
        }
        synchronized (buffers) {
            bufferSize = desirableNewBufferSize;
        }
    }

    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        final long numBuffers;
        final long numBytes;
        final boolean finished;
        final boolean hasReadView;

        synchronized (buffers) {
            numBuffers = getTotalNumberOfBuffersUnsafe();
            numBytes = getTotalNumberOfBytesUnsafe();
            finished = isFinished;
            hasReadView = readView != null;
        }

        return String.format(
                "%s#%d [number of buffers: %d (%d bytes), number of buffers in backlog: %d, finished? %s, read view? %s]",
                this.getClass().getSimpleName(),
                getSubPartitionIndex(),
                numBuffers,
                numBytes,
                getBuffersInBacklogUnsafe(),
                finished,
                hasReadView);
    }

    @Override
    public int unsynchronizedGetNumberOfQueuedBuffers() {
        // since we do not synchronize, the size may actually be lower than 0!
        return Math.max(buffers.size(), 0);
    }

    @Override
    public void flush() {
        final boolean notifyDataAvailable;
        synchronized (buffers) {
            //buffers.isEmpty()：当前队列为空
            // 如果当前的 flushRequested 已经是 true 了，说明之前已经有别的线程（或者上游）请求过 Flush 了，Netty 线程已经被唤醒或者正在赶来的路上。此时不需要重复点灯，直接退出
            if (buffers.isEmpty() || flushRequested) {
                return;
            }
            // if there is more than 1 buffer, we already notified the reader
            // (at the latest when adding the second buffer)
            //队列里有且仅有唯一的一块 Buffer，而且它还没写满，属于正在写入的“未完成块”
            // 返回true 代表写入了新数据
            boolean isDataAvailableInUnfinishedBuffer = buffers.size() == 1 && buffers.peek().getBufferConsumer().isDataAvailable();
            //决定要不要唤醒网络线程
            // !isBlocked 当前通道绝对不能处于被对齐快照（Exactly-Once Barrier）锁死阻塞的状态 如果通道被 Block 住了，再有新数据也必须原地待命，绝对不通知
            notifyDataAvailable = !isBlocked && isDataAvailableInUnfinishedBuffer;
            flushRequested = buffers.size() > 1 || isDataAvailableInUnfinishedBuffer;
        }
        if (notifyDataAvailable) { // true
            notifyDataAvailable();
        }
    }

    @Override
    protected long getTotalNumberOfBuffersUnsafe() {
        return totalNumberOfBuffers;
    }

    @Override
    protected long getTotalNumberOfBytesUnsafe() {
        return totalNumberOfBytes;
    }

    Throwable getFailureCause() {
        return parent.getFailureCause();
    }

    private void updateStatistics(BufferConsumer buffer) {
        totalNumberOfBuffers++;
    }

    private void updateStatistics(Buffer buffer) {
        totalNumberOfBytes += buffer.getSize();
    }

    @GuardedBy("buffers")
    private void decreaseBuffersInBacklogUnsafe(boolean isBuffer) {
        assert Thread.holdsLock(buffers);
        if (isBuffer) {
            buffersInBacklog--;
        }
    }

    /**
     * Increases the number of non-event buffers by one after adding a non-event buffer into this
     * subpartition.
     */
    @GuardedBy("buffers")
    private void increaseBuffersInBacklog(BufferConsumer buffer) {
        assert Thread.holdsLock(buffers);

        if (buffer != null && buffer.isBuffer()) {
            buffersInBacklog++;
            System.out.println(Thread.currentThread().getName() + ": buffersInBacklog: " + buffersInBacklog);
        }
    }

    /** Gets the number of non-event buffers in this subpartition. */
    @SuppressWarnings("FieldAccessNotGuarded")
    @Override
    public int getBuffersInBacklogUnsafe() {
        if (isBlocked || buffers.isEmpty()) {
            return 0;
        }

        if (flushRequested
                || isFinished
                || !checkNotNull(buffers.peekLast()).getBufferConsumer().isBuffer()) {
            return buffersInBacklog;
        } else {
            return Math.max(buffersInBacklog - 1, 0);
        }
    }

    @GuardedBy("buffers")
    private boolean shouldNotifyDataAvailable() {
        // Notify only when we added first finished buffer.
        return readView != null
                && !flushRequested
                && !isBlocked
                && getNumberOfFinishedBuffers() == 1;
    }

    //通知有数据了
    private void notifyDataAvailable() {
        final PipelinedSubpartitionView readView = this.readView;
        if (readView != null) {
            //PipelinedSubpartitionView#notifyDataAvailable
            readView.notifyDataAvailable();
        }
    }

    private void notifyPriorityEvent(int prioritySequenceNumber) {
        final PipelinedSubpartitionView readView = this.readView;
        if (readView != null && prioritySequenceNumber != DEFAULT_PRIORITY_SEQUENCE_NUMBER) {
            readView.notifyPriorityEvent(prioritySequenceNumber);
        }
    }

    private int getNumberOfFinishedBuffers() {
        assert Thread.holdsLock(buffers);

        // NOTE: isFinished() is not guaranteed to provide the most up-to-date state here
        // worst-case: a single finished buffer sits around until the next flush() call
        // (but we do not offer stronger guarantees anyway)
        final int numBuffers = buffers.size();
        if (numBuffers == 1 && buffers.peekLast().getBufferConsumer().isFinished()) {
            return 1;
        }

        // We assume that only last buffer is not finished.
        return Math.max(0, numBuffers - 1);
    }

    Buffer buildSliceBuffer(BufferConsumerWithPartialRecordLength buffer) {//
        return buffer.build();//
    }

    /** for testing only. */
    @VisibleForTesting
    BufferConsumerWithPartialRecordLength getNextBuffer() {
        return buffers.poll();
    }

    /** for testing only. */
    // suppress this warning as it is only for testing.
    @SuppressWarnings("FieldAccessNotGuarded")
    @VisibleForTesting
    CompletableFuture<List<Buffer>> getChannelStateFuture() {
        return channelStateFuture;
    }

    // suppress this warning as it is only for testing.
    @SuppressWarnings("FieldAccessNotGuarded")
    @VisibleForTesting
    public long getChannelStateCheckpointId() {
        return channelStateCheckpointId;
    }
}
