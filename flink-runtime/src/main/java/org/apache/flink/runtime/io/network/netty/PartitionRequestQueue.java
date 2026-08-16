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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.NetworkSequenceViewReader;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FullyFilledBuffer;
import org.apache.flink.runtime.io.network.netty.NettyMessage.ErrorResponse;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.PartitionRequestListener;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

import static org.apache.flink.runtime.io.network.netty.NettyMessage.BufferResponse;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A nonEmptyReader of partition queues, which listens for channel writability changed events before
 * writing and flushing {@link Buffer} instances.
 */
class PartitionRequestQueue extends ChannelInboundHandlerAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionRequestQueue.class);

    private final ChannelFutureListener writeListener = new WriteAndFlushNextMessageIfPossibleListener();

    /** The readers which are already enqueued available for transferring data. */
    private final ArrayDeque<NetworkSequenceViewReader> availableReaders = new ArrayDeque<>();

    /** All the readers created for the consumers' partition requests. */
    private final ConcurrentMap<InputChannelID, NetworkSequenceViewReader> allReaders = new ConcurrentHashMap<>();

    private boolean fatalError;

    private ChannelHandlerContext ctx;

    //在整个 TCP 连接的生命周期中，channelRegistered 只会触发一次（早于连接激活 channelActive 和任何数据的读写）
    @Override
    public void channelRegistered(final ChannelHandlerContext ctx) throws Exception {
        if (this.ctx == null) {
            this.ctx = ctx;
        }
        super.channelRegistered(ctx);
    }

    // reader = CreditBasedSequenceNumberingViewReader
    //todo 通知当前的读取器可以读取Buffer数据了
    void notifyReaderNonEmpty(final NetworkSequenceViewReader reader) {
        // The notification might come from the same thread. For the initial writes this
        // might happen before the reader has set its reference to the view, because
        // creating the queue and the initial notification happen in the same method call.
        // This can be resolved by separating the creation of the view and allowing
        // notifications.

        // TODO This could potentially have a bad performance impact as in the
        // worst case (network consumes faster than the producer) each buffer
        // will trigger a separate event loop task being scheduled.
        ctx.executor().execute(
                //todo 会调用 userEventTriggered
                () -> ctx.pipeline().fireUserEventTriggered(reader));
    }

    /**
     * Try to enqueue the reader once receiving credit notification from the consumer or receiving
     * non-empty reader notification from the producer.
     *
     * <p>NOTE: Only one thread would trigger the actual enqueue after checking the reader's
     * availability, so there is no race condition here.
     */
    // reader = CreditBasedSequenceNumberingViewReader
    private void enqueueAvailableReader(final NetworkSequenceViewReader reader) throws Exception {//
        if (reader.isRegisteredAsAvailable()) {
            //因为上游 Task 线程的写入和下游 Credit 的回传是完全并发、互不相关的异步事件，很有可能在极短时间内连续发射两个通知
            return;
        }
        //CreditBasedSequenceNumberingViewReader#getAvailabilityAndBacklog
        ResultSubpartitionView.AvailabilityWithBacklog availabilityWithBacklog = reader.getAvailabilityAndBacklog();
        if (!availabilityWithBacklog.isAvailable()) {
            //如果 isAvailable() 返回 false，说明要么没数据，要么有数据但没 Credit（被反压了）。此时，该 Reader 直接被拒绝入队，本次发送流程到此戛然而止
            int backlog = availabilityWithBacklog.getBacklog();
            if (backlog > 0 && reader.needAnnounceBacklog()) {
                //专门向下游发射一个纯控制面的报文，告诉下游：“我这里还有大批货物积压，赶快腾出内存给我发 Credit！”
                announceBacklog(reader, backlog);
            }
            return;
        }

        // Queue an available reader for consumption. If the queue is empty,
        // we try trigger the actual write. Otherwise this will be handled by
        // the writeAndFlushNextMessageIfPossible calls.
        //走到这说明当前既有数据、又有 Credit
        boolean triggerWrite = availableReaders.isEmpty();
        //正式将当前 Reader 塞进 availableReaders 队列，并将其状态标记为已注册
        registerAvailableReader(reader);//

        //场景 A（队列本来是空的，triggerWrite == true）：说明在这之前，网络层处于无事可做的闲置状态，或者之前的任务全发完了。
        // 现在来了新活，必须主动、立刻调用我们上一问分析的 writeAndFlushNextMessageIfPossible(ctx.channel()) 去开闸放水，点火启动发送流水线

        //场景 B（队列里已经有别的 Reader 在排队了，triggerWrite == false）：说明此时 Netty 线程已经在忙碌地执行 writeAndFlushNextMessageIfPossible 的发送大循环了。由于前面的 Reader 发完后，Netty 的 writeListener 会自动触发并继续消费队列里的下一个 Reader，
        //因此这里只需要把新来的 Reader 静静地排在队伍末尾即可，绝对不能重复调用 writeAndFlush... 去抢占和打乱当前正在运行的 Netty 异步发送链！
        if (triggerWrite) {//true
            //【重点】
            writeAndFlushNextMessageIfPossible(ctx.channel());//
        }

    }

    /**
     * Accesses internal state to verify reader registration in the unit tests.
     *
     * <p><strong>Do not use anywhere else!</strong>
     *
     * @return readers which are enqueued available for transferring data
     */
    @VisibleForTesting
    ArrayDeque<NetworkSequenceViewReader> getAvailableReaders() {
        return availableReaders;
    }

    public void notifyReaderCreated(final NetworkSequenceViewReader reader) {
        allReaders.put(reader.getReceiverId(), reader);
    }

    public void cancel(InputChannelID receiverId) {
        ctx.pipeline().fireUserEventTriggered(receiverId);
    }

    public void close() throws IOException {
        if (ctx != null) {
            ctx.channel().close();
        }

        releaseAllResources();
    }

    /**
     * Adds unannounced credits from the consumer or resumes data consumption after an exactly-once
     * checkpoint and enqueues the corresponding reader for this consumer (if not enqueued yet).
     *
     * @param receiverId The input channel id to identify the consumer.
     * @param operation The operation to be performed (add credit or resume data consumption).
     */
    void addCreditOrResumeConsumption(
            InputChannelID receiverId, Consumer<NetworkSequenceViewReader> operation)
            throws Exception {
        if (fatalError) {
            return;
        }

        NetworkSequenceViewReader reader = obtainReader(receiverId);

        operation.accept(reader);
        enqueueAvailableReader(reader);
    }

    void acknowledgeAllRecordsProcessed(InputChannelID receiverId) {
        if (fatalError) {
            return;
        }

        obtainReader(receiverId).acknowledgeAllRecordsProcessed();
    }

    void notifyNewBufferSize(InputChannelID receiverId, int newBufferSize) {
        if (fatalError) {
            return;
        }

        // It is possible to receive new buffer size before the reader would be created since the
        // downstream task could calculate buffer size even using the data from one channel but it
        // sends new buffer size into all upstream even if they don't ready yet. In this case, just
        // ignore the new buffer size.
        NetworkSequenceViewReader reader = allReaders.get(receiverId);
        if (reader != null) {
            reader.notifyNewBufferSize(newBufferSize);
        }
    }

    /**
     * Notify the id of required segment from the consumer.
     *
     * @param receiverId The input channel id to identify the consumer.
     * @param subpartitionId The id of the corresponding subpartition.
     * @param segmentId The id of required segment.
     */
    void notifyRequiredSegmentId(InputChannelID receiverId, int subpartitionId, int segmentId) {
        if (fatalError) {
            return;
        }
        NetworkSequenceViewReader reader = allReaders.get(receiverId);
        if (reader != null) {
            reader.notifyRequiredSegmentId(subpartitionId, segmentId);
        }
    }

    NetworkSequenceViewReader obtainReader(InputChannelID receiverId) {
        NetworkSequenceViewReader reader = allReaders.get(receiverId);
        if (reader == null) {
            throw new IllegalStateException(
                    "No reader for receiverId = " + receiverId + " exists.");
        }

        return reader;
    }

    /**
     * Announces remaining backlog to the consumer after the available data notification or data
     * consumption resumption.
     */
    private void announceBacklog(NetworkSequenceViewReader reader, int backlog) {
        checkArgument(backlog > 0, "Backlog must be positive.");

        NettyMessage.BacklogAnnouncement announcement =
                new NettyMessage.BacklogAnnouncement(backlog, reader.getReceiverId());
        ctx.channel()
                .writeAndFlush(announcement)
                .addListener(
                        (ChannelFutureListener)
                                future -> {
                                    if (!future.isSuccess()) {
                                        onChannelFutureFailure(future);
                                    }
                                });
    }

    // 会被 notifyReaderNonEmpty 方法的 ctx.pipeline().fireUserEventTriggered(reader) 触发
    //此方法是专门用于在线程安全的情况下，传递 Reader 队列状态以及处理被取消生产者的底层回调
    // msg CreditBasedSequenceNumberingViewReader
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object msg) throws Exception {
        // The user event triggered event loop callback is used for thread-safe
        // hand over of reader queues and cancelled producers.

        if (msg instanceof NetworkSequenceViewReader) {
            //触发源头：
            // 1. 上游 Task 线程写完数据触发了刷新。
            // 2. 下游回传了新的 Credit。它们都会导致 Reader 变为“可读且有配额”，并向上发射一个以 NetworkSequenceViewReader 本身作为 msg 的用户事件
            enqueueAvailableReader((NetworkSequenceViewReader) msg);//
        } else if (msg.getClass() == InputChannelID.class) {
            //下游取消消费，上游紧急刹车（清理机制）
            //下游 Task 因为发生异常、或者整个 Flink 作业被取消（Cancel）时，下游会发送一个取消消费的控制流通知
            // Release partition view that get a cancel request.
            InputChannelID toCancel = (InputChannelID) msg;
            // 1. 从排队等待发送的活跃队列中移除，Netty 再也不会为它发数据了
            // remove reader from queue of available readers
            availableReaders.removeIf(reader -> reader.getReceiverId().equals(toCancel));

            // remove reader from queue of all readers and release its resource
            // 2. 从所有的 Reader 注册表（allReaders）中剔除
            final NetworkSequenceViewReader toRelease = allReaders.remove(toCancel);
            // 3. 彻底释放资源：回收底层的 Buffer、解绑视图
            if (toRelease != null) {
                releaseViewReader(toRelease);
            }
        } else if (msg instanceof PartitionRequestListener) {
            //分区查找超时，告知下游失败（异常控制面）
            PartitionRequestListener partitionRequestListener = (PartitionRequestListener) msg;

            // Send partition not found message to the downstream task when the listener is timeout.
            final ResultPartitionID resultPartitionId =
                    partitionRequestListener.getResultPartitionId();
            final InputChannelID inputChannelId = partitionRequestListener.getReceiverId();
            // 1. 同样将其从两张大表中无条件清除
            availableReaders.remove(partitionRequestListener.getViewReader());
            allReaders.remove(inputChannelId);
            try {
                // 2. 主动向下游发射一个特化的 ErrorResponse 告知：分区没找到！
                ctx.writeAndFlush(
                        new NettyMessage.ErrorResponse(new PartitionNotFoundException(resultPartitionId), inputChannelId));
            } catch (Exception e) {
                LOG.warn(
                        "Write partition not found exception to {} for result partition {} fail",
                        inputChannelId,
                        resultPartitionId,
                        e);
            }
        } else {
            //如果这个事件不是 Flink 自定义的网络控制元数据（比如是 Netty 官方自带的心跳检测 IdleStateEvent），
            // 则调用 fireUserEventTriggered 继续往 Pipeline 的下一个 ChannelHandler 传递
            ctx.fireUserEventTriggered(msg);
        }
    }

    //一旦网络变好，操作系统缓冲区腾出了地方，Netty 底层会向上发射一个 channelWritabilityChanged 事件
    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        writeAndFlushNextMessageIfPossible(ctx.channel());
    }

    //[ 下游 TaskManager ]                                       [ 上游 TaskManager ]
    //  (SingleInputGate 侧)                                      (ResultPartition 侧)
    //           │                                                          │
    //           │  1. 下游算子消费了数据，空出 BufferPool 内存                  │
    //           │                                                          │
    //           ├───────────────── 发送 NettyMessage.PartitionRequest ────►│ (建立逻辑拉取连接)
    //           ├───────────────── 发送 NettyMessage.AddCredit ───────────►│ (告知上游: 我有空位了!)
    //           │                                                          │
    //           │                                                          ▼
    //           │                                              [PartitionRequestQueue]
    //           │                                              监听到下游的 Credit 增量
    //           │                                              从 ResultPartition 提取数据
    //           │                                                          │
    //           │◄──────────────── 执行 channel.writeAndFlush(msg) ────────┘
    //           │                  (msg = NettyMessage.BufferResponse)
    //           │                   ▲
    //           │                   └─ 物理上是在【通知下游】处理新数据
    //           ▼
    //  下游 Netty 线程收到 msg
    //  调用 decodeBufferOrEvent
    //  唤醒下游 Mailbox 线程开始消费!
    private void writeAndFlushNextMessageIfPossible(final Channel channel) throws IOException {
        if (fatalError || !channel.isWritable()) {
            //fatalError：如果之前发生过严重的网络或内存致命错误，直接拒绝发送
            //!channel.isWritable() Netty 的水位线反压开关。如果当前网络带宽被挤爆，或者底层操作系统的 TCP 发送缓冲区满了，
            //Netty 会将 isWritable() 置为 false。此时 Flink 直接退出方法，一脚踩下刹车，不再从 ViewReader 中拉取数据
            return;
        }

        // The logic here is very similar to the combined input gate and local
        // input channel logic. You can think of this class acting as the input
        // gate and the consumed views as the local input channels.

        BufferAndAvailability next = null;
        int nextSubpartitionId = -1;
        try {
            while (true) {
                //从准备好数据的活跃 Reader 队列中弹出一个 reader
                NetworkSequenceViewReader reader = pollAvailableReader();

                // No queue with available data. We allow this here, because
                // of the write callbacks that are executed after each write.
                if (reader == null) {
                    return;
                }
                //CreditBasedSequenceNumberingViewReader#peekNextBufferSubpartitionId
                nextSubpartitionId = reader.peekNextBufferSubpartitionId();
                //CreditBasedSequenceNumberingViewReader#getNextBuffer   最终会从 PipelinedSubpartition的buffers队列获取数据
                next = reader.getNextBuffer();
                if (next == null) {
                    //没数据
                    if (!reader.isReleased()) {
                        // 如果 Buffer 为空但 Reader 没释放，可能触发了上游阻塞（isBlockingUpstream），跳过继续循环
                        continue;
                    }

                    Throwable cause = reader.getFailureCause();
                    if (cause != null) {
                        ErrorResponse msg = new ErrorResponse(cause, reader.getReceiverId());
                        // 如果上游 Task 失败了，把错误包装成 ErrorResponse 扔给下游，促使下游一同 Failover
                        ctx.writeAndFlush(msg);
                    }
                } else {
                    // This channel was now removed from the available reader queue.
                    // We re-add it into the queue if it is still available
                    //有数据
                    if (next.moreAvailable()) {
                        //如果刚拉完 Buffer 后，发现这个 Reader 里面还有积压的数据，
                        // 且下游还有剩余的 Credit（即 next.moreAvailable() 返回 true），则立刻把这个 Reader 重新塞回活跃队列，等待下一次发送循环
                        registerAvailableReader(reader);//
                    }

                    BufferResponse msg = new BufferResponse(
                                    next.buffer(),//数据
                                    next.getSequenceNumber(),//序列号
                                    reader.getReceiverId(),//
                                    nextSubpartitionId,
                                    next.buffer() instanceof FullyFilledBuffer
                                            ? ((FullyFilledBuffer) next.buffer())
                                                    .getPartialBuffers()
                                                    .size()
                                            : 0,
                                    next.buffersInBacklog());

                    // Write and flush and wait until this is done before
                    // trying to continue with the next buffer.
                    //PartitionRequestQueue 是运行在上游Task
                    // channel.writeAndFlush(msg) 是在向“下游”发送数据或控制事件
                    // writeListener = WriteAndFlushNextMessageIfPossibleListener
                    //每获取一个buffer 发起一次请求
                    //当 Netty 异步把这这一个包成功推到网卡后，writeListener 的回调函数会再次触发并重新调用 writeAndFlushNextMessageIfPossible
                    channel.writeAndFlush(msg).addListener(writeListener);

                    return;
                }
            }
        } catch (Throwable t) {
            if (next != null) {
                next.buffer().recycleBuffer();
            }

            throw new IOException(t.getMessage(), t);
        }
    }

    private void registerAvailableReader(NetworkSequenceViewReader reader) {
        availableReaders.add(reader);
        reader.setRegisteredAsAvailable(true);
    }

    @Nullable
    private NetworkSequenceViewReader pollAvailableReader() {
        NetworkSequenceViewReader reader = availableReaders.poll();
        if (reader != null) {
            reader.setRegisteredAsAvailable(false);
        }
        return reader;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        releaseAllResources();

        ctx.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        handleException(ctx.channel(), cause);
    }

    private void handleException(Channel channel, Throwable cause) throws IOException {
        LOG.error(
                "Encountered error while consuming partitions (connection to {})",
                channel.remoteAddress(),
                cause);

        fatalError = true;
        releaseAllResources();

        if (channel.isActive()) {
            channel.writeAndFlush(new ErrorResponse(cause))
                    .addListener(ChannelFutureListener.CLOSE);
        }
    }

    private void releaseAllResources() throws IOException {
        // note: this is only ever executed by one thread: the Netty IO thread!
        for (NetworkSequenceViewReader reader : allReaders.values()) {
            releaseViewReader(reader);
        }

        availableReaders.clear();
        allReaders.clear();
    }

    private void releaseViewReader(NetworkSequenceViewReader reader) throws IOException {
        reader.setRegisteredAsAvailable(false);
        reader.releaseAllResources();
    }

    private void onChannelFutureFailure(ChannelFuture future) throws Exception {
        if (future.cause() != null) {
            handleException(future.channel(), future.cause());
        } else {
            handleException(
                    future.channel(), new IllegalStateException("Sending cancelled by user."));
        }
    }

    public void notifyPartitionRequestTimeout(PartitionRequestListener partitionRequestListener) {
        ctx.pipeline().fireUserEventTriggered(partitionRequestListener);
    }

    // This listener is called after an element of the current nonEmptyReader has been
    // flushed. If successful, the listener triggers further processing of the
    // queues.
    private class WriteAndFlushNextMessageIfPossibleListener implements ChannelFutureListener {

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            try {
                if (future.isSuccess()) {
                    // 发送成功
                    writeAndFlushNextMessageIfPossible(future.channel());
                } else {
                    //发送失败
                    onChannelFutureFailure(future);
                }
            } catch (Throwable t) {
                handleException(future.channel(), t);
            }
        }
    }
}
