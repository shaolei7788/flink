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
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.PartitionRequestListener;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Simple wrapper for the subpartition view used in the new network credit-based mode.
 *
 * <p>It also keeps track of available buffers and notifies the outbound handler about
 * non-emptiness, similar to the {@link LocalInputChannel}.
 */
//      [下游 Task (InputGate)]                 [上游 Task (ResultPartition)]
//
//        |                                       |
//        |---- 1. 建立连接并请求分区 (Request) ---->|  <-- 仅在启动时拉取一次
//        |                                       |
//        |                                       |== 2. 上游产生数据 (Buffer)
//        |<--- 3. 主动推送数据 (Push Buffer) -------|  <-- 只要 Credit 够，就疯狂推送
//        |                                       |
//        |==== 4. 消费数据，释放空闲 Buffer       |
//        |                                       |
//        |---- 5. 反馈剩余额度 (Send Credit) ----->|  <-- 下游给上游发放“准送证”
//在 Flink（包括 2.2 版本）的网络传输架构中，数据传输采用的是 “推（Push）模式”为主、结合“拉（Pull）反馈” 的混合机制。
// 简单概括：上游一有数据就主动推送（Push）给下游，但下游通过信用额度（Credit）来控制上游推进的速度
//基于信用额度（Credit-based）流量控制模型 的核心组件之一。它负责将 ResultSubpartitionView 读取的数据（Buffer）发送到网络中 给下游
class CreditBasedSequenceNumberingViewReader
        implements BufferAvailabilityListener, NetworkSequenceViewReader {

    private final Object requestLock = new Object();

    //下游消费端（RemoteInputChannel）的唯一标识符
    private final InputChannelID receiverId;

    //作用：管理所有发送任务的 Netty 队列。解析：这是一个单线程（Netty 线程）处理的队列。
    // 当该 Reader 有数据可以发送（即：既有数据积压，又有下游给的 Credit）时，它会被放进这个 requestQueue。Netty 线程会循环消费这个队列，调用 Reader 去真正读取并发送数据
    private final PartitionRequestQueue requestQueue;

    //作用：初始信用额度（Buffer 数量）。解析：由配置决定（通常对应下游每个 Channel 预分配的独占 Buffer 数）。
    // 在刚建立连接、下游还没发送反向信用反馈时，作为初始的流量控制边界
    private final int initialCredit;

    /**
     * Cache of the index of the only subpartition if the underlining {@link ResultSubpartitionView}
     * only consumes one subpartition, or -1 otherwise.
     */
    //如果当前 View 仅消费一个特定的子分区，该字段缓存其索引；否则为 -1
    private int subpartitionId;

    //PipelinedSubpartitionView
    //这是数据流的“源头”。Reader 正是通过它来调用 getNextBuffer() 获取内存中的数据块（Buffer）。使用 volatile
    //是因为该 View 的创建、初始化或销毁（释放资源）可能发生在不同的线程（如 RPC 线程、JobMaster 协调线程或 Task 线程）
    private volatile ResultSubpartitionView subpartitionView;

    //用于监听分区请求的状态变化。例如，当发生非正常断开、重试或动态资源调整时，
    //通过该监听器向上层或框架报告当前的请求状态。同样使用 volatile 保证跨线程可见性
    private volatile PartitionRequestListener partitionRequestListener;

    /**
     * The status indicating whether this reader is already enqueued in the pipeline for
     * transferring data or not.
     *
     * <p>It is mainly used to avoid repeated registrations but should be accessed by a single
     * thread only since there is no synchronisation.
     */
    // 当 Reader 发现有数据可读且有 Credit 时，需要把自己注册到 PartitionRequestQueue 中
    //为了避免重复注册导致队列膨胀和死锁，用该变量标记“我是否已经在队列里了”
    private boolean isRegisteredAsAvailable = false;

    /** The number of available buffers for holding data on the consumer side. */
    //当前可用的信用额度（剩余可发送的 Buffer 数量）
    //下游每消费完一个 Buffer 并释放后，会通过网络向上传递一个 Credit。上游收到后，numCreditsAvailable 就会增加
    private int numCreditsAvailable;

    CreditBasedSequenceNumberingViewReader(InputChannelID receiverId, int initialCredit, PartitionRequestQueue requestQueue) {//
        checkArgument(initialCredit >= 0, "Must be non-negative.");

        this.receiverId = receiverId;
        this.initialCredit = initialCredit;//2
        this.numCreditsAvailable = initialCredit;//2
        this.requestQueue = requestQueue;
        this.subpartitionId = -1;
    }

    //当下游发送分区请求时，上游尝试去绑定（获取）对应的数据源视图（SubpartitionView）；
    // 如果数据源还没准备好，就注册一个监听器等它准备好。绑定成功后，立刻触发数据推送流程
    @Override
    public void requestSubpartitionViewOrRegisterListener(//
            ResultPartitionProvider partitionProvider,
            ResultPartitionID resultPartitionId,
            ResultSubpartitionIndexSet subpartitionIndexSet)
            throws IOException {
        synchronized (requestLock) {
            //严防重复请求。如果 subpartitionView 或 partitionRequestListener 已经存在，说明该 Channel 已经处理过请求，直接抛出异常，防止逻辑混乱
            checkState(subpartitionView == null, "Subpartitions already requested");
            checkState(
                    partitionRequestListener == null, "Partition request listener already created");
            //
            partitionRequestListener =
                    new NettyPartitionRequestListener(partitionProvider, this, subpartitionIndexSet, resultPartitionId);
            // The partition provider will create subpartitionView if resultPartition is
            // registered, otherwise it will register a listener of partition request to the result
            // partition manager.
            //PipelinedSubpartitionView
            Optional<ResultSubpartitionView> subpartitionViewOptional =
                    //ResultPartitionManager#createSubpartitionViewOrRegisterListener
                    partitionProvider.createSubpartitionViewOrRegisterListener(
                            resultPartitionId,
                            subpartitionIndexSet,
                            this,
                            partitionRequestListener);
            if (subpartitionViewOptional.isPresent()) {
                this.subpartitionView = subpartitionViewOptional.get();
                if (subpartitionIndexSet.size() == 1) {
                    //它会把这个子分区的 ID 提取出来缓存到 subpartitionId 局部变量中
                    subpartitionId = subpartitionIndexSet.values().iterator().next();
                }
            } else {
                // If the subpartitionView is not exist, it means that the requested partition is not registered.
                return;
            }
        }
        //主动触发一次通知，告诉框架“我现在有视图了，去看看里面有没有上游之前已经积压的数据”
        notifyDataAvailable(subpartitionView);//
        //通知 PartitionRequestQueue，告知有一个新的 Reader 创建成功并加入了队列，Netty 队列会将其纳入轮询和调度管理中
        requestQueue.notifyReaderCreated(this);
    }

    @Override
    public void notifySubpartitionsCreated(
            ResultPartition partition, ResultSubpartitionIndexSet subpartitionIndexSet)
            throws IOException {
        synchronized (requestLock) {
            checkState(subpartitionView == null, "Subpartitions already requested");
            subpartitionView = partition.createSubpartitionView(subpartitionIndexSet, this);
            if (subpartitionIndexSet.size() == 1) {
                subpartitionId = subpartitionIndexSet.values().iterator().next();
            }
        }

        notifyDataAvailable(subpartitionView);
        requestQueue.notifyReaderCreated(this);
    }

    @Override
    public void addCredit(int creditDeltas) {
        numCreditsAvailable += creditDeltas;
    }

    @Override
    public void notifyRequiredSegmentId(int subpartitionId, int segmentId) {
        subpartitionView.notifyRequiredSegmentId(subpartitionId, segmentId);
    }

    @Override
    public void resumeConsumption() {
        if (initialCredit == 0) {
            // reset available credit if no exclusive buffer is available at the
            // consumer side for all floating buffers must have been released
            numCreditsAvailable = 0;
        }
        subpartitionView.resumeConsumption();
    }

    @Override
    public void acknowledgeAllRecordsProcessed() {
        subpartitionView.acknowledgeAllDataProcessed();
    }

    @Override
    public void setRegisteredAsAvailable(boolean isRegisteredAvailable) {
        this.isRegisteredAsAvailable = isRegisteredAvailable;
    }

    @Override
    public boolean isRegisteredAsAvailable() {
        return isRegisteredAsAvailable;
    }

    /**
     * Returns true only if the next buffer is an event or the reader has both available credits and
     * buffers.
     *
     * @implSpec BEWARE: this must be in sync with {@link #getNextDataType(BufferAndBacklog)}, such
     *     that {@code getNextDataType(bufferAndBacklog) != NONE <=>
     *     AvailabilityWithBacklog#isAvailable()}!
     */
    @Override
    public ResultSubpartitionView.AvailabilityWithBacklog getAvailabilityAndBacklog() {
        //PipelinedSubpartitionView#getAvailabilityAndBacklog
        return subpartitionView.getAvailabilityAndBacklog(numCreditsAvailable > 0);
    }

    /**
     * Returns the {@link org.apache.flink.runtime.io.network.buffer.Buffer.DataType} of the next
     * buffer in line.
     *
     * <p>Returns the next data type only if the next buffer is an event or the reader has both
     * available credits and buffers.
     *
     * @implSpec BEWARE: this must be in sync with {@link #getAvailabilityAndBacklog()}, such that
     *     {@code getNextDataType(bufferAndBacklog) != NONE <=>
     *     AvailabilityWithBacklog#isAvailable()}!
     * @param bufferAndBacklog current buffer and backlog including information about the next
     *     buffer
     * @return the next data type if the next buffer can be pulled immediately or {@link
     *     Buffer.DataType#NONE}
     */
    private Buffer.DataType getNextDataType(BufferAndBacklog bufferAndBacklog) {
        final Buffer.DataType nextDataType = bufferAndBacklog.getNextDataType();
        if (numCreditsAvailable > 0 || nextDataType.isEvent()) {
            return nextDataType;
        }
        return Buffer.DataType.NONE;
    }

    @Override
    public InputChannelID getReceiverId() {
        return receiverId;
    }

    @Override
    public void notifyNewBufferSize(int newBufferSize) {
        subpartitionView.notifyNewBufferSize(newBufferSize);
    }

    @Override
    public void notifyPartitionRequestTimeout(PartitionRequestListener partitionRequestListener) {
        requestQueue.notifyPartitionRequestTimeout(partitionRequestListener);
        this.partitionRequestListener = null;
    }

    @VisibleForTesting
    int getNumCreditsAvailable() {
        return numCreditsAvailable;
    }

    @VisibleForTesting
    ResultSubpartitionView.AvailabilityWithBacklog hasBuffersAvailable() {
        return subpartitionView.getAvailabilityAndBacklog(true);
    }

    @Override
    public int peekNextBufferSubpartitionId() throws IOException {
        if (subpartitionId >= 0) {
            return subpartitionId;
        }
        return subpartitionView.peekNextBufferSubpartitionId();
    }

    //真正向底层的子分区（Subpartition）消费一个数据块（Buffer），并在出队时强行对“信用额度（Credit）”进行扣减和合规性校验，最后将包装好的数据和后续可用状态返回给 Netty 框架发走
    @Nullable
    @Override
    public BufferAndAvailability getNextBuffer() throws IOException {
        //PipelinedSubpartitionView#getNextBuffer
        BufferAndBacklog next = subpartitionView.getNextBuffer();
        if (next != null) {
            //next.buffer().isBuffer()：判断这是否是一个普通的数据 Buffer
            //如果是普通数据，则将本地缓存的、下游给的可用信用额度（Credit）先减 1。防守断言：如果减完之后发现小了 0（即变成了 -1），
            // 意味着下游明明已经没有内存空间（Credit = 0）来接收这个 Buffer 了，
            // 上游却执意发了出去。这是流控协议发生了严重的 Bug，因此直接抛出 IllegalStateException("no credit available") 终止进程
            if (next.buffer().isBuffer() && --numCreditsAvailable < 0) {
                throw new IllegalStateException("no credit available");
            }

            final Buffer.DataType nextDataType = getNextDataType(next);
            //提前把下一个 Buffer 的类型封装起来发给下游。当下游网络层发现“下一个是 Barrier 事件”时，就可以提前做好准备，甚至在当前 Buffer 刚到时就做好紧急切换的准备，实现超前的网络流控控制
            return new BufferAndAvailability(
                    //数据本身
                    next.buffer(),
                    //下一个数据的类型
                    nextDataType,
                    //当前剩下的积压量
                    next.buffersInBacklog(),
                    //保证顺序的序列号
                    next.getSequenceNumber());
        } else {
            return null;
        }
    }

    @Override
    public boolean needAnnounceBacklog() {
        return initialCredit == 0 && numCreditsAvailable == 0;
    }

    @Override
    public boolean isReleased() {
        return subpartitionView.isReleased();
    }

    @Override
    public Throwable getFailureCause() {
        return subpartitionView.getFailureCause();
    }

    @Override
    public void releaseAllResources() throws IOException {
        if (partitionRequestListener != null) {
            partitionRequestListener.releaseListener();
        }
        subpartitionView.releaseAllResources();
    }

    @Override
    public void notifyDataAvailable(ResultSubpartitionView view) {//
        //PartitionRequestQueue#notifyReaderNonEmpty
        requestQueue.notifyReaderNonEmpty(this);//
    }

    @Override
    public void notifyPriorityEvent(int prioritySequenceNumber) {
        notifyDataAvailable(this.subpartitionView);
    }

    @Override
    public String toString() {
        return "CreditBasedSequenceNumberingViewReader{"
                + "requestLock="
                + requestLock
                + ", receiverId="
                + receiverId
                + ", numCreditsAvailable="
                + numCreditsAvailable
                + ", isRegisteredAsAvailable="
                + isRegisteredAsAvailable
                + '}';
    }
}
