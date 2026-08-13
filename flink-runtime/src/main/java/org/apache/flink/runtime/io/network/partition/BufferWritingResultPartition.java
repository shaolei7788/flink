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
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.metrics.TimerGauge;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkElementIndex;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link ResultPartition} which writes buffers directly to {@link ResultSubpartition}s. This is
 * in contrast to implementations where records are written to a joint structure, from which the
 * subpartitions draw the data after the write phase is finished, for example the sort-based
 * partitioning.
 *
 * <p>To avoid confusion: On the read side, all subpartitions return buffers (and backlog) to be
 * transported through the network.
 */
public abstract class BufferWritingResultPartition extends ResultPartition {

    /** The subpartitions of this partition. At least one. */
    protected final ResultSubpartition[] subpartitions;

    /**
     * For non-broadcast mode, each subpartition maintains a separate BufferBuilder which might be
     * null.
     */
    private final BufferBuilder[] unicastBufferBuilders;

    /** For broadcast mode, a single BufferBuilder is shared by all subpartitions. */
    private BufferBuilder broadcastBufferBuilder;

    private TimerGauge hardBackPressuredTimeMsPerSecond = new TimerGauge();

    private long totalWrittenBytes;

    public BufferWritingResultPartition(
            String owningTaskName,
            int partitionIndex,
            ResultPartitionID partitionId,
            ResultPartitionType partitionType,
            ResultSubpartition[] subpartitions,
            int numTargetKeyGroups,
            ResultPartitionManager partitionManager,
            @Nullable BufferCompressor bufferCompressor,
            SupplierWithException<BufferPool, IOException> bufferPoolFactory) {

        super(
                owningTaskName,
                partitionIndex,
                partitionId,
                partitionType,
                subpartitions.length,
                numTargetKeyGroups,
                partitionManager,
                bufferCompressor,
                bufferPoolFactory);

        this.subpartitions = checkNotNull(subpartitions);
        this.unicastBufferBuilders = new BufferBuilder[subpartitions.length];
    }

    @Override
    protected void setupInternal() throws IOException {
        // bufferPool.getNumberOfRequiredMemorySegments() = 3
        // getNumberOfSubpartitions() = 2
        checkState(bufferPool.getNumberOfRequiredMemorySegments() >= getNumberOfSubpartitions(),
                "Bug in result partition setup logic: Buffer pool has not enough guaranteed buffers for"
                        + " this result partition.");
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        int totalBuffers = 0;

        for (ResultSubpartition subpartition : subpartitions) {
            totalBuffers += subpartition.unsynchronizedGetNumberOfQueuedBuffers();
        }

        return totalBuffers;
    }

    @Override
    public long getSizeOfQueuedBuffersUnsafe() {
        long totalNumberOfBytes = 0;

        for (ResultSubpartition subpartition : subpartitions) {
            totalNumberOfBytes += Math.max(0, subpartition.getTotalNumberOfBytesUnsafe());
        }

        return totalWrittenBytes - totalNumberOfBytes;
    }

    @Override
    public int getNumberOfQueuedBuffers(int targetSubpartition) {
        checkArgument(targetSubpartition >= 0 && targetSubpartition < numSubpartitions);
        return subpartitions[targetSubpartition].unsynchronizedGetNumberOfQueuedBuffers();
    }

    protected void flushSubpartition(int targetSubpartition, boolean finishProducers) {
        if (finishProducers) {
            finishBroadcastBufferBuilder();
            finishUnicastBufferBuilder(targetSubpartition);
        }

        subpartitions[targetSubpartition].flush();
    }

    //强制将所有子通道内处于“在途、未填满”状态的内存数据块立即对外刷写（Flush）并唤醒网络层发射的“全网总动员令”
    protected void flushAllSubpartitions(boolean finishProducers) {
        if (finishProducers) {//false
            //finishProducers 标志位代表是否要彻底宣告关闭上游的写入流。
            // 如果是 true：通常发生在整个作业彻底运行结束（EndOfPartition）时。
            // Flink 会顺便把广播（Broadcast）和单播（Unicast）的 BufferBuilder 彻底锁死并强行切块（Finish），此后不再接收任何新数据。
            // 当前是 false：说明作业还在高高兴兴地正常运行（Running），只是由于定时刷新器（OutputFlusher）到期或者代码中触发了轻量级周期性的 flush()。
            // 因此，Flink 绝对不能关闭上游的 BufferBuilder（因为等一下还有新数据要写），所以这个 if 块在当前场景下被直接安全跳过
            finishBroadcastBufferBuilder();
            finishUnicastBufferBuilders();
        }

        for (ResultSubpartition subpartition : subpartitions) {
            //PipelinedSubpartition#flush
            subpartition.flush();
        }
    }

    // (HeapByteBuffer,第几个分区)
    @Override
    public void emitRecord(ByteBuffer record, int targetSubpartition) throws IOException {
        //累加当前记录的字节长度
        totalWrittenBytes += record.remaining();
        //获取或向内存池申请一个 BufferBuilder（当前子分区的写缓存区），并尝试把这条新数据（New Record）塞进去
        //三种可能的结果：
        // 情况 A：Buffer 空间很足，Record 被完整写入，且 Buffer 没满。
        // 情况 B：Buffer 恰好满了，Record 也恰好完整写入。
        // 情况 C：Buffer 空间不够，Record 只写了一部分（前半部分），Buffer 就满了
        BufferBuilder buffer = appendUnicastDataForNewRecord(record, targetSubpartition);
        //循环处理大对象或跨 Buffer
        while (record.hasRemaining()) {
            // full buffer, partial record
            //情况 C  即 Buffer 满了，但 record 还没写完
            //把当前已经写满的 Buffer “封口”并发送到发送队列，通知下游可以来拿数据了
            finishUnicastBufferBuilder(targetSubpartition);
            buffer = appendUnicastDataForRecordContinuation(record, targetSubpartition);
        }
        // 走到这里说明record 全部写进Buffer了
        if (buffer.isFull()) {
            // full buffer, full record
            // 情况 B  将其送入发送队列
            finishUnicastBufferBuilder(targetSubpartition);
        }
        // partial buffer, full record
        //如果代码走到最后，Buffer 没满，数据也写完了。Flink 什么都不做
        //【重点】为什么不封口？：为了提高吞吐量。这个 Buffer 会留在内存里，等待下一条 emitRecord 进来时继续往里面拼数据（即 Buffer 复用机制），
        // 直到它满了或者触发了 flush 刷新器（如超时器触发）

    }

    @Override
    public void broadcastRecord(ByteBuffer record) throws IOException {
        totalWrittenBytes += ((long) record.remaining() * numSubpartitions);

        BufferBuilder buffer = appendBroadcastDataForNewRecord(record);

        while (record.hasRemaining()) {
            // full buffer, partial record
            finishBroadcastBufferBuilder();
            buffer = appendBroadcastDataForRecordContinuation(record);
        }

        if (buffer.isFull()) {
            // full buffer, full record
            finishBroadcastBufferBuilder();
        }

        // partial buffer, full record
    }

    @Override
    public void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException {
        checkInProduceState();
        finishBroadcastBufferBuilder();
        finishUnicastBufferBuilders();

        try (BufferConsumer eventBufferConsumer =
                EventSerializer.toBufferConsumer(event, isPriorityEvent)) {
            totalWrittenBytes += ((long) eventBufferConsumer.getWrittenBytes() * numSubpartitions);
            for (ResultSubpartition subpartition : subpartitions) {
                // Retain the buffer so that it can be recycled by each subpartition of
                // targetPartition
                subpartition.add(eventBufferConsumer.copy(), 0);
            }
        }
    }

    @Override
    public void alignedBarrierTimeout(long checkpointId) throws IOException {
        for (ResultSubpartition subpartition : subpartitions) {
            subpartition.alignedBarrierTimeout(checkpointId);
        }
    }

    @Override
    public void abortCheckpoint(long checkpointId, CheckpointException cause) {
        for (ResultSubpartition subpartition : subpartitions) {
            subpartition.abortCheckpoint(checkpointId, cause);
        }
    }

    @Override
    public void setMetricGroup(TaskIOMetricGroup metrics) {
        super.setMetricGroup(metrics);
        hardBackPressuredTimeMsPerSecond = metrics.getHardBackPressuredTimePerSecond();
    }

    @Override
    protected ResultSubpartitionView createSubpartitionView(
            int subpartitionIndex, BufferAvailabilityListener availabilityListener)
            throws IOException {
        checkElementIndex(subpartitionIndex, numSubpartitions, "Subpartition not found.");
        checkState(!isReleased(), "Partition released.");

        ResultSubpartition subpartition = subpartitions[subpartitionIndex];
        //PipelinedSubpartitionView
        ResultSubpartitionView readView = subpartition.createReadView(availabilityListener);//

        LOG.debug("Created {}", readView);

        return readView;
    }

    @Override
    public void finish() throws IOException {
        finishBroadcastBufferBuilder();
        finishUnicastBufferBuilders();

        for (ResultSubpartition subpartition : subpartitions) {
            totalWrittenBytes += subpartition.finish();
        }

        super.finish();
    }

    @Override
    protected void releaseInternal() {
        // Release all subpartitions
        for (ResultSubpartition subpartition : subpartitions) {
            try {
                subpartition.release();
            }
            // Catch this in order to ensure that release is called on all subpartitions
            catch (Throwable t) {
                LOG.error("Error during release of result subpartition: " + t.getMessage(), t);
            }
        }
    }

    @Override
    public void close() {
        // We can not close these buffers in the release method because of the potential race
        // condition. This close method will be only called from the Task thread itself.
        if (broadcastBufferBuilder != null) {
            broadcastBufferBuilder.close();
            broadcastBufferBuilder = null;
        }
        for (int i = 0; i < unicastBufferBuilders.length; ++i) {
            if (unicastBufferBuilders[i] != null) {
                unicastBufferBuilders[i].close();
                unicastBufferBuilders[i] = null;
            }
        }
        super.close();
    }

    //为当前即将写入的新数据（New Record），在指定的下游子分区（Subpartition）中寻找或申请一个可用的内存块（BufferBuilder），并将数据尽可能多地写入进去
    private BufferBuilder appendUnicastDataForNewRecord(final ByteBuffer record, final int targetSubpartition) throws IOException {
        //确保传入的下游子分区索引（targetSubpartition）在合法范围内
        //unicastBufferBuilders 是一个数组，长度等于当前 Task 下游的消费者数量（即并行度）。每个元素对应一个子分区的写缓存
        if (targetSubpartition < 0 || targetSubpartition > unicastBufferBuilders.length) {
            throw new ArrayIndexOutOfBoundsException(targetSubpartition);
        }
        //获取或申请 BufferBuilder（核心逻辑）
        BufferBuilder buffer = unicastBufferBuilders[targetSubpartition];

        if (buffer == null) {
            System.out.println(Thread.currentThread().getName() + ": Creating buffer for subpartition " + targetSubpartition);
            //情况 B（首次写入或旧 Buffer 已满）：如果 buffer == null，说明这是任务刚启动、或者上一个 Buffer 刚刚写满并被“封口”清空了。
            // 向 Task 的本地内存池（LocalBufferPool）申请一块全新的、干净的 32KB 内存块（MemorySegment）
            buffer = requestNewUnicastBufferBuilder(targetSubpartition);//
            //只要申请到新 Buffer，Flink 会立刻将这个 Buffer 的引用丢进下游子分区的发送队列中。
            // 下游的 Netty 线程此时就能看到这个 Buffer，甚至可以在当前 Task 还在往里面写数据的同时，
            // 并行地把已经写进去的部分网络发送出去（这就是 Flink 高吞吐、低延迟的数据流水线机制）
            addToSubpartition(buffer, targetSubpartition, 0, record.remaining());//
        }
        //情况 A（缓存命中）：如果 buffer != null，说明上一次写入后，这个 Buffer 还没有被写满（处于 partial buffer 状态）。Flink 会直接复用它，继续往里面追加数据。
        //更新写入的偏移量
        append(record, buffer);//

        return buffer;
    }

    private int append(ByteBuffer record, BufferBuilder buffer) {
        // Try to avoid hard back-pressure in the subsequent calls to request buffers
        // by ignoring Buffer Debloater hints and extending the buffer if possible (trim).
        // This decreases the probability of hard back-pressure in cases when
        // the output size varies significantly and BD suggests too small values.
        // The hint will be re-applied on the next iteration.
        //检查当前正准备写进去的这条数据尺寸（record.remaining()），是否大于或等于当前 BufferBuilder 剩余的可写字节数（buffer.getWritableBytes()）
        //如果空间足够装下这条数据，直接跳过 if 块，秒进最后的追加逻辑。只有当“现有的坑装不下这条大记录”时，才会触发接下来的安全安检
        if (record.remaining() >= buffer.getWritableBytes()) {
            // This 2nd check is expensive, so it shouldn't be re-ordered.
            // However, it has the same cost as the subsequent call to request buffer, so it doesn't
            // affect the performance much.
            //当发现当前 Buffer 不够装时，Flink 并没有急着去申请新 Buffer，而是先看一眼全局的本地缓冲池 bufferPool.isAvailable() 是否还有余粮
            if (!bufferPool.isAvailable()) {
                // add 1 byte to prevent immediately flushing the buffer and potentially fit the
                // next record
                //计算出将这条大数据完完整整装进去所需要的绝对最小空间，并且故意多加了 1 个字节（+ 1）
                int newSize = buffer.getMaxCapacity() + (record.remaining() - buffer.getWritableBytes()) + 1;
                //强制命令当前的 BufferBuilder 别管 Buffer Debloater 之前给的缩小建议了，
                //就地直接向后拉伸、扩大这块物理内存的可用边界（Limit），强行把它的容量撑大，直到能够吞下这条大记录
                buffer.trim(Math.max(buffer.getMaxCapacity(), newSize));
            }
        }
        //BufferBuilder#appendAndCommit
        //将 ByteBuffer 里的字节拷贝进 BufferBuilder 持有的物理 MemorySegment 堆外内存中，并自动推进（Commit）写指针，
        // 同时更新我们在上两个问题中提到过的共享指针标记 positionMarker，以便下游的 Netty 线程能够实时顺着网线把刚写进去的这段热乎字节读走
        return buffer.appendAndCommit(record);//
    }

    //将一个已经写入了部分或全部数据的内存块（BufferBuilder），
    // 正式“交付”给指定输出通道队列（ResultSubpartition）进行网络排队发送的核心物理枢纽，同时它还负责根据下游的反馈动态调整后续内存块的大小
    private void addToSubpartition(
            BufferBuilder buffer,
            int targetSubpartition,//1
            int partialRecordLength,//0
            int minDesirableBufferSize)
            throws IOException {
        //创建一个只读的消费者对象 BufferConsumer
        BufferConsumer bufferConsumer = buffer.createBufferConsumerFromBeginning();//
        //partialRecordLength ：代表跨内存段截断的未完成记录长度。如果是 0，说明这是一条干净、完整的记录或者刚好写完
        //这是 Flink 2.x 信道自适应流控（Adaptive Buffer Size） 的硬核体现。下游子通道在接收到这块内存的同时，
        // 会根据当前的队列积压情况和网络拥堵状态，反向计算并返回一个“建议的未来缓冲区大小（desirableBufferSize）”。
        //如果网络很顺畅，它会建议继续使用大 Buffer；如果发生反压积压，它会要求缩小接下来的 Buffer 尺寸以降低延迟
        // 【重点】 desirableBufferSize =  2147483647  PipelinedSubpartition#add
        int desirableBufferSize = subpartitions[targetSubpartition].add(bufferConsumer, partialRecordLength);//
        //动态调容：根据下游反馈动态改变后续内存块大小
        resizeBuffer(buffer, desirableBufferSize, minDesirableBufferSize);
    }

    protected int addToSubpartition(
            int targetSubpartition, BufferConsumer bufferConsumer, int partialRecordLength)
            throws IOException {
        totalWrittenBytes += bufferConsumer.getWrittenBytes();
        return subpartitions[targetSubpartition].add(bufferConsumer, partialRecordLength);//
    }

    private void resizeBuffer(
            BufferBuilder buffer, int desirableBufferSize, int minDesirableBufferSize) {
        if (desirableBufferSize > 0) {
            // !! If some of partial data has written already to this buffer, the result size can
            // not be less than written value.
            buffer.trim(Math.max(minDesirableBufferSize, desirableBufferSize));
        }
    }

    private BufferBuilder appendUnicastDataForRecordContinuation(
            final ByteBuffer remainingRecordBytes, final int targetSubpartition)
            throws IOException {
        final BufferBuilder buffer = requestNewUnicastBufferBuilder(targetSubpartition);
        // !! Be aware, in case of partialRecordBytes != 0, partial length and data has to
        // `appendAndCommit` first
        // before consumer is created. Otherwise it would be confused with the case the buffer
        // starting
        // with a complete record.
        // !! The next two lines can not change order.
        final int partialRecordBytes = append(remainingRecordBytes, buffer);
        addToSubpartition(buffer, targetSubpartition, partialRecordBytes, partialRecordBytes);

        return buffer;
    }

    private BufferBuilder appendBroadcastDataForNewRecord(final ByteBuffer record)
            throws IOException {
        BufferBuilder buffer = broadcastBufferBuilder;

        if (buffer == null) {
            buffer = requestNewBroadcastBufferBuilder();
            createBroadcastBufferConsumers(buffer, 0, record.remaining());
        }

        append(record, buffer);

        return buffer;
    }

    private BufferBuilder appendBroadcastDataForRecordContinuation(
            final ByteBuffer remainingRecordBytes) throws IOException {
        final BufferBuilder buffer = requestNewBroadcastBufferBuilder();
        // !! Be aware, in case of partialRecordBytes != 0, partial length and data has to
        // `appendAndCommit` first
        // before consumer is created. Otherwise it would be confused with the case the buffer
        // starting
        // with a complete record.
        // !! The next two lines can not change order.
        final int partialRecordBytes = append(remainingRecordBytes, buffer);
        createBroadcastBufferConsumers(buffer, partialRecordBytes, partialRecordBytes);

        return buffer;
    }

    private void createBroadcastBufferConsumers(
            BufferBuilder buffer, int partialRecordBytes, int minDesirableBufferSize)
            throws IOException {
        try (final BufferConsumer consumer = buffer.createBufferConsumerFromBeginning()) {
            int desirableBufferSize = Integer.MAX_VALUE;
            for (ResultSubpartition subpartition : subpartitions) {
                int subPartitionBufferSize = subpartition.add(consumer.copy(), partialRecordBytes);
                if (subPartitionBufferSize != ResultSubpartition.ADD_BUFFER_ERROR_CODE) {
                    desirableBufferSize = Math.min(desirableBufferSize, subPartitionBufferSize);
                }
            }
            resizeBuffer(buffer, desirableBufferSize, minDesirableBufferSize);
        }
    }

    //在确保当前分区状态合法、且处于单播（Unicast）模式的前提下，向本地缓冲池申请一块全新的物理内存页（BufferBuilder），并将其登记到当前子分区的缓存槽位中，供后续数据写入使用
    private BufferBuilder requestNewUnicastBufferBuilder(int targetSubpartition)
            throws IOException {
        //检查当前 ResultPartition 的状态是否允许写入数据
        //Flink 的 ResultPartition 拥有生命周期状态机（如 INITIALIZED, PRODUCING, CONSUMED, RELEASED）。
        // 如果整个 Task 已经进入取消（Canceling）、失败（Failing）或结束（Finished）阶段，
        // 该校验会直接抛出异常（例如 IllegalStateException），防止在非法状态下继续申请内存或写入数据，造成内存泄漏
        checkInProduceState();
        //传输模式校验
        //作用：强制确保当前运行在单播（Unicast）模式下。
        // 背后的原理：Flink 在输出数据时有两种主要模式：
        //    单播（Unicast）：数据只发往某一个特定的下游子分区（如按 Key 路由或轮询路由）。
        //    广播（Broadcast）：数据需要同时发往所有下游子分区。该方法专门服务于单播场景。在执行前进行此校验，是为了防止模式冲突（例如在广播模式下错误地调用了单播的内存申请逻辑）
        ensureUnicastMode();
        //真正向当前 Task 专属的 LocalBufferPool（本地缓冲池） 发起内存申请请求
        // 该方法会从池中切出一块固定大小（默认 32KB）的堆外内存（Direct MemorySegment）
        final BufferBuilder bufferBuilder = requestNewBufferBuilderFromPool(targetSubpartition);//
        //将新申请到的 bufferBuilder 放入 unicastBufferBuilders 数组中对应的子分区索引（targetSubpartition）槽位上
        unicastBufferBuilders[targetSubpartition] = bufferBuilder;

        return bufferBuilder;
    }

    private BufferBuilder requestNewBroadcastBufferBuilder() throws IOException {
        checkInProduceState();
        ensureBroadcastMode();

        final BufferBuilder bufferBuilder = requestNewBufferBuilderFromPool(0);
        broadcastBufferBuilder = bufferBuilder;
        return bufferBuilder;
    }

    //向当前 Task 绑定的本地缓冲池（LocalBufferPool）申请一块 32KB 的堆外内存（Direct MemorySegment）；
    // 如果池中内存不够，它会阻塞当前线程等待可用内存，并开始统计反压时间
    private BufferBuilder requestNewBufferBuilderFromPool(int targetSubpartition) throws IOException {
        //首先尝试以【非阻塞】（Non-blocking）的方式向 bufferPool 申请内存
        //LocalBufferPool#requestBufferBuilder
        BufferBuilder bufferBuilder = bufferPool.requestBufferBuilder(targetSubpartition);//
        if (bufferBuilder != null) {
            //成功 说明此时本地缓冲池里还有空闲的内存块（MemorySegment）。Flink 直接将其返回，整个过程非常高效，没有任何线程切换或等待开销
            return bufferBuilder;
        }
        //失败（bufferBuilder == null）
        //说明本地缓冲池的内存已经被榨干了。为什么会干？因为下游消费太慢，之前发出去的 Buffer 还没有被 Netty 发送完毕或没有收到下游的 Credit（信用额度）反馈，导致内存块无法释放回池中
        //一但非阻塞申请失败，意味着任务即将进入阻塞状态。Flink 在这里立刻调用 markStart()，开始计算当前 Task 的硬反压时间（Hard Backpressure Time）
        hardBackPressuredTimeMsPerSecond.markStart();
        try {
            //【阻塞等待内存】
            //当前 Task 的执行线程（即处理数据、执行 map/filter 的用户线程）会直接挂起（Wait）。
            // 直到下游网络层通过 Netty 把数据发送出去并把内存块归还给 bufferPool 时，该线程才会被唤醒（Notify）并成功拿到 bufferBuilder
            bufferBuilder = bufferPool.requestBufferBuilderBlocking(targetSubpartition);
            //一旦成功拿到 Buffer 被唤醒，立刻调用 markEnd() 停止反压计时。这样就精确统计出了由于等待内存导致线程卡顿的时间
            hardBackPressuredTimeMsPerSecond.markEnd();
            return bufferBuilder;
        } catch (InterruptedException e) {
            //如果在阻塞等待期间，整个 Flink 任务被用户手动 Cancel、或者上游某个算子发生异常导致 Task 需要关闭，其他线程会向当前等待线程发送中断信号（Interrupt）
            //捕获中断异常，将其转化为 IOException 向上抛出，从而安全地终止当前 Task，防止线程永远死锁在等待 Buffer 的地方（导致 Task 卡在 Canceling 状态）
            throw new IOException("Interrupted while waiting for buffer");
        }
    }

    private void finishUnicastBufferBuilder(int targetSubpartition) {
        final BufferBuilder bufferBuilder = unicastBufferBuilders[targetSubpartition];
        if (bufferBuilder != null) {
            int bytes = bufferBuilder.finish();
            resultPartitionBytes.inc(targetSubpartition, bytes);
            numBytesOut.inc(bytes);
            numBuffersOut.inc();
            unicastBufferBuilders[targetSubpartition] = null;
            bufferBuilder.close();
        }
    }

    private void finishUnicastBufferBuilders() {
        for (int subpartition = 0; subpartition < numSubpartitions; subpartition++) {
            finishUnicastBufferBuilder(subpartition);
        }
    }

    private void finishBroadcastBufferBuilder() {
        if (broadcastBufferBuilder != null) {
            // 广播模式
            int bytes = broadcastBufferBuilder.finish();
            resultPartitionBytes.incAll(bytes);
            numBytesOut.inc(bytes * numSubpartitions);
            numBuffersOut.inc(numSubpartitions);
            broadcastBufferBuilder.close();
            broadcastBufferBuilder = null;
        }
    }

    private void ensureUnicastMode() {
        finishBroadcastBufferBuilder();//
    }

    private void ensureBroadcastMode() {
        finishUnicastBufferBuilders();
    }

    @VisibleForTesting
    public TimerGauge getHardBackPressuredTimeMsPerSecond() {
        return hardBackPressuredTimeMsPerSecond;
    }

    @VisibleForTesting
    public ResultSubpartition[] getAllPartitions() {
        return subpartitions;
    }
}
