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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.core.memory.MemorySegmentProvider;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.RecoveryMetadata;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.PrioritizedDeque;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageInputChannelId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyServiceImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.AvailabilityNotifier;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageConsumerClient;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageConsumerSpec;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.throughput.BufferDebloater;
import org.apache.flink.runtime.throughput.ThroughputCalculator;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.SupplierWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An input gate consumes one or more partitions of a single produced intermediate result.
 *
 * <p>Each intermediate result is partitioned over its producing parallel subtasks; each of these
 * partitions is furthermore partitioned into one or more subpartitions.
 *
 * <p>As an example, consider a map-reduce program, where the map operator produces data and the
 * reduce operator consumes the produced data.
 *
 * <pre>{@code
 * +-----+              +---------------------+              +--------+
 * | Map | = produce => | Intermediate Result | <= consume = | Reduce |
 * +-----+              +---------------------+              +--------+
 * }</pre>
 *
 * <p>When deploying such a program in parallel, the intermediate result will be partitioned over
 * its producing parallel subtasks; each of these partitions is furthermore partitioned into one or
 * more subpartitions.
 *
 * <pre>{@code
 *                            Intermediate result
 *               +-----------------------------------------+
 *               |                      +----------------+ |              +-----------------------+
 * +-------+     | +-------------+  +=> | Subpartition 1 | | <=======+=== | Input Gate | Reduce 1 |
 * | Map 1 | ==> | | Partition 1 | =|   +----------------+ |         |    +-----------------------+
 * +-------+     | +-------------+  +=> | Subpartition 2 | | <==+    |
 *               |                      +----------------+ |    |    | Subpartition request
 *               |                                         |    |    |
 *               |                      +----------------+ |    |    |
 * +-------+     | +-------------+  +=> | Subpartition 1 | | <==+====+
 * | Map 2 | ==> | | Partition 2 | =|   +----------------+ |    |         +-----------------------+
 * +-------+     | +-------------+  +=> | Subpartition 2 | | <==+======== | Input Gate | Reduce 2 |
 *               |                      +----------------+ |              +-----------------------+
 *               +-----------------------------------------+
 * }</pre>
 *
 * <p>In the above example, two map subtasks produce the intermediate result in parallel, resulting
 * in two partitions (Partition 1 and 2). Each of these partitions is further partitioned into two
 * subpartitions -- one for each parallel reduce subtask.
 */
//负责对接上游所有的发送端，把杂乱、异步的网络数据网络块（Buffer）打包整理好，以统一的迭代器形式提供给下游的算子线程（Task 线程）消费
//1. 输入信道的集合管理器（Channel Aggregator）一个算子通常会接收来自上游多个并发实例（Subtasks）的数据。
//   SingleInputGate 内部管理着一组 InputChannel。如果上游在同台机器（同 TaskManager），
//   它会路由给 LocalInputChannel（走内存复制，极快）。如果上游在远程机器，它会路由给 RemoteInputChannel（走 Netty 网络传输）。
//   SingleInputGate 将这些不同类型的通道屏蔽掉，对上层展现出统一的输入源视图。
//2. 协调基于 Credit 的流量控制（Credit Coordinator）正如前面提到的，Flink 是通过 Credit（信用额度）来控制反压的。
//   SingleInputGate 负责向其持有的所有 RemoteInputChannel 分配全局的、动态的内存缓冲块（Buffer Pool）。当它的专属内存池（BufferPool）有空闲时，
//   它会触发各个 RemoteInputChannel 向网络上游发送 Credit（即发放“准送证”），从而在最前端控制数据的流入速度。
//3. 跨通道的数据反序列化流控（Buffer 级别的多路复用）上游并发发送过来的数据在网络层是交织在一起的。
//   SingleInputGate 内部维护了一个全局的可用数据队列（inputChannelsWithData）。
//   任何一个子通道（Local 或 Remote）一旦收到了一个完整的 Buffer 数据，
//   就会把自己登记到 SingleInputGate 的这个就绪队列中。
//4. 统一的数据拉取接口（面向 Task 线程）对于下游的算子执行线程（如 StreamTask）来说，它不需要感知复杂的网络细节。它只需要不断地调用：inputGate.getNext()
//   就会从就绪队列中弹出一个 Buffer（真实数据）或者 Event（如 Checkpoint Barrier、Watermark、EndOfPartition 信号），交给算子处理
public class SingleInputGate extends IndexedInputGate {

    private static final Logger LOG = LoggerFactory.getLogger(SingleInputGate.class);

    /** Lock object to guard partition requests and runtime channel updates. */
    private final Object requestLock = new Object();

    //记录当前 SingleInputGate 所属的消费端 Task 的名称（如 "Source: Custom Source -> Flat Map (1/4)"）
    /** The name of the owning task, for logging purposes. */
    private final String owningTaskName;

    //当前 InputGate 在所属 Task 所有的输入网关（InputGates）列表中的唯一索引序号（从 0 开始）
    private final int gateIndex;

    /**
     * The ID of the consumed intermediate result. Each input gate consumes partitions of the
     * intermediate result specified by this ID. This ID also identifies the input gate at the
     * consuming task.
     */
    //当前 InputGate 所消费的逻辑中间结果集（IntermediateDataSet）**的唯一 ID。物理意义：
    // 在 Flink 生成的执行拓扑图（JobGraph/ExecutionGraph）中，上游算子产生的输出是一个逻辑数据集。此 ID 建立了当前输入网关与上游数据源的逻辑绑定关系
    private final IntermediateDataSetID consumedResultId;

    //标记上游生产端的分区数据传输与存储类型（如 PIPELINED 流式、BLOCKING 批处理、HYBRID 混合等）
    //如果是 PIPELINED，InputGate 会采用基于 Credit 的流控持续接收数据。如果是 BLOCKING（如 MapReduce 的 Shuffle），则需要等待上游全部写完后，采取批量拉取或文件的形式消费
    /** The type of the partition the input gate is consuming. */
    private final ResultPartitionType consumedPartitionType;

    /** The number of input channels (equivalent to the number of consumed partitions). */
    //当前 InputGate 内部包含的输入通道（InputChannel）的总数量。
    // 物理意义：在数量上它严格等于上游并发的分区数（Subpartitions）。这个值一旦在部署时确定，其底层对应的物理通道数组大小也就固定了，用于边界检查和循环遍历
    private final int numberOfInputChannels;

    //核心作用：一个多维 Map 嵌套结构，用于通过上游分区的物理 ID 或通道流信息，快速反查对应的 InputChannel 实例。
    // 物理意义：专为运行时动态更新（Runtime Updates）设计。在 Flink 发生局部 Failover（容错恢复）、动态扩缩容或下游 Task 延迟拉起时，上游的部署位置可能会发生改变。
    // 当 ResourceManager 通知 Task 改变上游物理连接时，Flink 靠这个 Map 快速精确定位并替换/更新某一个特定的物理通道，而不需要重建整个 InputGate
    /** Input channels. We store this in a map for runtime updates of single channels. */
    private final Map<IntermediateResultPartitionID, Map<InputChannelInfo, InputChannel>> inputChannels;

    //一个一维数组，按顺序存放了当前 InputGate 拥有的所有 InputChannel（包括远程 Netty 通道和本地内存通道）
    @GuardedBy("requestLock")
    private final InputChannel[] channels;

    //个带有优先级的双端队列（Deque），里面存放的是当前已经有可用数据 Buffer 溢满、等待被下游消费的 InputChannel 列表
    /** Channels, which notified this input gate about available data. */
    private final PrioritizedDeque<InputChannel> inputChannelsWithData = new PrioritizedDeque<>();

    /**
     * Field guaranteeing uniqueness for inputChannelsWithData queue. Both of those fields should be
     * unified onto one.
     */
    //核心作用：一个基于位图（BitSet）的标记结构，每一位（Bit）对应一个 Channel 的数组下标。
    // 物理意义：保证 inputChannelsWithData 队列的唯一性与去重，实现极高的无锁/低锁性能。
    // 痛点：上游网络数据是源源不断流入的，一个 Channel 内部可能积压了多个 Buffer。
    // 如果每次来一个 Buffer 都把 Channel 插入到队列里，会导致队列重复、 OOM 或破坏消费顺序。
    // 解决手段：当 Channel 有数据时，先通过 BitSet.get(channelIndex) 检查它是否已经在就绪队列里了。
    // 如果在，仅把数据追加到 Channel 内部的私有队列，不再重复进 InputGate 的总队列；
    // 如果不在，才将其入队并将 BitSet 设为 true。当该 Channel 的数据被完全消费完时，BitSet 重新置为 false。
    // 注释中也提到 “Both of those fields should be unified onto one.”（未来应将这两个字段合二为一），
    // 因为它们在逻辑上具有极强的原子绑定性，必须使用内部锁 inputChannelsWithData 来保证它们状态的绝对一致
    @GuardedBy("inputChannelsWithData")
    private final BitSet enqueuedInputChannelsWithData;

    //通过位图（BitSet）记录哪些输入通道（Channel）已经接收到了 EndOfPartitionEvent
    //当某个上游并发任务（Task）彻底跑完并把所有数据发完后，会发送一个结束墓碑标识（EndOfPartitionEvent）。
    // SingleInputGate 靠这个位图实时对每个通道进行打勾。当位图中置 1 的数量等于通道总数时，意味着所有上游的数据已经全部接收完毕
    @GuardedBy("inputChannelsWithData")
    private final BitSet channelsWithEndOfPartitionEvents;

    //记录哪些通道已经接收到了 EndOfData 事件（即用户业务数据已结束，但可能还有后续的 Checkpoint Barrier 或元数据事件）
    //物理意义：这是 Flink 引入的两阶段流关闭机制的核心载体。它区分了“没有业务数据了”和“通道完全关闭”。
    // 当所有通道在该位图上都标记为 1 时，Task 就会触发 finish() 生命周期，开始处理最后残留的定时器（Timers）或触发最后的快照，随后才完全退出
    @GuardedBy("inputChannelsWithData")
    private final BitSet channelsWithEndOfUserRecords;

    //记录每个 Channel 上一次处理的高优先级事件（如 Checkpoint Barrier）的序列号（Sequence Number）
    @GuardedBy("inputChannelsWithData")
    private int[] lastPrioritySequenceNumber;

    //一个状态监听器/提供者，用于向 JobManager（或 TaskExecutor）反向查询上游生产端分区的真实运行状态
    /** The partition producer state listener. */
    private final PartitionProducerStateProvider partitionProducerStateProvider;

    /**
     * Buffer pool for incoming buffers. Incoming data from remote channels is copied to buffers
     * from this pool.
     */
    //当前 InputGate 专属的本地内存池（通常是 LocalBufferPool 实例）
    private BufferPool bufferPool;

    private boolean hasReceivedAllEndOfPartitionEvents;

    private boolean hasReceivedEndOfData;

    //标记当前 InputGate 是否已经向上游发起过数据拉取请求（Request Partitions）
    //防止重复请求。Flink 的 Task 启动后，InputGate 需要主动向对应的物理节点（本地或远程）发送连接请求拉取数据。该布尔值初始为 false，
    // 在 Task 准备就绪并第一次调用 requestPartitions() 后置为 true。它保证了网络连接通道的单次初始化路由逻辑
    /** Flag indicating whether partitions have been requested. */
    private boolean requestedPartitionsFlag;

    //用于暂存那些在物理通道（Channels）完全建立之前，或者在特定生命周期阶段到达的控制事件
    private final List<TaskEvent> pendingEvents = new ArrayList<>();

    //记录当前 InputGate 内部尚未完成初始化的通道数量
    private int numberOfUninitializedChannels;

    /** A timer to retrigger local partition requests. Only initialized if actually needed. */
    //核心作用：本地连接重试定时器
    //物理意义：在上游 Task 刚刚部署，或者因局部异常正在重启时，下游的 LocalInputChannel 去申请本地内存共享队列可能会遭遇“生产者尚未就绪”的情况。
    // 该定时器用于实现指数退避重试（Exponential Backoff Retry），在不阻塞主线程的前提下，异步、定时地重新发起本地数据拉取请求
    private Timer retriggerLocalRequestTimer;

    private final SupplierWithException<BufferPool, IOException> bufferPoolFactory;

    //核心作用：当前 InputGate 销毁/关闭状态的异步未来凭证。物理意义：当系统触发取消（Cancel）或者作业正常结束调用 close() 时，
    // closeFuture 会被激活。TaskExecutor 的守护线程或其他上层监控组件通过监听这个 Future，
    // 能够以非阻塞的方式百分之百确保该 InputGate 的所有网络连接已断开、所有 Buffer 内存已彻底释放归还，防止因死锁导致的 Task 残留和内存泄漏
    private final CompletableFuture<Void> closeFuture;

    //Buffer 数据解压器
    //在批处理（Batch）或者大吞吐 Shuffle 场景下，上游通常会对写往磁盘/网络的 Buffer 进行压缩（如 LZ4 算法）以节省带宽和 I/O。
    // 当数据流进入 SingleInputGate 后，该组件在数据递交给下游反序列化前，对其进行高效的硬件级/内存级解压
    @Nullable private final BufferDecompressor bufferDecompressor;

    //核心作用：内存段工厂提供者。物理意义：为上述 unpooledSegment 或其它临时需要的急用内存块提供分配和回收的底层抽象，通常直接对接 TaskManager 的全局大内存池
    private final MemorySegmentProvider memorySegmentProvider;

    /**
     * The segment to read data from file region of bounded blocking partition by local input
     * channel.
     */
    //非池化的、独立的内存段
    //物理意义：在**批处理阻塞分区（Bounded Blocking Partition）**的本地消费场景下，下游 Task 需要通过文件通道（File Region）直接读取本地磁盘文件。
    // 为了不挤占高频流处理的 bufferPool 动态内存，
    // InputGate 专门开辟这块固定不变的独占内存（unpooledSegment），专门用于将本地磁盘中的 Shuffle 文件块直接 DMA 拷贝或读取到内存中。
    private final MemorySegment unpooledSegment;

    //实时吞吐量计算器。它通过滑动窗口（Sliding Window）精准统计 SingleInputGate 每一秒钟实际消费了多少个字节（Bytes）和多少个 Buffer
    private final ThroughputCalculator throughputCalculator;
    //核心作用：Buffer 自动减重/去虚胖器。
    // 物理意义：解决 Flink 传统反压模型中积压延迟太高的问题
    private final BufferDebloater bufferDebloater;
    //核心作用：控制在收到 EndOfData 事件时，是否排空（Drain）当前算子内部积压的数据。物理意义：在 Flink 的 Stop-with-Savepoint（带状态暂停） 流程中，当系统发出停止信号后，上游会发一个 EndOfData。如果该布尔值为 true，
    // InputGate 会强制要求下游算子必须把当前网络通道和算子内部缓存里的所有残留业务数据彻底处理完，并在其后立即触发一次最终的同步快照，确保数据“不重不漏”
    private boolean shouldDrainOnEndOfData = true;

    //核心作用：分层存储的消费端客户端接口。物理意义：当开启 Tiered Storage 后，
    // SingleInputGate 不再完全依赖传统的 RemoteInputChannel 或 LocalInputChannel 去硬编码拉取数据，
    // 而是将“向何处读、怎么读”的逻辑全权委托给这个 Client。Client 内部会决定是去调 Netty 远程拉取，还是直接去对象存储、本地磁盘读取对应的 Shuffle 文件块
    // The consumer client will be null if the tiered storage is not enabled.
    @Nullable private TieredStorageConsumerClient tieredStorageConsumerClient;

    //核心作用：分层存储消费规范/描述符列表。物理意义：存储了当前 InputGate 消费上游分区的核心元数据（如上游 Partition ID、消费的 Subpartition 范围、使用的存储层类型等）。
    // Client 根据这些 Spec 规范去精确定位并初始化底层的读取流
    // The consumer specs in tiered storage will be null if the tiered storage is not enabled.
    @Nullable private List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs;

    //分层存储的数据可用性通知器。物理意义：由于分层存储可能对接异步的外部系统（如 HDFS 或混合层），
    // 当底层存储层准备好下一批数据块时，该组件会异步唤醒 SingleInputGate 或者是其内部队列，实现了事件驱动的异步流式消费，避免消费线程盲目轮询
    // The availability notifier will be null if the tiered storage is not enabled.
    @Nullable private AvailabilityNotifier availabilityNotifier;

    /**
     * A map containing the status of the last consumed buffer in each input channel. The status
     * contains the following information: 1) whether the buffer contains partial record, and 2) the
     * index of the subpartition where the buffer comes from.
     */
    //记录分层存储模式下，每个通道上一次消费的 Buffer 状态映射表
    //Boolean（是否包含不完整记录）：如果一个长记录（Record）被切分到了两个连续的 Buffer 中，该状态会标记为 true。
    // 反序列化器（Deserializer）看到后，就知道该 Buffer 的数据必须和下一个 Buffer 进行拼接，防止因分层存储预拉取或切块导致的数据断裂
    private final Map<Integer, Tuple2<Boolean, Integer>> lastBufferStatusMapInTieredStore = new HashMap<>();

    /** A map of counters for the number of {@link EndOfData}s received from each input channel. */
    //核心作用：每个通道接收到 EndOfData 和 EndOfPartitionEvent 的计数器数组（长度等于通道数）。物理意义：用来支撑比第一阶段的 BitSet 更复杂的动态拓扑结构（例如在上游算子动态缩容、多次重调度、或者多层嵌套流合并时）。
    // 普通的 BitSet 只能表达“到没到”，而计数器可以精准应对上游因为推流重试导致多次发送结束标记的情况，提供更鲁棒的流生命周期终结判定
    private final int[] endOfDatas;

    /**
     * A map of counters for the number of {@link EndOfPartitionEvent}s received from each input
     * channel.
     */
    private final int[] endOfPartitions;

    public SingleInputGate(
            String owningTaskName,
            int gateIndex,
            IntermediateDataSetID consumedResultId,
            final ResultPartitionType consumedPartitionType,
            int numberOfInputChannels,
            PartitionProducerStateProvider partitionProducerStateProvider,
            SupplierWithException<BufferPool, IOException> bufferPoolFactory,
            @Nullable BufferDecompressor bufferDecompressor,
            MemorySegmentProvider memorySegmentProvider,
            int segmentSize,
            ThroughputCalculator throughputCalculator,
            @Nullable BufferDebloater bufferDebloater) {

        this.owningTaskName = checkNotNull(owningTaskName);
        Preconditions.checkArgument(0 <= gateIndex, "The gate index must be positive.");
        this.gateIndex = gateIndex;

        this.consumedResultId = checkNotNull(consumedResultId);
        this.consumedPartitionType = checkNotNull(consumedPartitionType);
        this.bufferPoolFactory = checkNotNull(bufferPoolFactory);

        checkArgument(numberOfInputChannels > 0);
        this.numberOfInputChannels = numberOfInputChannels;

        this.inputChannels = CollectionUtil.newHashMapWithExpectedSize(numberOfInputChannels);
        this.channels = new InputChannel[numberOfInputChannels];
        this.channelsWithEndOfPartitionEvents = new BitSet(numberOfInputChannels);
        this.channelsWithEndOfUserRecords = new BitSet(numberOfInputChannels);
        this.enqueuedInputChannelsWithData = new BitSet(numberOfInputChannels);
        this.lastPrioritySequenceNumber = new int[numberOfInputChannels];
        Arrays.fill(lastPrioritySequenceNumber, Integer.MIN_VALUE);

        this.partitionProducerStateProvider = checkNotNull(partitionProducerStateProvider);

        this.bufferDecompressor = bufferDecompressor;
        this.memorySegmentProvider = checkNotNull(memorySegmentProvider);

        this.closeFuture = new CompletableFuture<>();

        this.unpooledSegment = MemorySegmentFactory.allocateUnpooledSegment(segmentSize);
        this.bufferDebloater = bufferDebloater;
        this.throughputCalculator = checkNotNull(throughputCalculator);

        this.tieredStorageConsumerClient = null;
        this.tieredStorageConsumerSpecs = null;
        this.availabilityNotifier = null;

        this.endOfDatas = new int[numberOfInputChannels];
        Arrays.fill(endOfDatas, 0);
        this.endOfPartitions = new int[numberOfInputChannels];
        Arrays.fill(endOfPartitions, 0);
    }

    protected PrioritizedDeque<InputChannel> getInputChannelsWithData() {
        return inputChannelsWithData;
    }

    @Override
    public void setup() throws IOException {
        checkState(
                this.bufferPool == null,
                "Bug in input gate setup logic: Already registered buffer pool.");
        //todo resultPartitionFactory = ResultPartitionFactory   对应SingleInputGateFactory#createBufferPoolFactory
        // bufferPool = LocalBufferPool
        // 2. 创建 LocalBufferPool（浮动 Buffer 池）
        BufferPool bufferPool = bufferPoolFactory.get();
        //设置bufferPool
        setBufferPool(bufferPool);
        if (tieredStorageConsumerClient != null) {
            tieredStorageConsumerClient.setup(bufferPool);
        }
        //为所有的InputChannel分配专用buffer
        setupChannels();
    }

    @Override
    public CompletableFuture<Void> getStateConsumedFuture() {
        synchronized (requestLock) {
            List<CompletableFuture<?>> futures = new ArrayList<>(numberOfInputChannels);
            for (InputChannel inputChannel : inputChannels()) {
                if (inputChannel instanceof RecoveredInputChannel) {
                    futures.add(((RecoveredInputChannel) inputChannel).getStateConsumedFuture());
                }
            }
            return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        }
    }

    @Override
    public void requestPartitions() {
        synchronized (requestLock) {
            if (!requestedPartitionsFlag) {
                if (closeFuture.isDone()) {
                    throw new IllegalStateException("Already released.");
                }

                // Sanity checks
                long numInputChannels =
                        inputChannels.values().stream().mapToLong(x -> x.values().size()).sum();
                if (numberOfInputChannels != numInputChannels) {
                    throw new IllegalStateException(
                            String.format(
                                    "Bug in input gate setup logic: mismatch between "
                                            + "number of total input channels [%s] and the currently set number of input "
                                            + "channels [%s].",
                                    numInputChannels, numberOfInputChannels));
                }

                convertRecoveredInputChannels();//
                internalRequestPartitions();//
            }

            requestedPartitionsFlag = true;
            // Start the reader only when all InputChannels have been converted to either
            // LocalInputChannel or RemoteInputChannel, as this will prevent RecoveredInputChannels
            // from being queued again.
            if (enabledTieredStorage()) {
                tieredStorageConsumerClient.start();
            }
        }
    }

    @VisibleForTesting
    public void convertRecoveredInputChannels() {
        LOG.debug("Converting recovered input channels ({} channels)", getNumberOfInputChannels());
        for (Map<InputChannelInfo, InputChannel> inputChannelsForCurrentPartition :
                inputChannels.values()) {
            Set<InputChannelInfo> oldInputChannelInfos =
                    new HashSet<>(inputChannelsForCurrentPartition.keySet());
            for (InputChannelInfo inputChannelInfo : oldInputChannelInfos) {
                InputChannel inputChannel = inputChannelsForCurrentPartition.get(inputChannelInfo);
                if (inputChannel instanceof RecoveredInputChannel) {
                    try {
                        InputChannel realInputChannel = ((RecoveredInputChannel) inputChannel).toInputChannel();//
                        inputChannel.releaseAllResources();
                        inputChannelsForCurrentPartition.remove(inputChannelInfo);
                        inputChannelsForCurrentPartition.put(
                                realInputChannel.getChannelInfo(), realInputChannel);
                        channels[inputChannel.getChannelIndex()] = realInputChannel;
                    } catch (Throwable t) {
                        inputChannel.setError(t);
                        return;
                    }
                }
            }
        }
    }

    private void internalRequestPartitions() {
        for (InputChannel inputChannel : inputChannels()) {
            try {
                //本地 LocalInputChannel#requestSubpartitions
                //远程 RemoteInputChannel#requestSubpartitions
                inputChannel.requestSubpartitions();//
            } catch (Throwable t) {
                inputChannel.setError(t);
                return;
            }
        }
    }

    @Override
    public void finishReadRecoveredState() throws IOException {
        for (final InputChannel channel : channels) {
            if (channel instanceof RecoveredInputChannel) {
                ((RecoveredInputChannel) channel).finishReadRecoveredState();
            }
        }
    }

    // ------------------------------------------------------------------------
    // Properties
    // ------------------------------------------------------------------------

    @Override
    public int getNumberOfInputChannels() {
        return numberOfInputChannels;
    }

    @Override
    public int getGateIndex() {
        return gateIndex;
    }

    @Override
    public List<InputChannelInfo> getUnfinishedChannels() {
        List<InputChannelInfo> unfinishedChannels =
                new ArrayList<>(
                        numberOfInputChannels - channelsWithEndOfPartitionEvents.cardinality());
        synchronized (inputChannelsWithData) {
            for (int i = channelsWithEndOfPartitionEvents.nextClearBit(0);
                    i < numberOfInputChannels;
                    i = channelsWithEndOfPartitionEvents.nextClearBit(i + 1)) {
                unfinishedChannels.add(getChannel(i).getChannelInfo());
            }
        }

        return unfinishedChannels;
    }

    @VisibleForTesting
    int getBuffersInUseCount() {
        int total = 0;
        for (InputChannel channel : channels) {
            total += channel.getBuffersInUseCount();
        }
        return total;
    }

    @VisibleForTesting
    public void announceBufferSize(int newBufferSize) {
        for (InputChannel channel : channels) {
            if (!channel.isReleased()) {
                channel.announceBufferSize(newBufferSize);
            }
        }
    }

    @Override
    public void triggerDebloating() {
        if (isFinished() || closeFuture.isDone()) {
            return;
        }

        checkState(bufferDebloater != null, "Buffer debloater should not be null");
        final long currentThroughput = throughputCalculator.calculateThroughput();
        bufferDebloater
                .recalculateBufferSize(currentThroughput, getBuffersInUseCount())
                .ifPresent(this::announceBufferSize);
    }

    public Duration getLastEstimatedTimeToConsume() {
        return bufferDebloater.getLastEstimatedTimeToConsumeBuffers();
    }

    @Override
    public ResultPartitionType getConsumedPartitionType() {
        return consumedPartitionType;
    }

    BufferProvider getBufferProvider() {
        return bufferPool;
    }

    public BufferPool getBufferPool() {
        return bufferPool;
    }

    MemorySegmentProvider getMemorySegmentProvider() {
        return memorySegmentProvider;
    }

    public String getOwningTaskName() {
        return owningTaskName;
    }

    public int getNumberOfQueuedBuffers() {
        // re-try 3 times, if fails, return 0 for "unknown"
        for (int retry = 0; retry < 3; retry++) {
            try {
                int totalBuffers = 0;

                for (InputChannel channel : inputChannels()) {
                    totalBuffers += channel.unsynchronizedGetNumberOfQueuedBuffers();
                }

                return totalBuffers;
            } catch (Exception ex) {
                LOG.debug("Fail to get number of queued buffers :", ex);
            }
        }

        return 0;
    }

    public long getSizeOfQueuedBuffers() {
        // re-try 3 times, if fails, return 0 for "unknown"
        for (int retry = 0; retry < 3; retry++) {
            try {
                long totalSize = 0;

                for (InputChannel channel : inputChannels()) {
                    totalSize += channel.unsynchronizedGetSizeOfQueuedBuffers();
                }

                return totalSize;
            } catch (Exception ex) {
                LOG.debug("Fail to get size of queued buffers :", ex);
            }
        }

        return 0;
    }

    public CompletableFuture<Void> getCloseFuture() {
        return closeFuture;
    }

    @Override
    public InputChannel getChannel(int channelIndex) {
        return channels[channelIndex];
    }

    // ------------------------------------------------------------------------
    // Setup/Life-cycle
    // ------------------------------------------------------------------------

    public void setBufferPool(BufferPool bufferPool) {
        checkState(
                this.bufferPool == null,
                "Bug in input gate setup logic: buffer pool has"
                        + "already been set for this input gate.");

        this.bufferPool = checkNotNull(bufferPool);
    }

    /** Assign the exclusive buffers to all remote input channels directly for credit-based mode. */
    @VisibleForTesting
    public void setupChannels() throws IOException {
        // Allocate enough exclusive and floating buffers to guarantee that job can make progress.
        // Note: An exception will be thrown if there is no buffer available in the given timeout.

        // First allocate a single floating buffer to avoid potential deadlock when the exclusive
        // buffer is 0. See FLINK-24035 for more information.
        bufferPool.reserveSegments(1);

        // Next allocate the exclusive buffers per channel when the number of exclusive buffer is
        // larger than 0.
        synchronized (requestLock) {
            for (InputChannel inputChannel : inputChannels()) {
                inputChannel.setup();
            }
        }
    }

    public void setInputChannels(InputChannel... channels) {
        if (channels.length != numberOfInputChannels) {
            throw new IllegalArgumentException(
                    "Expected "
                            + numberOfInputChannels
                            + " channels, "
                            + "but got "
                            + channels.length);
        }
        synchronized (requestLock) {
            System.arraycopy(channels, 0, this.channels, 0, numberOfInputChannels);
            for (InputChannel inputChannel : channels) {
                if (inputChannels
                                        .computeIfAbsent(
                                                inputChannel.getPartitionId().getPartitionId(),
                                                ignored -> new HashMap<>())
                                        .put(inputChannel.getChannelInfo(), inputChannel)
                                == null
                        && inputChannel instanceof UnknownInputChannel) {

                    numberOfUninitializedChannels++;
                }
            }
        }
    }

    public void setTieredStorageService(
            List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs,
            TieredStorageConsumerClient client,
            TieredStorageNettyServiceImpl nettyService) {
        this.tieredStorageConsumerSpecs = tieredStorageConsumerSpecs;
        this.tieredStorageConsumerClient = client;
        if (client != null) {
            this.availabilityNotifier = new AvailabilityNotifierImpl();
            setupTieredStorageNettyService(nettyService, tieredStorageConsumerSpecs);
            client.registerAvailabilityNotifier(availabilityNotifier);
        }
    }

    public void updateInputChannel(
            ResourceID localLocation, NettyShuffleDescriptor shuffleDescriptor)
            throws IOException, InterruptedException {
        synchronized (requestLock) {
            if (closeFuture.isDone()) {
                // There was a race with a task failure/cancel
                return;
            }

            IntermediateResultPartitionID partitionId =
                    shuffleDescriptor.getResultPartitionID().getPartitionId();

            Map<InputChannelInfo, InputChannel> newInputChannels = new HashMap<>();
            for (InputChannel current : inputChannels.get(partitionId).values()) {
                if (current instanceof UnknownInputChannel) {
                    UnknownInputChannel unknownChannel = (UnknownInputChannel) current;
                    boolean isLocal = shuffleDescriptor.isLocalTo(localLocation);
                    InputChannel newChannel;
                    if (isLocal) {
                        newChannel =
                                unknownChannel.toLocalInputChannel(
                                        shuffleDescriptor.getResultPartitionID());
                    } else {
                        RemoteInputChannel remoteInputChannel =
                                unknownChannel.toRemoteInputChannel(
                                        shuffleDescriptor.getConnectionId(),
                                        shuffleDescriptor.getResultPartitionID());
                        remoteInputChannel.setup();
                        newChannel = remoteInputChannel;
                    }
                    LOG.debug(
                            "{}: Updated unknown input channel to {}.", owningTaskName, newChannel);

                    newInputChannels.put(newChannel.getChannelInfo(), newChannel);
                    channels[current.getChannelIndex()] = newChannel;

                    if (requestedPartitionsFlag) {
                        newChannel.requestSubpartitions();
                    }

                    for (TaskEvent event : pendingEvents) {
                        newChannel.sendTaskEvent(event);
                    }

                    if (--numberOfUninitializedChannels == 0) {
                        pendingEvents.clear();
                    }
                    if (enabledTieredStorage()) {
                        TieredStoragePartitionId tieredStoragePartitionId =
                                TieredStorageIdMappingUtils.convertId(
                                        shuffleDescriptor.getResultPartitionID());
                        TieredStorageConsumerSpec spec =
                                checkNotNull(tieredStorageConsumerSpecs)
                                        .get(current.getChannelIndex());
                        for (int subpartitionId : spec.getSubpartitionIds().values()) {
                            tieredStorageConsumerClient.updateTierShuffleDescriptors(
                                    tieredStoragePartitionId,
                                    spec.getInputChannelId(),
                                    new TieredStorageSubpartitionId(subpartitionId),
                                    checkNotNull(shuffleDescriptor.getTierShuffleDescriptors()));
                        }
                        queueChannel(newChannel, null, false);
                    }
                }
            }

            inputChannels.put(partitionId, newInputChannels);
        }
    }

    /** Retriggers a partition request. */
    public void retriggerPartitionRequest(
            IntermediateResultPartitionID partitionId, InputChannelInfo inputChannelInfo)
            throws IOException {
        synchronized (requestLock) {
            if (!closeFuture.isDone()) {
                final InputChannel ch = inputChannels.get(partitionId).get(inputChannelInfo);

                checkNotNull(ch, "Unknown input channel with ID " + partitionId);

                LOG.debug(
                        "{}: Retriggering partition request {}:{}.",
                        owningTaskName,
                        ch.partitionId,
                        ch.getConsumedSubpartitionIndexSet());

                if (ch.getClass() == RemoteInputChannel.class) {
                    final RemoteInputChannel rch = (RemoteInputChannel) ch;
                    rch.retriggerSubpartitionRequest();
                } else if (ch.getClass() == LocalInputChannel.class) {
                    final LocalInputChannel ich = (LocalInputChannel) ch;

                    if (retriggerLocalRequestTimer == null) {
                        retriggerLocalRequestTimer = new Timer(true);
                    }

                    ich.retriggerSubpartitionRequest(retriggerLocalRequestTimer);
                } else {
                    throw new IllegalStateException(
                            "Unexpected type of channel to retrigger partition: " + ch.getClass());
                }
            }
        }
    }

    @VisibleForTesting
    Timer getRetriggerLocalRequestTimer() {
        return retriggerLocalRequestTimer;
    }

    MemorySegment getUnpooledSegment() {
        return unpooledSegment;
    }

    @Override
    public void close() throws IOException {
        boolean released = false;
        synchronized (requestLock) {
            if (!closeFuture.isDone()) {
                try {
                    LOG.debug("{}: Releasing {}.", owningTaskName, this);

                    if (retriggerLocalRequestTimer != null) {
                        retriggerLocalRequestTimer.cancel();
                    }

                    for (InputChannel inputChannel : inputChannels()) {
                        try {
                            inputChannel.releaseAllResources();
                        } catch (IOException e) {
                            LOG.warn(
                                    "{}: Error during release of channel resources: {}.",
                                    owningTaskName,
                                    e.getMessage(),
                                    e);
                        }
                    }

                    // The buffer pool can actually be destroyed immediately after the
                    // reader received all of the data from the input channels.
                    if (bufferPool != null) {
                        bufferPool.lazyDestroy();
                    }
                } finally {
                    released = true;
                    closeFuture.complete(null);
                }
            }
        }

        if (released) {
            synchronized (inputChannelsWithData) {
                inputChannelsWithData.notifyAll();
            }
            if (enabledTieredStorage()) {
                tieredStorageConsumerClient.close();
            }
        }
    }

    @Override
    public boolean isFinished() {
        return hasReceivedAllEndOfPartitionEvents;
    }

    @Override
    public EndOfDataStatus hasReceivedEndOfData() {
        if (!hasReceivedEndOfData) {
            return EndOfDataStatus.NOT_END_OF_DATA;
        } else if (shouldDrainOnEndOfData) {
            return EndOfDataStatus.DRAINED;
        } else {
            return EndOfDataStatus.STOPPED;
        }
    }

    @Override
    public String toString() {
        return "SingleInputGate{"
                + "owningTaskName='"
                + owningTaskName
                + '\''
                + ", gateIndex="
                + gateIndex
                + '}';
    }

    // ------------------------------------------------------------------------
    // Consume
    // ------------------------------------------------------------------------

    @Override
    public Optional<BufferOrEvent> getNext() throws IOException, InterruptedException {
        return getNextBufferOrEvent(true);
    }

    @Override
    public Optional<BufferOrEvent> pollNext() throws IOException, InterruptedException {//
        return getNextBufferOrEvent(false);//
    }

    private Optional<BufferOrEvent> getNextBufferOrEvent(boolean blocking) throws IOException, InterruptedException {//
        if (hasReceivedAllEndOfPartitionEvents) {
            //说明上游所有的分区数据和结束事件都已经彻底消费并处理完毕。此时直接返回 Optional.empty()，这是一个标准的流结束信号，下游收到后就会开始进入 Task 的销毁流程
            return Optional.empty();
        }

        if (closeFuture.isDone()) {
            //如果当前 InputGate 已经被关闭（例如用户手动 Cancel 任务，或者并发线程中发生了重大物理故障触发了 close()），
            // 则直接抛出 CancelTaskException。这能确保消费线程（Mailbox 线程）在遭遇任务取消时，能够在网络最底层瞬间响应，立即中断计算，避免线程卡死或引发无效的 OOM
            throw new CancelTaskException("Input gate is already closed.");
        }
        //会在加锁状态下，去排队队列中抓取一个就绪的物理 InputChannel，并从里面捞出一个可用的物理 Buffer 返回
        Optional<InputWithData<InputChannel, Buffer>> next = waitAndGetNextData(blocking);//
        if (!next.isPresent()) {
            //暂停吞吐量计时
            throughputCalculator.pauseMeasurement();
            return Optional.empty();
        }

        throughputCalculator.resumeMeasurement();
        //InputWithData 包含的是纯粹的物理 Buffer（一块连续的 MemorySegment 字节内存）
        InputWithData<InputChannel, Buffer> inputWithData = next.get();
        //该方法将底层的 Buffer 升级为面向算子层的 BufferOrEvent 统一包装对象。如果这块内存存储的是业务用户数据（User Records），
        // 它会被标记为 Buffer，下游的序列化器随后会将其还原为具体的 Java Object。如果这块内存存储的是控制信号（如 CheckpointBarrier、Watermark、EndOfData），
        // 转换器会解析其元数据头部，将其组装成对应的物理/逻辑事件（Event）。同时，它把通道的索引、是否还有更多普通数据/急件等网络状态打包带走，供上层调度决策
        final BufferOrEvent bufferOrEvent = transformToBufferOrEvent(
                        inputWithData.data,
                        inputWithData.moreAvailable,
                        inputWithData.input,
                        inputWithData.morePriorityEvents);
        //将本次拿到的数据块的真实物理大小（bufferOrEvent.getSize()）喂给吞吐量计算器
        throughputCalculator.incomingDataSize(bufferOrEvent.getSize());
        return Optional.of(bufferOrEvent);
    }

    //不断拉取上游发来的每一个网络数据块（Buffer）或控制事件
    private Optional<InputWithData<InputChannel, Buffer>> waitAndGetNextData(boolean blocking) throws IOException, InterruptedException {//
        while (true) {
            synchronized (inputChannelsWithData) {
                //获取一个有数据的物理通道 非阻塞式
                Optional<InputChannel> inputChannelOpt = getChannel(blocking);//
                if (!inputChannelOpt.isPresent()) {
                    //如果是非阻塞模式（blocking=false）且当前没有可用数据，
                    //或者阻塞模式（blocking=true）下整个 InputGate 已经关闭，则直接返回 Optional.empty()，让下游线程释放 CPU
                    return Optional.empty();
                }

                final InputChannel inputChannel = inputChannelOpt.get();
                //如果作业正在进行 Failover 容错恢复，它会优先读取本地快照中恢复出来的物理 Buffer（Recovered Buffer）；
                //如果是正常运行期，则读取网络或本地内存中正常的传输 Buffer（Normal Buffer）
                Optional<Buffer> buffer = readRecoveredOrNormalBuffer(inputChannel);//
                if (!buffer.isPresent()) {
                    //更新可用性状态
                    checkUnavailability();
                    continue;
                }
                // numSubpartitions = 1
                int numSubpartitions = inputChannel.getConsumedSubpartitionIndexSet().size();
                if (numSubpartitions > 1) {
                    switch (buffer.get().getDataType()) {
                        case END_OF_DATA:
                            //计数器自增 1
                            endOfDatas[inputChannel.getChannelIndex()]++;
                            if (endOfDatas[inputChannel.getChannelIndex()] < numSubpartitions) {
                                //如果计数不够，说明还有其他子分区数据在传输中，则立刻调用 recycleBuffer() 回收该事件内存，并通过 continue 放弃本次分发，重新拉取别的数据
                                buffer.get().recycleBuffer();
                                continue;
                            }
                            //只有当接收到的结束信号数量 严格等于 该通道绑定的子分区总数时，才说明该通道所有多路复用的数据流已经全部完结
                            //此时才会跳出 switch 将结束事件真正传递给下游算子
                            break;
                        case END_OF_PARTITION:
                            //计数器自增 1
                            endOfPartitions[inputChannel.getChannelIndex()]++;
                            if (endOfPartitions[inputChannel.getChannelIndex()] < numSubpartitions) {
                                buffer.get().recycleBuffer();
                                continue;
                            }
                            break;
                        default:
                            break;
                    }
                }
                //急件优先级（Priority）维护与数据包装返回  morePriorityEvents = false
                final boolean morePriorityEvents = inputChannelsWithData.getNumPriorityElements() > 0;
                //检查当前吐出来的 Buffer 是不是一个紧急事件（比如 Checkpoint Barrier 或者是 CancelCheckpointMarker）
                if (buffer.get().getDataType().hasPriority()) {//false
                    //检查优先队列里是否还有更多急件
                    if (!morePriorityEvents) {
                        //如果没有了，通过 priorityAvailabilityHelper 充置当前的可用性状态（告诉系统急件已清空）
                        priorityAvailabilityHelper.resetUnavailable();
                    }
                }
                checkUnavailability();
                return Optional.of(
                        //包装类
                        new InputWithData<>(
                                inputChannel,
                                buffer.get(),
                                !inputChannelsWithData.isEmpty(),//告诉下游普通队列里还有数据
                                morePriorityEvents));//告诉特快优先队列里还有急件，下游处理完这个 Buffer 后必须以最高优先级立刻再次来拉
            }
        }
    }

    private Optional<Buffer> readRecoveredOrNormalBuffer(InputChannel inputChannel)
            throws IOException, InterruptedException {
        // Firstly, read the buffers from the recovered channel
        if (inputChannel instanceof RecoveredInputChannel && !inputChannel.isReleased()) {
            Optional<Buffer> buffer = readBufferFromInputChannel(inputChannel);
            if (!((RecoveredInputChannel) inputChannel).getStateConsumedFuture().isDone()) {
                return buffer;
            }
        }

        //  After the recovered buffers are read, read the normal buffers
        return enabledTieredStorage() // enabledTieredStorage() = false
                ? readBufferFromTieredStore(inputChannel)
                : readBufferFromInputChannel(inputChannel);//
    }

    private Optional<Buffer> readBufferFromInputChannel(InputChannel inputChannel)
            throws IOException, InterruptedException {
        //【重点】
        // 如果是 LocalInputChannel#getNextBuffer   最终会从PipelinedSubpartition 的buffers队列获取消息
        // 如果是 RemoteInputChannel#getNextBuffer   最终会从 RemoteInputChannel 的 receivedBuffers 队列获取消息
        Optional<BufferAndAvailability> bufferAndAvailabilityOpt = inputChannel.getNextBuffer();//
        if (!bufferAndAvailabilityOpt.isPresent()) {//fasle
            return Optional.empty();
        }
        final BufferAndAvailability bufferAndAvailability = bufferAndAvailabilityOpt.get();
        if (bufferAndAvailability.moreAvailable()) {//false
            // enqueue the inputChannel at the end to avoid starvation
            queueChannelUnsafe(inputChannel, bufferAndAvailability.morePriorityEvents());
        }
        if (bufferAndAvailability.hasPriority()) {//false
            lastPrioritySequenceNumber[inputChannel.getChannelIndex()] =
                    bufferAndAvailability.getSequenceNumber();
        }

        Buffer buffer = bufferAndAvailability.buffer();
        if (buffer.getDataType() == Buffer.DataType.RECOVERY_METADATA) {
            RecoveryMetadata recoveryMetadata =
                    (RecoveryMetadata)
                            EventSerializer.fromSerializedEvent(
                                    buffer.getNioBufferReadable(), getClass().getClassLoader());
            lastBufferStatusMapInTieredStore.put(
                    inputChannel.getChannelIndex(),
                    Tuple2.of(
                            buffer.getDataType().isPartialRecord(),
                            recoveryMetadata.getFinalBufferSubpartitionId()));
        }
        return Optional.of(bufferAndAvailability.buffer());
    }

    private Optional<Buffer> readBufferFromTieredStore(InputChannel inputChannel)
            throws IOException {
        TieredStorageConsumerSpec tieredStorageConsumerSpec =
                checkNotNull(tieredStorageConsumerSpecs).get(inputChannel.getChannelIndex());
        Tuple2<Boolean, Integer> lastBufferStatus =
                lastBufferStatusMapInTieredStore.computeIfAbsent(
                        inputChannel.getChannelIndex(), key -> Tuple2.of(false, -1));
        boolean isLastBufferPartialRecord = lastBufferStatus.f0;
        int lastSubpartitionId = lastBufferStatus.f1;

        while (true) {
            int subpartitionId;
            if (isLastBufferPartialRecord) {
                subpartitionId = lastSubpartitionId;
            } else {
                subpartitionId =
                        checkNotNull(tieredStorageConsumerClient)
                                .peekNextBufferSubpartitionId(
                                        tieredStorageConsumerSpec.getPartitionId(),
                                        tieredStorageConsumerSpec.getSubpartitionIds());
            }

            if (subpartitionId < 0) {
                return Optional.empty();
            }

            // If the data is available in the specific partition and subpartition, read buffer
            // through consumer client.
            Optional<Buffer> buffer =
                    checkNotNull(tieredStorageConsumerClient)
                            .getNextBuffer(
                                    tieredStorageConsumerSpec.getPartitionId(),
                                    new TieredStorageSubpartitionId(subpartitionId));

            if (buffer.isPresent()) {
                if (!(inputChannel instanceof RecoveredInputChannel)) {
                    queueChannel(checkNotNull(inputChannel), null, false);
                }
                lastBufferStatusMapInTieredStore.put(
                        inputChannel.getChannelIndex(),
                        Tuple2.of(buffer.get().getDataType().isPartialRecord(), subpartitionId));
            } else {
                if (!isLastBufferPartialRecord
                        && inputChannel.getConsumedSubpartitionIndexSet().size() > 1) {
                    // Continue to check other subpartitions that have been marked as
                    // available.
                    continue;
                }
            }

            return buffer;
        }
    }

    private boolean enabledTieredStorage() {
        return tieredStorageConsumerClient != null;
    }

    private void checkUnavailability() {
        assert Thread.holdsLock(inputChannelsWithData);

        if (inputChannelsWithData.isEmpty()) {
            availabilityHelper.resetUnavailable();
        }
    }

    private BufferOrEvent transformToBufferOrEvent(
            Buffer buffer,
            boolean moreAvailable,
            InputChannel currentChannel,
            boolean morePriorityEvents)
            throws IOException, InterruptedException {
        if (buffer.isBuffer()) {
            return transformBuffer(buffer, moreAvailable, currentChannel, morePriorityEvents);
        } else {
            return transformEvent(buffer, moreAvailable, currentChannel, morePriorityEvents);
        }
    }

    private BufferOrEvent transformBuffer(
            Buffer buffer,
            boolean moreAvailable,
            InputChannel currentChannel,
            boolean morePriorityEvents) {
        return new BufferOrEvent(
                decompressBufferIfNeeded(buffer),
                currentChannel.getChannelInfo(),
                moreAvailable,
                morePriorityEvents);
    }

    private BufferOrEvent transformEvent(
            Buffer buffer,
            boolean moreAvailable,
            InputChannel currentChannel,
            boolean morePriorityEvents)
            throws IOException, InterruptedException {
        final AbstractEvent event;
        try {
            event = EventSerializer.fromBuffer(buffer, getClass().getClassLoader());
        } finally {
            buffer.recycleBuffer();
        }

        if (event.getClass() == EndOfPartitionEvent.class) {
            synchronized (inputChannelsWithData) {
                checkState(!channelsWithEndOfPartitionEvents.get(currentChannel.getChannelIndex()));
                channelsWithEndOfPartitionEvents.set(currentChannel.getChannelIndex());
                hasReceivedAllEndOfPartitionEvents =
                        channelsWithEndOfPartitionEvents.cardinality() == numberOfInputChannels;

                enqueuedInputChannelsWithData.clear(currentChannel.getChannelIndex());
                if (inputChannelsWithData.contains(currentChannel)) {
                    inputChannelsWithData.getAndRemove(channel -> channel == currentChannel);
                }
            }
            if (hasReceivedAllEndOfPartitionEvents) {
                // Because of race condition between:
                // 1. releasing inputChannelsWithData lock in this method and reaching this place
                // 2. empty data notification that re-enqueues a channel we can end up with
                // moreAvailable flag set to true, while we expect no more data.
                checkState(!moreAvailable || !pollNext().isPresent());
                moreAvailable = false;
                markAvailable();
            }

            currentChannel.releaseAllResources();
        } else if (event.getClass() == EndOfData.class) {
            synchronized (inputChannelsWithData) {
                checkState(!channelsWithEndOfUserRecords.get(currentChannel.getChannelIndex()));
                channelsWithEndOfUserRecords.set(currentChannel.getChannelIndex());
                hasReceivedEndOfData =
                        channelsWithEndOfUserRecords.cardinality() == numberOfInputChannels;
                shouldDrainOnEndOfData &= ((EndOfData) event).getStopMode() == StopMode.DRAIN;
            }
        }

        return new BufferOrEvent(
                event,
                buffer.getDataType().hasPriority(),
                currentChannel.getChannelInfo(),
                moreAvailable,
                buffer.getSize(),
                morePriorityEvents);
    }

    private Buffer decompressBufferIfNeeded(Buffer buffer) {
        if (buffer.isCompressed()) {
            try {
                checkNotNull(bufferDecompressor, "Buffer decompressor not set.");
                return bufferDecompressor.decompressToIntermediateBuffer(buffer);
            } finally {
                buffer.recycleBuffer();
            }
        }
        return buffer;
    }

    private void markAvailable() {
        CompletableFuture<?> toNotify;
        synchronized (inputChannelsWithData) {
            toNotify = availabilityHelper.getUnavailableToResetAvailable();
        }
        toNotify.complete(null);
    }

    @Override
    public void sendTaskEvent(TaskEvent event) throws IOException {
        synchronized (requestLock) {
            for (InputChannel inputChannel : inputChannels()) {
                inputChannel.sendTaskEvent(event);
            }

            if (numberOfUninitializedChannels > 0) {
                pendingEvents.add(event);
            }
        }
    }

    public void resumeGateConsumption() throws IOException {
        checkState(!isFinished(), "InputGate already finished.");
        for (InputChannel inputChannel : channels) {
            inputChannel.resumeConsumption();
        }
    }

    @Override
    public void resumeConsumption(InputChannelInfo channelInfo) throws IOException {
        checkState(!isFinished(), "InputGate already finished.");
        // BEWARE: consumption resumption only happens for streaming jobs in which all slots
        // are allocated together so there should be no UnknownInputChannel. As a result, it
        // is safe to not synchronize the requestLock here. We will refactor the code to not
        // rely on this assumption in the future.
        channels[channelInfo.getInputChannelIdx()].resumeConsumption();
    }

    @Override
    public void acknowledgeAllRecordsProcessed(InputChannelInfo channelInfo) throws IOException {
        checkState(!isFinished(), "InputGate already finished.");
        if (!enabledTieredStorage()) {
            channels[channelInfo.getInputChannelIdx()].acknowledgeAllRecordsProcessed();
        }
    }

    // ------------------------------------------------------------------------
    // Channel notifications
    // ------------------------------------------------------------------------
    //通知数据不为空 告知 InputGate 当前 channel 有数据
    void notifyChannelNonEmpty(InputChannel channel) {
        if (enabledTieredStorage()) {
            TieredStorageConsumerSpec tieredStorageConsumerSpec =
                    checkNotNull(tieredStorageConsumerSpecs).get(channel.getChannelIndex());
            checkNotNull(availabilityNotifier)
                    .notifyAvailable(
                            tieredStorageConsumerSpec.getPartitionId(),
                            tieredStorageConsumerSpec.getInputChannelId());
        } else {
            //todo 某个channel有可写数据了
            queueChannel(checkNotNull(channel), null, false);//
        }
    }

    /**
     * Notifies that the respective channel has a priority event at the head for the given buffer
     * number.
     *
     * <p>The buffer number limits the notification to the respective buffer and voids the whole
     * notification in case that the buffer has been polled in the meantime. That is, if task thread
     * polls the enqueued priority buffer before this notification occurs (notification is not
     * performed under lock), this buffer number allows {@link #queueChannel(InputChannel, Integer,
     * boolean)} to avoid spurious priority wake-ups.
     */
    void notifyPriorityEvent(InputChannel inputChannel, int prioritySequenceNumber) {
        queueChannel(checkNotNull(inputChannel), prioritySequenceNumber, false);
    }

    void notifyPriorityEventForce(InputChannel inputChannel) {
        queueChannel(checkNotNull(inputChannel), null, true);
    }

    void triggerPartitionStateCheck(
            ResultPartitionID partitionId, InputChannelInfo inputChannelInfo) {
        partitionProducerStateProvider.requestPartitionProducerState(
                consumedResultId,
                partitionId,
                ((PartitionProducerStateProvider.ResponseHandle responseHandle) -> {
                    boolean isProducingState =
                            new RemoteChannelStateChecker(partitionId, owningTaskName)
                                    .isProducerReadyOrAbortConsumption(responseHandle);
                    if (isProducingState) {
                        try {
                            retriggerPartitionRequest(
                                    partitionId.getPartitionId(), inputChannelInfo);
                        } catch (IOException t) {
                            responseHandle.failConsumption(t);
                        }
                    }
                }));
    }

    private void queueChannel(InputChannel channel, @Nullable Integer prioritySequenceNumber, boolean forcePriority) {//
        try (GateNotificationHelper notification = new GateNotificationHelper(this, inputChannelsWithData)) {//
            synchronized (inputChannelsWithData) {
                //因为多个网络 Netty 线程可能会并发往不同的 InputChannel 灌数据 所以必须锁住这个全局的就绪队列 synchronized (inputChannelsWithData)
                //检查当前进来的这批数据，是不是带有非对齐检查点特权（Unaligned Checkpoint Barrier）**的高优先级事件。如果是，priority 就会变成功为 true
                boolean priority = prioritySequenceNumber != null || forcePriority;

                if (!forcePriority
                        && priority
                        //探测出当前传进来的 prioritySequenceNumber 已经比本地记录的还要旧（过期了），说明这是个迟到的通知。Flink 会果断通过 return 将其抛弃
                        && isOutdated(prioritySequenceNumber, lastPrioritySequenceNumber[channel.getChannelIndex()])) {
                    // priority event at the given offset already polled (notification is not atomic
                    // in respect to
                    // buffer enqueuing), so just ignore the notification
                    return;
                }
                //todo 判断channel是否在inputChannelsWithData 队列 不在则放入 将通道入队列
                if (!queueChannelUnsafe(channel, priority)) {//
                    return;
                }
                // priority = false
                if (priority && inputChannelsWithData.getNumPriorityElements() == 1) {
                    //如果插队成功，且当前整个就绪队列里有且仅有这唯一的一个高优先级元素（getNumPriorityElements() == 1），说明这是个刚发生的紧急事件，立刻点亮优先级通知灯
                    notification.notifyPriority();
                }
                if (inputChannelsWithData.size() == 1) {
                    //todo 通知数据可用
                    //算子的主计算线程因为管道没数据而处于休眠（阻塞在 InputGate.getNext() 上）。
                    // 此时点亮数据可用灯，准备去唤醒它！如果 size 已经大于 1 了，说明算子线程本就是醒着的，不需要重复通知
                    notification.notifyDataAvailable();
                }
            }
        }
        //todo Java 自动隐式调用 notification.close()
    }

    private boolean isOutdated(int sequenceNumber, int lastSequenceNumber) {
        if ((lastSequenceNumber < 0) != (sequenceNumber < 0)
                && Math.max(lastSequenceNumber, sequenceNumber) > Integer.MAX_VALUE / 2) {
            // probably overflow of one of the two numbers, the negative one is greater then
            return lastSequenceNumber < 0;
        }
        return lastSequenceNumber >= sequenceNumber;
    }

    /**
     * Queues the channel if not already enqueued and not received EndOfPartition, potentially
     * raising the priority.
     *
     * @return true iff it has been enqueued/prioritized = some change to {@link
     *     #inputChannelsWithData} happened
     */
    private boolean queueChannelUnsafe(InputChannel channel, boolean priority) {//
        assert Thread.holdsLock(inputChannelsWithData);
        if (channelsWithEndOfPartitionEvents.get(channel.getChannelIndex())) {
            return false;
        }

        final boolean alreadyEnqueued = enqueuedInputChannelsWithData.get(channel.getChannelIndex());
        if (alreadyEnqueued && (!priority || inputChannelsWithData.containsPriorityElement(channel))) {
            // already notified / prioritized (double notification), ignore
            return false;
        }
        //发现当前 Channel 不在就绪队列中
        inputChannelsWithData.add(channel, priority, alreadyEnqueued);
        if (!alreadyEnqueued) {
            //
            enqueuedInputChannelsWithData.set(channel.getChannelIndex());
        }
        return true;
    }

    //在线程安全的前提下，从就绪队列中精准挑选并弹出一个“当前已有物理数据到达”的 InputChannel；如果队列为空，则负责对消费线程进行优雅的阻塞（Wait）或状态重置。
    private Optional<InputChannel> getChannel(boolean blocking) throws InterruptedException {
        assert Thread.holdsLock(inputChannelsWithData);

        while (inputChannelsWithData.isEmpty()) {
            if (closeFuture.isDone()) {
                //如果在等待期间，InputGate 被外部线程释放或关闭（Released），为了防止消费线程永久卡死在下面的 wait() 中，直接抛出异常中断执行
                throw new IllegalStateException("Released");
            }

            if (blocking) {//false
                inputChannelsWithData.wait();
            } else {
                //通过 availabilityHelper 把自己标记为“不可用（Unavailable）”状态
                availabilityHelper.resetUnavailable();
                return Optional.empty();
            }
        }
        //队列不为空
        //从带有优先级的双端队列队头弹出一个最紧急/最早到达的 InputChannel
        InputChannel inputChannel = inputChannelsWithData.poll();
        //将该通道在位图中对应的 Bit 位重新清零（置为 false）
        //一旦清零，意味着当前 Channel 已经从 InputGate 的总就绪队列中被取出来了。如果此时网络层（Netty 线程）又收到了该 Channel 的下一个新 Buffer，
        // 网络线程就可以合规地再次把这个 Channel 推进 inputChannelsWithData 队列中排队。这套机制用极低的 CPU 损耗，实现了Channel 级别的完美去重与流控路由。
        enqueuedInputChannelsWithData.clear(inputChannel.getChannelIndex());

        return Optional.of(inputChannel);
    }

    private void setupTieredStorageNettyService(
            TieredStorageNettyServiceImpl nettyService,
            List<TieredStorageConsumerSpec> tieredStorageConsumerSpecs) {
        List<Supplier<InputChannel>> channelSuppliers = new ArrayList<>();
        for (int index = 0; index < channels.length; ++index) {
            int channelIndex = index;
            channelSuppliers.add(() -> channels[channelIndex]);
        }
        nettyService.setupInputChannels(tieredStorageConsumerSpecs, channelSuppliers);
    }

    /** The default implementation of {@link AvailabilityNotifier}. */
    private class AvailabilityNotifierImpl implements AvailabilityNotifier {

        private AvailabilityNotifierImpl() {}

        @Override
        public void notifyAvailable(
                TieredStoragePartitionId partitionId, TieredStorageInputChannelId inputChannelId) {
            Map<InputChannelInfo, InputChannel> channels =
                    inputChannels.get(partitionId.getPartitionID().getPartitionId());
            if (channels == null) {
                return;
            }
            InputChannelInfo inputChannelInfo =
                    new InputChannelInfo(gateIndex, inputChannelId.getInputChannelId());
            InputChannel inputChannel = channels.get(inputChannelInfo);
            if (inputChannel != null) {
                queueChannel(inputChannel, null, false);
            }
        }
    }

    // ------------------------------------------------------------------------

    @VisibleForTesting
    public Map<Tuple2<IntermediateResultPartitionID, InputChannelInfo>, InputChannel>
            getInputChannels() {
        Map<Tuple2<IntermediateResultPartitionID, InputChannelInfo>, InputChannel> result =
                new HashMap<>();
        for (Map.Entry<IntermediateResultPartitionID, Map<InputChannelInfo, InputChannel>>
                mapEntry : inputChannels.entrySet()) {
            for (Map.Entry<InputChannelInfo, InputChannel> entry : mapEntry.getValue().entrySet()) {
                result.put(Tuple2.of(mapEntry.getKey(), entry.getKey()), entry.getValue());
            }
        }
        return result;
    }

    public Iterable<InputChannel> inputChannels() {
        return () ->
                new Iterator<InputChannel>() {
                    private final Iterator<Map<InputChannelInfo, InputChannel>> mapIterator =
                            inputChannels.values().iterator();

                    private Iterator<InputChannel> iterator = null;

                    @Override
                    public boolean hasNext() {
                        return (iterator != null && iterator.hasNext()) || mapIterator.hasNext();
                    }

                    @Override
                    public InputChannel next() {
                        if ((iterator == null || !iterator.hasNext()) && mapIterator.hasNext()) {
                            iterator = mapIterator.next().values().iterator();
                        }

                        if (iterator == null || !iterator.hasNext()) {
                            return null;
                        }

                        return iterator.next();
                    }
                };
    }
}
