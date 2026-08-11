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

/** Type of a result partition. */
//ResultPartitionType 是 Flink 网络数据传输层（Network Stack）的关键枚举类。它决定了上游 Task 产生的数据如何缓存、何时分发，以及下游 Task 如何去消费这些数据。
public enum ResultPartitionType {

    /**
     * Blocking partitions represent blocking data exchanges, where the data stream is first fully
     * produced and then consumed. This is an option that is only applicable to bounded streams and
     * can be used in bounded stream runtime and recovery.
     *
     * <p>Blocking partitions can be consumed multiple times and concurrently.
     *
     * <p>The partition is not automatically released after being consumed (like for example the
     * {@link #PIPELINED} partitions), but only released through the scheduler, when it determines
     * that the partition is no longer needed.
     */
    //经典的批处理（Batch）传统数据交换模式
            //先产出，后消费：上游 Task 必须完全运行结束，将所有数据完整写入磁盘/外部存储后，下游 Task 才可以启动并开始消费
    BLOCKING(true, false, false, ConsumingConstraint.BLOCKING, ReleaseBy.SCHEDULER),

    /**
     * BLOCKING_PERSISTENT partitions are similar to {@link #BLOCKING} partitions, but have a
     * user-specified life cycle.
     *
     * <p>BLOCKING_PERSISTENT partitions are dropped upon explicit API calls to the JobManager or
     * ResourceManager, rather than by the scheduler.
     *
     * <p>Otherwise, the partition may only be dropped by safety-nets during failure handling
     * scenarios, like when the TaskManager exits or when the TaskManager loses connection to
     * JobManager / ResourceManager for too long.
     */
    //持久化阻塞类型
    //应用场景：用于 Flink 批处理中的中间数据集缓存（Intermediate Dataset Caching）。例如用户提交两个有关联的作业，作业 A 生成中间表并固化到集群，作业 B 随后启动直接读取该表，避免重复计算
    BLOCKING_PERSISTENT(true, false, true, ConsumingConstraint.BLOCKING, ReleaseBy.SCHEDULER),

    /**
     * A pipelined streaming data exchange. This is applicable to both bounded and unbounded
     * streams.
     *
     * <p>Pipelined results can be consumed only once by a single consumer and are automatically
     * disposed when the stream has been consumed.
     *
     * <p>This result partition type may keep an arbitrary amount of data in-flight, in contrast to
     * the {@link #PIPELINED_BOUNDED} variant.
     */
    // 这是典型的流处理（Streaming）数据交换模式
    //数据完全在内存中进行流式传输，上游 Task 一边生产数据，网络层就一边打包发送给下游 Task。它的生命周期是一次性消费的，当下游成功读取并消费完毕后，内存中的 Buffer 就会被自动释放
    PIPELINED(false, false, false, ConsumingConstraint.MUST_BE_PIPELINED, ReleaseBy.UPSTREAM),

    /**
     * Pipelined partitions with a bounded (local) buffer pool.
     *
     * <p>For streaming jobs, a fixed limit on the buffer pool size should help avoid that too much
     * data is being buffered and checkpoint barriers are delayed. In contrast to limiting the
     * overall network buffer pool size, this, however, still allows to be flexible with regards to
     * the total number of partitions by selecting an appropriately big network buffer pool size.
     *
     * <p>For batch jobs, it will be best to keep this unlimited ({@link #PIPELINED}) since there
     * are no checkpoint barriers.
     */
    //：单纯的 PIPELINED 在理论上是无限延伸的管道，但计算机内存是有限的。
    // PIPELINED_BOUNDED 指的是一种在内存中分配了固定大小限制（Upper Bound）的流式传输管道
    //PIPELINED_BOUNDED 最关键的职责就是触发和传递 Flink 的反压机制。由于网络内存大小被设置了硬性上限，它的工作流遵循以下逻辑：
    // 当下游算子消费极慢（如写入外部数据库卡住）时，下游的 InputGate 缓冲区很快会被填满。
    // 下游停止从网络层接收数据，导致上游 TaskManager 专门分配给这个 ResultPartition 的 Local Buffer Pool（有界内存池）也被瞬间填满。
    // 此时，由于它是 BOUNDED（有界的），上游 Task 无法再申请到新的空闲 Buffer 来存放新生产的数据。
    // 上游 Task 线程被强制阻塞挂起（RecordWriter 无法写入），从而实现了将下游的压力完美反向传递给上游，这就是 Flink 基于 Credit-based 流控反压机制的底层物质基础
    PIPELINED_BOUNDED(
            false, true, false, ConsumingConstraint.MUST_BE_PIPELINED, ReleaseBy.UPSTREAM),

    /**
     * Pipelined partitions with a bounded (local) buffer pool to support downstream task to
     * continue consuming data after reconnection in Approximate Local-Recovery.
     *
     * <p>Pipelined results can be consumed only once by a single consumer at one time. {@link
     * #PIPELINED_APPROXIMATE} is different from {@link #PIPELINED} and {@link #PIPELINED_BOUNDED}
     * in that {@link #PIPELINED_APPROXIMATE} partition can be reconnected after down stream task
     * fails.
     */
    //专门针对容错性要求不高、但追求极致低延迟故障恢复的特殊流场景
            //应用场景：在线机器学习（Online Learning）、实时采样统计、或者对部分数据丢失不敏感的监控大屏
    PIPELINED_APPROXIMATE(
            false, true, false, ConsumingConstraint.CAN_BE_PIPELINED, ReleaseBy.UPSTREAM),

    /**
     * Hybrid partitions with a bounded (local) buffer pool to support downstream task to
     * simultaneous reading and writing shuffle data.
     *
     * <p>Hybrid partitions can be consumed any time, whether fully produced or not.
     *
     * <p>HYBRID_FULL partitions is re-consumable, so double calculation can be avoided during
     * failover.
     */
    //允许上游一边生产，下游一边消费（类似 PIPELINED），
    // 如果下游算子此时刚好有 Slot 资源并启动了，直接从内存拿数据走。如果下游此时没有资源启动，数据会自动写入磁盘暂存（类似 BLOCKING），等下游有资源启动后去读盘
    HYBRID_FULL(true, false, false, ConsumingConstraint.CAN_BE_PIPELINED, ReleaseBy.SCHEDULER),

    /**
     * HYBRID_SELECTIVE partitions are similar to {@link #HYBRID_FULL} partitions, but it is not
     * re-consumable.
     */
    //在 HYBRID_FULL 的基础上做了极致的内存优化。
    //上游产出的数据只有当下游 Task 已经运行并开始实时消费时，才会在内存保留拷贝。如果下游还没运行，数据直接选择性刷写落盘，最大化释放内存空间给计算算子
    HYBRID_SELECTIVE(
            false, false, false, ConsumingConstraint.CAN_BE_PIPELINED, ReleaseBy.SCHEDULER);

    /**
     * Can this result partition be consumed by multiple downstream consumers for multiple times.
     */
    private final boolean isReconsumable;

    /** Does this partition use a limited number of (network) buffers? */
    private final boolean isBounded;

    /** This partition will not be released after consuming if 'isPersistent' is true. */
    private final boolean isPersistent;

    private final ConsumingConstraint consumingConstraint;

    private final ReleaseBy releaseBy;

    /** ConsumingConstraint indicates when can the downstream consume the upstream. */
    private enum ConsumingConstraint {
        /** Upstream must be finished before downstream consume. */
        BLOCKING,
        /** Downstream can consume while upstream is running. */
        CAN_BE_PIPELINED,
        /** Downstream must consume while upstream is running. */
        MUST_BE_PIPELINED
    }

    /**
     * ReleaseBy indicates who is responsible for releasing the result partition.
     *
     * <p>Attention: This may only be a short-term solution to deal with the partition release
     * logic. We can discuss the issue of unifying the partition release logic in FLINK-27948. Once
     * the ticket is resolved, we can remove the enumeration here.
     */
    private enum ReleaseBy {
        UPSTREAM,
        SCHEDULER
    }

    /** Specifies the behaviour of an intermediate result partition at runtime. */
    ResultPartitionType(
            boolean isReconsumable,
            boolean isBounded,
            boolean isPersistent,
            ConsumingConstraint consumingConstraint,
            ReleaseBy releaseBy) {
        this.isReconsumable = isReconsumable;
        this.isBounded = isBounded;
        this.isPersistent = isPersistent;
        this.consumingConstraint = consumingConstraint;
        this.releaseBy = releaseBy;
    }

    /** return if this partition's upstream and downstream must be scheduled in the same time. */
    public boolean mustBePipelinedConsumed() {
        return consumingConstraint == ConsumingConstraint.MUST_BE_PIPELINED;
    }

    /** return if this partition's upstream and downstream support scheduling in the same time. */
    //返回 false 意味着这是一条 Blocking 边（批处理落盘）。上游启动了，下游也不能立刻启动，必须等上游全部运行完。所以此时不需要把下游加到 nextRegions 里。
    //只有返回 true（Pipelined 边，流式管道），说明上游只要一启动，下游就必须同时启动来消费数据
    public boolean canBePipelinedConsumed() {
        return consumingConstraint == ConsumingConstraint.CAN_BE_PIPELINED || consumingConstraint == ConsumingConstraint.MUST_BE_PIPELINED;
    }

    public boolean isReleaseByScheduler() {
        return releaseBy == ReleaseBy.SCHEDULER;
    }

    public boolean isReleaseByUpstream() {
        return releaseBy == ReleaseBy.UPSTREAM;
    }

    /**
     * {@link #isBlockingOrBlockingPersistentResultPartition()} is used to judge whether it is the
     * specified {@link #BLOCKING} or {@link #BLOCKING_PERSISTENT} resultPartitionType.
     *
     * <p>this method suitable for judgment conditions related to the specific implementation of
     * {@link ResultPartitionType}.
     *
     * <p>this method not related to data consumption and partition release. As for the logic
     * related to partition release, use {@link #isReleaseByScheduler()} instead, and as consume
     * type, use {@link #mustBePipelinedConsumed()} or {@link #canBePipelinedConsumed()} instead.
     */
    public boolean isBlockingOrBlockingPersistentResultPartition() {
        return this == BLOCKING || this == BLOCKING_PERSISTENT;
    }

    /**
     * {@link #isHybridResultPartition()} is used to judge whether it is the specified {@link
     * #HYBRID_FULL} or {@link #HYBRID_SELECTIVE} resultPartitionType.
     *
     * <p>this method suitable for judgment conditions related to the specific implementation of
     * {@link ResultPartitionType}.
     *
     * <p>this method not related to data consumption and partition release. As for the logic
     * related to partition release, use {@link #isReleaseByScheduler()} instead, and as consume
     * type, use {@link #mustBePipelinedConsumed()} or {@link #canBePipelinedConsumed()} instead.
     */
    public boolean isHybridResultPartition() {
        return this == HYBRID_FULL || this == HYBRID_SELECTIVE;
    }

    /**
     * {@link #isPipelinedOrPipelinedBoundedResultPartition()} is used to judge whether it is the
     * specified {@link #PIPELINED} or {@link #PIPELINED_BOUNDED} resultPartitionType.
     *
     * <p>This method suitable for judgment conditions related to the specific implementation of
     * {@link ResultPartitionType}.
     *
     * <p>This method not related to data consumption and partition release. As for the logic
     * related to partition release, use {@link #isReleaseByScheduler()} instead, and as consume
     * type, use {@link #mustBePipelinedConsumed()} or {@link #canBePipelinedConsumed()} instead.
     */
    public boolean isPipelinedOrPipelinedBoundedResultPartition() {
        return this == PIPELINED || this == PIPELINED_BOUNDED;
    }

    /**
     * Whether this partition uses a limited number of (network) buffers or not.
     *
     * @return <tt>true</tt> if the number of buffers should be bound to some limit
     */
    public boolean isBounded() {
        return isBounded;
    }

    public boolean isPersistent() {
        return isPersistent;
    }

    public boolean supportCompression() {
        return isBlockingOrBlockingPersistentResultPartition()
                || this == HYBRID_FULL
                || this == HYBRID_SELECTIVE;
    }

    public boolean isReconsumable() {
        return isReconsumable;
    }
}
