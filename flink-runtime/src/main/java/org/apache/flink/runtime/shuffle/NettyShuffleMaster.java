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

package org.apache.flink.runtime.shuffle;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle.AllTieredShuffleMasterSnapshots;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle.TieredInternalShuffleMaster;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle.TieredInternalShuffleMasterSnapshot;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierShuffleDescriptor;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor.LocalExecutionPartitionConnectionInfo;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor.NetworkPartitionConnectionInfo;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor.PartitionConnectionInfo;
import org.apache.flink.runtime.util.ConfigurationParserUtils;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.flink.api.common.BatchShuffleMode.ALL_EXCHANGES_HYBRID_FULL;
import static org.apache.flink.api.common.BatchShuffleMode.ALL_EXCHANGES_HYBRID_SELECTIVE;
import static org.apache.flink.configuration.ExecutionOptions.BATCH_SHUFFLE_MODE;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Default {@link ShuffleMaster} for netty and local file based shuffle implementation. */
public class NettyShuffleMaster implements ShuffleMaster<NettyShuffleDescriptor> {

    //每个 InputChannel（逻辑输入通道）所需的专属 Buffer 数量基准值（默认流模式为 2）
    private final int buffersPerInputChannel;

    //每个 InputGate（对应一个 Task 的所有输入连接）允许共享的流动 Buffer 数量基准值（默认 8）
    private final int floatingBuffersPerGate;

    //每个 InputGate 能够向集群索要的保底（Required）Buffer 的最大上限
    private final Optional<Integer> maxRequiredBuffersPerGate;

    //触发 Sort-Shuffle 的最小并行度阈值（默认 128）     批模式
    private final int sortShuffleMinParallelism;

    //运行 Sort-Shuffle 所需的最小 Buffer 内存块数量（默认 64） 批模式
    private final int sortShuffleMinBuffers;

    //单个网络页（MemorySegment）的物理大小（默认 32KB）
    private final int networkBufferSize;

    //【下面几个参数都是批模式下使用的】

    //是否启用 JobMaster 级别故障恢复时的 Shuffle 数据保留开关。深意：当 JobMaster 发生异常重启（例如 JM 漂移、HA 切换）时，如果该项为 true，
    // 已经运行完成的上游 TaskManager 上的 Shuffle 数据（特别是 Batch 作业在本地磁盘留存的中间结果）不会被强行清理。
    // 新接管的 JobMaster 可以通过元数据直接无缝对接到原有的 TaskManager 上读取数据，避免了整个作业全部从 Source 端重跑，极大提升了批处理的容错效率
    private final boolean enableJobMasterFailover;

    //如果作业启用了分层存储，这个组件就会被激活（不为 null）。它负责在全局调度不同的存储层（如内存层、本地磁盘层、远端分布式文件系统层 OSS/HDFS），是 Flink 现代流批一体、存算分离网络栈的核心中枢
    @Nullable
    private final TieredInternalShuffleMaster tieredInternalShuffleMaster;

    //缓存每个运行中作业的 Shuffle 上下文
    //一个 JobManager 实例可以同时运行多个不同的作业（Job）。这个 Map 用 JobID 做隔离，
    // 里面包含了跟底层 ResourceManager、TaskManager 进行网络通信的回调句柄（Context），用于在运行时动态接收 TM 的网络状态报告
    private final Map<JobID, JobShuffleContext> jobShuffleContexts = new HashMap<>();

    //当上游 Task 在某个 TaskManager 上成功部署并初始化了 ResultPartition 后，它会把自己的物理网络地址（IP、端口、PartitionID）上报给 JobMaster
    //JobMaster 收到后，会将其封装为 ShuffleDescriptor（数据传输说明书），并塞进这个双层 Map 中。
    //当下游 Task 准备启动时，JobMaster 会去这个 Map 里查询：“你要消费的那个 ResultPartitionID 现在在哪个机器上？”，然后把查到的 ShuffleDescriptor 发给下游 Task。下游 Task 正是拿着这个说明书，去调用我们在前几问分析的 NettyPartitionRequestClient#requestSubpartition 真正发起网络物理连接的
    private final Map<JobID, Map<ResultPartitionID, ShuffleDescriptor>> jobShuffleDescriptors = new HashMap<>();

    public NettyShuffleMaster(ShuffleMasterContext shuffleMasterContext) {//
        Configuration conf = shuffleMasterContext.getConfiguration();
        checkNotNull(conf);
        buffersPerInputChannel = 2;
        floatingBuffersPerGate = 8;
        maxRequiredBuffersPerGate = conf.getOptional(NettyShuffleEnvironmentOptions.NETWORK_READ_MAX_REQUIRED_BUFFERS_PER_GATE);//
        sortShuffleMinParallelism = 1;//1
        sortShuffleMinBuffers = conf.get(NettyShuffleEnvironmentOptions.NETWORK_SORT_SHUFFLE_MIN_BUFFERS);//512
        networkBufferSize = ConfigurationParserUtils.getPageSize(conf);//32768

        if (isHybridShuffleEnabled(conf)) {//false
            tieredInternalShuffleMaster = new TieredInternalShuffleMaster(shuffleMasterContext, this::getShuffleDescriptor);
        } else {
            tieredInternalShuffleMaster = null;
        }
        //false
        enableJobMasterFailover = conf.get(BatchExecutionOptions.JOB_RECOVERY_ENABLED) && supportsBatchSnapshot();

        checkArgument(
                !maxRequiredBuffersPerGate.isPresent() || maxRequiredBuffersPerGate.get() >= 1,
                String.format(
                        "At least one buffer is required for each gate, please increase the value of %s.",
                        NettyShuffleEnvironmentOptions.NETWORK_READ_MAX_REQUIRED_BUFFERS_PER_GATE
                                .key()));
    }

    @Override
    public CompletableFuture<NettyShuffleDescriptor> registerPartitionWithProducer(
            JobID jobID,
            PartitionDescriptor partitionDescriptor,
            ProducerDescriptor producerDescriptor) {

        ResultPartitionID resultPartitionID =
                new ResultPartitionID(
                        partitionDescriptor.getPartitionId(),
                        producerDescriptor.getProducerExecutionId());

        List<TierShuffleDescriptor> tierShuffleDescriptors = null;
        if (tieredInternalShuffleMaster != null) {
            tierShuffleDescriptors =
                    tieredInternalShuffleMaster.addPartitionAndGetShuffleDescriptor(
                            jobID,
                            partitionDescriptor.getNumberOfSubpartitions(),
                            resultPartitionID);
        }

        NettyShuffleDescriptor shuffleDeploymentDescriptor = new NettyShuffleDescriptor(
                        producerDescriptor.getProducerLocation(),
                        createConnectionInfo(producerDescriptor, partitionDescriptor.getConnectionIndex()),
                        resultPartitionID,
                        tierShuffleDescriptors);
        if (enableJobMasterFailover) {
            Map<ResultPartitionID, ShuffleDescriptor> shuffleDescriptorMap =
                    jobShuffleDescriptors.computeIfAbsent(jobID, k -> new HashMap<>());
            shuffleDescriptorMap.put(resultPartitionID, shuffleDeploymentDescriptor);
        }
        return CompletableFuture.completedFuture(shuffleDeploymentDescriptor);
    }

    @Override
    public void releasePartitionExternally(ShuffleDescriptor shuffleDescriptor) {
        if (tieredInternalShuffleMaster != null) {
            tieredInternalShuffleMaster.releasePartition(shuffleDescriptor);
        }
    }

    public Optional<ShuffleDescriptor> getShuffleDescriptor(
            JobID jobID, ResultPartitionID resultPartitionID) {
        return Optional.ofNullable(jobShuffleDescriptors.get(jobID))
                .map(descriptorMap -> descriptorMap.get(resultPartitionID));
    }

    private static PartitionConnectionInfo createConnectionInfo(
            ProducerDescriptor producerDescriptor, int connectionIndex) {
        return producerDescriptor.getDataPort() >= 0
                ? NetworkPartitionConnectionInfo.fromProducerDescriptor(
                        producerDescriptor, connectionIndex)
                : LocalExecutionPartitionConnectionInfo.INSTANCE;
    }

    /**
     * JM announces network memory requirement from the calculating result of this method. Please
     * note that the calculating algorithm depends on both I/O details of a vertex and network
     * configuration, which means we should always keep the consistency of configurations between
     * JM, RM and TM in fine-grained resource management, thus to guarantee that the processes of
     * memory announcing and allocating respect each other.
     */
    @Override
    public MemorySize computeShuffleMemorySizeForTask(TaskInputsOutputsDescriptor desc) {
        checkNotNull(desc);

        int numRequiredNetworkBuffers =
                NettyShuffleUtils.computeNetworkBuffersForAnnouncing(
                        buffersPerInputChannel,
                        floatingBuffersPerGate,
                        maxRequiredBuffersPerGate,
                        sortShuffleMinParallelism,
                        sortShuffleMinBuffers,
                        desc.getInputChannelNums(),
                        desc.getPartitionReuseCount(),
                        desc.getSubpartitionNums(),
                        desc.getInputPartitionTypes(),
                        desc.getPartitionTypes());

        return new MemorySize((long) networkBufferSize * numRequiredNetworkBuffers);
    }

    private boolean isHybridShuffleEnabled(Configuration conf) {
        return (conf.get(BATCH_SHUFFLE_MODE) == ALL_EXCHANGES_HYBRID_FULL
                || conf.get(BATCH_SHUFFLE_MODE) == ALL_EXCHANGES_HYBRID_SELECTIVE);
    }

    @Override
    public CompletableFuture<Collection<PartitionWithMetrics>> getPartitionWithMetrics(
            JobID jobId, Duration timeout, Set<ResultPartitionID> expectedPartitions) {
        if (tieredInternalShuffleMaster != null) {
            return tieredInternalShuffleMaster.getPartitionWithMetrics(
                    jobShuffleContexts.get(jobId), timeout, expectedPartitions);
        }

        return checkNotNull(jobShuffleContexts.get(jobId))
                .getPartitionWithMetrics(timeout, expectedPartitions);
    }

    @Override
    public void registerJob(JobShuffleContext context) {
        jobShuffleContexts.put(context.getJobId(), context);
        if (tieredInternalShuffleMaster != null) {
            tieredInternalShuffleMaster.registerJob(context);
        }
    }

    @Override
    public void unregisterJob(JobID jobId) {
        jobShuffleContexts.remove(jobId);
        if (tieredInternalShuffleMaster != null) {
            if (enableJobMasterFailover) {
                jobShuffleDescriptors.remove(jobId);
            }
            tieredInternalShuffleMaster.unregisterJob(jobId);
        }
    }

    @Override
    public boolean supportsBatchSnapshot() {
        if (tieredInternalShuffleMaster != null) {
            return tieredInternalShuffleMaster.supportsBatchSnapshot();
        }

        return true;
    }

    @Override
    public void snapshotState(
            CompletableFuture<ShuffleMasterSnapshot> snapshotFuture,
            ShuffleMasterSnapshotContext context,
            JobID jobId) {
        if (tieredInternalShuffleMaster != null) {
            Map<ResultPartitionID, ShuffleDescriptor> shuffleDescriptorMap =
                    jobShuffleDescriptors.remove(jobId);
            CompletableFuture<AllTieredShuffleMasterSnapshots> allSnapshotFuture =
                    new CompletableFuture<>();
            tieredInternalShuffleMaster.snapshotState(allSnapshotFuture, context, jobId);
            allSnapshotFuture.thenAccept(
                    allSnap ->
                            snapshotFuture.complete(
                                    new TieredInternalShuffleMasterSnapshot(
                                            shuffleDescriptorMap, allSnap)));
            return;
        }

        snapshotFuture.complete(EmptyShuffleMasterSnapshot.getInstance());
    }

    @Override
    public void snapshotState(CompletableFuture<ShuffleMasterSnapshot> snapshotFuture) {
        if (tieredInternalShuffleMaster != null) {
            CompletableFuture<AllTieredShuffleMasterSnapshots> allSnapshotFuture =
                    new CompletableFuture<>();
            tieredInternalShuffleMaster.snapshotState(allSnapshotFuture);
            allSnapshotFuture.thenAccept(
                    allSnap ->
                            snapshotFuture.complete(
                                    new TieredInternalShuffleMasterSnapshot(null, allSnap)));
            return;
        }

        snapshotFuture.complete(EmptyShuffleMasterSnapshot.getInstance());
    }

    @Override
    public void restoreState(ShuffleMasterSnapshot snapshot) {
        if (tieredInternalShuffleMaster != null) {
            checkState(snapshot instanceof TieredInternalShuffleMasterSnapshot);
            tieredInternalShuffleMaster.restoreState(
                    (TieredInternalShuffleMasterSnapshot) snapshot);
        }
    }

    @Override
    public void restoreState(List<ShuffleMasterSnapshot> snapshots, JobID jobId) {
        if (tieredInternalShuffleMaster != null) {
            List<TieredInternalShuffleMasterSnapshot> snapshotList =
                    snapshots.stream()
                            .map(
                                    snap -> {
                                        checkState(
                                                snap
                                                        instanceof
                                                        TieredInternalShuffleMasterSnapshot);
                                        Map<ResultPartitionID, ShuffleDescriptor>
                                                shuffleDescriptors =
                                                        ((TieredInternalShuffleMasterSnapshot) snap)
                                                                .getShuffleDescriptors();
                                        if (shuffleDescriptors != null) {
                                            jobShuffleDescriptors
                                                    .computeIfAbsent(jobId, k -> new HashMap<>())
                                                    .putAll(shuffleDescriptors);
                                        }
                                        return (TieredInternalShuffleMasterSnapshot) snap;
                                    })
                            .collect(Collectors.toList());
            tieredInternalShuffleMaster.restoreState(snapshotList, jobId);
        }
    }

    @Override
    public void notifyPartitionRecoveryStarted(JobID jobId) {
        checkNotNull(jobShuffleContexts.get(jobId)).notifyPartitionRecoveryStarted();
    }

    @Override
    public void close() throws Exception {
        if (tieredInternalShuffleMaster != null) {
            tieredInternalShuffleMaster.close();
        }
    }
}
