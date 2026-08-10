/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory;
import org.apache.flink.runtime.executiongraph.DefaultExecutionGraphBuilder;
import org.apache.flink.runtime.executiongraph.ExecutionDeploymentListener;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionStateUpdateListener;
import org.apache.flink.runtime.executiongraph.MarkPartitionFinishedStrategy;
import org.apache.flink.runtime.executiongraph.VertexAttemptNumberStore;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobmaster.ExecutionDeploymentTracker;
import org.apache.flink.runtime.jobmaster.ExecutionDeploymentTrackerDeploymentListenerAdapter;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.scheduler.adaptivebatch.ExecutionPlanSchedulingContext;
import org.apache.flink.runtime.shuffle.ShuffleMaster;

import org.slf4j.Logger;

import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.util.Preconditions.checkNotNull;

//负责将逻辑拓扑（JobGraph） 转换为 物理执行图（ExecutionGraph） 的核心工厂类。在转换过程中，
// 物理图需要深度对接集群的各类底层基础设施（如网络 Shuffle、Jar 包管理、指标监控等）。
// 这些属性正是工厂为了给 ExecutionGraph “注入”这些底层能力而持有的基础设施网关
/** Default {@link ExecutionGraphFactory} implementation. */
public class DefaultExecutionGraphFactory implements ExecutionGraphFactory {

    //作用：集群和作业级别的全局配置项（flink-conf.yaml 以及提交作业时指定的参数）。
    // 职责：工厂在创建执行图时，需要读取其中的关键调优参数。例如：任务部署超时时间、Slot 申请策略、背压监控采样频率等，并将这些配置透传给 ExecutionGraph
    private final Configuration configuration;

    //作用：用户代码的类加载器（通常是 ChildFirstClassLoader）。职责：Flink 引擎本身和用户的业务代码（Jar包）是隔离的。
    // 在构建物理图时，需要实例化用户的自定义算子（如 MapFunction）、
    // 自定义数据类型序列化器（TypeSerializer）以及状态后端。工厂必须持有这个加载器，才能正确反序列化和加载用户 Jar 包中的类，防止出现 ClassNotFoundException
    private final ClassLoader userCodeClassLoader;

    //作用：Task 部署状态跟踪器。职责：用来实时追踪集群中所有正在部署（Deploying）或已经运行（Running）的 Task。
    //它是一个全局的簿记系统，当 TaskManager 异常掉线时，执行图可以通过这个跟踪器快速清点有哪些物理 Task 已经彻底失联，从而指导后续的资源清理和重新部署
    private final ExecutionDeploymentTracker executionDeploymentTracker;

    //作用：定时的、专门处理 Future 回调的后台线程池。职责：物理执行图的很多操作是高度异步的（例如等待 Slot 分配、等待 TaskManager 的 RPC 响应）。当这些异步操作（CompletableFuture）完成或超时时，
    // 需要一个线程池来执行后续的监听器代码（thenAcceptAsync 等）。futureExecutor 就是用来处理这些并发回调的，防止阻塞 JobManager 的主线程
    private final ScheduledExecutorService futureExecutor;

    //作用：专门处理重量级 I/O 操作的线程池。职责：用于执行图内部任何涉及磁盘、网络或外部存储的耗时操作。
    //例如：在恢复图时从分布式存储（HDFS/S3）中读取 Checkpoint 的元数据文件。将这类耗时 I/O 扔进 ioExecutor，可以确保 JobManager 核心调度逻辑的极高响应速度
    private final Executor ioExecutor;

    //执行图在向外（如向 Akka/Pekko Actor、TaskManager、ResourceManager）发送远程过程调用时，
    // 必须带上这个超时时间。一旦由于网络拥堵或对方卡死导致在这个时间内没收到响应，执行图就会触发超时容错机制
    private final Duration rpcTimeout;
    private final JobManagerJobMetricGroup jobManagerJobMetricGroup;

    //作用：二进制大对象（BLOB）存储写入网关。
    // 职责：主要用于管理作业的大型文件。
    //当执行图在运行时由于 Failover 需要重新分发用户 Jar 包、或者需要持久化存储某些临时的运行时大文件时，它通过 blobWriter 将文件上传到 JobManager 的 BlobServer 中，TM 随后会来下载
    private final BlobWriter blobWriter;
    //流批一体的核心网络组件。它负责在 JobManager 端注册和追踪作业所有中间结果集（IntermediateResult）的物理分区生命周期。
    // 无论是流式的 Netty 直连 Shuffle，还是批处理的外部存储（如 Remote Shuffle Service / 磁盘文件）Shuffle，其核心资源申请和释放指令都必须通过它来控制
    private final ShuffleMaster<?> shuffleMaster;
    //作用：作业主节点分区跟踪器。
    // 职责：它与 shuffleMaster 配合，专门用来跟踪在 TaskManager 上生成的物理数据分区（Data Partitions）。
    // 特别是在批处理或有限流中，当上游 Task 运行结束并把数据落盘后，
    // 该组件负责在 JM 端记录“某份 Shuffle 数据目前在 A 机器的某个目录下”，直到当下游 Task 消费完毕或者作业结束后，再通知 TM 释放这些物理数据
    private final JobMasterPartitionTracker jobMasterPartitionTracker;
    //作用：是否开启动态图（Dynamic Graph）调度的开关。
    // 职责：这是 Flink 近几个版本在批处理（Batch） 上的核心演进。如果为 true，意味着执行图在初始化时不会一口气把所有物理节点全部建出来，
    // 而是随着上游数据的产出，动态地生成和展开下游的 ExecutionJobVertex。这极大地减少了超大规模批处理作业在 JM 端的内存占用
    private final boolean isDynamicGraph;
    //作用：物理作业顶点（ExecutionJobVertex）的实例化工厂。职责：解耦设计。
    //用于具体决定如何将 JobGraph 中的一个 JobVertex 转换为物理的 ExecutionJobVertex。通过这个工厂，Flink 可以根据不同的运行模式或特定的实验性功能，注入不同的顶点构建行为
    private final ExecutionJobVertex.Factory executionJobVertexFactory;

    //作用：混合 Shuffle 模式（Hybrid Shuffle）下的分区状态精细化控制策略。
    // 职责：Flink 引入了强大的 Hybrid Shuffle 模式（介于流式 Pipelined 和批式 Blocking 之间，支持一边写内存一边落盘，下游动态消费）。
    // 这个布尔值是一个核心策略开关：当上游算子还没有完全运行结束（Non-Finished）时，它所产出的 Hybrid 分区在拓扑上应该被标记为什么状态。
    // 如果设为 true（通常是 Unknown），当下游 Task 尝试去调度时，调度器会因为状态未知而采取更加稳妥或者特定的资源等待策略，防止引发下游 Task 空等（Starvation）或死锁
    private final boolean nonFinishedHybridPartitionShouldBeUnknown;

    public DefaultExecutionGraphFactory(
            Configuration configuration,
            ClassLoader userCodeClassLoader,
            ExecutionDeploymentTracker executionDeploymentTracker,
            ScheduledExecutorService futureExecutor,
            Executor ioExecutor,
            Duration rpcTimeout,
            JobManagerJobMetricGroup jobManagerJobMetricGroup,
            BlobWriter blobWriter,
            ShuffleMaster<?> shuffleMaster,
            JobMasterPartitionTracker jobMasterPartitionTracker) {
        this(
                configuration,
                userCodeClassLoader,
                executionDeploymentTracker,
                futureExecutor,
                ioExecutor,
                rpcTimeout,
                jobManagerJobMetricGroup,
                blobWriter,
                shuffleMaster,
                jobMasterPartitionTracker,
                false,
                new ExecutionJobVertex.Factory(),
                false);
    }

    public DefaultExecutionGraphFactory(
            Configuration configuration,
            ClassLoader userCodeClassLoader,
            ExecutionDeploymentTracker executionDeploymentTracker,
            ScheduledExecutorService futureExecutor,
            Executor ioExecutor,
            Duration rpcTimeout,
            JobManagerJobMetricGroup jobManagerJobMetricGroup,
            BlobWriter blobWriter,
            ShuffleMaster<?> shuffleMaster,
            JobMasterPartitionTracker jobMasterPartitionTracker,
            boolean isDynamicGraph,
            ExecutionJobVertex.Factory executionJobVertexFactory,
            boolean nonFinishedHybridPartitionShouldBeUnknown) {
        this.configuration = configuration;
        this.userCodeClassLoader = userCodeClassLoader;
        this.executionDeploymentTracker = executionDeploymentTracker;
        this.futureExecutor = futureExecutor;
        this.ioExecutor = ioExecutor;
        this.rpcTimeout = rpcTimeout;
        this.jobManagerJobMetricGroup = jobManagerJobMetricGroup;
        this.blobWriter = blobWriter;
        this.shuffleMaster = shuffleMaster;
        this.jobMasterPartitionTracker = jobMasterPartitionTracker;
        this.isDynamicGraph = isDynamicGraph;
        this.executionJobVertexFactory = checkNotNull(executionJobVertexFactory);
        this.nonFinishedHybridPartitionShouldBeUnknown = nonFinishedHybridPartitionShouldBeUnknown;
    }

    //把客户端提交的逻辑拓扑（JobGraph）全量组装成分布式环境下的物理执行图（ExecutionGraph），
    //然后立即去高可用存储中寻找历史备份，将作业恢复到最近一次正确的运行状态（Checkpoint 或 Savepoint）
    @Override
    public ExecutionGraph createAndRestoreExecutionGraph(
            JobGraph jobGraph,
            CompletedCheckpointStore completedCheckpointStore,
            CheckpointsCleaner checkpointsCleaner,
            CheckpointIDCounter checkpointIdCounter,
            CheckpointStatsTracker checkpointStatsTracker,
            TaskDeploymentDescriptorFactory.PartitionLocationConstraint partitionLocationConstraint,
            long initializationTimestamp,
            VertexAttemptNumberStore vertexAttemptNumberStore,
            VertexParallelismStore vertexParallelismStore,
            ExecutionStateUpdateListener executionStateUpdateListener,
            MarkPartitionFinishedStrategy markPartitionFinishedStrategy,
            ExecutionPlanSchedulingContext executionPlanSchedulingContext,
            Logger log)
            throws Exception {
        //用来监听 Task 在 TaskManager 上的实际部署动作
        ExecutionDeploymentListener executionDeploymentListener =
                new ExecutionDeploymentTrackerDeploymentListenerAdapter(executionDeploymentTracker);
        //这是 Flink 内存管理和容错的底层安全机制。
        //当 Task 彻底结束时，必须立刻在 Tracker 中注销它，释放 JobManager 内存中对该 Task 部署信息的追踪，防止大作业在频繁重启或长期运行后引发 JM 端的 OOM
        ExecutionStateUpdateListener combinedExecutionStateUpdateListener =
                (execution, previousState, newState) -> {
                    //
                    executionStateUpdateListener.onStateUpdate(execution, previousState, newState);
                    if (newState.isTerminal()) {
                        //一旦某个 Task 的状态流转到了终态（Terminal State）（如 FINISHED、FAILED 或 CANCELED），
                        //就立刻调用 executionDeploymentTracker.stopTrackingDeploymentOf(execution)
                        executionDeploymentTracker.stopTrackingDeploymentOf(execution);
                    }
                };

        final ExecutionGraph newExecutionGraph = DefaultExecutionGraphBuilder.buildGraph(//
                        jobGraph,
                        configuration,
                        futureExecutor,
                        ioExecutor,
                        userCodeClassLoader,
                        completedCheckpointStore,
                        checkpointsCleaner,
                        checkpointIdCounter,
                        rpcTimeout,
                        blobWriter,
                        log,
                        shuffleMaster,
                        jobMasterPartitionTracker,
                        partitionLocationConstraint,
                        executionDeploymentListener,
                        combinedExecutionStateUpdateListener,
                        initializationTimestamp,
                        vertexAttemptNumberStore,
                        vertexParallelismStore,
                        checkpointStatsTracker,
                        isDynamicGraph,
                        executionJobVertexFactory,
                        markPartitionFinishedStrategy,
                        nonFinishedHybridPartitionShouldBeUnknown,
                        jobManagerJobMetricGroup,
                        executionPlanSchedulingContext);

        //在 Flink 高可用（HA）模式下（如配合 ZooKeeper/Kubernetes），如果 JobManager 意外崩溃重启，
        // completedCheckpointStore 里会残留最近一次集群自动成功提交的 Checkpoint 元数据。
        // Flink 会优先读取它，如果找到了有效的 Checkpoint，会直接把分布式状态（State）分发给新图的各个 Vertex 节点，并返回 true
        final CheckpointCoordinator checkpointCoordinator = newExecutionGraph.getCheckpointCoordinator();

        if (checkpointCoordinator != null) {
            // check whether we find a valid checkpoint
            // 1. 尝试从最近自动生成的 Checkpoint 恢复
            if (!checkpointCoordinator.restoreInitialCheckpointIfPresent(
                    new HashSet<>(newExecutionGraph.getAllVertices().values()))) {
                // 2. 如果 Checkpoint 恢复失败或不存在，尝试从用户手动指定的 Savepoint 恢复
                // check whether we can restore from a savepoint
                tryRestoreExecutionGraphFromSavepoint(
                        newExecutionGraph, jobGraph.getSavepointRestoreSettings());
            }
        }

        return newExecutionGraph;
    }

    /**
     * Tries to restore the given {@link ExecutionGraph} from the provided {@link
     * SavepointRestoreSettings}, iff checkpointing is enabled.
     *
     * @param executionGraphToRestore {@link ExecutionGraph} which is supposed to be restored
     * @param savepointRestoreSettings {@link SavepointRestoreSettings} containing information about
     *     the savepoint to restore from
     * @throws Exception if the {@link ExecutionGraph} could not be restored
     */
    private void tryRestoreExecutionGraphFromSavepoint(
            ExecutionGraph executionGraphToRestore,
            SavepointRestoreSettings savepointRestoreSettings)
            throws Exception {
        if (savepointRestoreSettings.restoreSavepoint()) {
            final CheckpointCoordinator checkpointCoordinator =
                    executionGraphToRestore.getCheckpointCoordinator();
            if (checkpointCoordinator != null) {
                checkpointCoordinator.restoreSavepoint(
                        savepointRestoreSettings,
                        executionGraphToRestore.getAllVertices(),
                        userCodeClassLoader);
            }
        }
    }
}
