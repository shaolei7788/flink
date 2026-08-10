/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobInfo;
import org.apache.flink.api.common.JobInfoImpl;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CheckpointScheduling;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.DefaultCheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.SubTaskInitializationMetrics;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.DefaultVertexAttemptNumberStore;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionStateUpdateListener;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.executiongraph.JobStatusProvider;
import org.apache.flink.runtime.executiongraph.MarkPartitionFinishedStrategy;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.executiongraph.failover.ResultPartitionAvailabilityChecker;
import org.apache.flink.runtime.executiongraph.metrics.DownTimeGauge;
import org.apache.flink.runtime.executiongraph.metrics.UpTimeGauge;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmanager.PartitionProducerDisposedException;
import org.apache.flink.runtime.jobmaster.SerializedInputSplit;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinatorHolder;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.query.UnknownKvStateLocation;
import org.apache.flink.runtime.scheduler.adaptivebatch.ExecutionPlanSchedulingContext;
import org.apache.flink.runtime.scheduler.exceptionhistory.FailureHandlingResultSnapshot;
import org.apache.flink.runtime.scheduler.exceptionhistory.RootExceptionHistoryEntry;
import org.apache.flink.runtime.scheduler.metrics.AllSubTasksRunningOrFinishedStateTimeMetrics;
import org.apache.flink.runtime.scheduler.metrics.DeploymentStateTimeMetrics;
import org.apache.flink.runtime.scheduler.metrics.ExecutionStatusMetricsRegistrar;
import org.apache.flink.runtime.scheduler.metrics.JobStatusMetrics;
import org.apache.flink.runtime.scheduler.metrics.MetricsRegistrar;
import org.apache.flink.runtime.scheduler.stopwithsavepoint.StopWithSavepointTerminationHandlerImpl;
import org.apache.flink.runtime.scheduler.stopwithsavepoint.StopWithSavepointTerminationManager;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.util.BoundedFIFOQueue;
import org.apache.flink.runtime.util.IntArrayList;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.IterableUtils;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.flink.configuration.TraceOptions.CHECKPOINT_SPAN_DETAIL_LEVEL;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphUtils.isAnyOutputBlocking;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Base class which can be used to implement {@link SchedulerNG}. */
public abstract class SchedulerBase implements SchedulerNG, CheckpointScheduling {

    private final Logger log;

    //作用：客户端提交上来的原始作业拓扑图。
    //职责：包含了用户代码编译出的 JobVertex、并行度配置以及全局的 ExecutionConfig。调度器需要对照它来构建和恢复物理执行图。它是整个作业的“出厂配置说明书”
    private final JobGraph jobGraph;

    //作用：作业的基本身份信息封装。
    //职责：包含作业 ID（JobID）和作业名称（JobName）等静态元数据。主要用于日志打印、监控组装以及向外部集群（如 Kubernetes, YARN）声明身份
    protected final JobInfo jobInfo;

    //作用：核心物理执行图。职责：Flink 调度的“灵魂数据结构”。它将 JobGraph 的逻辑节点展开为具体的物理节点（ExecutionVertex 和 Execution 尝试）。
    //它死死掐住所有 Task 当前的物理状态（是 DEPLOYING、RUNNING 还是 FAILED），并管理所有的中间结果数据块（IntermediateResult）
    private final ExecutionGraph executionGraph;

    //作用：面向调度算法的“轻量级拓扑视图”。职责：这是 ExecutionGraph 的一个只读、解耦的视图。
    //为了不让调度策略（SchedulingStrategy）直接修改沉重的 ExecutionGraph，Flink 抽象出了它。
    // 调度算法（如基于流水线区域的调度）只需要查询它，就能知道节点之间的拓扑依赖关系
    private final SchedulingTopology schedulingTopology;

    //作用：状态（Keyed State）数据位置检索器。职责：当作业从 Checkpoint 或 Savepoint 恢复时，有些状态很大，已经下载到了某些具体的 TaskManager 节点的本地磁盘上。
    // 这个组件告诉调度器：“某个 Subtask 的状态在 A 机器上”，调度器就会优先把该 Subtask 调度到 A 机器，从而避免跨节点传输几十 GB 状态数据的网络开销（状态本地性）
    protected final StateLocationRetriever stateLocationRetriever;

    //作用：上游输入数据位置检索器。职责：主要用于运行期调度（尤其是批处理和上下游有依赖的流处理）。
    //它能实时查到上游 Task 运行在哪台机器、哪个端口。当下游 Task 准备启动时，调度器利用它来让下游尽量贴近上游部署，实现“数据本地性”，减少 Shuffle 时的网络带宽消耗
    protected final InputsLocationsRetriever inputsLocationsRetriever;

    //作用：已完成检查点的存储管理器。职责：负责在外部持久化存储（如 HDFS、S3 或 ZooKeeper/Etcd 节点）中维护最近成功的 Checkpoint 元数据列表。
    //当整个集群发生灾难性崩溃重启时，调度器会从这里捞出最新的一次成功记录来进行全盘状态恢复
    private final CompletedCheckpointStore completedCheckpointStore;

    //作用：过期/废弃检查点文件的异步清理器。职责：防止外部存储被垃圾文件塞满。
    //当新的 Checkpoint 成功导致旧的过期，或者作业被彻底取消时，它会通过专门的线程池异步去 HDFS/S3 上删除那些不再需要的历史状态文件，确保不阻塞主调度的执行
    private final CheckpointsCleaner checkpointsCleaner;

    //作用：全局自增的 Checkpoint ID 计数器。职责：在分布式环境下保证生成的 Checkpoint ID（如 1, 2, 3...）是全局唯一且严格递增的。
    //它通常由高可用组件（如 ZooKeeper 或 Kubernetes ConfigMap）来持久化驱动，即使 JobManager 挂了，重启后拿到的 ID 也能续得上
    private final CheckpointIDCounter checkpointIdCounter;

    //作用：作业级别的监控指标组（Metrics Group）。职责：Flink 监控系统的入口。
    //作业当前的吞吐量、延时、Checkpoint 成功率、当前状态、重启次数等所有您在 Web UI 上看得到的指标，都是通过这个属性注册并上报给 Prometheus 或 InfluxDB 的
    protected final JobManagerJobMetricGroup jobManagerJobMetricGroup;

    //作用：任务版本（Version）控制与并发安全校验器。职责：防止**“过期的并发修改”**。在高度异步的分布式环境下，Slot 申请成功通知、TaskManager 挂掉通知可能是交织在一起的。
    // 该组件为每个 ExecutionVertex 维护一个版本号（ExecutionVertexVersion）。
    // 当调度器下发指令或处理 Failover 时，会比对版本号，如果发现是个“迟到的过期通知”，则直接丢弃，避免引发状态错乱
    protected final ExecutionVertexVersioner executionVertexVersioner;

    //作用：键值状态（Queryable State）查询处理器。职责：用于支持 Flink 的“可查询状态”功能。
    //当外部客户端想不通过 Sink 算子，直接通过 RPC 查询某个正在运行的算子内部的 State 时，该组件负责定位该 Key 存储在哪个 TaskManager 上，并协调路由
    private final KvStateHandler kvStateHandler;

    //作用：执行图的辅助处理器/对外包装器。
    // 职责：辅助 SchedulerBase 处理一些与 ExecutionGraph 相关的异步和对外 RPC 请求。例如，将一些复杂的内部执行状态转换为外部 REST API 能够识别的 JSON 格式数据
    private final ExecutionGraphHandler executionGraphHandler;

    //作用：算子协调器（Operator Coordinator）的核心处理器。职责：Flink 许多高级数据源/汇（如新版 Kafka Source、Iceberg/Hudi Sink）不仅仅运行在 TaskManager 上，在 JobManager 端还有一个“大脑”，即 OperatorCoordinator。该组件负责管理这些大脑的生命周期。
    //典型场景：Kafka Source 的 Coordinator 负责在 JobManager 端动态发现 Topic 分区，并通过这个 Handler 将分区分配指令发送给各个 TM 上的 Task。它还深度参与 Checkpoint 的触发与对齐
    protected final OperatorCoordinatorHandler operatorCoordinatorHandler;

    //作用：调度器主线程执行器。职责：这是 Flink 架构设计中最核心的安全防线。
    // Flink 为了避免使用复杂的分布式锁和多线程同步锁，采用了Actor单线程模型。所有改变作业状态、处理 Task 失败、触发 Checkpoint 的代码，
    // 都必须包裹在 mainThreadExecutor.execute(...) 中。它确保了 SchedulerBase 内部的所有操作都是串行、无锁且线程安全的
    private final ComponentMainThreadExecutor mainThreadExecutor;

    //作用：固定大小的先进先出（FIFO）异常历史队列。职责：负责存储最近发生的一批根源异常（Root Exceptions）。当作业频繁发生局部失败或重启时，
    // 它会记录每一次失败的详细堆栈、发生时间以及受影响的 Task。这就是您在 Flink Web UI 的 "Exceptions" -> "Exception History" 标签页中看到的数据来源
    private final BoundedFIFOQueue<RootExceptionHistoryEntry> exceptionHistory;

    //作用：最近一次发生的根源异常记录指针。职责：单独指向最后一次导致作业发生全局或局部 Failover 的那个核心异常。
    // 当外部组件（如 REST API）快速查询当前作业为什么又重启时，可以直接返回这个指针指向的内容，无需遍历整个历史队列
    private RootExceptionHistoryEntry latestRootExceptionEntry;

    //作用：执行图工厂。职责：负责创建 ExecutionGraph 实例。从 JobGraph 转换到 ExecutionGraph 的逻辑非常复杂（涉及高可用恢复、安全配置、流/批模式判定、各种 Coordinator 的注入），
    // 这些繁琐的组装逻辑被抽离到了这个工厂类中，使 SchedulerBase 的构造函数更加干净
    private final ExecutionGraphFactory executionGraphFactory;

    //作用：作业状态指标的配置策略开关。职责：控制哪些作业状态（如 RUNNING, FAILING, RESTARTING）的时间指标需要被统计并上报。
    // 为了性能考虑，Flink 允许通过配置（如 MetricOptions.JOB_STATUS_METRICS）来裁剪不需要的监控指标，该属性负责读取和执行这些裁剪规则
    private final MetricOptions.JobStatusMetricsSettings jobStatusMetricsSettings;

    //作用：部署状态耗时监控指标。职责：专门用来监控 Task 从 CREATED -> SCHEDULED -> DEPLOYING -> RUNNING 各个阶段所消耗的时间。
    // 在排查集群资源瓶颈时极其有用。如果这个指标显示 DEPLOYING 时间极长，说明网络下发 Jar 包慢或者 TaskManager 初始化过慢
    private final DeploymentStateTimeMetrics deploymentStateTimeMetrics;

    //作用：Task 执行状态指标注册器列表。职责：负责把物理图上成百上千个具体的 Execution（Subtask 尝试）的状态计数器注册到 Flink 的 Metrics 系统中。通过它，
    // Prometheus 才能拉取到诸如“当前有多少个 Task 处于 FAILED 状态”这样的聚合指标
    private final List<ExecutionStatusMetricsRegistrar> executionStateMetricsRegistrars;

    //作用：节点数据结束（EndOfData）事件监听器。职责：在流批一体架构中（尤其是批处理或有限流中），
    // 当一个 JobVertex 的所有上游 Subtask 都发送完了最后一批数据，会向下游发送一个 EndOfData 信号。该监听器在 JobManager 端捕捉这个事件，
    // 用来精准判定“某个阶段的任务已经把活干完了”，从而通知调度器可以开始回收资源，或者允许下游算子做最后的 close() / endInput() 善后工作
    private final VertexEndOfDataListener vertexEndOfDataListener;

    public SchedulerBase(
            final Logger log,
            final JobGraph jobGraph,
            final Executor ioExecutor,
            final Configuration jobMasterConfiguration,
            final CheckpointsCleaner checkpointsCleaner,
            final CheckpointRecoveryFactory checkpointRecoveryFactory,
            final JobManagerJobMetricGroup jobManagerJobMetricGroup,
            final ExecutionVertexVersioner executionVertexVersioner,
            long initializationTimestamp,
            final ComponentMainThreadExecutor mainThreadExecutor,
            final JobStatusListener jobStatusListener,
            final ExecutionGraphFactory executionGraphFactory,
            final VertexParallelismStore vertexParallelismStore,
            ExecutionPlanSchedulingContext executionPlanSchedulingContext)
            throws Exception {

        this.log = checkNotNull(log);
        this.jobGraph = checkNotNull(jobGraph);
        this.jobInfo = new JobInfoImpl(jobGraph.getJobID(), jobGraph.getName());
        this.executionGraphFactory = executionGraphFactory;

        this.jobManagerJobMetricGroup = checkNotNull(jobManagerJobMetricGroup);
        this.executionVertexVersioner = checkNotNull(executionVertexVersioner);
        this.mainThreadExecutor = mainThreadExecutor;

        this.checkpointsCleaner = checkpointsCleaner;
        this.completedCheckpointStore =
                SchedulerUtils.createCompletedCheckpointStoreIfCheckpointingIsEnabled(
                        jobGraph,
                        jobMasterConfiguration,
                        checkNotNull(checkpointRecoveryFactory),
                        ioExecutor,
                        log);
        this.checkpointIdCounter =
                SchedulerUtils.createCheckpointIDCounterIfCheckpointingIsEnabled(
                        jobGraph, checkNotNull(checkpointRecoveryFactory));

        this.jobStatusMetricsSettings =
                MetricOptions.JobStatusMetricsSettings.fromConfiguration(jobMasterConfiguration);
        this.deploymentStateTimeMetrics =
                new DeploymentStateTimeMetrics(jobGraph.getJobType(), jobStatusMetricsSettings);

        this.executionStateMetricsRegistrars = new ArrayList<>(2);
        this.executionStateMetricsRegistrars.add(
                new DeploymentStateTimeMetrics(jobGraph.getJobType(), jobStatusMetricsSettings));
        if (jobGraph.getJobType() == JobType.STREAMING) {
            // 作业类型
            this.executionStateMetricsRegistrars.add(
                    new AllSubTasksRunningOrFinishedStateTimeMetrics(
                            jobGraph.getJobType(), jobStatusMetricsSettings));
        }

        final CheckpointStatsTracker checkpointStatsTracker =
                SchedulerUtils.createCheckpointStatsTrackerIfCheckpointingIsEnabled(
                        jobGraph,
                        () ->
                                new DefaultCheckpointStatsTracker(
                                        jobMasterConfiguration.get(
                                                WebOptions.CHECKPOINTS_HISTORY_SIZE),
                                        jobManagerJobMetricGroup,
                                        jobMasterConfiguration.get(CHECKPOINT_SPAN_DETAIL_LEVEL),
                                        null));
        this.executionGraph = createAndRestoreExecutionGraph(//
                        completedCheckpointStore,
                        checkpointsCleaner,
                        checkpointIdCounter,
                        checkpointStatsTracker,
                        initializationTimestamp,
                        mainThreadExecutor,
                        jobStatusListener,
                        vertexParallelismStore,
                        executionPlanSchedulingContext);

        this.schedulingTopology = executionGraph.getSchedulingTopology();

        stateLocationRetriever =
                executionVertexId ->
                        getExecutionVertex(executionVertexId).getPreferredLocationBasedOnState();
        inputsLocationsRetriever =
                new ExecutionGraphToInputsLocationsRetrieverAdapter(executionGraph);

        this.kvStateHandler = new KvStateHandler(executionGraph);
        this.executionGraphHandler =
                new ExecutionGraphHandler(executionGraph, log, ioExecutor, this.mainThreadExecutor);

        this.operatorCoordinatorHandler =
                new DefaultOperatorCoordinatorHandler(executionGraph, this::handleGlobalFailure);
        operatorCoordinatorHandler.initializeOperatorCoordinators(this.mainThreadExecutor);

        this.exceptionHistory =
                new BoundedFIFOQueue<>(
                        jobMasterConfiguration.get(WebOptions.MAX_EXCEPTION_HISTORY_SIZE));

        this.vertexEndOfDataListener = new VertexEndOfDataListener(executionGraph);
    }

    private void shutDownCheckpointServices(JobStatus jobStatus) {
        Exception exception = null;

        try {
            completedCheckpointStore.shutdown(jobStatus, checkpointsCleaner);
        } catch (Exception e) {
            exception = e;
        }

        try {
            checkpointIdCounter.shutdown(jobStatus).get();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        if (exception != null) {
            log.error("Error while shutting down checkpoint services.", exception);
        }
    }

    private static int normalizeParallelism(int parallelism) {
        if (parallelism == ExecutionConfig.PARALLELISM_DEFAULT) {
            return 1;
        }
        return parallelism;
    }

    /**
     * Get a default value to use for a given vertex's max parallelism if none was specified.
     *
     * @param vertex the vertex to compute a default max parallelism for
     * @return the computed max parallelism
     */
    public static int getDefaultMaxParallelism(JobVertex vertex) {
        return getDefaultMaxParallelism(vertex.getParallelism());
    }

    public static int getDefaultMaxParallelism(int parallelism) {
        return KeyGroupRangeAssignment.computeDefaultMaxParallelism(
                normalizeParallelism(parallelism));
    }

    public static VertexParallelismStore computeVertexParallelismStore(
            Iterable<JobVertex> vertices, Function<JobVertex, Integer> defaultMaxParallelismFunc) {
        //
        return computeVertexParallelismStore(
                vertices, defaultMaxParallelismFunc, SchedulerBase::normalizeParallelism);
    }

    /**
     * Compute the {@link VertexParallelismStore} for all given vertices, which will set defaults
     * and ensure that the returned store contains valid parallelisms, with a custom function for
     * default max parallelism calculation and a custom function for normalizing vertex parallelism.
     *
     * @param vertices the vertices to compute parallelism for
     * @param defaultMaxParallelismFunc a function for computing a default max parallelism if none
     *     is specified on a given vertex
     * @param normalizeParallelismFunc a function for normalizing vertex parallelism
     * @return the computed parallelism store
     */
    public static VertexParallelismStore computeVertexParallelismStore(
            Iterable<JobVertex> vertices,
            Function<JobVertex, Integer> defaultMaxParallelismFunc,
            Function<Integer, Integer> normalizeParallelismFunc) {
        DefaultVertexParallelismStore store = new DefaultVertexParallelismStore();

        for (JobVertex vertex : vertices) {
            int parallelism = normalizeParallelismFunc.apply(vertex.getParallelism());

            int maxParallelism = vertex.getMaxParallelism();
            final boolean autoConfigured;
            // if no max parallelism was configured by the user, we calculate and set a default
            if (maxParallelism == JobVertex.MAX_PARALLELISM_DEFAULT) {
                maxParallelism = defaultMaxParallelismFunc.apply(vertex);
                autoConfigured = true;
            } else {
                autoConfigured = false;
            }

            VertexParallelismInformation parallelismInfo =
                    new DefaultVertexParallelismInfo(
                            parallelism,
                            maxParallelism,
                            // Allow rescaling if the max parallelism was not set explicitly by the
                            // user
                            (newMax) ->
                                    autoConfigured
                                            ? Optional.empty()
                                            : Optional.of(
                                                    "Cannot override a configured max parallelism."));
            store.setParallelismInfo(vertex.getID(), parallelismInfo);
        }

        return store;
    }

    /**
     * Compute the {@link VertexParallelismStore} for all given vertices, which will set defaults
     * and ensure that the returned store contains valid parallelisms.
     *
     * @param vertices the vertices to compute parallelism for
     * @return the computed parallelism store
     */
    public static VertexParallelismStore computeVertexParallelismStore(
            Iterable<JobVertex> vertices) {
        //
        return computeVertexParallelismStore(vertices, SchedulerBase::getDefaultMaxParallelism);
    }

    /**
     * Compute the {@link VertexParallelismStore} for all vertices of a given job graph, which will
     * set defaults and ensure that the returned store contains valid parallelisms.
     *
     * @param jobGraph the job graph to retrieve vertices from
     * @return the computed parallelism store
     */
    public static VertexParallelismStore computeVertexParallelismStore(JobGraph jobGraph) {
        //
        return computeVertexParallelismStore(jobGraph.getVertices());
    }

    private ExecutionGraph createAndRestoreExecutionGraph(
            CompletedCheckpointStore completedCheckpointStore,
            CheckpointsCleaner checkpointsCleaner,
            CheckpointIDCounter checkpointIdCounter,
            CheckpointStatsTracker checkpointStatsTracker,
            long initializationTimestamp,
            ComponentMainThreadExecutor mainThreadExecutor,
            JobStatusListener jobStatusListener,
            VertexParallelismStore vertexParallelismStore,
            ExecutionPlanSchedulingContext executionPlanSchedulingContext)
            throws Exception {
        //1. 聚合状态更新监听器 (Metric 联动)
        //这里将之前传入的多个指标注册器（executionStateMetricsRegistrars）合并为一个统一的监听器。这个监听器会被注入到执行图中。
        // 一旦后续某个具体的 Subtask 状态发生改变（比如从 DEPLOYING 变成 RUNNING），该监听器就会第一时间收到通知，并立刻更新 Web UI 和 Prometheus 上的 Metrics 计数器
        final ExecutionStateUpdateListener combinedExecutionStateUpdateListener;
        if (executionStateMetricsRegistrars.size() == 1) {
            combinedExecutionStateUpdateListener = executionStateMetricsRegistrars.get(0);
        } else {
            combinedExecutionStateUpdateListener =
                    ExecutionStateUpdateListener.combine(
                            executionStateMetricsRegistrars.toArray(
                                    new ExecutionStateUpdateListener[0]));
        }
        //2. 调用工厂真正构建与恢复物理图
        // Create（创建）：它委托给 executionGraphFactory（执行图工厂），根据最初的 jobGraph（用户的拓扑和并行度参数）将逻辑节点展开成物理节点。
        // Restore（恢复）：这是流处理容错的核心。它将 completedCheckpointStore（已完成的检查点仓库）传进去。
        //如果该作业是从某个 Checkpoint 或 Savepoint 恢复的，工厂会在这个阶段将历史的状态元数据重新绑定到各个物理算子节点上
        final ExecutionGraph newExecutionGraph = executionGraphFactory.createAndRestoreExecutionGraph(//
                        jobGraph,
                        completedCheckpointStore,
                        checkpointsCleaner,
                        checkpointIdCounter,
                        checkpointStatsTracker,
                        TaskDeploymentDescriptorFactory.PartitionLocationConstraint.fromJobType(
                                jobGraph.getJobType()),
                        initializationTimestamp,
                        new DefaultVertexAttemptNumberStore(),
                        vertexParallelismStore,
                        combinedExecutionStateUpdateListener,
                        getMarkPartitionFinishedStrategy(),
                        executionPlanSchedulingContext,
                        log);
        //3. 绑定内部失败监听器
        //作用：建立 Task 崩溃时的“报警热线”。解释：当作业运行起来后，如果某个 TaskManager 上的 Task 发生了异常（如空指针、网络断开），
        // ExecutionGraph 内部会最先感知到。通过绑定这个 UpdateSchedulerNgOnInternalFailuresListener(this)，
        // 一旦发生内部 Task 失败，就会立刻反向通知外部的调度器（即当前的 SchedulerBase），从而触发 Flink 的局部恢复（Region Failover）或全局重启策略
        newExecutionGraph.setInternalTaskFailuresListener(
                new UpdateSchedulerNgOnInternalFailuresListener(this));
        //4. 注册作业状态监听器 (生命周期监控)
        //作用：监控作业整体状态的流转。解释：注册一个监听器来死死盯着整个 Job 的状态（如 JobStatus 从 CREATED 变为 RUNNING，或变为 FINISHED/FAILED）。
        // 当状态改变时，通知外部组件（如 Dispatcher、Web UI 服务）同步更新状态
        newExecutionGraph.registerJobStatusListener(jobStatusListener);
        //5. 绑定并启动主线程执行器
        //将核心的单线程执行器 mainThreadExecutor 传给 ExecutionGraph。
        // 从这一刻起，整个 ExecutionGraph 内部的所有状态修改逻辑，都将严格在 Flink 的这个主线程中串行执行，确保了多线程环境下的绝对数据安全。
        // 调用 start() 也意味着物理执行图正式进入就绪状态
        //DefaultExecutionGraph#start
        newExecutionGraph.start(mainThreadExecutor);

        return newExecutionGraph;
    }

    protected void resetForNewExecutions(final Collection<ExecutionVertexID> vertices) {
        vertices.stream().forEach(this::resetForNewExecution);
    }

    protected void resetForNewExecution(final ExecutionVertexID executionVertexId) {
        getExecutionVertex(executionVertexId).resetForNewExecution();
    }

    protected void restoreState(
            final Set<ExecutionVertexID> vertices, final boolean isGlobalRecovery)
            throws Exception {
        vertexEndOfDataListener.restoreVertices(vertices);

        final CheckpointCoordinator checkpointCoordinator =
                executionGraph.getCheckpointCoordinator();

        if (checkpointCoordinator == null) {
            // batch failover case - we only need to notify the OperatorCoordinators,
            // not do any actual state restore
            if (isGlobalRecovery) {
                notifyCoordinatorsOfEmptyGlobalRestore();
            } else {
                notifyCoordinatorsOfSubtaskRestore(
                        getInvolvedExecutionJobVerticesAndSubtasks(vertices),
                        OperatorCoordinator.NO_CHECKPOINT);
            }
            return;
        }

        // if there is checkpointed state, reload it into the executions

        // abort pending checkpoints to
        // i) enable new checkpoint triggering without waiting for last checkpoint expired.
        // ii) ensure the EXACTLY_ONCE semantics if needed.
        checkpointCoordinator.abortPendingCheckpoints(
                new CheckpointException(CheckpointFailureReason.JOB_FAILOVER_REGION));

        if (isGlobalRecovery) {
            final Set<ExecutionJobVertex> jobVerticesToRestore =
                    getInvolvedExecutionJobVertices(vertices);

            checkpointCoordinator.restoreLatestCheckpointedStateToAll(jobVerticesToRestore, true);

        } else {
            final Map<ExecutionJobVertex, IntArrayList> subtasksToRestore =
                    getInvolvedExecutionJobVerticesAndSubtasks(vertices);

            final OptionalLong restoredCheckpointId =
                    checkpointCoordinator.restoreLatestCheckpointedStateToSubtasks(
                            subtasksToRestore.keySet());

            // Ideally, the Checkpoint Coordinator would call OperatorCoordinator.resetSubtask, but
            // the Checkpoint Coordinator is not aware of subtasks in a local failover. It always
            // assigns state to all subtasks, and for the subtask execution attempts that are still
            // running (or not waiting to be deployed) the state assignment has simply no effect.
            // Because of that, we need to do the "subtask restored" notification here.
            // Once the Checkpoint Coordinator is properly aware of partial (region) recovery,
            // this code should move into the Checkpoint Coordinator.
            final long checkpointId =
                    restoredCheckpointId.orElse(OperatorCoordinator.NO_CHECKPOINT);
            notifyCoordinatorsOfSubtaskRestore(subtasksToRestore, checkpointId);
        }
    }

    private void notifyCoordinatorsOfSubtaskRestore(
            final Map<ExecutionJobVertex, IntArrayList> restoredSubtasks, final long checkpointId) {

        for (final Map.Entry<ExecutionJobVertex, IntArrayList> vertexSubtasks :
                restoredSubtasks.entrySet()) {
            final ExecutionJobVertex jobVertex = vertexSubtasks.getKey();
            final IntArrayList subtasks = vertexSubtasks.getValue();

            final Collection<OperatorCoordinatorHolder> coordinators =
                    jobVertex.getOperatorCoordinators();
            if (coordinators.isEmpty()) {
                continue;
            }

            while (!subtasks.isEmpty()) {
                final int subtask =
                        subtasks.removeLast(); // this is how IntArrayList implements iterations
                for (final OperatorCoordinatorHolder opCoordinator : coordinators) {
                    opCoordinator.subtaskReset(subtask, checkpointId);
                }
            }
        }
    }

    private void notifyCoordinatorsOfEmptyGlobalRestore() throws Exception {
        for (final ExecutionJobVertex ejv : getExecutionGraph().getAllVertices().values()) {
            if (!ejv.isInitialized()) {
                continue;
            }
            for (final OperatorCoordinatorHolder coordinator : ejv.getOperatorCoordinators()) {
                coordinator.resetToCheckpoint(OperatorCoordinator.NO_CHECKPOINT, null);
            }
        }
    }

    private Set<ExecutionJobVertex> getInvolvedExecutionJobVertices(
            final Set<ExecutionVertexID> executionVertices) {

        final Set<ExecutionJobVertex> tasks = new HashSet<>();
        for (ExecutionVertexID executionVertexID : executionVertices) {
            final ExecutionVertex executionVertex = getExecutionVertex(executionVertexID);
            tasks.add(executionVertex.getJobVertex());
        }
        return tasks;
    }

    private Map<ExecutionJobVertex, IntArrayList> getInvolvedExecutionJobVerticesAndSubtasks(
            final Set<ExecutionVertexID> executionVertices) {

        final HashMap<ExecutionJobVertex, IntArrayList> result = new HashMap<>();

        for (ExecutionVertexID executionVertexID : executionVertices) {
            final ExecutionVertex executionVertex = getExecutionVertex(executionVertexID);
            final IntArrayList subtasks =
                    result.computeIfAbsent(
                            executionVertex.getJobVertex(), (key) -> new IntArrayList(32));
            subtasks.add(executionVertex.getParallelSubtaskIndex());
        }

        return result;
    }

    protected void setGlobalFailureCause(@Nullable final Throwable cause, long timestamp) {
        if (cause != null) {
            executionGraph.initFailureCause(cause, timestamp);
        }
    }

    protected ComponentMainThreadExecutor getMainThreadExecutor() {
        return mainThreadExecutor;
    }

    protected void failJob(
            Throwable cause, long timestamp, CompletableFuture<Map<String, String>> failureLabels) {
        incrementVersionsOfAllVertices();
        cancelAllPendingSlotRequestsInternal();
        executionGraph.failJob(cause, timestamp);
        getJobTerminationFuture().thenRun(() -> archiveGlobalFailure(cause, failureLabels));
    }

    protected final SchedulingTopology getSchedulingTopology() {
        return schedulingTopology;
    }

    protected final ResultPartitionAvailabilityChecker getResultPartitionAvailabilityChecker() {
        return executionGraph.getResultPartitionAvailabilityChecker();
    }

    protected final void transitionToRunning() {
        //DefaultExecutionGraph#transitionToRunning 将状态从创建改为运行
        executionGraph.transitionToRunning();
    }

    public ExecutionVertex getExecutionVertex(final ExecutionVertexID executionVertexId) {
        return executionGraph
                .getAllVertices()
                .get(executionVertexId.getJobVertexId())
                .getTaskVertices()[executionVertexId.getSubtaskIndex()];
    }

    public ExecutionJobVertex getExecutionJobVertex(final JobVertexID jobVertexId) {
        return executionGraph.getAllVertices().get(jobVertexId);
    }

    protected JobGraph getJobGraph() {
        return jobGraph;
    }

    protected abstract long getNumberOfRestarts();

    protected abstract long getNumberOfRescales();

    protected MarkPartitionFinishedStrategy getMarkPartitionFinishedStrategy() {
        // blocking partition always need mark finished.
        return ResultPartitionType::isBlockingOrBlockingPersistentResultPartition;
    }

    private Map<ExecutionVertexID, ExecutionVertexVersion> incrementVersionsOfAllVertices() {
        return executionVertexVersioner.recordVertexModifications(
                IterableUtils.toStream(schedulingTopology.getVertices())
                        .map(SchedulingExecutionVertex::getId)
                        .collect(Collectors.toSet()));
    }

    protected abstract void cancelAllPendingSlotRequestsInternal();

    protected void transitionExecutionGraphState(
            final JobStatus current, final JobStatus newState) {
        executionGraph.transitionState(current, newState);
    }

    @VisibleForTesting
    CheckpointCoordinator getCheckpointCoordinator() {
        return executionGraph.getCheckpointCoordinator();
    }

    /**
     * ExecutionGraph is exposed to make it easier to rework tests to be based on the new scheduler.
     * ExecutionGraph is expected to be used only for state check. Yet at the moment, before all the
     * actions are factored out from ExecutionGraph and its sub-components, some actions may still
     * be performed directly on it.
     */
    @VisibleForTesting
    public ExecutionGraph getExecutionGraph() {
        return executionGraph;
    }

    // ------------------------------------------------------------------------
    // SchedulerNG
    // ------------------------------------------------------------------------

    @Override
    public final void startScheduling() {
        mainThreadExecutor.assertRunningInMainThread();
        registerJobMetrics(
                jobManagerJobMetricGroup,
                executionGraph,
                this::getNumberOfRestarts,
                this::getNumberOfRescales,
                executionStateMetricsRegistrars,
                executionGraph::registerJobStatusListener,
                executionGraph.getStatusTimestamp(JobStatus.INITIALIZING),
                jobStatusMetricsSettings);
        operatorCoordinatorHandler.startAllOperatorCoordinators();
        //DefaultScheduler#startSchedulingInternal
        startSchedulingInternal();//
    }

    public static void registerJobMetrics(
            MetricGroup metrics,
            JobStatusProvider jobStatusProvider,
            Gauge<Long> numberOfRestarts,
            Gauge<Long> numberOfRescales,
            Collection<? extends MetricsRegistrar> metricsRegistrars,
            Consumer<JobStatusListener> jobStatusListenerRegistrar,
            long initializationTimestamp,
            MetricOptions.JobStatusMetricsSettings jobStatusMetricsSettings) {
        metrics.gauge(DownTimeGauge.METRIC_NAME, new DownTimeGauge(jobStatusProvider));
        metrics.gauge(UpTimeGauge.METRIC_NAME, new UpTimeGauge(jobStatusProvider));
        metrics.gauge(MetricNames.NUM_RESTARTS, numberOfRestarts::getValue);
        metrics.gauge(MetricNames.NUM_RESCALES, numberOfRescales::getValue);

        final JobStatusMetrics jobStatusMetrics =
                new JobStatusMetrics(initializationTimestamp, jobStatusMetricsSettings);
        jobStatusMetrics.registerMetrics(metrics);
        jobStatusListenerRegistrar.accept(jobStatusMetrics);

        for (MetricsRegistrar metricsRegistrar : metricsRegistrars) {
            metricsRegistrar.registerMetrics(metrics);
        }
    }

    protected abstract void startSchedulingInternal();

    @Override
    public CompletableFuture<Void> closeAsync() {
        mainThreadExecutor.assertRunningInMainThread();

        final FlinkException cause = new FlinkException("Scheduler is being stopped.");

        final CompletableFuture<Void> checkpointServicesShutdownFuture =
                FutureUtils.composeAfterwardsAsync(
                        executionGraph
                                .getTerminationFuture()
                                .thenAcceptAsync(
                                        this::shutDownCheckpointServices, getMainThreadExecutor()),
                        checkpointsCleaner::closeAsync,
                        getMainThreadExecutor());

        FutureUtils.assertNoException(checkpointServicesShutdownFuture);

        incrementVersionsOfAllVertices();
        cancelAllPendingSlotRequestsInternal();
        executionGraph.suspend(cause);
        operatorCoordinatorHandler.disposeAllOperatorCoordinators();
        return checkpointServicesShutdownFuture;
    }

    @Override
    public void cancel() {
        mainThreadExecutor.assertRunningInMainThread();

        incrementVersionsOfAllVertices();
        cancelAllPendingSlotRequestsInternal();
        executionGraph.cancel();
    }

    @Override
    public CompletableFuture<JobStatus> getJobTerminationFuture() {
        return executionGraph.getTerminationFuture();
    }

    protected final void archiveGlobalFailure(
            Throwable failure, CompletableFuture<Map<String, String>> failureLabels) {
        archiveGlobalFailure(
                failure,
                executionGraph.getStatusTimestamp(JobStatus.FAILED),
                failureLabels,
                StreamSupport.stream(executionGraph.getAllExecutionVertices().spliterator(), false)
                        .map(ExecutionVertex::getCurrentExecutionAttempt)
                        .collect(Collectors.toSet()));
    }

    private void archiveGlobalFailure(
            Throwable failure,
            long timestamp,
            CompletableFuture<Map<String, String>> failureLabels,
            Iterable<Execution> executions) {
        latestRootExceptionEntry =
                RootExceptionHistoryEntry.fromGlobalFailure(
                        failure, timestamp, failureLabels, executions);
        exceptionHistory.add(latestRootExceptionEntry);
        log.debug("Archive global failure.", failure);
    }

    protected final void archiveFromFailureHandlingResult(
            FailureHandlingResultSnapshot failureHandlingResult) {
        if (!failureHandlingResult.isRootCause()) {
            // Handle all subsequent exceptions as the concurrent exceptions when it's not a new
            // attempt.
            checkState(
                    latestRootExceptionEntry != null,
                    "A root exception entry should exist if failureHandlingResult wasn't "
                            + "generated as part of a new error handling cycle.");
            List<Execution> concurrentlyExecutions = new ArrayList<>();
            failureHandlingResult.getRootCauseExecution().ifPresent(concurrentlyExecutions::add);
            concurrentlyExecutions.addAll(failureHandlingResult.getConcurrentlyFailedExecution());

            latestRootExceptionEntry.addConcurrentExceptions(concurrentlyExecutions);
        } else if (failureHandlingResult.getRootCauseExecution().isPresent()) {
            final Execution rootCauseExecution =
                    failureHandlingResult.getRootCauseExecution().get();

            latestRootExceptionEntry =
                    RootExceptionHistoryEntry.fromFailureHandlingResultSnapshot(
                            failureHandlingResult);
            exceptionHistory.add(latestRootExceptionEntry);

            log.debug(
                    "Archive local failure causing attempt {} to fail: {}",
                    rootCauseExecution.getAttemptId(),
                    latestRootExceptionEntry.getExceptionAsString());
        } else {
            archiveGlobalFailure(
                    failureHandlingResult.getRootCause(),
                    failureHandlingResult.getTimestamp(),
                    failureHandlingResult.getFailureLabels(),
                    failureHandlingResult.getConcurrentlyFailedExecution());
        }
    }

    @Override
    public boolean updateTaskExecutionState(final TaskExecutionStateTransition taskExecutionState) {

        final ExecutionAttemptID attemptId = taskExecutionState.getID();
        final Execution execution = executionGraph.getRegisteredExecutions().get(attemptId);
        if (execution != null && executionGraph.updateState(taskExecutionState)) {
            onTaskExecutionStateUpdate(execution, taskExecutionState);
            return true;
        }

        return false;
    }

    private void onTaskExecutionStateUpdate(
            final Execution execution, final TaskExecutionStateTransition taskExecutionState) {

        // only notifies a state update if it's effective, namely it successfully
        // turns the execution state to the expected value.
        if (execution.getState() != taskExecutionState.getExecutionState()) {
            return;
        }

        // only notifies FINISHED and FAILED states which are needed at the moment.
        // can be refined in FLINK-14233 after the actions are factored out from ExecutionGraph.
        switch (taskExecutionState.getExecutionState()) {
            case FINISHED:
                onTaskFinished(execution, taskExecutionState.getIOMetrics());
                break;
            case FAILED:
                onTaskFailed(execution);
                break;
        }
    }

    protected abstract void onTaskFinished(final Execution execution, final IOMetrics ioMetrics);

    protected abstract void onTaskFailed(final Execution execution);

    @Override
    public SerializedInputSplit requestNextInputSplit(
            JobVertexID vertexID, ExecutionAttemptID executionAttempt) throws IOException {
        mainThreadExecutor.assertRunningInMainThread();

        return executionGraphHandler.requestNextInputSplit(vertexID, executionAttempt);
    }

    @Override
    public ExecutionState requestPartitionState(
            final IntermediateDataSetID intermediateResultId,
            final ResultPartitionID resultPartitionId)
            throws PartitionProducerDisposedException {

        mainThreadExecutor.assertRunningInMainThread();

        return executionGraphHandler.requestPartitionState(intermediateResultId, resultPartitionId);
    }

    @VisibleForTesting
    public Iterable<RootExceptionHistoryEntry> getExceptionHistory() {
        return exceptionHistory.toArrayList();
    }

    @Override
    public ExecutionGraphInfo requestJob() {
        mainThreadExecutor.assertRunningInMainThread();
        return new ExecutionGraphInfo(
                ArchivedExecutionGraph.createFrom(executionGraph), getExceptionHistory());
    }

    @Override
    public CheckpointStatsSnapshot requestCheckpointStats() {
        mainThreadExecutor.assertRunningInMainThread();
        return executionGraph.getCheckpointStatsSnapshot();
    }

    @Override
    public JobStatus requestJobStatus() {
        return executionGraph.getState();
    }

    @Override
    public KvStateLocation requestKvStateLocation(final JobID jobId, final String registrationName)
            throws UnknownKvStateLocation, FlinkJobNotFoundException {
        mainThreadExecutor.assertRunningInMainThread();

        return kvStateHandler.requestKvStateLocation(jobId, registrationName);
    }

    @Override
    public void notifyKvStateRegistered(
            final JobID jobId,
            final JobVertexID jobVertexId,
            final KeyGroupRange keyGroupRange,
            final String registrationName,
            final KvStateID kvStateId,
            final InetSocketAddress kvStateServerAddress)
            throws FlinkJobNotFoundException {
        mainThreadExecutor.assertRunningInMainThread();

        kvStateHandler.notifyKvStateRegistered(
                jobId,
                jobVertexId,
                keyGroupRange,
                registrationName,
                kvStateId,
                kvStateServerAddress);
    }

    @Override
    public void notifyKvStateUnregistered(
            final JobID jobId,
            final JobVertexID jobVertexId,
            final KeyGroupRange keyGroupRange,
            final String registrationName)
            throws FlinkJobNotFoundException {
        mainThreadExecutor.assertRunningInMainThread();

        kvStateHandler.notifyKvStateUnregistered(
                jobId, jobVertexId, keyGroupRange, registrationName);
    }

    @Override
    public void updateAccumulators(final AccumulatorSnapshot accumulatorSnapshot) {
        mainThreadExecutor.assertRunningInMainThread();

        executionGraph.updateAccumulators(accumulatorSnapshot);
    }

    @Override
    public CompletableFuture<String> triggerSavepoint(
            final String targetDirectory,
            final boolean cancelJob,
            final SavepointFormatType formatType) {
        mainThreadExecutor.assertRunningInMainThread();

        if (isAnyOutputBlocking(executionGraph)) {
            // TODO: Introduce a more general solution to mark times when
            //  checkpoints are disabled, as well as the detailed reason.
            //  https://issues.apache.org/jira/browse/FLINK-34519
            return FutureUtils.completedExceptionally(
                    new CheckpointException(CheckpointFailureReason.BLOCKING_OUTPUT_EXIST));
        }

        final CheckpointCoordinator checkpointCoordinator =
                executionGraph.getCheckpointCoordinator();
        StopWithSavepointTerminationManager.checkSavepointActionPreconditions(
                checkpointCoordinator, targetDirectory, getJobId(), log);

        log.info(
                "Triggering {}savepoint for job {}.",
                cancelJob ? "cancel-with-" : "",
                jobGraph.getJobID());

        if (cancelJob) {
            stopCheckpointScheduler();
        }

        return checkpointCoordinator
                .triggerSavepoint(targetDirectory, formatType)
                .thenApply(CompletedCheckpoint::getExternalPointer)
                .handleAsync(
                        (path, throwable) -> {
                            if (throwable != null) {
                                if (cancelJob) {
                                    startCheckpointScheduler();
                                }
                                throw new CompletionException(throwable);
                            } else if (cancelJob) {
                                log.info(
                                        "Savepoint stored in {}. Now cancelling {}.",
                                        path,
                                        jobGraph.getJobID());
                                cancel();
                            }
                            return path;
                        },
                        mainThreadExecutor);
    }

    @Override
    public CompletableFuture<CompletedCheckpoint> triggerCheckpoint(CheckpointType checkpointType) {
        mainThreadExecutor.assertRunningInMainThread();

        final CheckpointCoordinator checkpointCoordinator =
                executionGraph.getCheckpointCoordinator();
        final JobID jobID = jobGraph.getJobID();
        if (checkpointCoordinator == null) {
            throw new IllegalStateException(String.format("Job %s is not a streaming job.", jobID));
        }
        log.info("Triggering a manual checkpoint for job {}.", jobID);

        return checkpointCoordinator
                .triggerCheckpoint(checkpointType)
                .handleAsync(
                        (path, throwable) -> {
                            if (throwable != null) {
                                throw new CompletionException(throwable);
                            }
                            return path;
                        },
                        mainThreadExecutor);
    }

    @Override
    public void stopCheckpointScheduler() {
        final CheckpointCoordinator checkpointCoordinator = getCheckpointCoordinator();
        if (checkpointCoordinator == null) {
            log.info(
                    "Periodic checkpoint scheduling could not be stopped due to the CheckpointCoordinator being shutdown.");
        } else {
            checkpointCoordinator.stopCheckpointScheduler();
        }
    }

    @Override
    public void startCheckpointScheduler() {
        mainThreadExecutor.assertRunningInMainThread();
        final CheckpointCoordinator checkpointCoordinator = getCheckpointCoordinator();

        if (checkpointCoordinator == null) {
            log.info(
                    "Periodic checkpoint scheduling could not be started due to the CheckpointCoordinator being shutdown.");
        } else if (checkpointCoordinator.isPeriodicCheckpointingConfigured()) {
            try {
                checkpointCoordinator.startCheckpointScheduler();
            } catch (IllegalStateException ignored) {
                // Concurrent shut down of the coordinator
            }
        }
    }

    @Override
    public void acknowledgeCheckpoint(
            final JobID jobID,
            final ExecutionAttemptID executionAttemptID,
            final long checkpointId,
            final CheckpointMetrics checkpointMetrics,
            final TaskStateSnapshot checkpointState) {

        executionGraphHandler.acknowledgeCheckpoint(
                jobID, executionAttemptID, checkpointId, checkpointMetrics, checkpointState);
    }

    @Override
    public void declineCheckpoint(final DeclineCheckpoint decline) {

        executionGraphHandler.declineCheckpoint(decline);
    }

    @Override
    public void reportCheckpointMetrics(
            JobID jobID, ExecutionAttemptID attemptId, long id, CheckpointMetrics metrics) {
        executionGraphHandler.reportCheckpointMetrics(attemptId, id, metrics);
    }

    @Override
    public void reportInitializationMetrics(
            JobID jobId,
            ExecutionAttemptID executionAttemptId,
            SubTaskInitializationMetrics initializationMetrics) {
        executionGraphHandler.reportInitializationMetrics(
                executionAttemptId, initializationMetrics);
    }

    @Override
    public CompletableFuture<String> stopWithSavepoint(
            @Nullable final String targetDirectory,
            final boolean terminate,
            final SavepointFormatType formatType) {
        mainThreadExecutor.assertRunningInMainThread();

        if (isAnyOutputBlocking(executionGraph)) {
            return FutureUtils.completedExceptionally(
                    new CheckpointException(CheckpointFailureReason.BLOCKING_OUTPUT_EXIST));
        }

        final CheckpointCoordinator checkpointCoordinator =
                executionGraph.getCheckpointCoordinator();

        StopWithSavepointTerminationManager.checkSavepointActionPreconditions(
                checkpointCoordinator, targetDirectory, executionGraph.getJobID(), log);

        log.info("Triggering stop-with-savepoint for job {}.", jobGraph.getJobID());

        // we stop the checkpoint coordinator so that we are guaranteed
        // to have only the data of the synchronous savepoint committed.
        // in case of failure, and if the job restarts, the coordinator
        // will be restarted by the CheckpointCoordinatorDeActivator.
        stopCheckpointScheduler();

        final CompletableFuture<Collection<ExecutionState>> executionTerminationsFuture =
                getCombinedExecutionTerminationFuture();

        final CompletableFuture<CompletedCheckpoint> savepointFuture =
                checkpointCoordinator.triggerSynchronousSavepoint(
                        terminate, targetDirectory, formatType);

        final StopWithSavepointTerminationManager stopWithSavepointTerminationManager =
                new StopWithSavepointTerminationManager(
                        new StopWithSavepointTerminationHandlerImpl(
                                jobGraph.getJobID(), this, log));

        return stopWithSavepointTerminationManager.stopWithSavepoint(
                savepointFuture, executionTerminationsFuture, mainThreadExecutor);
    }

    /**
     * Returns a {@code CompletableFuture} collecting the termination states of all {@link Execution
     * Executions} of the underlying {@link ExecutionGraph}.
     *
     * @return a {@code CompletableFuture} that completes after all underlying {@code Executions}
     *     have been terminated.
     */
    private CompletableFuture<Collection<ExecutionState>> getCombinedExecutionTerminationFuture() {
        return FutureUtils.combineAll(
                StreamSupport.stream(executionGraph.getAllExecutionVertices().spliterator(), false)
                        .map(ExecutionVertex::getCurrentExecutionAttempt)
                        .map(Execution::getTerminalStateFuture)
                        .collect(Collectors.toList()));
    }

    // ------------------------------------------------------------------------
    //  Operator Coordinators
    //
    //  Note: It may be worthwhile to move the OperatorCoordinators out
    //        of the scheduler (have them owned by the JobMaster directly).
    //        Then we could avoid routing these events through the scheduler and
    //        doing this lazy initialization dance. However, this would require
    //        that the Scheduler does not eagerly construct the CheckpointCoordinator
    //        in the ExecutionGraph and does not eagerly restore the savepoint while
    //        doing that. Because during savepoint restore, the OperatorCoordinators
    //        (or at least their holders) already need to exist, to accept the restored
    //        state. But some components they depend on (Scheduler and MainThreadExecutor)
    //        are not fully usable and accessible at that point.
    // ------------------------------------------------------------------------

    @Override
    public void deliverOperatorEventToCoordinator(
            final ExecutionAttemptID taskExecutionId,
            final OperatorID operatorId,
            final OperatorEvent evt)
            throws FlinkException {

        operatorCoordinatorHandler.deliverOperatorEventToCoordinator(
                taskExecutionId, operatorId, evt);
    }

    @Override
    public CompletableFuture<CoordinationResponse> deliverCoordinationRequestToCoordinator(
            OperatorID operator, CoordinationRequest request) throws FlinkException {

        return operatorCoordinatorHandler.deliverCoordinationRequestToCoordinator(
                operator, request);
    }

    @Override
    public void notifyEndOfData(ExecutionAttemptID executionAttemptID) {
        if (jobGraph.getJobType() == JobType.STREAMING
                && jobGraph.isCheckpointingEnabled()
                && jobGraph.getCheckpointingSettings()
                        .getCheckpointCoordinatorConfiguration()
                        .isEnableCheckpointsAfterTasksFinish()) {
            vertexEndOfDataListener.recordTaskEndOfData(executionAttemptID);
            if (vertexEndOfDataListener.areAllTasksOfJobVertexEndOfData(
                    executionAttemptID.getJobVertexId())) {
                List<OperatorIDPair> operatorIDPairs =
                        executionGraph
                                .getJobVertex(executionAttemptID.getJobVertexId())
                                .getOperatorIDs();
                CheckpointCoordinator checkpointCoordinator =
                        executionGraph.getCheckpointCoordinator();
                if (checkpointCoordinator != null) {
                    for (OperatorIDPair operatorIDPair : operatorIDPairs) {
                        checkpointCoordinator.setIsProcessingBacklog(
                                operatorIDPair.getGeneratedOperatorID(), false);
                    }
                }
            }
            if (vertexEndOfDataListener.areAllTasksEndOfData()) {
                triggerCheckpoint(CheckpointType.CONFIGURED);
            }
        }
    }

    // ------------------------------------------------------------------------
    //  access utils for testing
    // ------------------------------------------------------------------------

    @VisibleForTesting
    protected JobID getJobId() {
        return jobGraph.getJobID();
    }

    @VisibleForTesting
    VertexEndOfDataListener getVertexEndOfDataListener() {
        return vertexEndOfDataListener;
    }
}
