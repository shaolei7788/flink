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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.events.Events;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.blocklist.BlockedNode;
import org.apache.flink.runtime.blocklist.BlocklistContext;
import org.apache.flink.runtime.blocklist.BlocklistHandler;
import org.apache.flink.runtime.blocklist.BlocklistUtils;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.SubTaskInitializationMetrics;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.heartbeat.HeartbeatListener;
import org.apache.flink.runtime.heartbeat.HeartbeatManager;
import org.apache.flink.runtime.heartbeat.HeartbeatReceiver;
import org.apache.flink.runtime.heartbeat.HeartbeatSender;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.NoOpHeartbeatManager;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.io.network.partition.PartitionTrackerFactory;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmanager.PartitionProducerDisposedException;
import org.apache.flink.runtime.jobmaster.factories.JobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.jobmaster.slotpool.BlocklistDeclarativeSlotPoolFactory;
import org.apache.flink.runtime.jobmaster.slotpool.DeclarativeSlotPoolFactory;
import org.apache.flink.runtime.jobmaster.slotpool.DefaultDeclarativeSlotPoolFactory;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPoolService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.query.UnknownKvStateLocation;
import org.apache.flink.runtime.registration.RegisteredRpcConnection;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.registration.RetryingRegistration;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.FencedRpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcServiceUtils;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.scheduler.SchedulerNG;
import org.apache.flink.runtime.shuffle.JobShuffleContext;
import org.apache.flink.runtime.shuffle.JobShuffleContextImpl;
import org.apache.flink.runtime.shuffle.PartitionWithMetrics;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutorToJobManagerHeartbeatPayload;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation.ResolutionMode;
import org.apache.flink.runtime.taskmanager.UnresolvedTaskManagerLocation;
import org.apache.flink.streaming.api.graph.ExecutionPlan;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.MdcUtils;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.checkpoint.TaskStateSnapshot.deserializeTaskStateSnapshot;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * JobMaster implementation. The job master is responsible for the execution of a single {@link
 * ExecutionPlan}.
 *
 * <p>It offers the following methods as part of its rpc interface to interact with the JobMaster
 * remotely:
 *
 * <ul>
 *   <li>{@link #updateTaskExecutionState} updates the task execution state for given task
 * </ul>
 */
//负责管理单一的具体作业（管 Task 调度）
    // 启动的时候会向ResourceManager进行注册 申请资源
public class JobMaster extends FencedRpcEndpoint<JobMasterId>
        implements JobMasterGateway, JobMasterService {

    /** Default names for Flink's distributed components. */
    public static final String JOB_MANAGER_NAME = "jobmanager";

    // ------------------------------------------------------------------------

    private final JobMasterConfiguration jobMasterConfiguration;

    //当前 JobMaster 进程的唯一身份标识（通常是一个 UUID）。
    // 当 JobMaster 向 ResourceManager 注册、或者与 TaskManager 建立连接时，必须携带这个 ResourceID，以便其他组件在集群大盘中识别并跟踪它
    private final ResourceID resourceId;

    //作业的拓扑结构图（执行计划）。这是 Flink 2.2 引入的最新抽象（替代了旧版的 JobGraph）。
    // 它包含了用户代码编译出来的算子链（JobVertex）、数据流向关系以及作业 ID（JobID）。JobMaster 的所有调度工作（构建 ExecutionGraph）都以此为蓝本
    private final ExecutionPlan executionPlan;

    //RPC 远程调用的硬性超时时间限制。JobMaster 在向外（如向 ResourceManager 申请资源、向 TaskManager 部署任务）
    // 发起异步 Pekko RPC 请求时，所有的 ask() 操作都会绑定这个超时时间。一旦超过该时间未收到响应，就会抛出 AskTimeoutException
    private final Duration rpcTimeout;

    //高可用（HA）服务门面接口。它提供了访问分布式协调中心（如 ZooKeeper、Kubernetes ConfigMaps）的能力。
    // JobMaster 借助它来获取集群其他核心组件（如 ResourceManager）的 Leader 地址信息，并在作业结束时通过它清理 HA 存储中的元数据
    private final HighAvailabilityServices highAvailabilityServices;

    //中央文件服务（BlobServer）的写入器。在作业运行期间，如果有动态生成的持久化数据
    // （如大规模的执行计划归档、运行时产生的特定指标文件、或者需要分发给 TaskManager 的临时数据），JobMaster 会通过 blobWriter 将其上传到集群的 BlobServer 中
    private final BlobWriter blobWriter;

    //心跳服务工厂。JobMaster 启动后，需要维持与 ResourceManager 以及所有承载该作业的 TaskManager 之间的双向心跳。
    // 该服务负责创建和管理这些心跳监控器（HeartbeatMonitor），一旦判定某台 TaskManager 心跳超时（失联），立即触发任务局部重启
    private final HeartbeatServices heartbeatServices;

    //专用的定时与延迟任务线程池。主要负责处理有时间属性的异步逻辑，例如：Slot 申请的超时检查、延迟重启策略（发生故障后等待 5 秒再重启）、定期清理内部缓存等
    private final ScheduledExecutorService futureExecutor;

    //专用的阻塞式 I/O 线程池。为了不阻塞核心的 RPC 线程（Pekko 线程），所有涉及磁盘读写、本地文件加载、网络大文件流传输（如读取或写入分布式存储、反序列化大对象）的操作，JobMaster 都会强制丢进 ioExecutor 线程池中异步执行
    private final Executor ioExecutor;

    //作业终结状态回调通知器。当整个作业正常执行完毕（FINISHED），或者遭遇不可逆的失败（FAILED）、以及被用户手动取消（CANCELED）达到最终状态时，
    // JobMaster 会调用这个接口。它会向上通知 Dispatcher：“我的项目做完了，结果是 X，你可以把我注销并回收资源了”
    private final OnCompletionActions jobCompletionActions;

    //致命错误熔断器。处理 JVM 级别的灾难性错误（如 OutOfMemoryError 堆溢出、未捕获的线程死锁或极端的 RPC 系统崩溃）。
    // 一旦触发，它会直接让整个 JobMaster 崩溃退出或让进程自杀，从而依赖外部（如 K8s/Yarn）的容器自愈机制重新拉起，避免僵尸进程卡死
    private final FatalErrorHandler fatalErrorHandler;

    //用户代码类加载器（通常是 AppClassLoader 或 Flink 自定义的倒置类加载器 ChildFirstClassLoader）。JobMaster 在反序列化用户提交的自定义 DataStream 算子、UDF（用户自定义函数）、或者解析 ExecutionPlan 中的具体配置时，
    // 必须使用这个特定的加载器，以防止用户依赖的第三方 JAR 包与 Flink 框架自身的系统 JAR 包发生类冲突（ClassCastException/NoClassDefFoundError）
    private final ClassLoader userCodeLoader;

    //Slot 资源池服务。作业运行所需的所有 Slot 都由它来统一管理。它充当作业内部的“资源账本”，记录当前作业向外申请到了多少 Slot、哪些被占用了、哪些正处于空闲
    private final SlotPoolService slotPoolService;

    //JobMaster 实例化的时间戳（毫秒）。记录该 JobMaster 对象在内存中被 new 出来的精确物理时间点
    private final long initializationTimestamp;

    //作用：控制是否需要反向解析 TaskManager 的主机名。设计意图：一个开关参数（对应配置项）。
    // 当 TaskManager 向 JobMaster 提供 Slot 时，如果此开关为 true，JobMaster 会尝试通过网络解析将 IP 地址转换为 Domain/HostName（主机名）。
    // 在某些没有配置内网 DNS 或容器内网络环境复杂的集群（如某些 K8s 部署）中，反向解析可能导致严重的网络超时卡顿，此时将其设为 false 可以直接使用 IP 从而跳过解析
    private final boolean retrieveTaskManagerHostName;

    // --------- ResourceManager --------
    //用于动态监听并获取当前集群中最新的 Leader ResourceManager 的 RPC 地址
    private final LeaderRetrievalService resourceManagerLeaderRetriever;

    // --------- TaskManagers --------
    //只有在 registeredTaskManagers 列表里的 TaskManager，它的 Slot 才能被该作业正式部署 Task。
    // 当某个 TaskManager 带着 Slot 来向作业报到（offerSlots）并握手成功后，它的身份元数据（TaskManagerRegistration）就会被记录在这里；
    // 一旦心跳断开，则会从该 Map 中移除，实现了单个作业视角下的物理节点健康状态追踪
    private final Map<ResourceID, TaskManagerRegistration> registeredTaskManagers;

    //shuffleMaster 负责在 Master 端 为作业的所有中间结果分区（IntermediateResultPartition）申请、分配、注册和销毁物理存储资源。
    // 当作业结束或需要清理中间数据时，JobMaster 会通过它通知底层的物理混洗大盘释放网络和磁盘缓存
    private final ShuffleMaster<?> shuffleMaster;

    // --------- Scheduler --------
    //下一代调度器引擎（如 AdaptiveScheduler 或 DefaultScheduler）。
    // JobMaster 本身不直接做调度算法，而是委托给它。它负责决定何时启动 Task、按什么顺序调度，以及在发生 Task 失败时如何触发局部或全局重启策略
    private final SchedulerNG schedulerNG;

    //当 schedulerNG 驱动作业的状态发生切换时（例如从 CREATED \(\rightarrow \) RUNNING，或者 RUNNING \(\rightarrow \) FAILING），该监听器会第一时间捕获事件。
    // 它负责将状态变更同步传导给外部组件（如通知 Dispatcher 更新全局大盘、触发 JobResultStore 的持久化写入，或通知 ExecutionGraphCache 清空 Web UI 缓存）
    private final JobManagerJobStatusListener jobStatusListener;

    //作用：当前作业在 Master 端的指标监控根分组。设计意图：这是 Flink 强大的 Metric 系统的锚点。通过这个组，
    // JobMaster 可以向外界暴露和注册当前作业特有的各项核心监控指标，例如：作业当前的重启次数（numRestarts）、作业运行总时间、当前处于各种状态的任务数量（Running Tasks, Failed Tasks）、以及 Checkpoint 的耗时与成功率。
    // 我们在 Flink Web UI 页面上看到的各类实时折线图和计数器，底层数据源全部由它在收集和输出。
    private final JobManagerJobMetricGroup jobManagerJobMetricGroup;

    // -------- Misc ---------
    //作用：分布式用户累加器在 Master 端的中央汇聚表。设计意图：当用户在代码中使用 LongCounter 或自定义 Accumulator 时，各个 TaskManager 子任务在运行期间会不断累加本地数据。当 Task 结束、触发 Checkpoint 或定期发送 RPC 状态汇报时，会把结果捎带回 JobMaster。
    // JobMaster 通过这个 Map 进行全局合并（Merge），供 Web UI（对应 SubtasksAllAccumulatorsHandler）或在作业结束后供 Client 读取
    private final Map<String, Object> accumulators;

    //作用：中间数据分区（Shuffle 数据）生命周期追踪器。设计意图：负责记录作业运行过程中，哪些 TaskManager 产生了哪些物理数据块（ResultPartition），
    // 尤其是流处理的动态交换数据以及批处理产生的临时落盘分区。
    // 当 Task 因故障失败时，partitionTracker 决定哪些数据分区依然有效可读，哪些分区已经损坏需要通知下游重新拉起上游重算
    private final JobMasterPartitionTracker partitionTracker;

    //Task 部署状态“追踪器” 实时记录和跟踪每个 Task 当前被真正部署到了哪个具体的 TaskManager 上
    private final ExecutionDeploymentTracker executionDeploymentTracker;
    //则是一个对账器。当 JobMaster 与某个 TaskManager 建立或恢复连接时，对账器会强制两边核对账本：“我（Master）认为有 3 个 Task 在你那跑，
    // 你（TM）看看对不对”。如果对不上（比如 TM 侧其实已经 OOM 重启了，或者 TM 上多出了僵尸 Task），对账器会下发清理或纠正指令
    private final ExecutionDeploymentReconciler executionDeploymentReconciler;

    //当作业发生崩溃抛出复杂的 StackTrace（堆栈异常）时，普通的日志很难看懂。这些富化器插件会介入，对异常文本进行捕获和深度分析（例如判断是由于 HDFS 网卡打满导致的超时，还是由于用户 UDF 报了空指针），
    // 并将分析后的友好标签和诊断分类注入到异常信息中，最终呈现在 Web UI 上（对应 JobExceptionsHandler）
    private final Collection<FailureEnricher> failureEnrichers;

    // -------- Mutable fields ---------

    @Nullable private ResourceManagerAddress resourceManagerAddress;

    @Nullable private ResourceManagerConnection resourceManagerConnection;

    @Nullable private EstablishedResourceManagerConnection establishedResourceManagerConnection;


    private HeartbeatManager<TaskExecutorToJobManagerHeartbeatPayload, AllocatedSlotReport> taskManagerHeartbeatManager;

    private HeartbeatManager<Void, Void> resourceManagerHeartbeatManager;

    //集群黑名单熔断处理器 在云原生或大规模混合部署环境中，经常会出现“坏节点（Bad Node）”——机器没挂，但磁盘坏了或网络极慢，
    // 导致调度到它上面的 Task 屡屡失败（黑洞效应）。blocklistHandler 会统计 Task 的失败率，
    // 一旦发现某个 TaskManager 表现异常，会将其拉入黑名单，在接下来的调度中禁止把任务分给它，从而保障整个作业的整体产出率
    private final BlocklistHandler blocklistHandler;

    //作用：带有监控指标的数据分区缓存映射表。设计意图：专门用于高频缓存和监控混洗（Shuffle）过程中的物理分区指标。
    // 例如记录每个数据分区当前的网络吞吐、积压的数据量（Backlog）大小等。
    // Web UI 在渲染背压图或 Shuffle 数据大盘时，会通过 RPC 访问这个 Map，避免每次都实时计算，极大提高了监控数据的检索效率
    private final Map<ResultPartitionID, PartitionWithMetrics> fetchedPartitionsWithMetrics = new HashMap<>();

    /**
     * A flag that indicates whether to fetch and retain partitions on task managers. This will
     * apply to future TaskManager registrations as well as already registered TaskManagers. The
     * flag will be set to true when starting batch job recovery and set to false after all required
     * partitions, as defined in {@code requireToFetchPartitions}, are either fetched or when a
     * timeout occurs.
     */
    //作用：是否开启“获取并保留分区指标”的控制开关（状态标记）。设计意图：这是一个动态的布尔标记。当没有外部监控请求（如 Web UI 对应页面未打开）时，该值为 false，JobMaster 不会主动去高频维护复杂的分区指标，节省 CPU 与网络带宽。
    // 一旦 Web UI 发起请求或者内部调度器（如自适应调度、预测执行）需要评估数据积压时，该值会被置为 true，激活接下来的指标抓取流水线
    private boolean fetchAndRetainPartitions = false;

    //作用：待抓取的目标分区 ID 任务集合。设计意图：一个内存 HashSet，记录了当前明确需要获取最新指标的物理数据分区清单。因为一个分布式作业可能会产生千万级的数据分区，全量抓取不切实际。该属性精准过滤出当前处于活跃状态、或下游正在等待消费的、或前端页面正在请求的 ResultPartitionID。
    // JobMaster 会拿着这个精确的清单，定向去对应的 TaskManager 发起 RPC 抓取，实现按需拉取（Lazy/On-Demand Fetch）
    private Set<ResultPartitionID> partitionsToFetch;

    //异步抓取分区指标的“期约通道”（Future 凭证）。设计意图：由于向多台远端 TaskManager 请求分区指标是一个耗时的网络 I/O 过程，JobMaster 绝不能原地阻塞等待。当抓取任务启动时，JobMaster 会先实例化这个 fetchPartitionsFuture 并直接返回给调用方（例如正在等待响应的 REST Handler）。随后，底层的网络线程池会在后台异步收集各个 TaskManager 返回的 PartitionWithMetrics（带有吞吐、反压、积压量等指标的分区实体）。当清单 partitionsToFetch 中的所有分区指标全部收集齐备并写入缓存后，该 Future 会被调用 complete(...)。此时，
    // 挂起等待的 REST 请求会被立刻唤醒，将数据序列化为 JSON 吐给 Web UI，整个过程实现了完全的非阻塞事件驱动（Event-Driven）
    private CompletableFuture<Collection<PartitionWithMetrics>> fetchPartitionsFuture;

    // ------------------------------------------------------------------------

    public JobMaster(
            RpcService rpcService,
            JobMasterId jobMasterId,
            JobMasterConfiguration jobMasterConfiguration,
            ResourceID resourceId,
            ExecutionPlan executionPlan,
            HighAvailabilityServices highAvailabilityService,
            SlotPoolServiceSchedulerFactory slotPoolServiceSchedulerFactory,
            JobManagerSharedServices jobManagerSharedServices,
            HeartbeatServices heartbeatServices,
            JobManagerJobMetricGroupFactory jobMetricGroupFactory,
            OnCompletionActions jobCompletionActions,
            FatalErrorHandler fatalErrorHandler,
            ClassLoader userCodeLoader,
            ShuffleMaster<?> shuffleMaster,
            PartitionTrackerFactory partitionTrackerFactory,
            ExecutionDeploymentTracker executionDeploymentTracker,
            ExecutionDeploymentReconciler.Factory executionDeploymentReconcilerFactory,
            BlocklistHandler.Factory blocklistHandlerFactory,
            Collection<FailureEnricher> failureEnrichers,
            long initializationTimestamp)
            throws Exception {

        super(
                rpcService,
                RpcServiceUtils.createRandomName(JOB_MANAGER_NAME),
                jobMasterId,
                MdcUtils.asContextData(executionPlan.getJobID()));

        final ExecutionDeploymentReconciliationHandler executionStateReconciliationHandler =
                new ExecutionDeploymentReconciliationHandler() {

                    @Override
                    public void onMissingDeploymentsOf(
                            Collection<ExecutionAttemptID> executionAttemptIds, ResourceID host) {
                        log.debug(
                                "Failing deployments {} due to no longer being deployed.",
                                executionAttemptIds);
                        for (ExecutionAttemptID executionAttemptId : executionAttemptIds) {
                            schedulerNG.updateTaskExecutionState(
                                    new TaskExecutionState(
                                            executionAttemptId,
                                            ExecutionState.FAILED,
                                            new FlinkException(
                                                    String.format(
                                                            "Execution %s is unexpectedly no longer running on task executor %s.",
                                                            executionAttemptId, host))));
                        }
                    }

                    @Override
                    public void onUnknownDeploymentsOf(
                            Collection<ExecutionAttemptID> executionAttemptIds, ResourceID host) {
                        log.debug(
                                "Canceling left-over deployments {} on task executor {}.",
                                executionAttemptIds,
                                host);
                        for (ExecutionAttemptID executionAttemptId : executionAttemptIds) {
                            TaskManagerRegistration taskManagerRegistration =
                                    registeredTaskManagers.get(host);
                            if (taskManagerRegistration != null) {
                                taskManagerRegistration
                                        .getTaskExecutorGateway()
                                        .cancelTask(executionAttemptId, rpcTimeout);
                            }
                        }
                    }
                };
        final String jobName = executionPlan.getName();
        final JobID jid = executionPlan.getJobID();

        this.executionPlan = checkNotNull(executionPlan);

        log.info("Initializing job '{}' ({}).", jobName, jid);

        this.executionDeploymentTracker = executionDeploymentTracker;
        this.executionDeploymentReconciler =
                executionDeploymentReconcilerFactory.create(executionStateReconciliationHandler);

        this.jobMasterConfiguration = checkNotNull(jobMasterConfiguration);
        this.resourceId = checkNotNull(resourceId);
        this.rpcTimeout = jobMasterConfiguration.getRpcTimeout();
        this.highAvailabilityServices = checkNotNull(highAvailabilityService);
        this.blobWriter = jobManagerSharedServices.getBlobWriter();
        this.futureExecutor =
                MdcUtils.scopeToJob(jid, jobManagerSharedServices.getFutureExecutor());
        this.ioExecutor = MdcUtils.scopeToJob(jid, jobManagerSharedServices.getIoExecutor());
        this.jobCompletionActions = checkNotNull(jobCompletionActions);
        this.fatalErrorHandler = checkNotNull(fatalErrorHandler);
        this.userCodeLoader = checkNotNull(userCodeLoader);
        this.initializationTimestamp = initializationTimestamp;
        this.retrieveTaskManagerHostName =
                jobMasterConfiguration
                        .getConfiguration()
                        .get(JobManagerOptions.RETRIEVE_TASK_MANAGER_HOSTNAME);

        resourceManagerLeaderRetriever =
                highAvailabilityServices.getResourceManagerLeaderRetriever();

        this.registeredTaskManagers = new HashMap<>();
        this.blocklistHandler =
                blocklistHandlerFactory.create(
                        new JobMasterBlocklistContext(),
                        this::getNodeIdOfTaskManager,
                        getMainThreadExecutor(),
                        log);
        //slotPoolService = DeclarativeSlotPoolBridge
        this.slotPoolService =
                checkNotNull(slotPoolServiceSchedulerFactory)
                        .createSlotPoolService(
                                jid,
                                createDeclarativeSlotPoolFactory(
                                        jobMasterConfiguration.getConfiguration()),
                                getMainThreadExecutor());

        this.partitionTracker =
                checkNotNull(partitionTrackerFactory)
                        .create(
                                resourceID -> {
                                    return Optional.ofNullable(
                                                    registeredTaskManagers.get(resourceID))
                                            .map(TaskManagerRegistration::getTaskExecutorGateway);
                                });

        this.shuffleMaster = checkNotNull(shuffleMaster);

        this.jobManagerJobMetricGroup = jobMetricGroupFactory.create(executionPlan);
        this.jobStatusListener = new JobManagerJobStatusListener();

        this.failureEnrichers = checkNotNull(failureEnrichers);

        JobStatusListener schedulerListener =
                JobStatusListener.combine(
                        jobStatusListener,
                        (jobId, newJobStatus, timestamp) ->
                                jobManagerJobMetricGroup.addEvent(
                                        Events.JobStatusChangeEvent.builder(JobMaster.class)
                                                .setObservedTsMillis(timestamp)
                                                .setSeverity("INFO")
                                                .setAttribute(
                                                        "newJobStatus", newJobStatus.name())));
        // DefaultScheduler 【重点】
        this.schedulerNG = createScheduler(//
                        slotPoolServiceSchedulerFactory,
                        executionDeploymentTracker,
                        jobManagerJobMetricGroup,
                        schedulerListener);

        this.heartbeatServices = checkNotNull(heartbeatServices);
        this.taskManagerHeartbeatManager = NoOpHeartbeatManager.getInstance();
        this.resourceManagerHeartbeatManager = NoOpHeartbeatManager.getInstance();

        this.resourceManagerConnection = null;
        this.establishedResourceManagerConnection = null;

        this.accumulators = new HashMap<>();
    }

    private SchedulerNG createScheduler(
            SlotPoolServiceSchedulerFactory slotPoolServiceSchedulerFactory,
            ExecutionDeploymentTracker executionDeploymentTracker,
            JobManagerJobMetricGroup jobManagerJobMetricGroup,
            JobStatusListener jobStatusListener)
            throws Exception {
        //DefaultScheduler  如果 executionPlan instanceof StreamGraph 会生成JobGraph
        final SchedulerNG scheduler = slotPoolServiceSchedulerFactory.createScheduler(//
                        log,
                        executionPlan,
                        ioExecutor,
                        jobMasterConfiguration.getConfiguration(),
                        slotPoolService,
                        futureExecutor,
                        userCodeLoader,
                        highAvailabilityServices.getCheckpointRecoveryFactory(),
                        rpcTimeout,
                        blobWriter,
                        jobManagerJobMetricGroup,
                        jobMasterConfiguration.getSlotRequestTimeout(),
                        shuffleMaster,
                        partitionTracker,
                        executionDeploymentTracker,
                        initializationTimestamp,
                        getMainThreadExecutor(),
                        fatalErrorHandler,
                        jobStatusListener,
                        failureEnrichers,
                        blocklistHandler::addNewBlockedNodes);

        return scheduler;
    }

    private HeartbeatManager<Void, Void> createResourceManagerHeartbeatManager(
            HeartbeatServices heartbeatServices) {
        return heartbeatServices.createHeartbeatManager(
                resourceId, new ResourceManagerHeartbeatListener(), getMainThreadExecutor(), log);
    }

    private HeartbeatManager<TaskExecutorToJobManagerHeartbeatPayload, AllocatedSlotReport>
            createTaskManagerHeartbeatManager(HeartbeatServices heartbeatServices) {
        return heartbeatServices.createHeartbeatManagerSender(
                resourceId, new TaskManagerHeartbeatListener(), getMainThreadExecutor(), log);
    }

    private DeclarativeSlotPoolFactory createDeclarativeSlotPoolFactory(
            Configuration configuration) {
        if (BlocklistUtils.isBlocklistEnabled(configuration)) {
            return new BlocklistDeclarativeSlotPoolFactory(blocklistHandler::isBlockedTaskManager);
        } else {
            return new DefaultDeclarativeSlotPoolFactory();
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Lifecycle management
    // ----------------------------------------------------------------------------------------------

    @Override
    protected void onStart() throws JobMasterException {
        try {
            //开始作业的调度
            startJobExecution();
        } catch (Exception e) {
            final JobMasterException jobMasterException =
                    new JobMasterException("Could not start the JobMaster.", e);
            handleJobMasterError(jobMasterException);
            throw jobMasterException;
        }
    }

    /** Suspend the job and shutdown all other services including rpc. */
    @Override
    public CompletableFuture<Void> onStop() {
        try (MdcUtils.MdcCloseable ignored =
                MdcUtils.withContext(MdcUtils.asContextData(executionPlan.getJobID()))) {
            log.info(
                    "Stopping the JobMaster for job '{}' ({}).",
                    executionPlan.getName(),
                    executionPlan.getJobID());

            // make sure there is a graceful exit
            return stopJobExecution(
                            new FlinkException(
                                    String.format(
                                            "Stopping JobMaster for job '%s' (%s).",
                                            executionPlan.getName(), executionPlan.getJobID())))
                    .exceptionally(
                            exception -> {
                                throw new CompletionException(
                                        new JobMasterException(
                                                "Could not properly stop the JobMaster.",
                                                exception));
                            });
        }
    }

    // ----------------------------------------------------------------------------------------------
    // RPC methods
    // ----------------------------------------------------------------------------------------------

    @Override
    public CompletableFuture<Acknowledge> cancel(Duration timeout) {
        schedulerNG.cancel();

        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    /**
     * Updates the task execution state for a given task.
     *
     * @param taskExecutionState New task execution state for a given task
     * @return Acknowledge the task execution state update
     */
    //接收 Task 状态变更的 RPC 汇报入口。当远端 TaskManager 上的某个 Subtask 状态发生改变（例如从 DEPLOYING 变成 RUNNING，或者发生异常变成 FAILED），
    // TaskManager 会通过此 RPC 接口向 JobMaster 报告。JobMaster 收到后会更新 executionGraph 并触发相应的后续逻辑（如推进下游或启动 Failover）
    @Override
    public CompletableFuture<Acknowledge> updateTaskExecutionState(
            final TaskExecutionState taskExecutionState) {
        FlinkException taskExecutionException;
        try {
            checkNotNull(taskExecutionState, "taskExecutionState");

            if (schedulerNG.updateTaskExecutionState(taskExecutionState)) {
                return CompletableFuture.completedFuture(Acknowledge.get());
            } else {
                taskExecutionException =
                        new ExecutionGraphException(
                                "The execution attempt "
                                        + taskExecutionState.getID()
                                        + " was not found.");
            }
        } catch (Exception e) {
            taskExecutionException =
                    new JobMasterException(
                            "Could not update the state of task execution for JobMaster.", e);
            handleJobMasterError(taskExecutionException);
        }
        return FutureUtils.completedExceptionally(taskExecutionException);
    }

    @Override
    public void notifyEndOfData(final ExecutionAttemptID executionAttempt) {
        schedulerNG.notifyEndOfData(executionAttempt);
    }

    @Override
    public CompletableFuture<SerializedInputSplit> requestNextInputSplit(
            final JobVertexID vertexID, final ExecutionAttemptID executionAttempt) {

        try {
            return CompletableFuture.completedFuture(
                    schedulerNG.requestNextInputSplit(vertexID, executionAttempt));
        } catch (IOException e) {
            log.warn("Error while requesting next input split", e);
            return FutureUtils.completedExceptionally(e);
        }
    }

    @Override
    public CompletableFuture<ExecutionState> requestPartitionState(
            final IntermediateDataSetID intermediateResultId,
            final ResultPartitionID resultPartitionId) {

        try {
            return CompletableFuture.completedFuture(
                    schedulerNG.requestPartitionState(intermediateResultId, resultPartitionId));
        } catch (PartitionProducerDisposedException e) {
            log.info("Error while requesting partition state", e);
            return FutureUtils.completedExceptionally(e);
        }
    }

    @Override
    public CompletableFuture<Acknowledge> disconnectTaskManager(
            final ResourceID resourceID, final Exception cause) {

        taskManagerHeartbeatManager.unmonitorTarget(resourceID);
        slotPoolService.releaseTaskManager(resourceID, cause);
        partitionTracker.stopTrackingPartitionsFor(resourceID);

        TaskManagerRegistration taskManagerRegistration = registeredTaskManagers.remove(resourceID);

        if (taskManagerRegistration != null) {
            log.info(
                    "Disconnect TaskExecutor {} because: {}",
                    resourceID.getStringWithMetadata(),
                    cause.getMessage(),
                    ExceptionUtils.returnExceptionIfUnexpected(cause.getCause()));
            ExceptionUtils.logExceptionIfExcepted(cause.getCause(), log);

            taskManagerRegistration
                    .getTaskExecutorGateway()
                    .disconnectJobManager(executionPlan.getJobID(), cause);
        }

        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    // TODO: This method needs a leader session ID
    //TaskExecutor（算子任务所在的节点）向 JobMaster 汇报 Checkpoint 成功完成 的核心入口
    @Override
    public void acknowledgeCheckpoint(
            final JobID jobID,
            final ExecutionAttemptID executionAttemptID,
            final long checkpointId,
            final CheckpointMetrics checkpointMetrics,
            @Nullable final SerializedValue<TaskStateSnapshot> checkpointState) {
        System.out.println(checkpointId + ",acknowledgeCheckpoint:" + executionAttemptID);
        try (MdcUtils.MdcCloseable ignored = MdcUtils.withContext(MdcUtils.asContextData(jobID))) {
            //SchedulerBase#acknowledgeCheckpoint
            schedulerNG.acknowledgeCheckpoint(//
                    jobID,
                    executionAttemptID,
                    checkpointId,
                    checkpointMetrics,
                    //反序列化 TaskStateSnapshot
                    deserializeTaskStateSnapshot(checkpointState, getClass().getClassLoader()));
        }
    }

    @Override
    public void reportCheckpointMetrics(
            JobID jobID,
            ExecutionAttemptID executionAttemptID,
            long checkpointId,
            CheckpointMetrics checkpointMetrics) {

        schedulerNG.reportCheckpointMetrics(
                jobID, executionAttemptID, checkpointId, checkpointMetrics);
    }

    @Override
    public void reportInitializationMetrics(
            JobID jobId,
            ExecutionAttemptID executionAttemptId,
            SubTaskInitializationMetrics initializationMetrics) {
        schedulerNG.reportInitializationMetrics(jobId, executionAttemptId, initializationMetrics);
    }

    // TODO: This method needs a leader session ID
    @Override
    public void declineCheckpoint(DeclineCheckpoint decline) {
        schedulerNG.declineCheckpoint(decline);
    }

    @Override
    public CompletableFuture<Acknowledge> sendOperatorEventToCoordinator(
            final ExecutionAttemptID task,
            final OperatorID operatorID,
            final SerializedValue<OperatorEvent> serializedEvent) {

        try {
            final OperatorEvent evt = serializedEvent.deserializeValue(userCodeLoader);
            schedulerNG.deliverOperatorEventToCoordinator(task, operatorID, evt);
            return CompletableFuture.completedFuture(Acknowledge.get());
        } catch (Exception e) {
            return FutureUtils.completedExceptionally(e);
        }
    }

    @Override
    public CompletableFuture<CoordinationResponse> sendRequestToCoordinator(
            OperatorID operatorID, SerializedValue<CoordinationRequest> serializedRequest) {
        try {
            final CoordinationRequest request = serializedRequest.deserializeValue(userCodeLoader);
            return schedulerNG.deliverCoordinationRequestToCoordinator(operatorID, request);
        } catch (Exception e) {
            return FutureUtils.completedExceptionally(e);
        }
    }

    @Override
    public CompletableFuture<KvStateLocation> requestKvStateLocation(
            final JobID jobId, final String registrationName) {
        try {
            return CompletableFuture.completedFuture(
                    schedulerNG.requestKvStateLocation(jobId, registrationName));
        } catch (UnknownKvStateLocation | FlinkJobNotFoundException e) {
            log.info("Error while request key-value state location", e);
            return FutureUtils.completedExceptionally(e);
        }
    }

    @Override
    public CompletableFuture<Acknowledge> notifyKvStateRegistered(
            final JobID jobId,
            final JobVertexID jobVertexId,
            final KeyGroupRange keyGroupRange,
            final String registrationName,
            final KvStateID kvStateId,
            final InetSocketAddress kvStateServerAddress) {

        try {
            schedulerNG.notifyKvStateRegistered(
                    jobId,
                    jobVertexId,
                    keyGroupRange,
                    registrationName,
                    kvStateId,
                    kvStateServerAddress);
            return CompletableFuture.completedFuture(Acknowledge.get());
        } catch (FlinkJobNotFoundException e) {
            log.info("Error while receiving notification about key-value state registration", e);
            return FutureUtils.completedExceptionally(e);
        }
    }

    @Override
    public CompletableFuture<Acknowledge> notifyKvStateUnregistered(
            JobID jobId,
            JobVertexID jobVertexId,
            KeyGroupRange keyGroupRange,
            String registrationName) {
        try {
            schedulerNG.notifyKvStateUnregistered(
                    jobId, jobVertexId, keyGroupRange, registrationName);
            return CompletableFuture.completedFuture(Acknowledge.get());
        } catch (FlinkJobNotFoundException e) {
            log.info("Error while receiving notification about key-value state de-registration", e);
            return FutureUtils.completedExceptionally(e);
        }
    }


    // 处理 TaskManager 分配给 JobMaster slot的请求
    // JobMaster 会将其放入 slotPoolService 中供调度器分配给具体的 Task
    @Override
    public CompletableFuture<Collection<SlotOffer>> offerSlots(
            final ResourceID taskManagerId,
            final Collection<SlotOffer> slots,
            final Duration timeout) {
        //获取TaskManager 注册信息
        TaskManagerRegistration taskManagerRegistration = registeredTaskManagers.get(taskManagerId);

        if (taskManagerRegistration == null) {
            return FutureUtils.completedExceptionally(
                    new Exception("Unknown TaskManager " + taskManagerId));
        }
        //获取TaskManager 网关
        final RpcTaskManagerGateway rpcTaskManagerGateway =
                new RpcTaskManagerGateway(taskManagerRegistration.getTaskExecutorGateway(), getFencingToken());
        //DeclarativeSlotPoolBridge#offerSlots
        Collection<SlotOffer> slotOffers = slotPoolService.offerSlots(//
                taskManagerRegistration.getTaskManagerLocation(),
                rpcTaskManagerGateway,
                slots);
        return CompletableFuture.completedFuture(slotOffers);
    }

    @Override
    public void failSlot(
            final ResourceID taskManagerId,
            final AllocationID allocationId,
            final Exception cause) {

        if (registeredTaskManagers.containsKey(taskManagerId)) {
            internalFailAllocation(taskManagerId, allocationId, cause);
        } else {
            log.warn(
                    "Cannot fail slot "
                            + allocationId
                            + " because the TaskManager "
                            + taskManagerId
                            + " is unknown.");
        }
    }

    private void internalFailAllocation(
            @Nullable ResourceID resourceId, AllocationID allocationId, Exception cause) {
        final Optional<ResourceID> resourceIdOptional =
                slotPoolService.failAllocation(resourceId, allocationId, cause);
        resourceIdOptional.ifPresent(
                taskManagerId -> {
                    if (!partitionTracker.isTrackingPartitionsFor(taskManagerId)) {
                        releaseEmptyTaskManager(taskManagerId);
                    }
                });
    }

    private void releaseEmptyTaskManager(ResourceID resourceId) {
        disconnectTaskManager(
                resourceId,
                new FlinkException(
                        String.format(
                                "No more slots registered at JobMaster %s.",
                                resourceId.getStringWithMetadata())));
    }

    @Override
    public CompletableFuture<RegistrationResponse> registerTaskManager(
            final JobID jobId,
            final TaskManagerRegistrationInformation taskManagerRegistrationInformation,
            final Duration timeout) {

        if (!executionPlan.getJobID().equals(jobId)) {
            log.debug(
                    "Rejecting TaskManager registration attempt because of wrong job id {}.",
                    jobId);
            return CompletableFuture.completedFuture(
                    new JMTMRegistrationRejection(
                            String.format(
                                    "The JobManager is not responsible for job %s. Maybe the TaskManager used outdated connection information.",
                                    jobId)));
        }

        final TaskManagerLocation taskManagerLocation;
        try {
            taskManagerLocation =
                    resolveTaskManagerLocation(
                            taskManagerRegistrationInformation.getUnresolvedTaskManagerLocation());
        } catch (FlinkException exception) {
            log.error("Could not accept TaskManager registration.", exception);
            return CompletableFuture.completedFuture(new RegistrationResponse.Failure(exception));
        }

        final ResourceID taskManagerId = taskManagerLocation.getResourceID();
        final UUID sessionId = taskManagerRegistrationInformation.getTaskManagerSession();
        final TaskManagerRegistration taskManagerRegistration =
                registeredTaskManagers.get(taskManagerId);

        if (taskManagerRegistration != null) {
            if (taskManagerRegistration.getSessionId().equals(sessionId)) {
                log.debug(
                        "Ignoring registration attempt of TaskManager {} with the same session id {}.",
                        taskManagerId,
                        sessionId);
                final RegistrationResponse response = new JMTMRegistrationSuccess(resourceId);
                return CompletableFuture.completedFuture(response);
            } else {
                disconnectTaskManager(
                        taskManagerId,
                        new FlinkException(
                                String.format(
                                        "A registered TaskManager %s re-registered with a new session id. This indicates a restart of the TaskManager. Closing the old connection.",
                                        taskManagerId)));
            }
        }

        CompletableFuture<RegistrationResponse> registrationResponseFuture =
                getRpcService()
                        .connect(
                                taskManagerRegistrationInformation.getTaskManagerRpcAddress(),
                                TaskExecutorGateway.class)
                        .handleAsync(
                                (TaskExecutorGateway taskExecutorGateway, Throwable throwable) -> {
                                    if (throwable != null) {
                                        return new RegistrationResponse.Failure(throwable);
                                    }
                                    //DeclarativeSlotPoolService#registerTaskManager
                                    slotPoolService.registerTaskManager(taskManagerId);
                                    registeredTaskManagers.put(
                                            taskManagerId,
                                            TaskManagerRegistration.create(
                                                    taskManagerLocation,
                                                    taskExecutorGateway,
                                                    sessionId));

                                    // monitor the task manager as heartbeat target
                                    taskManagerHeartbeatManager.monitorTarget(
                                            taskManagerId,
                                            new TaskExecutorHeartbeatSender(taskExecutorGateway));

                                    return new JMTMRegistrationSuccess(resourceId);
                                },
                                getMainThreadExecutor());

        if (fetchAndRetainPartitions) {
            registrationResponseFuture.whenComplete(
                    (ignored, throwable) ->
                            fetchAndRetainPartitionWithMetricsOnTaskManager(taskManagerId));
        }

        return registrationResponseFuture;
    }

    @Nonnull
    private TaskManagerLocation resolveTaskManagerLocation(
            UnresolvedTaskManagerLocation unresolvedTaskManagerLocation) throws FlinkException {
        try {
            if (retrieveTaskManagerHostName) {
                return TaskManagerLocation.fromUnresolvedLocation(
                        unresolvedTaskManagerLocation, ResolutionMode.RETRIEVE_HOST_NAME);
            } else {
                return TaskManagerLocation.fromUnresolvedLocation(
                        unresolvedTaskManagerLocation, ResolutionMode.USE_IP_ONLY);
            }
        } catch (Throwable throwable) {
            final String errMsg =
                    String.format(
                            "TaskManager address %s cannot be resolved. %s",
                            unresolvedTaskManagerLocation.getExternalAddress(),
                            throwable.getMessage());
            throw new FlinkException(errMsg, throwable);
        }
    }

    @Override
    public void disconnectResourceManager(
            final ResourceManagerId resourceManagerId, final Exception cause) {

        if (resourceManagerAddress == null) {
            log.debug(
                    "Disconnecting ResourceManager {} was triggered with no ResourceManager address "
                            + "being set (anymore). That either indicates that the ResourceManager "
                            + "lost leadership in the mean time or the message was received while "
                            + "shutting down the JobMaster. No reconnect will be initiated.",
                    resourceManagerId);
        } else if (!resourceManagerAddress.getResourceManagerId().equals(resourceManagerId)) {
            log.debug(
                    "Disconnecting ResourceManager {} was received while this instance is currently "
                            + "connected to another ResourceManager {} indicating that a ResourceManager "
                            + "leader change happened. No reconnect will be initiated.",
                    resourceManagerId,
                    resourceManagerAddress.getResourceManagerId());
        } else {
            reconnectToResourceManager(cause);
        }
    }

    private void shutdownResourceManagerConnection(Exception cause) {
        // unsetting the resourceManagerAddress will prevent reconnection
        resourceManagerAddress = null;
        closeResourceManagerConnection(cause);
    }

    @Override
    public CompletableFuture<Void> heartbeatFromTaskManager(
            final ResourceID resourceID, TaskExecutorToJobManagerHeartbeatPayload payload) {
        return taskManagerHeartbeatManager.receiveHeartbeat(resourceID, payload);
    }

    @Override
    public CompletableFuture<Void> heartbeatFromResourceManager(final ResourceID resourceID) {
        return resourceManagerHeartbeatManager.requestHeartbeat(resourceID, null);
    }

    @Override
    public CompletableFuture<JobStatus> requestJobStatus(Duration timeout) {
        return CompletableFuture.completedFuture(schedulerNG.requestJobStatus());
    }

    @Override
    public CompletableFuture<ExecutionGraphInfo> requestJob(Duration timeout) {
        return CompletableFuture.completedFuture(schedulerNG.requestJob());
    }

    @Override
    public CompletableFuture<CheckpointStatsSnapshot> requestCheckpointStats(Duration timeout) {
        return CompletableFuture.completedFuture(schedulerNG.requestCheckpointStats());
    }

    //外部触发 Checkpoint 的入口（例如用户在 Web UI 上点击，或者通过命令手动触发）
    @Override
    public CompletableFuture<CompletedCheckpoint> triggerCheckpoint(
            final CheckpointType checkpointType, final Duration timeout) {
        return schedulerNG.triggerCheckpoint(checkpointType);
    }

    @Override
    public CompletableFuture<String> triggerSavepoint(
            @Nullable final String targetDirectory,
            final boolean cancelJob,
            final SavepointFormatType formatType,
            final Duration timeout) {

        return schedulerNG.triggerSavepoint(targetDirectory, cancelJob, formatType);
    }

    //带 Savepoint 停止作业（对应 DELETE /jobs/:jobid/stop）。它会触发一次特殊的 Savepoint，在确保存储成功后，优雅地停止所有的 Task 并让作业正常终结
    @Override
    public CompletableFuture<String> stopWithSavepoint(
            @Nullable final String targetDirectory,
            final SavepointFormatType formatType,
            final boolean terminate,
            final Duration timeout) {

        return schedulerNG.stopWithSavepoint(targetDirectory, terminate, formatType);
    }

    @Override
    public void notifyNotEnoughResourcesAvailable(
            Collection<ResourceRequirement> acquiredResources) {
        //通知没有足够的资源
        slotPoolService.notifyNotEnoughResourcesAvailable(acquiredResources);
    }

    @Override
    public CompletableFuture<Object> updateGlobalAggregate(
            String aggregateName, Object aggregand, byte[] serializedAggregateFunction) {

        AggregateFunction aggregateFunction = null;
        try {
            aggregateFunction =
                    InstantiationUtil.deserializeObject(
                            serializedAggregateFunction, userCodeLoader);
        } catch (Exception e) {
            log.error("Error while attempting to deserialize user AggregateFunction.");
            return FutureUtils.completedExceptionally(e);
        }

        Object accumulator = accumulators.get(aggregateName);
        if (null == accumulator) {
            accumulator = aggregateFunction.createAccumulator();
        }
        accumulator = aggregateFunction.add(aggregand, accumulator);
        accumulators.put(aggregateName, accumulator);
        return CompletableFuture.completedFuture(aggregateFunction.getResult(accumulator));
    }

    @Override
    public CompletableFuture<CoordinationResponse> deliverCoordinationRequestToCoordinator(
            OperatorID operatorId,
            SerializedValue<CoordinationRequest> serializedRequest,
            Duration timeout) {
        return this.sendRequestToCoordinator(operatorId, serializedRequest);
    }

    @Override
    public CompletableFuture<?> stopTrackingAndReleasePartitions(
            Collection<ResultPartitionID> partitionIds) {
        CompletableFuture<?> future = new CompletableFuture<>();
        try {
            partitionTracker.stopTrackingAndReleasePartitions(partitionIds, false);
            future.complete(null);
        } catch (Throwable throwable) {
            future.completeExceptionally(throwable);
        }
        return future;
    }

    @Override
    public CompletableFuture<Collection<PartitionWithMetrics>> getPartitionWithMetrics(
            Duration timeout, Set<ResultPartitionID> expectedPartitions) {
        this.partitionsToFetch = expectedPartitions;
        this.fetchPartitionsFuture = new CompletableFuture<>();

        // check already fetched partitions
        checkPartitionOnTaskManagerReportFinished();

        return FutureUtils.orTimeout(
                        fetchPartitionsFuture, timeout.toMillis(), TimeUnit.MILLISECONDS, null)
                .handleAsync(
                        (metrics, throwable) -> {
                            stopFetchAndRetainPartitionWithMetricsOnTaskManager();

                            if (throwable != null) {
                                if (throwable instanceof TimeoutException) {
                                    log.warn(
                                            "Timeout occurred after {} ms "
                                                    + "while fetching partition(s) ({}) from task managers.",
                                            timeout.toMillis(),
                                            expectedPartitions);

                                    return new ArrayList<>(fetchedPartitionsWithMetrics.values());
                                }
                                throw new CompletionException(throwable);
                            }

                            return new ArrayList<>(fetchedPartitionsWithMetrics.values());
                        },
                        getMainThreadExecutor());
    }

    @VisibleForTesting
    Map<ResultPartitionID, PartitionWithMetrics> getPartitionWithMetricsOnTaskManagers() {
        return fetchedPartitionsWithMetrics;
    }

    @Override
    public void startFetchAndRetainPartitionWithMetricsOnTaskManager() {
        fetchAndRetainPartitions = true;

        // process all already registered task managers
        registeredTaskManagers
                .keySet()
                .forEach(this::fetchAndRetainPartitionWithMetricsOnTaskManager);
    }

    private void fetchAndRetainPartitionWithMetricsOnTaskManager(ResourceID resourceId) {
        TaskManagerRegistration taskManager = registeredTaskManagers.get(resourceId);
        checkNotNull(taskManager);

        taskManager
                .getTaskExecutorGateway()
                .getAndRetainPartitionWithMetrics(executionPlan.getJobID())
                .thenAccept(
                        partitionWithMetrics -> {
                            if (fetchAndRetainPartitions) {
                                for (PartitionWithMetrics partitionWithMetric :
                                        partitionWithMetrics) {
                                    log.debug(
                                            "Received partition metrics for {} from Task Manager {}.",
                                            partitionWithMetric
                                                    .getPartition()
                                                    .getResultPartitionID(),
                                            resourceId);
                                    fetchedPartitionsWithMetrics.put(
                                            partitionWithMetric
                                                    .getPartition()
                                                    .getResultPartitionID(),
                                            partitionWithMetric);
                                }
                                checkPartitionOnTaskManagerReportFinished();
                            } else {
                                log.info(
                                        "Received late report of partition metrics from {}. Release the partitions.",
                                        resourceId);

                                taskManager
                                        .getTaskExecutorGateway()
                                        .releasePartitions(
                                                executionPlan.getJobID(),
                                                partitionWithMetrics.stream()
                                                        .map(PartitionWithMetrics::getPartition)
                                                        .map(
                                                                ShuffleDescriptor
                                                                        ::getResultPartitionID)
                                                        .collect(Collectors.toSet()));
                            }
                        });
    }

    private void stopFetchAndRetainPartitionWithMetricsOnTaskManager() {
        fetchAndRetainPartitions = false;
    }

    private void checkPartitionOnTaskManagerReportFinished() {
        if (fetchPartitionsFuture != null) {
            if (fetchedPartitionsWithMetrics.keySet().containsAll(partitionsToFetch)
                    && !fetchPartitionsFuture.isDone()) {
                fetchPartitionsFuture.complete(fetchedPartitionsWithMetrics.values());
            }
        }
    }

    @Override
    public CompletableFuture<Acknowledge> notifyNewBlockedNodes(Collection<BlockedNode> newNodes) {
        blocklistHandler.addNewBlockedNodes(newNodes);
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    @Override
    public CompletableFuture<JobResourceRequirements> requestJobResourceRequirements() {
        return CompletableFuture.completedFuture(schedulerNG.requestJobResourceRequirements());
    }

    @Override
    public CompletableFuture<Acknowledge> updateJobResourceRequirements(
            JobResourceRequirements jobResourceRequirements) {
        schedulerNG.updateJobResourceRequirements(jobResourceRequirements);
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    // ----------------------------------------------------------------------------------------------
    // Internal methods
    // ----------------------------------------------------------------------------------------------

    // -- job starting and stopping
    // -----------------------------------------------------------------

    private void startJobExecution() throws Exception {
        validateRunsInMainThread();

        JobShuffleContext context = new JobShuffleContextImpl(executionPlan.getJobID(), this);
        shuffleMaster.registerJob(context);
        //启动几个服务
        startJobMasterServices();//

        log.info(
                "Starting execution of job '{}' ({}) under job master id {}.",
                executionPlan.getName(),
                executionPlan.getJobID(),
                getFencingToken());
        //【重点】 会调度作业的执行
        startScheduling();
    }

    private void startJobMasterServices() throws Exception {
        try {
            // 创建跟TaskManager心跳的管理器
            this.taskManagerHeartbeatManager = createTaskManagerHeartbeatManager(heartbeatServices);
            // 创建跟ResourceManager心跳的管理器
            this.resourceManagerHeartbeatManager = createResourceManagerHeartbeatManager(heartbeatServices);

            // start the slot pool make sure the slot pool now accepts messages for this leader
            // 启动slotPoolService  DeclarativeSlotPoolService#start
            slotPoolService.start(getFencingToken(), getAddress());

            // job is ready to go, try to establish connection with resource manager
            //   - activate leader retrieval for the resource manager
            //   - on notification of the leader, the connection will be established and
            //     the slot pool will start requesting slots
            //todo
            // 启动获取resourceManager leader地址的服务
            // 监听器获取到地址了就会跟resourceManager 连接连接 也会申请所需要的slot资源
            resourceManagerLeaderRetriever.start(new ResourceManagerLeaderListener());
        } catch (Exception e) {
            handleStartJobMasterServicesError(e);
        }
    }

    private void handleStartJobMasterServicesError(Exception e) throws Exception {
        try {
            stopJobMasterServices();
        } catch (Exception inner) {
            e.addSuppressed(inner);
        }

        throw e;
    }

    private void stopJobMasterServices() throws Exception {
        Exception resultingException = null;

        try {
            resourceManagerLeaderRetriever.stop();
        } catch (Exception e) {
            resultingException = e;
        }

        // TODO: Distinguish between job termination which should free all slots and a loss of
        // leadership which should keep the slots
        slotPoolService.close();

        stopHeartbeatServices();

        ExceptionUtils.tryRethrowException(resultingException);
    }

    private CompletableFuture<Void> stopJobExecution(final Exception cause) {
        validateRunsInMainThread();

        final CompletableFuture<Void> terminationFuture = stopScheduling();

        return FutureUtils.runAfterwardsAsync(
                terminationFuture,
                () -> {
                    try (MdcUtils.MdcCloseable ignored =
                            MdcUtils.withContext(
                                    MdcUtils.asContextData(executionPlan.getJobID()))) {
                        shuffleMaster.unregisterJob(executionPlan.getJobID());
                        disconnectTaskManagerResourceManagerConnections(cause);
                        stopJobMasterServices();
                    }
                },
                getMainThreadExecutor());
    }

    private void disconnectTaskManagerResourceManagerConnections(Exception cause) {
        // disconnect from all registered TaskExecutors
        final Set<ResourceID> taskManagerResourceIds =
                new HashSet<>(registeredTaskManagers.keySet());

        for (ResourceID taskManagerResourceId : taskManagerResourceIds) {
            disconnectTaskManager(taskManagerResourceId, cause);
        }

        shutdownResourceManagerConnection(cause);
    }

    private void stopHeartbeatServices() {
        taskManagerHeartbeatManager.stop();
        resourceManagerHeartbeatManager.stop();
    }

    private void startScheduling() {
        // DefaultScheduler#startScheduling  SchedulerBase#startScheduling
        schedulerNG.startScheduling();
    }

    private CompletableFuture<Void> stopScheduling() {
        jobManagerJobMetricGroup.close();
        jobStatusListener.stop();

        return schedulerNG.closeAsync();
    }

    // ----------------------------------------------------------------------------------------------

    private void handleJobMasterError(final Throwable cause) {
        if (ExceptionUtils.isJvmFatalError(cause)) {
            log.error("Fatal error occurred on JobManager.", cause);
            // The fatal error handler implementation should make sure that this call is
            // non-blocking
            fatalErrorHandler.onFatalError(cause);
        } else {
            jobCompletionActions.jobMasterFailed(cause);
        }
    }

    private void jobStatusChanged(final JobStatus newJobStatus) {
        validateRunsInMainThread();
        if (newJobStatus.isGloballyTerminalState()) {
            CompletableFuture<Void> partitionPromoteFuture;
            if (newJobStatus == JobStatus.FINISHED) {
                Collection<ResultPartitionID> jobPartitions =
                        partitionTracker.getAllTrackedNonClusterPartitions().stream()
                                .map(d -> d.getShuffleDescriptor().getResultPartitionID())
                                .collect(Collectors.toList());
                partitionTracker.stopTrackingAndReleasePartitions(jobPartitions);
                Collection<ResultPartitionID> clusterPartitions =
                        partitionTracker.getAllTrackedClusterPartitions().stream()
                                .map(d -> d.getShuffleDescriptor().getResultPartitionID())
                                .collect(Collectors.toList());
                partitionPromoteFuture =
                        partitionTracker.stopTrackingAndPromotePartitions(clusterPartitions);
            } else {
                Collection<ResultPartitionID> allTracked =
                        partitionTracker.getAllTrackedPartitions().stream()
                                .map(d -> d.getShuffleDescriptor().getResultPartitionID())
                                .collect(Collectors.toList());
                partitionTracker.stopTrackingAndReleasePartitions(allTracked);
                partitionPromoteFuture = CompletableFuture.completedFuture(null);
            }

            final ExecutionGraphInfo executionGraphInfo = schedulerNG.requestJob();

            futureExecutor.execute(
                    () -> {
                        try {
                            partitionPromoteFuture.get();
                        } catch (Throwable e) {
                            // We do not want to fail the job in case of partition releasing and
                            // promoting fail. The TaskExecutors will release the partitions
                            // eventually when they find out the JobMaster is closed.
                            log.warn("Fail to release or promote partitions", e);
                        }
                        jobCompletionActions.jobReachedGloballyTerminalState(executionGraphInfo);
                    });
        }
    }

    //
    private void notifyOfNewResourceManagerLeader(
            final String newResourceManagerAddress, final ResourceManagerId resourceManagerId) {
        resourceManagerAddress = createResourceManagerAddress(newResourceManagerAddress, resourceManagerId);
        //与ResourceManager 建立连接
        reconnectToResourceManager(//
                new FlinkException(
                        String.format(
                                "ResourceManager leader changed to new address %s",
                                resourceManagerAddress)));
    }

    @Nullable
    private ResourceManagerAddress createResourceManagerAddress(
            @Nullable String newResourceManagerAddress,
            @Nullable ResourceManagerId resourceManagerId) {
        if (newResourceManagerAddress != null) {
            // the contract is: address == null <=> id == null
            checkNotNull(resourceManagerId);
            return new ResourceManagerAddress(newResourceManagerAddress, resourceManagerId);
        } else {
            return null;
        }
    }

    //与ResourceManager 建立连接
    private void reconnectToResourceManager(Exception cause) {
        closeResourceManagerConnection(cause);
        tryConnectToResourceManager();//
    }

    private void tryConnectToResourceManager() {
        if (resourceManagerAddress != null) {
            connectToResourceManager();//
        }
    }

    private void connectToResourceManager() {//
        assert (resourceManagerAddress != null);
        assert (resourceManagerConnection == null);
        assert (establishedResourceManagerConnection == null);
        //resourceManagerAddress
        log.info("Connecting to ResourceManager {}", resourceManagerAddress);//

        resourceManagerConnection =
                new ResourceManagerConnection(
                        log,
                        executionPlan.getJobID(),
                        resourceId,
                        getAddress(),
                        getFencingToken(),
                        resourceManagerAddress.getAddress(),
                        resourceManagerAddress.getResourceManagerId(),
                        futureExecutor);
        //JobMaster 向 resourceManager 声明所需要的资源  即所需slot数量
        resourceManagerConnection.start();
    }

    private void establishResourceManagerConnection(final JobMasterRegistrationSuccess success) {
        final ResourceManagerId resourceManagerId = success.getResourceManagerId();

        // verify the response with current connection
        if (resourceManagerConnection != null && Objects.equals(resourceManagerConnection.getTargetLeaderId(), resourceManagerId)) {

            log.info(
                    "JobManager successfully registered at ResourceManager, leader id: {}.",
                    resourceManagerId);
            //获取ResourceManager 的连接
            final ResourceManagerGateway resourceManagerGateway = resourceManagerConnection.getTargetGateway();

            final ResourceID resourceManagerResourceId = success.getResourceManagerResourceId();

            establishedResourceManagerConnection =
                    new EstablishedResourceManagerConnection(
                            resourceManagerGateway, resourceManagerResourceId);

            blocklistHandler.registerBlocklistListener(resourceManagerGateway);
            // JobMaster 向 resourceManager 声明所需要的资源  即所需slot数量
            slotPoolService.connectToResourceManager(resourceManagerGateway);//
            partitionTracker.connectToResourceManager(resourceManagerGateway);

            resourceManagerHeartbeatManager.monitorTarget(
                    resourceManagerResourceId,
                    new ResourceManagerHeartbeatReceiver(resourceManagerGateway));
        } else {
            log.debug(
                    "Ignoring resource manager connection to {} because it's duplicated or outdated.",
                    resourceManagerId);
        }
    }

    private void closeResourceManagerConnection(Exception cause) {
        if (establishedResourceManagerConnection != null) {
            dissolveResourceManagerConnection(establishedResourceManagerConnection, cause);
            establishedResourceManagerConnection = null;
        }

        if (resourceManagerConnection != null) {
            // stop a potentially ongoing registration process
            resourceManagerConnection.close();
            resourceManagerConnection = null;
        }
    }

    private void dissolveResourceManagerConnection(
            EstablishedResourceManagerConnection establishedResourceManagerConnection,
            Exception cause) {
        final ResourceID resourceManagerResourceID =
                establishedResourceManagerConnection.getResourceManagerResourceID();

        if (log.isDebugEnabled()) {
            log.debug(
                    "Close ResourceManager connection {}.",
                    resourceManagerResourceID.getStringWithMetadata(),
                    cause);
        } else {
            log.info(
                    "Close ResourceManager connection {}: {}",
                    resourceManagerResourceID.getStringWithMetadata(),
                    cause.getMessage());
        }

        resourceManagerHeartbeatManager.unmonitorTarget(resourceManagerResourceID);

        ResourceManagerGateway resourceManagerGateway =
                establishedResourceManagerConnection.getResourceManagerGateway();
        resourceManagerGateway.disconnectJobManager(
                executionPlan.getJobID(), schedulerNG.requestJobStatus(), cause);
        blocklistHandler.deregisterBlocklistListener(resourceManagerGateway);
        slotPoolService.disconnectResourceManager();
    }

    private String getNodeIdOfTaskManager(ResourceID taskManagerId) {
        checkState(registeredTaskManagers.containsKey(taskManagerId));
        return registeredTaskManagers.get(taskManagerId).getTaskManagerLocation().getNodeId();
    }

    // ----------------------------------------------------------------------------------------------
    // Service methods
    // ----------------------------------------------------------------------------------------------

    @Override
    public JobMasterGateway getGateway() {
        return getSelfGateway(JobMasterGateway.class);
    }

    // ----------------------------------------------------------------------------------------------
    // Utility classes
    // ----------------------------------------------------------------------------------------------

    private static final class TaskExecutorHeartbeatSender
            extends HeartbeatSender<AllocatedSlotReport> {
        private final TaskExecutorGateway taskExecutorGateway;

        private TaskExecutorHeartbeatSender(TaskExecutorGateway taskExecutorGateway) {
            this.taskExecutorGateway = taskExecutorGateway;
        }

        @Override
        public CompletableFuture<Void> requestHeartbeat(
                ResourceID resourceID, AllocatedSlotReport allocatedSlotReport) {
            return taskExecutorGateway.heartbeatFromJobManager(resourceID, allocatedSlotReport);
        }
    }

    private static final class ResourceManagerHeartbeatReceiver extends HeartbeatReceiver<Void> {
        private final ResourceManagerGateway resourceManagerGateway;

        private ResourceManagerHeartbeatReceiver(ResourceManagerGateway resourceManagerGateway) {
            this.resourceManagerGateway = resourceManagerGateway;
        }

        @Override
        public CompletableFuture<Void> receiveHeartbeat(ResourceID resourceID, Void payload) {
            return resourceManagerGateway.heartbeatFromJobManager(resourceID);
        }
    }

    private class ResourceManagerLeaderListener implements LeaderRetrievalListener {

        // 被 StandaloneLeaderRetrievalService#start 里面调用
        @Override
        public void notifyLeaderAddress(final String leaderAddress, final UUID leaderSessionID) {
            runAsync(
                    MdcUtils.wrapRunnable(
                            MdcUtils.asContextData(executionPlan.getJobID()),
                            () ->
                                    //todo 通知有新 ResourceManager leader 地址了
                                    notifyOfNewResourceManagerLeader(leaderAddress, ResourceManagerId.fromUuidOrNull(leaderSessionID))));
        }

        @Override
        public void handleError(final Exception exception) {
            handleJobMasterError(
                    new Exception("Fatal error in the ResourceManager leader service", exception));
        }
    }

    // ----------------------------------------------------------------------------------------------

    private class ResourceManagerConnection
            extends RegisteredRpcConnection<
                    ResourceManagerId,
                    ResourceManagerGateway,
                    JobMasterRegistrationSuccess,
                    RegistrationResponse.Rejection> {
        private final JobID jobID;

        private final ResourceID jobManagerResourceID;

        private final String jobManagerRpcAddress;

        private final JobMasterId jobMasterId;

        ResourceManagerConnection(
                final Logger log,
                final JobID jobID,
                final ResourceID jobManagerResourceID,
                final String jobManagerRpcAddress,
                final JobMasterId jobMasterId,
                final String resourceManagerAddress,
                final ResourceManagerId resourceManagerId,
                final Executor executor) {
            super(log, resourceManagerAddress, resourceManagerId, executor);
            this.jobID = checkNotNull(jobID);
            this.jobManagerResourceID = checkNotNull(jobManagerResourceID);
            this.jobManagerRpcAddress = checkNotNull(jobManagerRpcAddress);
            this.jobMasterId = checkNotNull(jobMasterId);
        }

        @Override
        protected RetryingRegistration<
                        ResourceManagerId,
                        ResourceManagerGateway,
                        JobMasterRegistrationSuccess,
                        RegistrationResponse.Rejection>
                generateRegistration() {
            return new RetryingRegistration<
                    ResourceManagerId,
                    ResourceManagerGateway,
                    JobMasterRegistrationSuccess,
                    RegistrationResponse.Rejection>(
                    log,
                    getRpcService(),
                    "ResourceManager",
                    ResourceManagerGateway.class,
                    getTargetAddress(),
                    getTargetLeaderId(),
                    jobMasterConfiguration.getRetryingRegistrationConfiguration()) {

                @Override
                protected CompletableFuture<RegistrationResponse> invokeRegistration(
                        ResourceManagerGateway gateway,
                        ResourceManagerId fencingToken,
                        long timeoutMillis) {
                    Duration timeout = Duration.ofMillis(timeoutMillis);

                    return gateway.registerJobMaster(
                            jobMasterId,
                            jobManagerResourceID,
                            jobManagerRpcAddress,
                            jobID,
                            timeout);
                }
            };
        }

        @Override
        protected void onRegistrationSuccess(final JobMasterRegistrationSuccess success) {
            runAsync(
                    () -> {
                        // filter out outdated connections
                        //noinspection ObjectEquality
                        if (this == resourceManagerConnection) {
                            //建立与 ResourceManager 的连接  并且 JobMaster 向 resourceManager 声明所需要的资源  即所需slot数量
                            establishResourceManagerConnection(success);//
                        }
                    });
        }

        @Override
        protected void onRegistrationRejection(RegistrationResponse.Rejection rejection) {
            handleJobMasterError(
                    new IllegalStateException(
                            "The ResourceManager should never reject a JobMaster registration."));
        }

        @Override
        protected void onRegistrationFailure(final Throwable failure) {
            handleJobMasterError(failure);
        }
    }

    // ----------------------------------------------------------------------------------------------

    private class JobManagerJobStatusListener implements JobStatusListener {

        private volatile boolean running = true;

        @Override
        public void jobStatusChanges(
                final JobID jobId, final JobStatus newJobStatus, final long timestamp) {

            if (running) {
                // run in rpc thread to avoid concurrency
                runAsync(() -> jobStatusChanged(newJobStatus));
            }
        }

        private void stop() {
            running = false;
        }
    }

    private class TaskManagerHeartbeatListener
            implements HeartbeatListener<
                    TaskExecutorToJobManagerHeartbeatPayload, AllocatedSlotReport> {

        @Override
        public void notifyHeartbeatTimeout(ResourceID resourceID) {
            final String message =
                    String.format(
                            "Heartbeat of TaskManager with id %s timed out.",
                            resourceID.getStringWithMetadata());

            log.info(message);
            handleTaskManagerConnectionLoss(resourceID, new TimeoutException(message));
        }

        //
        private void handleTaskManagerConnectionLoss(ResourceID resourceID, Exception cause) {
            validateRunsInMainThread();
            disconnectTaskManager(resourceID, cause);
        }

        @Override
        public void notifyTargetUnreachable(ResourceID resourceID) {
            final String message =
                    String.format(
                            "TaskManager with id %s is no longer reachable.",
                            resourceID.getStringWithMetadata());

            log.info(message);
            handleTaskManagerConnectionLoss(resourceID, new JobMasterException(message));
        }

        @Override
        public void reportPayload(
                ResourceID resourceID, TaskExecutorToJobManagerHeartbeatPayload payload) {
            validateRunsInMainThread();
            executionDeploymentReconciler.reconcileExecutionDeployments(
                    resourceID,
                    payload.getExecutionDeploymentReport(),
                    executionDeploymentTracker.getExecutionsOn(resourceID));
            for (AccumulatorSnapshot snapshot :
                    payload.getAccumulatorReport().getAccumulatorSnapshots()) {
                schedulerNG.updateAccumulators(snapshot);
            }
        }

        @Override
        public AllocatedSlotReport retrievePayload(ResourceID resourceID) {
            validateRunsInMainThread();
            return slotPoolService.createAllocatedSlotReport(resourceID);
        }
    }

    private class ResourceManagerHeartbeatListener implements HeartbeatListener<Void, Void> {

        //通知心跳超时
        @Override
        public void notifyHeartbeatTimeout(final ResourceID resourceId) {
            try (MdcUtils.MdcCloseable ignored =
                    MdcUtils.withContext(MdcUtils.asContextData(executionPlan.getJobID()))) {
                final String message =
                        String.format(
                                "The heartbeat of ResourceManager with id %s timed out.",
                                resourceId.getStringWithMetadata());
                log.info(message);

                handleResourceManagerConnectionLoss(resourceId, new TimeoutException(message));
            }
        }

        private void handleResourceManagerConnectionLoss(ResourceID resourceId, Exception cause) {
            validateRunsInMainThread();
            if (establishedResourceManagerConnection != null
                    && establishedResourceManagerConnection
                            .getResourceManagerResourceID()
                            .equals(resourceId)) {
                //重新跟ResourceManager 建立连接
                reconnectToResourceManager(cause);
            }
        }

        @Override
        public void notifyTargetUnreachable(ResourceID resourceID) {
            try (MdcUtils.MdcCloseable ignored =
                    MdcUtils.withContext(MdcUtils.asContextData(executionPlan.getJobID()))) {
                final String message =
                        String.format(
                                "ResourceManager with id %s is no longer reachable.",
                                resourceID.getStringWithMetadata());
                log.info(message);

                handleResourceManagerConnectionLoss(resourceID, new JobMasterException(message));
            }
        }

        @Override
        public void reportPayload(ResourceID resourceID, Void payload) {
            // nothing to do since the payload is of type Void
        }

        @Override
        public Void retrievePayload(ResourceID resourceID) {
            return null;
        }
    }

    private class JobMasterBlocklistContext implements BlocklistContext {

        @Override
        public void blockResources(Collection<BlockedNode> blockedNodes) {
            Set<String> blockedNodeIds =
                    blockedNodes.stream().map(BlockedNode::getNodeId).collect(Collectors.toSet());

            Collection<ResourceID> blockedTaskMangers =
                    registeredTaskManagers.keySet().stream()
                            .filter(
                                    taskManagerId ->
                                            blockedNodeIds.contains(
                                                    getNodeIdOfTaskManager(taskManagerId)))
                            .collect(Collectors.toList());

            blockedTaskMangers.forEach(
                    taskManagerId -> {
                        Exception cause =
                                new FlinkRuntimeException(
                                        String.format(
                                                "TaskManager %s is blocked.",
                                                taskManagerId.getStringWithMetadata()));
                        slotPoolService.releaseFreeSlotsOnTaskManager(taskManagerId, cause);
                    });
        }

        @Override
        public void unblockResources(Collection<BlockedNode> unblockedNodes) {}
    }
}
