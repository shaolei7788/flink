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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.resources.CPUResource;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.blocklist.BlockedTaskManagerChecker;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.SlotManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.rest.messages.taskmanager.SlotInfo;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.slots.ResourceRequirements;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.MdcUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// 细粒度 Slot 管理器
//支持动态、按需切分 TaskManager 的资源。
// 它可以根据作业中不同算子的实际大小（例如：这个 Task 要 0.5 核 CPU + 1GB 内存，那个 Task 要 1 核 CPU + 2GB 内存）进行极其精准的匹配与裁剪
/** Implementation of {@link SlotManager} supporting fine-grained resource management. */
public class FineGrainedSlotManager implements SlotManager {
    public static final Duration METRICS_UPDATE_INTERVAL = Duration.ofSeconds(1);

    private static final Logger LOG = LoggerFactory.getLogger(FineGrainedSlotManager.class);

    //TaskManager 状态追踪器。它在内存中实时维护当前集群所有已注册 TaskManager 的资源视图
    // （包括其总资源、已分配资源、以及剩余可切分的可用资源 ResourceProfile）
    private final TaskManagerTracker taskManagerTracker;

    //作业资源诉求追踪器。用于记录和管理各个正在运行或排队作业的声明式资源需求（ResourceRequirements）以及当前已经满足/分配的 Slot 状态
    private final ResourceTracker resourceTracker;
    //
    private final ResourceAllocationStrategy resourceAllocationStrategy;

    //Slot 状态同步器。负责处理 ResourceManager、TaskManager 之间有关 Slot 分配、释放、以及异常超时时的状态状态对齐
    private final SlotStatusSyncer slotStatusSyncer;

    //定时任务执行器
    //专门用来驱动下面几个带有 Delay 或 Timeout 属性的定时任务。例如，它负责在后台倒计时“TaskManager 已经空闲了多久”，或者将高频的需求变更请求推迟到几毫秒后批量合并处理
    /** Scheduled executor for timeouts. */
    private final ScheduledExecutor scheduledExecutor;

    //TaskManager 闲置超时时间
    //当某台动态扩容出来的 TaskManager 上的所有细粒度 Slot 都跑完并释放后，它会进入“闲置（Unused）”状态。Flink 不会立刻通知 K8s 销毁它。这个属性定义了一个观察期（默认几分钟）。
    // 在观察期内，如果有新任务进来，会优先复用它；只有超过这个时间仍无人使用，才会安全地调用 resourceAllocator 将其销毁，做到高吞吐与省钱的完美平衡
    /** Timeout after which an unused TaskManager is released. */
    private final Duration taskManagerTimeout;

    //作业需求检查的延迟合并时间
    //当作业刚启动或者大规模 Failover 时，JobMaster 会密集地发送几十次微小的资源需求变动（ResourceRequirements）。这个属性设置了一个缓冲时间（例如 50ms）。
    // SlotManager 收到变动后不立刻计算，而是等这个延迟结束，把这 50ms 内所有算子的诉求合并为一次批量大盘再统一执行匹配算法，极大地保护了主线程的性能
    /** Delay of the requirement change check in the slot manager. */
    private final Duration requirementsCheckDelay;

    //向外部资源层（K8s/Yarn）宣告资源缺口的延迟时间
    //计算出集群当前缺多少 CPU/内存后，Flink 也不会立刻调 K8s API 去要 Pod。它会延迟几百毫秒，
    //等整个集群的资源缺口数字彻底稳定下来后，再一次性向外界宣告一个最终的总量，避免把 K8s 的 API-Server 冲垮
    /** Delay of the resource declare in the slot manager. */
    private final Duration declareNeededResourceDelay;

    //Slot 管理器的指标组
    //负责将细粒度调度内部最核心的动态数据（例如：AvailableCPU、TotalMemory、PendingSlots、AllocatedSlots）注册并桥接到 Flink 的监控系统中，以便你在 Grafana 或 Flink WebUI 上看到实时的资源水位线
    private final SlotManagerMetricGroup slotManagerMetricGroup;

    //作业主控（JobMaster）的 RPC 目标地址映射表
    private final Map<JobID, String> jobMasterTargetAddresses = new HashMap<>();

    //全集群允许自动扩容的物理算力天花板
    private final CPUResource maxTotalCpu;
    private final MemorySize maxTotalMem;

    //是否发送“资源不足”通知的控制开关
    private boolean sendNotEnoughResourceNotifications = true;

    //被判定为“永远无法满足”的作业黑名单/死锁集合。
    //当一个作业申请的细粒度 Slot 规格（例如某个极其复杂的算子在代码里写明了单实例必须要 64 核 CPU 或 512GB 内存），
    // 已经彻底超过了当前集群单台物理机/单个容器所能提供的最大极限规格
    //FineGrainedSlotManager 在几轮算法尝试后会发现无论怎么扩容都永远不可能塞得下它。
    // 此时就会把该作业的 JobID 扔进 unfulfillableJobs 集合中。进入该集合的作业会被标记为“死锁/不可满足”，系统会停止为其进行无意义的扩容尝试
    private final Set<JobID> unfulfillableJobs = new HashSet<>();

    /** ResourceManager's id. */
    @Nullable private ResourceManagerId resourceManagerId;

    //主线程执行器。Flink 的 ResourceManager 是基于 RpcEndpoint 单线程模型运转的。
    //所有对 Slot 状态的修改、分配算法的执行，都必须通过这个 mainThreadExecutor 投递，以确保绝对的线程安全，避免并发修改状态导致的死锁或数据不一致
    /** Executor for future callbacks which have to be "synchronized". */
    @Nullable private Executor mainThreadExecutor;

    //FineGrainedSlotManager 本身只负责在内存中通过算法进行算力计算（数学题），
    // 它没有权限去直接操作底层的容器。当它计算完并确定需要补充机器（分配）或有机器闲置需要销毁（回收）时，会调用这个 resourceAllocator 的回调方法。
    // 它会最终桥接到外层的 ResourceManager（如 KubernetesResourceManager），由其向外部资源层发出真正的容器增删网络请求
    /** Callbacks for resource (de-)allocations. */
    @Nullable private ResourceAllocator resourceAllocator;

    //主要用于处理“资源不够了（Resource Not Enough）”或者资源满足度发生剧烈震荡时的应急通知。当作业需要的细粒度 CPU/内存大盘无法被现有集群满足，
    // 且达到了某种资源阈值时，它会触发该监听器，用于向上层组件发送告警、触发死锁检测、或者向作业控制台反馈“资源饥饿（Starvation）”状态
    /** Callbacks for resource not enough. */
    @Nullable private ResourceEventListener resourceEventListener;

    //在分布式系统里，SlotManager 内存中的账本和外部资源层（Yarn/K8s/物理机）的真实状态可能会因为网络抖动出现短暂的不一致。这是一个定时轮询任务（Reconciliation，常译为“对齐”或“和解”）。
    // 它会定期拉取外部真实的 Worker 状态，与 taskManagerTracker 里的数据进行强行校对，确保没有出现“幽灵节点”或“账目对不上”的情况
    @Nullable private ScheduledFuture<?> clusterReconciliationCheck;

    //细粒度模式下，Slot 的数量和规格是动态变化的。该定时任务会周期性地盘点当前集群的总 CPU、剩余 CPU、总内存、可用内存等细粒度指标，
    // 并将这些高频变化的动态数据刷新并注册到 Flink 的 MetricRegistry 中，供 Prometheus 或 WebUI 渲染
    @Nullable private ScheduledFuture<?> metricsUpdateFuture;

    //当大量并发作业或者同一作业内的多个算子同时提交资源诉求（ResourceRequirements）时，为了防止匹配算法在主线程中高频重复计算导致卡死，Flink 采用了异步或批次合并（Debounce / Batching）的处理方式。
    // 这个 Future 用来标记和追踪“当前是否有一个正在进行的需求资源匹配检查”，防止发起重复的无用匹配计算
    @Nullable private CompletableFuture<Void> requirementsCheckFuture;

    //在声明式调度中，Flink 不会盲目地“来一个请求就立马去 K8s 申请一个 Pod”。它会把当前所有作业的缺口打包，
    // 计算出一个集群总共需要的期望资源总量（Desired Resource），
    // 然后异步地调用外部接口去宣告。这个 Future 用于控制和追踪这一次“向外界要资源”的网络调用过程是否已经顺利完成
    @Nullable private CompletableFuture<Void> declareNeededResourceFuture;

    //这是 Flink 容错和抗断连能力中非常硬核的一个设计。在运行中，如果某台 TaskManager 频繁发生硬件故障（如磁盘坏道、网络间歇性丢包、频繁 OOM），
    // 为了防止 FineGrainedSlotManager 傻傻地继续把新算子分配到这台坏机器上导致作业无限重启，这个检查器会介入
    /** Blocked task manager checker. */
    @Nullable private BlockedTaskManagerChecker blockedTaskManagerChecker;

    /** True iff the component has been started. */
    private boolean started;

    /** Metrics. */
    private long lastNumberFreeSlots;

    private long lastNumberRegisteredSlots;

    public FineGrainedSlotManager(
            ScheduledExecutor scheduledExecutor,
            SlotManagerConfiguration slotManagerConfiguration,
            SlotManagerMetricGroup slotManagerMetricGroup,
            ResourceTracker resourceTracker,
            TaskManagerTracker taskManagerTracker,
            SlotStatusSyncer slotStatusSyncer,
            ResourceAllocationStrategy resourceAllocationStrategy) {

        this.scheduledExecutor = Preconditions.checkNotNull(scheduledExecutor);

        Preconditions.checkNotNull(slotManagerConfiguration);
        this.taskManagerTimeout = slotManagerConfiguration.getTaskManagerTimeout();
        this.requirementsCheckDelay =
                Preconditions.checkNotNull(slotManagerConfiguration.getRequirementCheckDelay());
        this.declareNeededResourceDelay =
                Preconditions.checkNotNull(
                        slotManagerConfiguration.getDeclareNeededResourceDelay());

        this.slotManagerMetricGroup = Preconditions.checkNotNull(slotManagerMetricGroup);

        this.resourceTracker = Preconditions.checkNotNull(resourceTracker);
        this.taskManagerTracker = Preconditions.checkNotNull(taskManagerTracker);
        this.slotStatusSyncer = Preconditions.checkNotNull(slotStatusSyncer);
        this.resourceAllocationStrategy = Preconditions.checkNotNull(resourceAllocationStrategy);
        //
        this.maxTotalCpu = Preconditions.checkNotNull(slotManagerConfiguration.getMaxTotalCpu());
        this.maxTotalMem = Preconditions.checkNotNull(slotManagerConfiguration.getMaxTotalMem());

        resourceManagerId = null;
        resourceAllocator = null;
        resourceEventListener = null;
        mainThreadExecutor = null;
        clusterReconciliationCheck = null;
        requirementsCheckFuture = null;
        metricsUpdateFuture = null;

        started = false;
    }

    @Override
    public void setFailUnfulfillableRequest(boolean failUnfulfillableRequest) {
        checkInit();
        // this sets up a grace period, e.g., when the cluster was started, to give task executors
        // time to connect
        sendNotEnoughResourceNotifications = failUnfulfillableRequest;

        if (failUnfulfillableRequest && !unfulfillableJobs.isEmpty()) {
            for (JobID jobId : unfulfillableJobs) {
                resourceEventListener.notEnoughResourceAvailable(
                        jobId, resourceTracker.getAcquiredResources(jobId));
            }
        }
    }

    @Override
    public void triggerResourceRequirementsCheck() {
        checkResourceRequirementsWithDelay();
    }

    // ---------------------------------------------------------------------------------------------
    // Component lifecycle methods
    // ---------------------------------------------------------------------------------------------

    /**
     * Starts the slot manager with the given leader id and resource manager actions.
     *
     * @param newResourceManagerId to use for communication with the task managers
     * @param newMainThreadExecutor to use to run code in the ResourceManager's main thread
     * @param newResourceAllocator to use for resource (de-)allocations
     * @param newBlockedTaskManagerChecker to query whether a task manager is blocked
     */
    @Override
    public void start(
            ResourceManagerId newResourceManagerId,
            Executor newMainThreadExecutor,
            ResourceAllocator newResourceAllocator,
            ResourceEventListener newResourceEventListener,
            BlockedTaskManagerChecker newBlockedTaskManagerChecker) {
        LOG.info("Starting the slot manager.");

        resourceManagerId = Preconditions.checkNotNull(newResourceManagerId);
        mainThreadExecutor = Preconditions.checkNotNull(newMainThreadExecutor);
        resourceAllocator = Preconditions.checkNotNull(newResourceAllocator);
        resourceEventListener = Preconditions.checkNotNull(newResourceEventListener);
        slotStatusSyncer.initialize(
                taskManagerTracker, resourceTracker, resourceManagerId, mainThreadExecutor);
        blockedTaskManagerChecker = Preconditions.checkNotNull(newBlockedTaskManagerChecker);

        started = true;

        if (resourceAllocator.isSupported()) {
            clusterReconciliationCheck =
                    scheduledExecutor.scheduleWithFixedDelay(
                            () -> mainThreadExecutor.execute(this::checkClusterReconciliation),
                            0L,
                            taskManagerTimeout.toMillis(),
                            TimeUnit.MILLISECONDS);
        }

        registerSlotManagerMetrics();
    }

    private void registerSlotManagerMetrics() {
        // Because taskManagerTracker is not thread-safe, metrics must be updated periodically on
        // the main thread to prevent concurrent modification issues.
        metricsUpdateFuture =
                scheduledExecutor.scheduleAtFixedRate(
                        this::updateMetrics,
                        0L,
                        METRICS_UPDATE_INTERVAL.toMillis(),
                        TimeUnit.MILLISECONDS);

        slotManagerMetricGroup.gauge(MetricNames.TASK_SLOTS_AVAILABLE, () -> lastNumberFreeSlots);
        slotManagerMetricGroup.gauge(MetricNames.TASK_SLOTS_TOTAL, () -> lastNumberRegisteredSlots);
    }

    private void updateMetrics() {
        Objects.requireNonNull(mainThreadExecutor)
                .execute(
                        () -> {
                            lastNumberFreeSlots = getNumberFreeSlots();
                            lastNumberRegisteredSlots = getNumberRegisteredSlots();
                        });
    }

    /** Suspends the component. This clears the internal state of the slot manager. */
    @Override
    public void suspend() {
        if (!started) {
            return;
        }

        LOG.info("Suspending the slot manager.");

        slotManagerMetricGroup.close();

        // stop the timeout checks for the TaskManagers
        if (clusterReconciliationCheck != null) {
            clusterReconciliationCheck.cancel(false);
            clusterReconciliationCheck = null;
        }

        // stop the metrics updates
        if (metricsUpdateFuture != null) {
            metricsUpdateFuture.cancel(false);
            metricsUpdateFuture = null;
        }

        slotStatusSyncer.close();
        taskManagerTracker.clear();
        resourceTracker.clear();

        unfulfillableJobs.clear();
        resourceManagerId = null;
        resourceAllocator = null;
        resourceEventListener = null;
        started = false;
    }

    /**
     * Closes the slot manager.
     *
     * @throws Exception if the close operation fails
     */
    @Override
    public void close() throws Exception {
        LOG.info("Closing the slot manager.");

        suspend();
    }

    // ---------------------------------------------------------------------------------------------
    // Public API
    // ---------------------------------------------------------------------------------------------

    @Override
    public void clearResourceRequirements(JobID jobId) {
        maybeReclaimInactiveSlots(jobId);
        jobMasterTargetAddresses.remove(jobId);
        resourceTracker.notifyResourceRequirements(jobId, Collections.emptyList());
        if (resourceAllocator.isSupported()) {
            taskManagerTracker.clearPendingAllocationsOfJob(jobId);
            checkResourcesNeedReconcile();
            declareNeededResourcesWithDelay();
        }
    }

    private void maybeReclaimInactiveSlots(JobID jobId) {
        // We don't notify the task manager tracker and resource tracker about the freeing of these
        // slots early, and rely on task managers to report the slots becoming available later to
        // keep the states consistent.
        if (!resourceTracker.getAcquiredResources(jobId).isEmpty()) {
            slotStatusSyncer.freeInactiveSlots(jobId);
        }
    }

    @Override
    public void processResourceRequirements(ResourceRequirements resourceRequirements) {
        checkInit();
        if (resourceRequirements.getResourceRequirements().isEmpty()
                && resourceTracker.isRequirementEmpty(resourceRequirements.getJobId())) {
            // Skip duplicate empty resource requirements.
            return;
        }

        if (resourceRequirements.getResourceRequirements().isEmpty()) {
            LOG.info("Clearing resource requirements of job {}", resourceRequirements.getJobId());
            jobMasterTargetAddresses.remove(resourceRequirements.getJobId());
            if (resourceAllocator.isSupported()) {
                taskManagerTracker.clearPendingAllocationsOfJob(resourceRequirements.getJobId());
            }
        } else {
            LOG.info(
                    "Received resource requirements from job {}: {}",
                    resourceRequirements.getJobId(),
                    resourceRequirements.getResourceRequirements());
            jobMasterTargetAddresses.put(
                    resourceRequirements.getJobId(), resourceRequirements.getTargetAddress());
        }

        resourceTracker.notifyResourceRequirements(
                resourceRequirements.getJobId(), resourceRequirements.getResourceRequirements());
        checkResourceRequirementsWithDelay();
    }

    //todo
    @Override
    public RegistrationResult registerTaskManager(
            final TaskExecutorConnection taskExecutorConnection,
            //SlotReport{SlotStatus{slotID=tm01_0, allocationID=null, jobID=null, resourceProfile=ResourceProfile{cpuCores=1, taskHeapMemory=512.000mb (536870912 bytes), taskOffHeapMemory=128.000mb (134217728 bytes), managedMemory=512.000mb (536870912 bytes), networkMemory=128.000mb (134217728 bytes)}}}
            SlotReport initialSlotReport,
            //ResourceProfile{cpuCores=1, taskHeapMemory=512.000mb (536870912 bytes), taskOffHeapMemory=128.000mb (134217728 bytes), managedMemory=512.000mb (536870912 bytes), networkMemory=128.000mb (134217728 bytes)}
            ResourceProfile totalResourceProfile,
            //ResourceProfile{cpuCores=1, taskHeapMemory=512.000mb (536870912 bytes), taskOffHeapMemory=128.000mb (134217728 bytes), managedMemory=512.000mb (536870912 bytes), networkMemory=128.000mb (134217728 bytes)}
            ResourceProfile defaultSlotResourceProfile) {
        checkInit();
        LOG.info(
                "Registering task executor {} under {} at the slot manager.",
                taskExecutorConnection.getResourceID(),
                taskExecutorConnection.getInstanceID());

        // we identify task managers by their instance id
        if (taskManagerTracker.getRegisteredTaskManager(taskExecutorConnection.getInstanceID()).isPresent()) {
            LOG.debug(
                    "Task executor {} was already registered.",
                    taskExecutorConnection.getResourceID());
            reportSlotStatus(taskExecutorConnection.getInstanceID(), initialSlotReport);
            return RegistrationResult.IGNORED;
        } else {
            //todo  initialSlotReport.hasAllocatedSlot() = false 判断该TaskManager 是否有被分配作业的id
            // matchedPendingTaskManagerOptional 为空
            Optional<PendingTaskManager> matchedPendingTaskManagerOptional = initialSlotReport.hasAllocatedSlot()
                            ? Optional.empty()
                            : findMatchingPendingTaskManager(totalResourceProfile, defaultSlotResourceProfile);

            if (!matchedPendingTaskManagerOptional.isPresent() && isMaxTotalResourceExceededAfterAdding(totalResourceProfile)) {

                LOG.info(
                        "Can not register task manager {}. The max total resource limitation <{}, {}> is reached.",
                        taskExecutorConnection.getResourceID(),
                        maxTotalCpu,
                        maxTotalMem.toHumanReadableString());
                return RegistrationResult.REJECTED;
            }
            //
            taskManagerTracker.addTaskManager(taskExecutorConnection, totalResourceProfile, defaultSlotResourceProfile);

            if (initialSlotReport.hasAllocatedSlot()) {
                slotStatusSyncer.reportSlotStatus(taskExecutorConnection.getInstanceID(), initialSlotReport);
            }

            if (matchedPendingTaskManagerOptional.isPresent()) {
                PendingTaskManager pendingTaskManager = matchedPendingTaskManagerOptional.get();
                allocateSlotsForRegisteredPendingTaskManager(pendingTaskManager, taskExecutorConnection.getInstanceID());
                taskManagerTracker.removePendingTaskManager(pendingTaskManager.getPendingTaskManagerId());
                return RegistrationResult.SUCCESS;
            }

            checkResourceRequirementsWithDelay();//
            return RegistrationResult.SUCCESS;
        }
    }

    private void declareNeededResourcesWithDelay() {
        Preconditions.checkState(resourceAllocator.isSupported());

        if (declareNeededResourceDelay.toMillis() <= 0) {
            declareNeededResources();
        } else {
            if (declareNeededResourceFuture == null || declareNeededResourceFuture.isDone()) {
                declareNeededResourceFuture = new CompletableFuture<>();
                scheduledExecutor.schedule(
                        () ->
                                mainThreadExecutor.execute(
                                        () -> {
                                            declareNeededResources();
                                            Preconditions.checkNotNull(declareNeededResourceFuture)
                                                    .complete(null);
                                        }),
                        declareNeededResourceDelay.toMillis(),
                        TimeUnit.MILLISECONDS);
            }
        }
    }

    /** DO NOT call this method directly. Use {@link #declareNeededResourcesWithDelay()} instead. */
    private void declareNeededResources() {
        Map<InstanceID, WorkerResourceSpec> unWantedTaskManagers =
                taskManagerTracker.getUnWantedTaskManager();
        Map<WorkerResourceSpec, Set<InstanceID>> unWantedTaskManagerBySpec =
                unWantedTaskManagers.entrySet().stream()
                        .collect(
                                Collectors.groupingBy(
                                        Map.Entry::getValue,
                                        Collectors.mapping(Map.Entry::getKey, Collectors.toSet())));

        // registered TaskManagers except unwanted worker.
        Stream<WorkerResourceSpec> registeredTaskManagerStream =
                taskManagerTracker.getRegisteredTaskManagers().stream()
                        .filter(t -> !unWantedTaskManagers.containsKey(t.getInstanceId()))
                        .map(
                                t ->
                                        WorkerResourceSpec.fromTotalResourceProfile(
                                                t.getTotalResource(), t.getDefaultNumSlots()));
        // pending TaskManagers.
        Stream<WorkerResourceSpec> pendingTaskManagerStream =
                taskManagerTracker.getPendingTaskManagers().stream()
                        .map(
                                t ->
                                        WorkerResourceSpec.fromTotalResourceProfile(
                                                t.getTotalResourceProfile(), t.getNumSlots()));

        Map<WorkerResourceSpec, Integer> requiredWorkers =
                Stream.concat(registeredTaskManagerStream, pendingTaskManagerStream)
                        .collect(
                                Collectors.groupingBy(
                                        Function.identity(), Collectors.summingInt(e -> 1)));

        Set<WorkerResourceSpec> workerResourceSpecs = new HashSet<>(requiredWorkers.keySet());
        workerResourceSpecs.addAll(unWantedTaskManagerBySpec.keySet());

        List<ResourceDeclaration> resourceDeclarations = new ArrayList<>();
        workerResourceSpecs.forEach(
                spec ->
                        resourceDeclarations.add(
                                new ResourceDeclaration(
                                        spec,
                                        requiredWorkers.getOrDefault(spec, 0),
                                        unWantedTaskManagerBySpec.getOrDefault(
                                                spec, Collections.emptySet()))));

        resourceAllocator.declareResourceNeeded(resourceDeclarations);
    }

    private void allocateSlotsForRegisteredPendingTaskManager(
            PendingTaskManager pendingTaskManager, InstanceID instanceId) {
        Map<JobID, Map<InstanceID, ResourceCounter>> allocations =
                pendingTaskManager.getPendingSlotAllocationRecords().entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        (e) -> Collections.singletonMap(instanceId, e.getValue())));
        allocateSlotsAccordingTo(allocations);
    }

    private Optional<PendingTaskManager> findMatchingPendingTaskManager(
            ResourceProfile totalResourceProfile, ResourceProfile defaultSlotResourceProfile) {
        //从 taskManagerTracker 中找出当前所有处于 Pending 状态（已被预订、正在创建中）的 TaskManager 虚拟对象
        Collection<PendingTaskManager> matchedPendingTaskManagers =
                taskManagerTracker.getPendingTaskManagersByTotalAndDefaultSlotResourceProfile(
                        totalResourceProfile, defaultSlotResourceProfile);

        Optional<PendingTaskManager> matchedPendingTaskManagerIdsWithAllocatedSlots =
                matchedPendingTaskManagers.stream()
                        .filter(
                                (pendingTaskManager) ->
                                        !pendingTaskManager
                                                .getPendingSlotAllocationRecords()
                                                .isEmpty())
                        .findAny();

        if (matchedPendingTaskManagerIdsWithAllocatedSlots.isPresent()) {
            //说明找到了符合紧凑策略、已经带了任务的在途机器
            return matchedPendingTaskManagerIdsWithAllocatedSlots;
        } else {
            //所有在途的 Pending 机器都跟白纸一样干净，没有任何任务预占它们
            //直接从最初粗筛出来的 matchedPendingTaskManagers 集合中通过 stream().findAny() 随便捞一台完全干净的机器返回
            return matchedPendingTaskManagers.stream().findAny();
        }
    }

    @Override
    public boolean unregisterTaskManager(InstanceID instanceId, Exception cause) {
        checkInit();

        LOG.info("Unregistering task executor {} from the slot manager.", instanceId);

        if (taskManagerTracker.getRegisteredTaskManager(instanceId).isPresent()) {
            Set<AllocationID> allocatedSlots =
                    new HashSet<>(
                            taskManagerTracker
                                    .getRegisteredTaskManager(instanceId)
                                    .get()
                                    .getAllocatedSlots()
                                    .keySet());
            for (AllocationID allocationId : allocatedSlots) {
                slotStatusSyncer.freeSlot(allocationId);
            }
            taskManagerTracker.removeTaskManager(instanceId);
            if (!allocatedSlots.isEmpty()) {
                checkResourceRequirementsWithDelay();
            }

            return true;
        } else {
            LOG.debug(
                    "There is no task executor registered with instance ID {}. Ignoring this message.",
                    instanceId);

            return false;
        }
    }

    /**
     * Reports the current slot allocations for a task manager identified by the given instance id.
     *
     * @param instanceId identifying the task manager for which to report the slot status
     * @param slotReport containing the status for all of its slots
     * @return true if the slot status has been updated successfully, otherwise false
     */
    @Override
    public boolean reportSlotStatus(InstanceID instanceId, SlotReport slotReport) {
        checkInit();

        LOG.debug("Received slot report from instance {}: {}.", instanceId, slotReport);

        if (taskManagerTracker.getRegisteredTaskManager(instanceId).isPresent()) {
            if (!slotStatusSyncer.reportSlotStatus(instanceId, slotReport)) {
                checkResourceRequirementsWithDelay();
            }
            return true;
        } else {
            LOG.debug(
                    "Received slot report for unknown task manager with instance id {}. Ignoring this report.",
                    instanceId);

            return false;
        }
    }

    /**
     * Free the given slot from the given allocation. If the slot is still allocated by the given
     * allocation id, then the slot will be freed.
     *
     * @param slotId identifying the slot to free, will be ignored
     * @param allocationId with which the slot is presumably allocated
     */
    @Override
    public void freeSlot(SlotID slotId, AllocationID allocationId) {
        checkInit();
        LOG.debug("Freeing slot {}.", allocationId);

        if (taskManagerTracker.getAllocatedOrPendingSlot(allocationId).isPresent()) {
            slotStatusSyncer.freeSlot(allocationId);
            checkResourceRequirementsWithDelay();
        } else {
            LOG.debug(
                    "Trying to free a slot {} which has not been allocated. Ignoring this message.",
                    allocationId);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Requirement matching
    // ---------------------------------------------------------------------------------------------

    /**
     * Depending on the implementation of {@link ResourceAllocationStrategy}, checking resource
     * requirements and potentially making a re-allocation can be heavy. In order to cover more
     * changes with each check, thus reduce the frequency of unnecessary re-allocations, the checks
     * are performed with a slight delay.
     */
    private void checkResourceRequirementsWithDelay() {
        if (requirementsCheckDelay.toMillis() <= 0) {
            checkResourceRequirements();
        } else {
            if (requirementsCheckFuture == null || requirementsCheckFuture.isDone()) {
                requirementsCheckFuture = new CompletableFuture<>();
                scheduledExecutor.schedule(
                        () ->
                                mainThreadExecutor.execute(
                                        () -> {
                                            //todo
                                            checkResourceRequirements();
                                            Preconditions.checkNotNull(requirementsCheckFuture)
                                                    .complete(null);
                                        }),
                        requirementsCheckDelay.toMillis(),
                        TimeUnit.MILLISECONDS);
            }
        }
    }

    /**
     * DO NOT call this method directly. Use {@link #checkResourceRequirementsWithDelay()} instead.
     */
    private void checkResourceRequirements() {
        if (!started) {
            return;
        }
        Map<JobID, Collection<ResourceRequirement>> missingResources =
                resourceTracker.getMissingResources();
        if (missingResources.isEmpty()) {
            if (resourceAllocator.isSupported()
                    && !taskManagerTracker.getPendingTaskManagers().isEmpty()) {
                taskManagerTracker.replaceAllPendingAllocations(Collections.emptyMap());
                checkResourcesNeedReconcile();
                declareNeededResourcesWithDelay();
            }
            return;
        }

        logMissingAndAvailableResource(missingResources);

        missingResources =
                missingResources.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey, e -> new ArrayList<>(e.getValue())));

        final ResourceAllocationResult result =
                resourceAllocationStrategy.tryFulfillRequirements(
                        missingResources, taskManagerTracker, this::isBlockedTaskManager);

        // Allocate slots according to the result
        allocateSlotsAccordingTo(result.getAllocationsOnRegisteredResources());//

        final Set<PendingTaskManagerId> failAllocations;
        if (resourceAllocator.isSupported()) {
            // Allocate task managers according to the result
            failAllocations =
                    allocateTaskManagersAccordingTo(result.getPendingTaskManagersToAllocate());

            // Record slot allocation of pending task managers
            final Map<PendingTaskManagerId, Map<JobID, ResourceCounter>>
                    pendingResourceAllocationResult =
                            new HashMap<>(result.getAllocationsOnPendingResources());
            pendingResourceAllocationResult.keySet().removeAll(failAllocations);
            taskManagerTracker.replaceAllPendingAllocations(pendingResourceAllocationResult);
        } else {
            failAllocations =
                    result.getPendingTaskManagersToAllocate().stream()
                            .map(PendingTaskManager::getPendingTaskManagerId)
                            .collect(Collectors.toSet());
        }

        unfulfillableJobs.clear();
        unfulfillableJobs.addAll(result.getUnfulfillableJobs());
        for (PendingTaskManagerId pendingTaskManagerId : failAllocations) {
            unfulfillableJobs.addAll(
                    result.getAllocationsOnPendingResources().get(pendingTaskManagerId).keySet());
        }
        // Notify jobs that can not be fulfilled
        if (sendNotEnoughResourceNotifications) {
            for (JobID jobId : unfulfillableJobs) {
                try (MdcUtils.MdcCloseable ignored =
                        MdcUtils.withContext(MdcUtils.asContextData(jobId))) {
                    LOG.warn("Could not fulfill resource requirements of job {}.", jobId);
                    resourceEventListener.notEnoughResourceAvailable(
                            jobId, resourceTracker.getAcquiredResources(jobId));
                }
            }
        }

        if (resourceAllocator.isSupported()) {
            checkResourcesNeedReconcile();
            declareNeededResourcesWithDelay();
        }
    }

    private void logMissingAndAvailableResource(
            Map<JobID, Collection<ResourceRequirement>> missingResources) {
        final StringJoiner lines = new StringJoiner(System.lineSeparator());
        lines.add("Matching resource requirements against available resources.");
        lines.add("Missing resources:");
        missingResources.forEach(
                (jobId, resources) -> {
                    lines.add("\t Job " + jobId);
                    resources.forEach(resource -> lines.add(String.format("\t\t%s", resource)));
                });
        lines.add("Current resources:");
        if (taskManagerTracker.getRegisteredTaskManagers().isEmpty()) {
            lines.add("\t(none)");
        } else {
            for (TaskManagerInfo taskManager : taskManagerTracker.getRegisteredTaskManagers()) {
                final ResourceID resourceId =
                        taskManager.getTaskExecutorConnection().getResourceID();
                lines.add("\tTaskManager " + resourceId);
                lines.add("\t\tAvailable: " + taskManager.getAvailableResource());
                lines.add("\t\tTotal:     " + taskManager.getTotalResource());
            }
        }
        LOG.info(lines.toString());
    }

    private void allocateSlotsAccordingTo(Map<JobID, Map<InstanceID, ResourceCounter>> result) {
        final List<CompletableFuture<Void>> allocationFutures = new ArrayList<>();
        for (Map.Entry<JobID, Map<InstanceID, ResourceCounter>> jobEntry : result.entrySet()) {
            final JobID jobID = jobEntry.getKey();
            for (Map.Entry<InstanceID, ResourceCounter> tmEntry : jobEntry.getValue().entrySet()) {
                final InstanceID instanceID = tmEntry.getKey();
                for (Map.Entry<ResourceProfile, Integer> slotEntry :
                        tmEntry.getValue().getResourcesWithCount()) {
                    for (int i = 0; i < slotEntry.getValue(); ++i) {
                        allocationFutures.add(
                                //
                                slotStatusSyncer.allocateSlot(
                                        instanceID,
                                        jobID,
                                        jobMasterTargetAddresses.get(jobID),
                                        slotEntry.getKey()));
                    }
                }
            }
        }
        FutureUtils.combineAll(allocationFutures)
                .whenCompleteAsync(
                        (s, t) -> {
                            if (t != null) {
                                // If there is allocation failure, we need to trigger it again.
                                checkResourceRequirementsWithDelay();
                            }
                        },
                        mainThreadExecutor);
    }

    /**
     * Allocate pending task managers, returns the ids of pending task managers that can not be
     * allocated.
     */
    private Set<PendingTaskManagerId> allocateTaskManagersAccordingTo(
            List<PendingTaskManager> pendingTaskManagers) {
        Preconditions.checkState(resourceAllocator.isSupported());
        final Set<PendingTaskManagerId> failedAllocations = new HashSet<>();
        for (PendingTaskManager pendingTaskManager : pendingTaskManagers) {
            if (!allocateResource(pendingTaskManager)) {
                failedAllocations.add(pendingTaskManager.getPendingTaskManagerId());
            }
        }
        return failedAllocations;
    }

    // ---------------------------------------------------------------------------------------------
    // Legacy APIs
    // ---------------------------------------------------------------------------------------------

    @Override
    public int getNumberRegisteredSlots() {
        return taskManagerTracker.getNumberRegisteredSlots();
    }

    @Override
    public int getNumberRegisteredSlotsOf(InstanceID instanceId) {
        return taskManagerTracker.getNumberRegisteredSlotsOf(instanceId);
    }

    @Override
    public int getNumberFreeSlots() {
        return taskManagerTracker.getNumberFreeSlots();
    }

    @Override
    public int getNumberFreeSlotsOf(InstanceID instanceId) {
        return taskManagerTracker.getNumberFreeSlotsOf(instanceId);
    }

    @Override
    public ResourceProfile getRegisteredResource() {
        return taskManagerTracker.getRegisteredResource();
    }

    @Override
    public ResourceProfile getRegisteredResourceOf(InstanceID instanceID) {
        return taskManagerTracker.getRegisteredResourceOf(instanceID);
    }

    @Override
    public ResourceProfile getFreeResource() {
        return taskManagerTracker.getFreeResource();
    }

    @Override
    public ResourceProfile getFreeResourceOf(InstanceID instanceID) {
        return taskManagerTracker.getFreeResourceOf(instanceID);
    }

    @Override
    public Collection<SlotInfo> getAllocatedSlotsOf(InstanceID instanceID) {
        return taskManagerTracker
                .getRegisteredTaskManager(instanceID)
                .map(TaskManagerInfo::getAllocatedSlots)
                .map(Map::values)
                .orElse(Collections.emptyList())
                .stream()
                .map(slot -> new SlotInfo(slot.getJobId(), slot.getResourceProfile()))
                .collect(Collectors.toList());
    }

    // ---------------------------------------------------------------------------------------------
    // Internal periodic check methods
    // ---------------------------------------------------------------------------------------------

    private void checkClusterReconciliation() {
        if (checkResourcesNeedReconcile()) {
            // only declare on needed.
            declareNeededResourcesWithDelay();
        }
    }

    private boolean checkResourcesNeedReconcile() {
        ResourceReconcileResult reconcileResult = resourceAllocationStrategy.tryReconcileClusterResources(taskManagerTracker);

        reconcileResult.getPendingTaskManagersToRelease().stream()
                .map(PendingTaskManager::getPendingTaskManagerId)
                .forEach(taskManagerTracker::removePendingTaskManager);

        for (TaskManagerInfo taskManagerToRelease : reconcileResult.getTaskManagersToRelease()) {
            releaseIdleTaskExecutorIfPossible(taskManagerToRelease);
        }

        reconcileResult.getPendingTaskManagersToAllocate().forEach(this::allocateResource);

        return reconcileResult.needReconcile();
    }

    private void releaseIdleTaskExecutorIfPossible(TaskManagerInfo taskManagerInfo) {
        final long idleSince = taskManagerInfo.getIdleSince();
        taskManagerInfo
                .getTaskExecutorConnection()
                .getTaskExecutorGateway()
                .canBeReleased()
                .thenAcceptAsync(
                        canBeReleased -> {
                            boolean stillIdle = idleSince == taskManagerInfo.getIdleSince();
                            if (stillIdle && canBeReleased) {
                                releaseIdleTaskExecutor(taskManagerInfo.getInstanceId());
                                declareNeededResourcesWithDelay();
                            }
                        },
                        mainThreadExecutor);
    }

    private void releaseIdleTaskExecutor(InstanceID taskManagerToRelease) {
        Preconditions.checkState(resourceAllocator.isSupported());
        taskManagerTracker.addUnWantedTaskManager(taskManagerToRelease);
    }

    private boolean allocateResource(PendingTaskManager pendingTaskManager) {
        Preconditions.checkState(resourceAllocator.isSupported());
        if (isMaxTotalResourceExceededAfterAdding(pendingTaskManager.getTotalResourceProfile())) {
            LOG.info(
                    "Could not allocate {}. Max total resource limitation <{}, {}> is reached.",
                    pendingTaskManager,
                    maxTotalCpu,
                    maxTotalMem.toHumanReadableString());
            return false;
        }
        //
        taskManagerTracker.addPendingTaskManager(pendingTaskManager);
        return true;
    }

    @VisibleForTesting
    public long getTaskManagerIdleSince(InstanceID instanceId) {
        return taskManagerTracker
                .getRegisteredTaskManager(instanceId)
                .map(TaskManagerInfo::getIdleSince)
                .orElse(0L);
    }

    // ---------------------------------------------------------------------------------------------
    // Internal utility methods
    // ---------------------------------------------------------------------------------------------

    private void checkInit() {
        Preconditions.checkState(started, "The slot manager has not been started.");
        Preconditions.checkNotNull(resourceManagerId);
        Preconditions.checkNotNull(mainThreadExecutor);
        Preconditions.checkNotNull(resourceAllocator);
        Preconditions.checkNotNull(resourceEventListener);
    }

    private boolean isMaxTotalResourceExceededAfterAdding(ResourceProfile newResource) {
        final ResourceProfile totalResourceAfterAdding =
                newResource
                        .merge(taskManagerTracker.getRegisteredResource())
                        .merge(taskManagerTracker.getPendingResource());
        return totalResourceAfterAdding.getCpuCores().compareTo(maxTotalCpu) > 0
                || totalResourceAfterAdding.getTotalMemory().compareTo(maxTotalMem) > 0;
    }

    private boolean isBlockedTaskManager(ResourceID resourceID) {
        Preconditions.checkNotNull(blockedTaskManagerChecker);
        return blockedTaskManagerChecker.isBlockedTaskManager(resourceID);
    }
}
