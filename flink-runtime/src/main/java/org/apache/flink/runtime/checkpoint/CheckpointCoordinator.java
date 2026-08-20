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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.FinishedTaskStateProvider.PartialFinishingNotSupportedByStateException;
import org.apache.flink.runtime.checkpoint.hooks.MasterHooks;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorInfo;
import org.apache.flink.runtime.persistence.PossibleInconsistentStateException;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageCoordinatorView;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.MdcUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;
import static org.apache.flink.runtime.checkpoint.CheckpointType.CHECKPOINT;
import static org.apache.flink.runtime.checkpoint.CheckpointType.FULL_CHECKPOINT;
import static org.apache.flink.util.ExceptionUtils.findThrowable;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The checkpoint coordinator coordinates the distributed snapshots of operators and state. It
 * triggers the checkpoint by sending the messages to the relevant tasks and collects the checkpoint
 * acknowledgements. It also collects and maintains the overview of the state handles reported by
 * the tasks that acknowledge the checkpoint.
 */
public class CheckpointCoordinator {

    private static final Logger LOG = LoggerFactory.getLogger(CheckpointCoordinator.class);

    /** The number of recent checkpoints whose IDs are remembered. */
    private static final int NUM_GHOST_CHECKPOINT_IDS = 16;

    // ------------------------------------------------------------------------

    /** Coordinator-wide lock to safeguard the checkpoint updates. */
    private final Object lock = new Object();

    /** The job whose checkpoint this coordinator coordinates. */
    private final JobID job;

    /** Default checkpoint properties. */
    //静态配置参数集合。定义了当前检查点是普通的 Checkpoint 还是 Savepoint、作业取消时是否保留、是强一致还是弱一致等策略
    private final CheckpointProperties checkpointProperties;

    /** The executor used for asynchronous calls, like potentially blocking I/O. */
    //用于执行非阻塞、异步调用的线程池。如异步去清除过期的检查点、写元数据等可能发生 I/O 阻塞的操作，防止阻塞 JobManager 的主线程
    private final Executor executor;

    //专职垃圾回收。在检查点过期、作业被取消、或某个检查点因失败被废弃时，它负责在后台异步清理不再需要的状态数据（文件）
    private final CheckpointsCleaner checkpointsCleaner;

    /** The operator coordinators that need to be checkpointed. */
    private final Collection<OperatorCoordinatorCheckpointContext> coordinatorsToCheckpoint;

    /** Map from checkpoint ID to the pending checkpoint. */
    //存放当前“正在进行中（尚未完成）”的检查点映射（Key 是检查点 ID，Value 是 PendingCheckpoint）。
    // 当所有的 Operator 反馈 ACK 确认后，对应的项会转化为 CompletedCheckpoint 并移交
    @GuardedBy("lock")
    private final Map<Long, PendingCheckpoint> pendingCheckpoints;

    /**
     * Completed checkpoints. Implementations can be blocking. Make sure calls to methods accessing
     * this don't block the job manager actor and run asynchronously.
     */
    //用于存储已经成功完成的检查点（CompletedCheckpoint）。Flink 在做故障恢复（Recovery）时，会从这里获取最新的可用检查点进行状态恢复
    private final CompletedCheckpointStore completedCheckpointStore;

    /**
     * The root checkpoint state backend, which is responsible for initializing the checkpoint,
     * storing the metadata, and cleaning up the checkpoint.
     */
    //检查点存储的根视图。负责在 JobManager 端初始化检查点（如分配检查点在分布式存储中的元数据路径）、存储全局元数据，以及在检查点失败时清理目录
    private final CheckpointStorageCoordinatorView checkpointStorageView;

    /** A list of recent expired checkpoint IDs, to identify late messages (vs invalid ones). */
    //最近刚过期的检查点 ID 队列。用来识别“迟到的消息”。
    //当一个 Task 慢吞吞地发送 ACK 回来时，协调器可以通过它判断这个 ACK 是由于检查点刚好过期导致的，还是由于完全无效的错误导致的
    private final ArrayDeque<Long> recentExpiredCheckpoints;

    /**
     * Checkpoint ID counter to ensure ascending IDs. In case of job manager failures, these need to
     * be ascending across job managers.
     */
    //全局检查点 ID（Checkpoint ID）计数器。它负责生成全局递增且唯一的 ID。即使 JobManager 发生故障切换（HA），也能保证新 Master 生成的 ID 依然严格递增
    private final CheckpointIDCounter checkpointIdCounter;

    /**
     * The checkpoint interval when there is no source reporting isProcessingBacklog=true. Actual
     * trigger time may be affected by the max concurrent checkpoints, minimum-pause values and
     * checkpoint interval during backlog.
     */
    private final long baseInterval;

    /**
     * The checkpoint interval when any source reports isProcessingBacklog=true. Actual trigger time
     * may be affected by the max concurrent checkpoints and minimum-pause values.
     */
    private final long baseIntervalDuringBacklog;

    /** The max time (in ms) that a checkpoint may take. */
    //超时限制。如果在规定时间内（如配置的 10 分钟）pendingCheckpoints 中的某个检查点没有收集齐所有 ACK，就判定为超时失败
    private final long checkpointTimeout;

    /**
     * The min time(in ms) to delay after a checkpoint could be triggered. Allows to enforce minimum
     * processing time between checkpoint attempts
     */
    //最小暂停时间。强制限制两个检查点触发动作之间的最小间隔。防止前一个检查点耗时太长、刚结束就立马触发下一个，导致 Task 被检查点拖死
    private final long minPauseBetweenCheckpoints;

    /**
     * The timer that handles the checkpoint timeouts and triggers periodic checkpoints. It must be
     * single-threaded. Eventually it will be replaced by main thread executor.
     */
    //单线程定时器。负责到点后触发周期性检查点，以及处理检查点超时事件
    private final ScheduledExecutor timer;

    /** The master checkpoint hooks executed by this checkpoint coordinator. */
    //Master 端的检查点钩子。在检查点触发/恢复时，
    //如果作业使用了一些第三方外部组件（例如某些外部文件系统或特殊 Connector），这些钩子可以先于算子在 JobManager 端做一些全局的预准备或恢复工作
    private final HashMap<String, MasterTriggerRestoreHook<?>> masterHooks;

    //是否开启了“非对齐检查点”（Unaligned Checkpoints）。开启后，Barrier 可以超越通道中的缓冲数据，在大反压场景下极大地提高检查点成功率
    private final boolean unalignedCheckpointsEnabled;

    //对齐超时时间。Flink 支持从“对齐CP”自动切换到“非对齐CP”。如果一个检查点在对齐状态下卡了超过这个时间，就会自动降级为非对齐检查点继续进行
    private final long alignedCheckpointTimeout;

    /** Actor that receives status updates from the execution graph this coordinator works for. */
    //作业状态监听器。用于感知当前 Job 状态的变化（如 RUNNING、FAILED、CANCELED），从而决定何时开启或彻底关闭定时检查点
    private JobStatusListener jobStatusListener;

    /**
     * The current periodic trigger. Used to deduplicate concurrently scheduled checkpoints if any.
     */
    @GuardedBy("lock")
    //当前正在生效的定时触发器及其句柄 用于取消或去重并发调度的检查点请求
    private ScheduledTrigger currentPeriodicTrigger;

    /** A handle to the current periodic trigger, to cancel it when necessary. */
    @GuardedBy("lock")
    private ScheduledFuture<?> currentPeriodicTriggerFuture;

    /**
     * The timestamp (via {@link Clock#relativeTimeMillis()}) when the next checkpoint will be
     * triggered.
     *
     * <p>If it's value is {@link Long#MAX_VALUE}, it means there is not a next checkpoint
     * scheduled.
     */
    //记录了下一次预计触发检查点的相对时间戳，用于精准计算定时和最小间隔
    @GuardedBy("lock")
    private long nextCheckpointTriggeringRelativeTime;

    /**
     * The timestamp (via {@link Clock#relativeTimeMillis()}) when the last checkpoint completed.
     */
    //记录了上一次成功完成检查点的相对时间戳，用于精准计算定时和最小间隔
    private long lastCheckpointCompletionRelativeTime;

    /**
     * Flag whether a triggered checkpoint should immediately schedule the next checkpoint.
     * Non-volatile, because only accessed in synchronized scope
     */
    //是否启用周期性自动触发检查点的标记
    private boolean periodicScheduling;

    /** Flag marking the coordinator as shut down (not accepting any messages any more). */
    //协调器是否已关闭的标志。如果作业被取消或失败，该值变为 true，此时协调器将不再接受、触发或处理任何新的检查点消息
    private volatile boolean shutdown;

    /** Optional tracker for checkpoint statistics. */
    //检查点统计跟踪器。你在 Flink UI 界面上看到的那些 Checkpoint 历史记录、成功/失败次数、端到端延迟、对齐时间等监控数据，全部是由它来收集和维护的
    private final CheckpointStatsTracker statsTracker;

    private final BiFunction<
                    Set<ExecutionJobVertex>,
                    Map<OperatorID, OperatorState>,
                    VertexFinishedStateChecker>
            vertexFinishedStateCheckerFactory;

    /** Id of checkpoint for which in-flight data should be ignored on recovery. */
    private final long checkpointIdOfIgnoredInFlightData;

    private final CheckpointFailureManager failureManager;

    private final Clock clock;

    private final boolean isExactlyOnceMode;

    /** Flag represents there is an in-flight trigger request. */
    private boolean isTriggering = false;

    private final CheckpointRequestDecider requestDecider;

    private final CheckpointPlanCalculator checkpointPlanCalculator;

    /** IDs of the source operators that are currently processing backlog. */
    @GuardedBy("lock")
    private final Set<OperatorID> backlogOperators = new HashSet<>();

    private boolean baseLocationsForCheckpointInitialized = false;

    private boolean forceFullSnapshot;

    // --------------------------------------------------------------------------------------------

    public CheckpointCoordinator(//
            JobID job,
            CheckpointCoordinatorConfiguration chkConfig,
            Collection<OperatorCoordinatorCheckpointContext> coordinatorsToCheckpoint,
            CheckpointIDCounter checkpointIDCounter,
            CompletedCheckpointStore completedCheckpointStore,
            CheckpointStorage checkpointStorage,
            Executor executor,
            CheckpointsCleaner checkpointsCleaner,
            ScheduledExecutor timer,
            CheckpointFailureManager failureManager,
            CheckpointPlanCalculator checkpointPlanCalculator,
            CheckpointStatsTracker statsTracker) {

        this(
                job,
                chkConfig,
                coordinatorsToCheckpoint,
                checkpointIDCounter,
                completedCheckpointStore,
                checkpointStorage,
                executor,
                checkpointsCleaner,
                timer,
                failureManager,
                checkpointPlanCalculator,
                SystemClock.getInstance(),
                statsTracker,
                VertexFinishedStateChecker::new);
    }

    @VisibleForTesting
    public CheckpointCoordinator(
            JobID job,
            CheckpointCoordinatorConfiguration chkConfig,
            Collection<OperatorCoordinatorCheckpointContext> coordinatorsToCheckpoint,
            CheckpointIDCounter checkpointIDCounter,
            CompletedCheckpointStore completedCheckpointStore,
            CheckpointStorage checkpointStorage,
            Executor executor,
            CheckpointsCleaner checkpointsCleaner,
            ScheduledExecutor timer,
            CheckpointFailureManager failureManager,
            CheckpointPlanCalculator checkpointPlanCalculator,
            Clock clock,
            CheckpointStatsTracker statsTracker,
            BiFunction<
                            Set<ExecutionJobVertex>,
                            Map<OperatorID, OperatorState>,
                            VertexFinishedStateChecker>
                    vertexFinishedStateCheckerFactory) {

        // sanity checks
        checkNotNull(checkpointStorage);

        // max "in between duration" can be one year - this is to prevent numeric overflows
        long minPauseBetweenCheckpoints = chkConfig.getMinPauseBetweenCheckpoints();
        if (minPauseBetweenCheckpoints > 365L * 24 * 60 * 60 * 1_000) {
            minPauseBetweenCheckpoints = 365L * 24 * 60 * 60 * 1_000;
        }

        // it does not make sense to schedule checkpoints more often then the desired
        // time between checkpoints
        long baseInterval = chkConfig.getCheckpointInterval();
        if (baseInterval < minPauseBetweenCheckpoints) {
            baseInterval = minPauseBetweenCheckpoints;
        }

        this.job = checkNotNull(job);
        this.baseInterval = baseInterval;
        this.baseIntervalDuringBacklog = chkConfig.getCheckpointIntervalDuringBacklog();
        this.nextCheckpointTriggeringRelativeTime = Long.MAX_VALUE;
        this.checkpointTimeout = chkConfig.getCheckpointTimeout();
        this.minPauseBetweenCheckpoints = minPauseBetweenCheckpoints;
        this.coordinatorsToCheckpoint =
                Collections.unmodifiableCollection(coordinatorsToCheckpoint);
        this.pendingCheckpoints = new LinkedHashMap<>();
        this.checkpointIdCounter = checkNotNull(checkpointIDCounter);
        this.completedCheckpointStore = checkNotNull(completedCheckpointStore);
        this.executor = checkNotNull(executor);
        this.checkpointsCleaner = checkNotNull(checkpointsCleaner);
        this.failureManager = checkNotNull(failureManager);
        this.checkpointPlanCalculator = checkNotNull(checkpointPlanCalculator);
        this.clock = checkNotNull(clock);
        this.isExactlyOnceMode = chkConfig.isExactlyOnce();
        this.unalignedCheckpointsEnabled = chkConfig.isUnalignedCheckpointsEnabled();
        this.alignedCheckpointTimeout = chkConfig.getAlignedCheckpointTimeout();
        this.checkpointIdOfIgnoredInFlightData = chkConfig.getCheckpointIdOfIgnoredInFlightData();

        this.recentExpiredCheckpoints = new ArrayDeque<>(NUM_GHOST_CHECKPOINT_IDS);
        this.masterHooks = new HashMap<>();

        this.timer = timer;

        this.checkpointProperties =
                CheckpointProperties.forCheckpoint(chkConfig.getCheckpointRetentionPolicy());

        try {
            this.checkpointStorageView = checkpointStorage.createCheckpointStorage(job);

            if (isPeriodicCheckpointingConfigured()) {
                checkpointStorageView.initializeBaseLocationsForCheckpoint();
                baseLocationsForCheckpointInitialized = true;
            }
        } catch (IOException e) {
            throw new FlinkRuntimeException(
                    "Failed to create checkpoint storage at checkpoint coordinator side.", e);
        }

        try {
            // Make sure the checkpoint ID enumerator is running. Possibly
            // issues a blocking call to ZooKeeper.
            checkpointIDCounter.start();
        } catch (Throwable t) {
            throw new RuntimeException(
                    "Failed to start checkpoint ID counter: " + t.getMessage(), t);
        }
        this.requestDecider =
                new CheckpointRequestDecider(
                        chkConfig.getMaxConcurrentCheckpoints(),
                        this::rescheduleTrigger,
                        this.clock,
                        this.minPauseBetweenCheckpoints,
                        this.pendingCheckpoints::size,
                        this.checkpointsCleaner::getNumberOfCheckpointsToClean);
        this.statsTracker = checkNotNull(statsTracker, "Statistic tracker can not be null");
        this.vertexFinishedStateCheckerFactory = checkNotNull(vertexFinishedStateCheckerFactory);
    }

    // --------------------------------------------------------------------------------------------
    //  Configuration
    // --------------------------------------------------------------------------------------------

    /**
     * Adds the given master hook to the checkpoint coordinator. This method does nothing, if the
     * checkpoint coordinator already contained a hook with the same ID (as defined via {@link
     * MasterTriggerRestoreHook#getIdentifier()}).
     *
     * @param hook The hook to add.
     * @return True, if the hook was added, false if the checkpoint coordinator already contained a
     *     hook with the same ID.
     */
    public boolean addMasterHook(MasterTriggerRestoreHook<?> hook) {
        checkNotNull(hook);

        final String id = hook.getIdentifier();
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(id), "The hook has a null or empty id");

        synchronized (lock) {
            if (!masterHooks.containsKey(id)) {
                masterHooks.put(id, hook);
                return true;
            } else {
                return false;
            }
        }
    }

    /** Gets the number of currently register master hooks. */
    public int getNumberOfRegisteredMasterHooks() {
        synchronized (lock) {
            return masterHooks.size();
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Clean shutdown
    // --------------------------------------------------------------------------------------------

    /**
     * Shuts down the checkpoint coordinator.
     *
     * <p>After this method has been called, the coordinator does not accept and further messages
     * and cannot trigger any further checkpoints.
     */
    public void shutdown() throws Exception {
        synchronized (lock) {
            if (!shutdown) {
                shutdown = true;
                LOG.info("Stopping checkpoint coordinator for job {}.", job);

                periodicScheduling = false;

                // shut down the hooks
                MasterHooks.close(masterHooks.values(), LOG);
                masterHooks.clear();

                final CheckpointException reason =
                        new CheckpointException(
                                CheckpointFailureReason.CHECKPOINT_COORDINATOR_SHUTDOWN);
                // clear queued requests and in-flight checkpoints
                abortPendingAndQueuedCheckpoints(reason);
            }
        }
    }

    public boolean isShutdown() {
        return shutdown;
    }

    /**
     * Reports whether a source operator is currently processing backlog.
     *
     * <p>If any source operator is processing backlog, the checkpoint interval would be decided by
     * {@code execution.checkpointing.interval-during-backlog} instead of {@code
     * execution.checkpointing.interval}.
     *
     * <p>If a source has not invoked this method, the source is considered to have
     * isProcessingBacklog=false. If a source operator has invoked this method multiple times, the
     * last reported value is used.
     *
     * @param operatorID the operator ID of the source operator.
     * @param isProcessingBacklog whether the source operator is processing backlog.
     */
    public void setIsProcessingBacklog(OperatorID operatorID, boolean isProcessingBacklog) {
        synchronized (lock) {
            if (isProcessingBacklog) {
                backlogOperators.add(operatorID);
            } else {
                backlogOperators.remove(operatorID);
            }

            long currentCheckpointInterval = getCurrentCheckpointInterval();
            if (currentCheckpointInterval
                    != CheckpointCoordinatorConfiguration.DISABLED_CHECKPOINT_INTERVAL) {
                long currentRelativeTime = clock.relativeTimeMillis();
                if (currentRelativeTime + currentCheckpointInterval
                        < nextCheckpointTriggeringRelativeTime) {
                    rescheduleTrigger(currentRelativeTime, currentCheckpointInterval);
                }
            }
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Triggering Checkpoints and Savepoints
    // --------------------------------------------------------------------------------------------

    /**
     * Triggers a savepoint with the given savepoint directory as a target.
     *
     * @param targetLocation Target location for the savepoint, optional. If null, the state
     *     backend's configured default will be used.
     * @return A future to the completed checkpoint
     * @throws IllegalStateException If no savepoint directory has been specified and no default
     *     savepoint directory has been configured
     */
    public CompletableFuture<CompletedCheckpoint> triggerSavepoint(
            @Nullable final String targetLocation, final SavepointFormatType formatType) {
        final CheckpointProperties properties =
                CheckpointProperties.forSavepoint(!unalignedCheckpointsEnabled, formatType);
        return triggerSavepointInternal(properties, targetLocation);
    }

    /**
     * Triggers a synchronous savepoint with the given savepoint directory as a target.
     *
     * @param terminate flag indicating if the job should terminate or just suspend
     * @param targetLocation Target location for the savepoint, optional. If null, the state
     *     backend's configured default will be used.
     * @return A future to the completed checkpoint
     * @throws IllegalStateException If no savepoint directory has been specified and no default
     *     savepoint directory has been configured
     */
    public CompletableFuture<CompletedCheckpoint> triggerSynchronousSavepoint(
            final boolean terminate,
            @Nullable final String targetLocation,
            SavepointFormatType formatType) {

        final CheckpointProperties properties =
                CheckpointProperties.forSyncSavepoint(
                        !unalignedCheckpointsEnabled, terminate, formatType);

        return triggerSavepointInternal(properties, targetLocation);
    }

    private CompletableFuture<CompletedCheckpoint> triggerSavepointInternal(
            final CheckpointProperties checkpointProperties,
            @Nullable final String targetLocation) {

        checkNotNull(checkpointProperties);

        return triggerCheckpointFromCheckpointThread(checkpointProperties, targetLocation, false);
    }

    private CompletableFuture<CompletedCheckpoint> triggerCheckpointFromCheckpointThread(
            CheckpointProperties checkpointProperties, String targetLocation, boolean isPeriodic) {
        // TODO, call triggerCheckpoint directly after removing timer thread
        // for now, execute the trigger in timer thread to avoid competition
        final CompletableFuture<CompletedCheckpoint> resultFuture = new CompletableFuture<>();
        timer.execute(
                () ->
                        triggerCheckpoint(checkpointProperties, targetLocation, isPeriodic)
                                .whenComplete(
                                        (completedCheckpoint, throwable) -> {
                                            if (throwable == null) {
                                                resultFuture.complete(completedCheckpoint);
                                            } else {
                                                resultFuture.completeExceptionally(throwable);
                                            }
                                        }));
        return resultFuture;
    }

    /**
     * Triggers a new standard checkpoint and uses the given timestamp as the checkpoint timestamp.
     * The return value is a future. It completes when the checkpoint triggered finishes or an error
     * occurred.
     *
     * @param isPeriodic Flag indicating whether this triggered checkpoint is periodic.
     * @return a future to the completed checkpoint.
     */
    public CompletableFuture<CompletedCheckpoint> triggerCheckpoint(boolean isPeriodic) {
        return triggerCheckpointFromCheckpointThread(checkpointProperties, null, isPeriodic);
    }

    /**
     * Triggers one new checkpoint with the given checkpointType. The returned future completes when
     * the triggered checkpoint finishes or an error occurred.
     *
     * @param checkpointType specifies the backup type of the checkpoint to trigger.
     * @return a future to the completed checkpoint.
     */
    public CompletableFuture<CompletedCheckpoint> triggerCheckpoint(CheckpointType checkpointType) {

        if (checkpointType == null) {
            throw new IllegalArgumentException("checkpointType cannot be null");
        }

        final SnapshotType snapshotType;
        switch (checkpointType) {
            case CONFIGURED:
                snapshotType = checkpointProperties.getCheckpointType();
                break;
            case FULL:
                snapshotType = FULL_CHECKPOINT;
                break;
            case INCREMENTAL:
                snapshotType = CHECKPOINT;
                break;
            default:
                throw new IllegalArgumentException("unknown checkpointType: " + checkpointType);
        }

        final CheckpointProperties properties =
                new CheckpointProperties(
                        checkpointProperties.forceCheckpoint(),
                        snapshotType,
                        checkpointProperties.discardOnSubsumed(),
                        checkpointProperties.discardOnJobFinished(),
                        checkpointProperties.discardOnJobCancelled(),
                        checkpointProperties.discardOnJobFailed(),
                        checkpointProperties.discardOnJobSuspended(),
                        checkpointProperties.isUnclaimed());
        return triggerCheckpointFromCheckpointThread(properties, null, false);
    }

    @VisibleForTesting
    CompletableFuture<CompletedCheckpoint> triggerCheckpoint(
            CheckpointProperties props,
            @Nullable String externalSavepointLocation,
            boolean isPeriodic) {

        CheckpointTriggerRequest request = new CheckpointTriggerRequest(props, externalSavepointLocation, isPeriodic);//
        Optional<CheckpointTriggerRequest> checkpointTriggerRequest = chooseRequestToExecute(request);//
        checkpointTriggerRequest.ifPresent(this::startTriggeringCheckpoint);//
        return request.onCompletionPromise;
    }

    private void startTriggeringCheckpoint(CheckpointTriggerRequest request) {
        try {
            synchronized (lock) {
                //进入方法后，首先进入全局唯一的 lock 临界区。preCheckGlobalState 会检查当前作业状态（是否处于 RUNNING，是否超过最大并发 Checkpoint 数等）进入方法后，
                // 首先进入全局唯一的 lock 临界区。
                // preCheckGlobalState 会检查当前作业状态（是否处于 RUNNING，是否超过最大并发 Checkpoint 数等）
                preCheckGlobalState(request.isPeriodic);//request.isPeriodic =
            }

            // we will actually trigger this checkpoint!
            //通过布尔标志位 isTriggering = true 进行强校验，确保在上一次触发的异步链条完全结束前，不会有第二个线程进来同时执行触发逻辑
            Preconditions.checkState(!isTriggering);
            isTriggering = true;

            final long timestamp = System.currentTimeMillis();
            //不阻塞，直接获取一个计算计划的 Future。该计划决定了当前哪些 Task（如 Source）需要接收 Barrier，哪些需要等待 ACK
            CompletableFuture<CheckpointPlan> checkpointPlanFuture = checkpointPlanCalculator.calculateCheckpointPlan();

            boolean initializeBaseLocations = !baseLocationsForCheckpointInitialized;
            baseLocationsForCheckpointInitialized = true;

            CompletableFuture<Void> masterTriggerCompletionPromise = new CompletableFuture<>();
            //申请 ID 与创建 Pending 账本
            final CompletableFuture<PendingCheckpoint> pendingCheckpointCompletableFuture =
                    checkpointPlanFuture
                            .thenApplyAsync(
                                    plan -> {
                                        try {
                                            // this must happen outside the coordinator-wide lock,
                                            // because it communicates with external services
                                            // (in HA mode) and may block for a while.
                                            //需要网络通信，严禁在 Coordinator 全局锁或主线程中执行
                                            long checkpointID = checkpointIdCounter.getAndIncrement();
                                            return new Tuple2<>(plan, checkpointID);
                                        } catch (Throwable e) {
                                            throw new CompletionException(e);
                                        }
                                    },
                                    executor)
                            .thenApplyAsync(
                                    (checkpointInfo) -> createPendingCheckpoint(
                                                    timestamp,
                                                    request.props,
                                                    checkpointInfo.f0,
                                                    request.isPeriodic,
                                                    checkpointInfo.f1,
                                                    request.getOnCompletionFuture(),
                                                    masterTriggerCompletionPromise),
                                    timer);

            final CompletableFuture<?> coordinatorCheckpointsComplete =
                    pendingCheckpointCompletableFuture
                            .thenApplyAsync(
                                    pendingCheckpoint -> {
                                        try {
                                            //这涉及到在远程文件系统（如 S3、HDFS）上创建本次 Checkpoint 的专属目录。由于是长耗时 I/O，必须异步
                                            CheckpointStorageLocation checkpointStorageLocation = initializeCheckpointLocation(
                                                            pendingCheckpoint.getCheckpointID(),
                                                            request.props,
                                                            request.externalSavepointLocation,
                                                            initializeBaseLocations);
                                            return Tuple2.of(pendingCheckpoint, checkpointStorageLocation);
                                        } catch (Throwable e) {
                                            throw new CompletionException(e);
                                        }
                                    },
                                    executor)
                            .thenComposeAsync(
                                    (checkpointInfo) -> {
                                        PendingCheckpoint pendingCheckpoint = checkpointInfo.f0;
                                        if (pendingCheckpoint.isDisposed()) {
                                            // The disposed checkpoint will be handled later,
                                            // skip snapshotting the coordinator states.
                                            return null;
                                        }
                                        synchronized (lock) {
                                            pendingCheckpoint.setCheckpointTargetLocation(
                                                    checkpointInfo.f1);
                                        }
                                        return OperatorCoordinatorCheckpoints
                                                .triggerAndAcknowledgeAllCoordinatorCheckpointsWithCompletion(
                                                        coordinatorsToCheckpoint,
                                                        pendingCheckpoint,
                                                        timer);
                                    },
                                    timer);

            // We have to take the snapshot of the master hooks after the coordinator checkpoints
            // has completed.
            // This is to ensure the tasks are checkpointed after the OperatorCoordinators in case
            // ExternallyInducedSource is used.
            //MasterHook 状态快照
            final CompletableFuture<?> masterStatesComplete =
                    coordinatorCheckpointsComplete.thenComposeAsync(
                            ignored -> {
                                // If the code reaches here, the pending checkpoint is guaranteed to
                                // be not null.
                                // We use FutureUtils.getWithoutException() to make compiler happy
                                // with checked
                                // exceptions in the signature.
                                PendingCheckpoint checkpoint = FutureUtils.getWithoutException(pendingCheckpointCompletableFuture);
                                if (checkpoint == null || checkpoint.isDisposed()) {
                                    // The disposed checkpoint will be handled later,
                                    // skip snapshotting the master states.
                                    return null;
                                }
                                return snapshotMasterState(checkpoint);
                            },
                            timer);

            //将 Master 状态和 Coordinator 状态合并。只要两者都成功，masterTriggerCompletionPromise 就会被完成
            FutureUtils.forward(
                    CompletableFuture.allOf(masterStatesComplete, coordinatorCheckpointsComplete),
                    masterTriggerCompletionPromise);

            FutureUtils.assertNoException(
                    masterTriggerCompletionPromise
                            .handleAsync(
                                    (ignored, throwable) -> {
                                        final PendingCheckpoint checkpoint = FutureUtils.getWithoutException(pendingCheckpointCompletableFuture);

                                        Preconditions.checkState(
                                                checkpoint != null || throwable != null,
                                                "Either the pending checkpoint needs to be created or an error must have occurred.");

                                        if (throwable != null) {
                                            // 任何一个异步环节报错，走失败清理
                                            // the initialization might not be finished yet
                                            if (checkpoint == null) {
                                                onTriggerFailure(request, throwable);
                                            } else {
                                                onTriggerFailure(checkpoint, throwable);
                                            }
                                        } else {
                                            // 【重点】全部中心化快照成功，向 TaskManager 下发指令
                                            triggerCheckpointRequest(request, timestamp, checkpoint);//
                                        }
                                        return null;
                                    },
                                    timer)
                            .exceptionally(
                                    error -> {
                                        if (!isShutdown()) {
                                            throw new CompletionException(error);
                                        } else if (findThrowable(
                                                        error, RejectedExecutionException.class)
                                                .isPresent()) {
                                            LOG.debug("Execution rejected during shutdown");
                                        } else {
                                            LOG.warn("Error encountered during shutdown", error);
                                        }
                                        return null;
                                    }));
        } catch (Throwable throwable) {
            onTriggerFailure(request, throwable);
        }
    }

    private void triggerCheckpointRequest(
            CheckpointTriggerRequest request, long timestamp, PendingCheckpoint checkpoint) {
        if (checkpoint.isDisposed()) {//false
            onTriggerFailure(
                    checkpoint,
                    new CheckpointException(
                            CheckpointFailureReason.TRIGGER_CHECKPOINT_FAILURE,
                            checkpoint.getFailureCause()));
        } else {
            //
            CompletableFuture<Void> triggerTasksFuture = triggerTasks(request, timestamp, checkpoint);
            triggerTasksFuture.exceptionally(
                            failure -> {
                                //出现异常
                                try (MdcUtils.MdcCloseable ignored =
                                        MdcUtils.withContext(MdcUtils.asContextData(job))) {
                                    LOG.info(
                                            "Triggering Checkpoint {} for job {} failed due to {}",
                                            checkpoint.getCheckpointID(),
                                            job,
                                            failure);
                                    final CheckpointException cause;
                                    if (failure instanceof CheckpointException) {
                                        cause = (CheckpointException) failure;
                                    } else {
                                        cause =
                                                new CheckpointException(
                                                        CheckpointFailureReason
                                                                .TRIGGER_CHECKPOINT_FAILURE,
                                                        failure);
                                    }
                                    timer.execute(
                                            () -> {
                                                synchronized (lock) {
                                                    abortPendingCheckpoint(checkpoint, cause);
                                                }
                                            });
                                    return null;
                                }
                            });

            // It is possible that the tasks has finished
            // checkpointing at this point.
            // So we need to complete this pending checkpoint.
            if (maybeCompleteCheckpoint(checkpoint)) {
                onTriggerSuccess();
            }
        }
    }

    private CompletableFuture<Void> triggerTasks(
            CheckpointTriggerRequest request, long timestamp, PendingCheckpoint checkpoint) {
        // no exception, no discarding, everything is OK
        final long checkpointId = checkpoint.getCheckpointID();

        final SnapshotType type;
        if (this.forceFullSnapshot && !request.props.isSavepoint()) {
            type = FULL_CHECKPOINT;
        } else {
            type = request.props.getCheckpointType();//
        }

        final CheckpointOptions checkpointOptions =
                CheckpointOptions.forConfig(
                        type,
                        checkpoint.getCheckpointStorageLocation().getLocationReference(),
                        isExactlyOnceMode,
                        unalignedCheckpointsEnabled,
                        alignedCheckpointTimeout);

        // send messages to the tasks to trigger their checkpoints
        List<CompletableFuture<Acknowledge>> acks = new ArrayList<>();
        //获取所有的 Source 算子实例
        for (Execution execution : checkpoint.getCheckpointPlan().getTasksToTrigger()) {
            if (request.props.isSynchronous()) {
                acks.add(
                        execution.triggerSynchronousSavepoint(
                                checkpointId, timestamp, checkpointOptions));
            } else {
                //触发请求
                CompletableFuture<Acknowledge> acknowledgeCompletableFuture = execution.triggerCheckpoint(checkpointId, timestamp, checkpointOptions);//
                acks.add(acknowledgeCompletableFuture);
            }
        }
        //acks size = 1
        return FutureUtils.waitForAll(acks);
    }

    /**
     * Initialize the checkpoint location asynchronously. It will be expected to be executed in io
     * thread due to it might be time-consuming.
     *
     * @param checkpointID checkpoint id
     * @param props checkpoint properties
     * @param externalSavepointLocation the external savepoint location, it might be null
     * @return the checkpoint location
     */
    private CheckpointStorageLocation initializeCheckpointLocation(
            long checkpointID,
            CheckpointProperties props,
            @Nullable String externalSavepointLocation,
            boolean initializeBaseLocations)
            throws Exception {
        final CheckpointStorageLocation checkpointStorageLocation;
        if (props.isSavepoint()) {
            checkpointStorageLocation =
                    checkpointStorageView.initializeLocationForSavepoint(
                            checkpointID, externalSavepointLocation);
        } else {
            if (initializeBaseLocations) {
                checkpointStorageView.initializeBaseLocationsForCheckpoint();
            }
            checkpointStorageLocation =
                    checkpointStorageView.initializeLocationForCheckpoint(checkpointID);
        }

        return checkpointStorageLocation;
    }

    private PendingCheckpoint createPendingCheckpoint(
            long timestamp,
            CheckpointProperties props,
            CheckpointPlan checkpointPlan,
            boolean isPeriodic,
            long checkpointID,
            CompletableFuture<CompletedCheckpoint> onCompletionPromise,
            CompletableFuture<Void> masterTriggerCompletionPromise) {

        synchronized (lock) {
            try {
                // since we haven't created the PendingCheckpoint yet, we need to check the
                // global state here.
                preCheckGlobalState(isPeriodic);
            } catch (Throwable t) {
                throw new CompletionException(t);
            }
        }

        PendingCheckpointStats pendingCheckpointStats =
                trackPendingCheckpointStats(checkpointID, checkpointPlan, props, timestamp);

        final PendingCheckpoint checkpoint =
                new PendingCheckpoint(
                        job,
                        checkpointID,
                        timestamp,
                        checkpointPlan,
                        OperatorInfo.getIds(coordinatorsToCheckpoint),
                        masterHooks.keySet(),
                        props,
                        onCompletionPromise,
                        pendingCheckpointStats,
                        masterTriggerCompletionPromise);

        synchronized (lock) {
            pendingCheckpoints.put(checkpointID, checkpoint);

            ScheduledFuture<?> cancellerHandle =
                    timer.schedule(
                            new CheckpointCanceller(checkpoint),
                            checkpointTimeout,
                            TimeUnit.MILLISECONDS);

            if (!checkpoint.setCancellerHandle(cancellerHandle)) {
                // checkpoint is already disposed!
                cancellerHandle.cancel(false);
            }
        }

        LOG.info(
                "Triggering checkpoint {} (type={}) @ {} for job {}.",
                checkpointID,
                checkpoint.getProps().getCheckpointType(),
                timestamp,
                job);
        return checkpoint;
    }

    /**
     * Snapshot master hook states asynchronously.
     *
     * @param checkpoint the pending checkpoint
     * @return the future represents master hook states are finished or not
     */
    private CompletableFuture<Void> snapshotMasterState(PendingCheckpoint checkpoint) {
        if (masterHooks.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        final long checkpointID = checkpoint.getCheckpointID();
        final long timestamp = checkpoint.getCheckpointTimestamp();

        final CompletableFuture<Void> masterStateCompletableFuture = new CompletableFuture<>();
        for (MasterTriggerRestoreHook<?> masterHook : masterHooks.values()) {
            MasterHooks.triggerHook(masterHook, checkpointID, timestamp, executor)
                    .whenCompleteAsync(
                            (masterState, throwable) -> {
                                try {
                                    synchronized (lock) {
                                        if (masterStateCompletableFuture.isDone()) {
                                            return;
                                        }
                                        if (checkpoint.isDisposed()) {
                                            throw new IllegalStateException(
                                                    "Checkpoint "
                                                            + checkpointID
                                                            + " has been discarded");
                                        }
                                        if (throwable == null) {
                                            checkpoint.acknowledgeMasterState(
                                                    masterHook.getIdentifier(), masterState);
                                            if (checkpoint.areMasterStatesFullyAcknowledged()) {
                                                masterStateCompletableFuture.complete(null);
                                            }
                                        } else {
                                            masterStateCompletableFuture.completeExceptionally(
                                                    throwable);
                                        }
                                    }
                                } catch (Throwable t) {
                                    masterStateCompletableFuture.completeExceptionally(t);
                                }
                            },
                            timer);
        }
        return masterStateCompletableFuture;
    }

    /** Trigger request is successful. NOTE, it must be invoked if trigger request is successful. */
    private void onTriggerSuccess() {
        isTriggering = false;
        executeQueuedRequest();
    }

    /**
     * The trigger request is failed prematurely without a proper initialization. There is no
     * resource to release, but the completion promise needs to fail manually here.
     *
     * @param onCompletionPromise the completion promise of the checkpoint/savepoint
     * @param throwable the reason of trigger failure
     */
    private void onTriggerFailure(
            CheckpointTriggerRequest onCompletionPromise, Throwable throwable) {
        final CheckpointException checkpointException =
                getCheckpointException(
                        CheckpointFailureReason.TRIGGER_CHECKPOINT_FAILURE, throwable);
        onCompletionPromise.completeExceptionally(checkpointException);
        onTriggerFailure((PendingCheckpoint) null, onCompletionPromise.props, checkpointException);
    }

    private void onTriggerFailure(PendingCheckpoint checkpoint, Throwable throwable) {
        checkArgument(checkpoint != null, "Pending checkpoint can not be null.");

        onTriggerFailure(checkpoint, checkpoint.getProps(), throwable);
    }

    /**
     * The trigger request is failed. NOTE, it must be invoked if trigger request is failed.
     *
     * @param checkpoint the pending checkpoint which is failed. It could be null if it's failed
     *     prematurely without a proper initialization.
     * @param throwable the reason of trigger failure
     */
    private void onTriggerFailure(
            @Nullable PendingCheckpoint checkpoint,
            CheckpointProperties checkpointProperties,
            Throwable throwable) {
        try {
            // beautify the stack trace a bit
            throwable = ExceptionUtils.stripCompletionException(throwable);

            coordinatorsToCheckpoint.forEach(
                    OperatorCoordinatorCheckpointContext::abortCurrentTriggering);

            final CheckpointException cause =
                    getCheckpointException(
                            CheckpointFailureReason.TRIGGER_CHECKPOINT_FAILURE, throwable);

            if (checkpoint != null && !checkpoint.isDisposed()) {
                synchronized (lock) {
                    abortPendingCheckpoint(checkpoint, cause);
                }
            } else {
                failureManager.handleCheckpointException(
                        checkpoint, checkpointProperties, cause, null, job, null, statsTracker);
            }
        } catch (Throwable secondThrowable) {
            secondThrowable.addSuppressed(throwable);
            throw secondThrowable;

        } finally {
            isTriggering = false;
            executeQueuedRequest();
        }
    }

    private void executeQueuedRequest() {
        chooseQueuedRequestToExecute().ifPresent(this::startTriggeringCheckpoint);
    }

    private Optional<CheckpointTriggerRequest> chooseQueuedRequestToExecute() {
        synchronized (lock) {
            return requestDecider.chooseQueuedRequestToExecute(
                    isTriggering, lastCheckpointCompletionRelativeTime);
        }
    }

    private Optional<CheckpointTriggerRequest> chooseRequestToExecute(
            CheckpointTriggerRequest request) {
        synchronized (lock) {
            Optional<CheckpointTriggerRequest> checkpointTriggerRequest =
                    requestDecider.chooseRequestToExecute(request, isTriggering, lastCheckpointCompletionRelativeTime);//
            return checkpointTriggerRequest;
        }
    }

    // Returns true if the checkpoint is successfully completed, false otherwise.
    private boolean maybeCompleteCheckpoint(PendingCheckpoint checkpoint) {
        synchronized (lock) {
            if (checkpoint.isFullyAcknowledged()) {
                try {
                    // we need to check inside the lock for being shutdown as well,
                    // otherwise we get races and invalid error log messages.
                    if (shutdown) {
                        return false;
                    }
                    completePendingCheckpoint(checkpoint);
                } catch (CheckpointException ce) {
                    onTriggerFailure(checkpoint, ce);
                    return false;
                }
            }
        }
        return true;
    }

    // --------------------------------------------------------------------------------------------
    //  Handling checkpoints and messages
    // --------------------------------------------------------------------------------------------

    /**
     * Receives a {@link DeclineCheckpoint} message for a pending checkpoint.
     *
     * @param message Checkpoint decline from the task manager
     * @param taskManagerLocationInfo The location info of the decline checkpoint message's sender
     */
    public void receiveDeclineMessage(DeclineCheckpoint message, String taskManagerLocationInfo) {
        if (shutdown || message == null) {
            return;
        }

        if (!job.equals(message.getJob())) {
            throw new IllegalArgumentException(
                    "Received DeclineCheckpoint message for job "
                            + message.getJob()
                            + " from "
                            + taskManagerLocationInfo
                            + " while this coordinator handles job "
                            + job);
        }

        final long checkpointId = message.getCheckpointId();
        final CheckpointException checkpointException =
                message.getSerializedCheckpointException().unwrap();
        final String reason = checkpointException.getMessage();

        PendingCheckpoint checkpoint;

        synchronized (lock) {
            // we need to check inside the lock for being shutdown as well, otherwise we
            // get races and invalid error log messages
            if (shutdown) {
                return;
            }

            checkpoint = pendingCheckpoints.get(checkpointId);

            if (checkpoint != null) {
                Preconditions.checkState(
                        !checkpoint.isDisposed(),
                        "Received message for discarded but non-removed checkpoint "
                                + checkpointId);
                LOG.info(
                        "Decline checkpoint {} by task {} of job {} at {}.",
                        checkpointId,
                        message.getTaskExecutionId(),
                        job,
                        taskManagerLocationInfo,
                        checkpointException.getCause());
                abortPendingCheckpoint(
                        checkpoint, checkpointException, message.getTaskExecutionId());
            } else if (LOG.isDebugEnabled()) {
                if (recentExpiredCheckpoints.contains(checkpointId)) {
                    // message is for an expired checkpoint
                    LOG.debug(
                            "Received another decline message for now expired checkpoint attempt {} from task {} of job {} at {} : {}",
                            checkpointId,
                            message.getTaskExecutionId(),
                            job,
                            taskManagerLocationInfo,
                            reason);
                } else {
                    // message is for an unknown checkpoint. might be so old that we don't even
                    // remember it any more
                    LOG.debug(
                            "Received decline message for unknown (too old?) checkpoint attempt {} from task {} of job {} at {} : {}",
                            checkpointId,
                            message.getTaskExecutionId(),
                            job,
                            taskManagerLocationInfo,
                            reason);
                }
            }
        }
    }

    /**
     * Receives an AcknowledgeCheckpoint message and returns whether the message was associated with
     * a pending checkpoint.
     *
     * @param message Checkpoint ack from the task manager
     * @param taskManagerLocationInfo The location of the acknowledge checkpoint message's sender
     * @return Flag indicating whether the ack'd checkpoint was associated with a pending
     *     checkpoint.
     * @throws CheckpointException If the checkpoint cannot be added to the completed checkpoint
     *     store.
     */
    public boolean receiveAcknowledgeMessage(//
            AcknowledgeCheckpoint message, String taskManagerLocationInfo)
            throws CheckpointException {
        if (shutdown || message == null) {
            return false;
        }

        if (!job.equals(message.getJob())) {
            LOG.error(
                    "Received wrong AcknowledgeCheckpoint message for job {} from {} : {}",
                    job,
                    taskManagerLocationInfo,
                    message);
            return false;
        }

        final long checkpointId = message.getCheckpointId();

        synchronized (lock) {
            // we need to check inside the lock for being shutdown as well, otherwise we
            // get races and invalid error log messages
            if (shutdown) {
                return false;
            }

            final PendingCheckpoint checkpoint = pendingCheckpoints.get(checkpointId);

            if (message.getSubtaskState() != null) {
                // Register shared state regardless of checkpoint state and task ACK state.
                // This way, shared state is
                // 1. kept if the message is late or state will be used by the task otherwise
                // 2. removed eventually upon checkpoint subsumption (or job cancellation)
                // Do not register savepoints' shared state, as Flink is not in charge of
                // savepoints' lifecycle
                if (checkpoint == null || !checkpoint.getProps().isSavepoint()) {
                    message.getSubtaskState()
                            .registerSharedStates(
                                    completedCheckpointStore.getSharedStateRegistry(),
                                    checkpointId);
                }
            }

            if (checkpoint != null && !checkpoint.isDisposed()) {

                switch (checkpoint.acknowledgeTask(
                        message.getTaskExecutionId(),
                        message.getSubtaskState(),
                        message.getCheckpointMetrics())) {
                    case SUCCESS:
                        LOG.debug(
                                "Received acknowledge message for checkpoint {} from task {} of job {} at {}.",
                                checkpointId,
                                message.getTaskExecutionId(),
                                message.getJob(),
                                taskManagerLocationInfo);

                        if (checkpoint.isFullyAcknowledged()) {
                            completePendingCheckpoint(checkpoint);//
                        }
                        break;
                    case DUPLICATE:
                        LOG.debug(
                                "Received a duplicate acknowledge message for checkpoint {}, task {}, job {}, location {}.",
                                message.getCheckpointId(),
                                message.getTaskExecutionId(),
                                message.getJob(),
                                taskManagerLocationInfo);
                        break;
                    case UNKNOWN:
                        LOG.warn(
                                "Could not acknowledge the checkpoint {} for task {} of job {} at {}, "
                                        + "because the task's execution attempt id was unknown. Discarding "
                                        + "the state handle to avoid lingering state.",
                                message.getCheckpointId(),
                                message.getTaskExecutionId(),
                                message.getJob(),
                                taskManagerLocationInfo);

                        discardSubtaskState(
                                message.getJob(),
                                message.getTaskExecutionId(),
                                message.getCheckpointId(),
                                message.getSubtaskState());

                        break;
                    case DISCARDED:
                        LOG.warn(
                                "Could not acknowledge the checkpoint {} for task {} of job {} at {}, "
                                        + "because the pending checkpoint had been discarded. Discarding the "
                                        + "state handle tp avoid lingering state.",
                                message.getCheckpointId(),
                                message.getTaskExecutionId(),
                                message.getJob(),
                                taskManagerLocationInfo);

                        discardSubtaskState(
                                message.getJob(),
                                message.getTaskExecutionId(),
                                message.getCheckpointId(),
                                message.getSubtaskState());
                }

                return true;
            } else if (checkpoint != null) {
                // this should not happen
                throw new IllegalStateException(
                        "Received message for discarded but non-removed checkpoint "
                                + checkpointId);
            } else {
                reportCheckpointMetrics(
                        message.getCheckpointId(),
                        message.getTaskExecutionId(),
                        message.getCheckpointMetrics());
                boolean wasPendingCheckpoint;

                // message is for an unknown checkpoint, or comes too late (checkpoint disposed)
                if (recentExpiredCheckpoints.contains(checkpointId)) {
                    wasPendingCheckpoint = true;
                    LOG.warn(
                            "Received late message for now expired checkpoint attempt {} from task "
                                    + "{} of job {} at {}.",
                            checkpointId,
                            message.getTaskExecutionId(),
                            message.getJob(),
                            taskManagerLocationInfo);
                } else {
                    LOG.debug(
                            "Received message for an unknown checkpoint {} from task {} of job {} at {}.",
                            checkpointId,
                            message.getTaskExecutionId(),
                            message.getJob(),
                            taskManagerLocationInfo);
                    wasPendingCheckpoint = false;
                }

                // try to discard the state so that we don't have lingering state lying around
                discardSubtaskState(
                        message.getJob(),
                        message.getTaskExecutionId(),
                        message.getCheckpointId(),
                        message.getSubtaskState());

                return wasPendingCheckpoint;
            }
        }
    }

    /**
     * Try to complete the given pending checkpoint.
     *
     * <p>Important: This method should only be called in the checkpoint lock scope.
     *
     * @param pendingCheckpoint to complete
     * @throws CheckpointException if the completion failed
     */
    private void completePendingCheckpoint(PendingCheckpoint pendingCheckpoint)
            throws CheckpointException {
        final long checkpointId = pendingCheckpoint.getCheckpointID();
        final CompletedCheckpoint completedCheckpoint;
        final CompletedCheckpoint lastSubsumed;
        final CheckpointProperties props = pendingCheckpoint.getProps();

        completedCheckpointStore.getSharedStateRegistry().checkpointCompleted(checkpointId);

        try {
            completedCheckpoint = finalizeCheckpoint(pendingCheckpoint);

            // the pending checkpoint must be discarded after the finalization
            Preconditions.checkState(pendingCheckpoint.isDisposed() && completedCheckpoint != null);

            if (!props.isSavepoint()) {
                lastSubsumed =
                        addCompletedCheckpointToStoreAndSubsumeOldest(
                                checkpointId, completedCheckpoint, pendingCheckpoint);
            } else {
                lastSubsumed = null;
            }

            reportCompletedCheckpoint(completedCheckpoint);
            pendingCheckpoint.getCompletionFuture().complete(completedCheckpoint);
        } catch (Exception exception) {
            // For robustness reasons, we need catch exception and try marking the checkpoint
            // completed.
            pendingCheckpoint.getCompletionFuture().completeExceptionally(exception);
            throw exception;
        } finally {
            pendingCheckpoints.remove(checkpointId);
            scheduleTriggerRequest();
        }
        //完成checkpoints
        cleanupAfterCompletedCheckpoint(pendingCheckpoint, checkpointId, completedCheckpoint, lastSubsumed, props);//
    }

    private void reportCompletedCheckpoint(CompletedCheckpoint completedCheckpoint) {
        failureManager.handleCheckpointSuccess(completedCheckpoint.getCheckpointID());
        CompletedCheckpointStats completedCheckpointStats = completedCheckpoint.getStatistic();
        if (completedCheckpointStats != null) {
            LOG.trace(
                    "Checkpoint {} size: {}Kb, duration: {}ms",
                    completedCheckpoint.getCheckpointID(),
                    completedCheckpointStats.getStateSize() == 0
                            ? 0
                            : completedCheckpointStats.getStateSize() / 1024,
                    completedCheckpointStats.getEndToEndDuration());
            // Finalize the statsCallback and give the completed checkpoint a
            // callback for discards.
            statsTracker.reportCompletedCheckpoint(completedCheckpointStats);
        }
    }

    private void cleanupAfterCompletedCheckpoint(
            PendingCheckpoint pendingCheckpoint,
            long checkpointId,
            CompletedCheckpoint completedCheckpoint,
            CompletedCheckpoint lastSubsumed,
            CheckpointProperties props) {

        // record the time when this was completed, to calculate
        // the 'min delay between checkpoints'
        lastCheckpointCompletionRelativeTime = clock.relativeTimeMillis();
        //
        logCheckpointInfo(completedCheckpoint);

        if (!props.isSavepoint() || props.isSynchronous()) {
            // drop those pending checkpoints that are at prior to the completed one
            dropSubsumedCheckpoints(checkpointId);

            // send the "notify complete" call to all vertices, coordinators, etc.
            sendAcknowledgeMessages(
                    pendingCheckpoint.getCheckpointPlan().getTasksToCommitTo(),
                    checkpointId,
                    completedCheckpoint.getTimestamp(),
                    extractIdIfDiscardedOnSubsumed(lastSubsumed));
        }
    }

    //完成checkpoint
    private void logCheckpointInfo(CompletedCheckpoint completedCheckpoint) {
        LOG.info(
                "Completed checkpoint {} for job {} ({} bytes, checkpointDuration={} ms, finalizationTime={} ms).",
                completedCheckpoint.getCheckpointID(),
                job,
                completedCheckpoint.getStateSize(),
                completedCheckpoint.getCompletionTimestamp() - completedCheckpoint.getTimestamp(),
                System.currentTimeMillis() - completedCheckpoint.getCompletionTimestamp());

        if (LOG.isDebugEnabled()) {
            StringBuilder builder = new StringBuilder();
            builder.append("Checkpoint state: ");
            for (OperatorState state : completedCheckpoint.getOperatorStates().values()) {
                builder.append(state);
                builder.append(", ");
            }
            // Remove last two chars ", "
            builder.setLength(builder.length() - 2);

            LOG.debug(builder.toString());
        }
    }

    private CompletedCheckpoint finalizeCheckpoint(PendingCheckpoint pendingCheckpoint)
            throws CheckpointException {
        try {
            final CompletedCheckpoint completedCheckpoint =
                    pendingCheckpoint.finalizeCheckpoint(
                            checkpointsCleaner, this::scheduleTriggerRequest, executor);

            return completedCheckpoint;
        } catch (Exception e1) {
            // abort the current pending checkpoint if we fails to finalize the pending
            // checkpoint.
            final CheckpointFailureReason failureReason =
                    e1 instanceof PartialFinishingNotSupportedByStateException
                            ? CheckpointFailureReason.CHECKPOINT_DECLINED_TASK_CLOSING
                            : CheckpointFailureReason.FINALIZE_CHECKPOINT_FAILURE;

            if (!pendingCheckpoint.isDisposed()) {
                abortPendingCheckpoint(
                        pendingCheckpoint, new CheckpointException(failureReason, e1));
            }

            throw new CheckpointException(
                    "Could not finalize the pending checkpoint "
                            + pendingCheckpoint.getCheckpointID()
                            + '.',
                    failureReason,
                    e1);
        }
    }

    private long extractIdIfDiscardedOnSubsumed(CompletedCheckpoint lastSubsumed) {
        final long lastSubsumedCheckpointId;
        if (lastSubsumed != null && lastSubsumed.getProperties().discardOnSubsumed()) {
            lastSubsumedCheckpointId = lastSubsumed.getCheckpointID();
        } else {
            lastSubsumedCheckpointId = CheckpointStoreUtil.INVALID_CHECKPOINT_ID;
        }
        return lastSubsumedCheckpointId;
    }

    private CompletedCheckpoint addCompletedCheckpointToStoreAndSubsumeOldest(
            long checkpointId,
            CompletedCheckpoint completedCheckpoint,
            PendingCheckpoint pendingCheckpoint)
            throws CheckpointException {
        List<ExecutionVertex> tasksToAbort =
                pendingCheckpoint.getCheckpointPlan().getTasksToCommitTo();
        try {
            final CompletedCheckpoint subsumedCheckpoint =
                    completedCheckpointStore.addCheckpointAndSubsumeOldestOne(
                            completedCheckpoint, checkpointsCleaner, this::scheduleTriggerRequest);
            // reset the force full snapshot flag, we should've completed at least one full
            // snapshot by now
            this.forceFullSnapshot = false;
            return subsumedCheckpoint;
        } catch (Exception exception) {
            pendingCheckpoint.getCompletionFuture().completeExceptionally(exception);
            if (exception instanceof PossibleInconsistentStateException) {
                LOG.warn(
                        "An error occurred while writing checkpoint {} to the underlying metadata"
                                + " store. Flink was not able to determine whether the metadata was"
                                + " successfully persisted. The corresponding state located at '{}'"
                                + " won't be discarded and needs to be cleaned up manually.",
                        completedCheckpoint.getCheckpointID(),
                        completedCheckpoint.getExternalPointer());
            } else {
                // we failed to store the completed checkpoint. Let's clean up
                checkpointsCleaner.cleanCheckpointOnFailedStoring(completedCheckpoint, executor);
            }

            final CheckpointException checkpointException =
                    new CheckpointException(
                            "Could not complete the pending checkpoint " + checkpointId + '.',
                            CheckpointFailureReason.FINALIZE_CHECKPOINT_FAILURE,
                            exception);
            reportFailedCheckpoint(pendingCheckpoint, checkpointException);
            sendAbortedMessages(tasksToAbort, checkpointId, completedCheckpoint.getTimestamp());
            throw checkpointException;
        }
    }

    private void reportFailedCheckpoint(
            PendingCheckpoint pendingCheckpoint, CheckpointException exception) {

        failureManager.handleCheckpointException(
                pendingCheckpoint,
                pendingCheckpoint.getProps(),
                exception,
                null,
                job,
                getStatsCallback(pendingCheckpoint),
                statsTracker);
    }

    void scheduleTriggerRequest() {
        synchronized (lock) {
            if (isShutdown()) {
                LOG.debug(
                        "Skip scheduling trigger request because the CheckpointCoordinator is shut down");
            } else {
                timer.execute(this::executeQueuedRequest);
            }
        }
    }

    @VisibleForTesting
    void sendAcknowledgeMessages(
            List<ExecutionVertex> tasksToCommit,
            long completedCheckpointId,
            long completedTimestamp,
            long lastSubsumedCheckpointId) {
        // commit tasks
        for (ExecutionVertex ev : tasksToCommit) {
            Execution ee = ev.getCurrentExecutionAttempt();
            if (ee != null) {
                ee.notifyCheckpointOnComplete(
                        completedCheckpointId, completedTimestamp, lastSubsumedCheckpointId);
            }
        }

        // commit coordinators
        for (OperatorCoordinatorCheckpointContext coordinatorContext : coordinatorsToCheckpoint) {
            coordinatorContext.notifyCheckpointComplete(completedCheckpointId);
        }
    }

    private void sendAbortedMessages(
            List<ExecutionVertex> tasksToAbort, long checkpointId, long timeStamp) {
        assert (Thread.holdsLock(lock));
        long latestCompletedCheckpointId = completedCheckpointStore.getLatestCheckpointId();

        // send notification of aborted checkpoints asynchronously.
        executor.execute(
                () -> {
                    // send the "abort checkpoint" messages to necessary vertices.
                    for (ExecutionVertex ev : tasksToAbort) {
                        Execution ee = ev.getCurrentExecutionAttempt();
                        if (ee != null) {
                            try {
                                ee.notifyCheckpointAborted(
                                        checkpointId, latestCompletedCheckpointId, timeStamp);
                            } catch (Throwable e) {
                                LOG.warn(
                                        "Could not send aborted message of checkpoint {} to task {} belonging to job {}.",
                                        checkpointId,
                                        ee.getAttemptId(),
                                        ee.getVertex().getJobId(),
                                        e);
                            }
                        }
                    }
                });

        // commit coordinators
        for (OperatorCoordinatorCheckpointContext coordinatorContext : coordinatorsToCheckpoint) {
            coordinatorContext.notifyCheckpointAborted(checkpointId);
        }
    }

    private void rememberRecentExpiredCheckpointId(long id) {
        if (recentExpiredCheckpoints.size() >= NUM_GHOST_CHECKPOINT_IDS) {
            recentExpiredCheckpoints.removeFirst();
        }
        recentExpiredCheckpoints.addLast(id);
    }

    private void dropSubsumedCheckpoints(long checkpointId) {
        abortPendingCheckpoints(
                checkpoint ->
                        checkpoint.getCheckpointID() < checkpointId && checkpoint.canBeSubsumed(),
                new CheckpointException(CheckpointFailureReason.CHECKPOINT_SUBSUMED));
    }

    // --------------------------------------------------------------------------------------------
    //  Checkpoint State Restoring
    // --------------------------------------------------------------------------------------------

    /**
     * Restores the latest checkpointed state to a set of subtasks. This method represents a "local"
     * or "regional" failover and does restore states to coordinators. Note that a regional failover
     * might still include all tasks.
     *
     * @param tasks Set of job vertices to restore. State for these vertices is restored via {@link
     *     Execution#setInitialState(JobManagerTaskRestore)}.
     * @return An {@code OptionalLong} with the checkpoint ID, if state was restored, an empty
     *     {@code OptionalLong} otherwise.
     * @throws IllegalStateException If the CheckpointCoordinator is shut down.
     * @throws IllegalStateException If no completed checkpoint is available and the <code>
     *     failIfNoCheckpoint</code> flag has been set.
     * @throws IllegalStateException If the checkpoint contains state that cannot be mapped to any
     *     job vertex in <code>tasks</code> and the <code>allowNonRestoredState</code> flag has not
     *     been set.
     * @throws IllegalStateException If the max parallelism changed for an operator that restores
     *     state from this checkpoint.
     * @throws IllegalStateException If the parallelism changed for an operator that restores
     *     <i>non-partitioned</i> state from this checkpoint.
     */
    public OptionalLong restoreLatestCheckpointedStateToSubtasks(
            final Set<ExecutionJobVertex> tasks) throws Exception {
        // when restoring subtasks only we accept potentially unmatched state for the
        // following reasons
        //   - the set frequently does not include all Job Vertices (only the ones that are part
        //     of the restarted region), meaning there will be unmatched state by design.
        //   - because what we might end up restoring from an original savepoint with unmatched
        //     state, if there is was no checkpoint yet.
        return restoreLatestCheckpointedStateInternal(
                tasks,
                OperatorCoordinatorRestoreBehavior
                        .SKIP, // local/regional recovery does not reset coordinators
                false, // recovery might come before first successful checkpoint
                true,
                false); // see explanation above
    }

    /**
     * Restores the latest checkpointed state to all tasks and all coordinators. This method
     * represents a "global restore"-style operation where all stateful tasks and coordinators from
     * the given set of Job Vertices are restored. are restored to their latest checkpointed state.
     *
     * @param tasks Set of job vertices to restore. State for these vertices is restored via {@link
     *     Execution#setInitialState(JobManagerTaskRestore)}.
     * @param allowNonRestoredState Allow checkpoint state that cannot be mapped to any job vertex
     *     in tasks.
     * @return <code>true</code> if state was restored, <code>false</code> otherwise.
     * @throws IllegalStateException If the CheckpointCoordinator is shut down.
     * @throws IllegalStateException If no completed checkpoint is available and the <code>
     *     failIfNoCheckpoint</code> flag has been set.
     * @throws IllegalStateException If the checkpoint contains state that cannot be mapped to any
     *     job vertex in <code>tasks</code> and the <code>allowNonRestoredState</code> flag has not
     *     been set.
     * @throws IllegalStateException If the max parallelism changed for an operator that restores
     *     state from this checkpoint.
     * @throws IllegalStateException If the parallelism changed for an operator that restores
     *     <i>non-partitioned</i> state from this checkpoint.
     */
    public boolean restoreLatestCheckpointedStateToAll(
            final Set<ExecutionJobVertex> tasks, final boolean allowNonRestoredState)
            throws Exception {

        final OptionalLong restoredCheckpointId =
                restoreLatestCheckpointedStateInternal(
                        tasks,
                        OperatorCoordinatorRestoreBehavior
                                .RESTORE_OR_RESET, // global recovery restores coordinators, or
                        // resets them to empty
                        false, // recovery might come before first successful checkpoint
                        allowNonRestoredState,
                        false);

        return restoredCheckpointId.isPresent();
    }

    /**
     * Restores the latest checkpointed at the beginning of the job execution. If there is a
     * checkpoint, this method acts like a "global restore"-style operation where all stateful tasks
     * and coordinators from the given set of Job Vertices are restored.
     *
     * @param tasks Set of job vertices to restore. State for these vertices is restored via {@link
     *     Execution#setInitialState(JobManagerTaskRestore)}.
     * @return True, if a checkpoint was found and its state was restored, false otherwise.
     */
    //
    public boolean restoreInitialCheckpointIfPresent(final Set<ExecutionJobVertex> tasks)
            throws Exception {
        final OptionalLong restoredCheckpointId =
                restoreLatestCheckpointedStateInternal(//
                        tasks,
                        OperatorCoordinatorRestoreBehavior.RESTORE_IF_CHECKPOINT_PRESENT,
                        false, // initial checkpoints exist only on JobManager failover. ok if not
                        // present.
                        false,
                        true); // JobManager failover means JobGraphs match exactly.

        return restoredCheckpointId.isPresent();
    }

    /**
     * Performs the actual restore operation to the given tasks.
     *
     * <p>This method returns the restored checkpoint ID (as an optional) or an empty optional, if
     * no checkpoint was restored.
     */
    private OptionalLong restoreLatestCheckpointedStateInternal(
            final Set<ExecutionJobVertex> tasks,
            final OperatorCoordinatorRestoreBehavior operatorCoordinatorRestoreBehavior,
            final boolean errorIfNoCheckpoint,
            final boolean allowNonRestoredState,
            final boolean checkForPartiallyFinishedOperators)
            throws Exception {

        synchronized (lock) {
            if (shutdown) {
                throw new IllegalStateException("CheckpointCoordinator is shut down");
            }
            long restoreTimestamp = SystemClock.getInstance().absoluteTimeMillis();
            statsTracker.reportInitializationStarted(
                    tasks.stream()
                            .map(ExecutionJobVertex::getTaskVertices)
                            .flatMap(Stream::of)
                            .map(ExecutionVertex::getCurrentExecutionAttempt)
                            .map(Execution::getAttemptId)
                            .collect(Collectors.toSet()),
                    restoreTimestamp);

            // Restore from the latest checkpoint
            CompletedCheckpoint latest = completedCheckpointStore.getLatestCheckpoint();

            if (latest == null) {
                LOG.info("No checkpoint found during restore.");

                if (errorIfNoCheckpoint) {
                    throw new IllegalStateException("No completed checkpoint available");
                }

                LOG.debug("Resetting the master hooks.");
                MasterHooks.reset(masterHooks.values(), LOG);

                if (operatorCoordinatorRestoreBehavior
                        == OperatorCoordinatorRestoreBehavior.RESTORE_OR_RESET) {
                    // we let the JobManager-side components know that there was a recovery,
                    // even if there was no checkpoint to recover from, yet
                    LOG.info("Resetting the Operator Coordinators to an empty state.");
                    restoreStateToCoordinators(
                            OperatorCoordinator.NO_CHECKPOINT, Collections.emptyMap());
                }

                return OptionalLong.empty();
            }
            // 有最近一次的checkpoint
            statsTracker.reportRestoredCheckpoint(
                    latest.getCheckpointID(),
                    latest.getProperties(),
                    latest.getExternalPointer(),
                    latest.getStateSize());

            LOG.info("Restoring job {} from {}.", job, latest);

            this.forceFullSnapshot = latest.getProperties().isUnclaimed();

            // re-assign the task states
            final Map<OperatorID, OperatorState> operatorStates = extractOperatorStates(latest);

            if (checkForPartiallyFinishedOperators) {
                VertexFinishedStateChecker vertexFinishedStateChecker =
                        vertexFinishedStateCheckerFactory.apply(tasks, operatorStates);
                vertexFinishedStateChecker.validateOperatorsFinishedState();
            }

            StateAssignmentOperation stateAssignmentOperation =
                    new StateAssignmentOperation(
                            latest.getCheckpointID(), tasks, operatorStates, allowNonRestoredState);

            stateAssignmentOperation.assignStates();

            // call master hooks for restore. we currently call them also on "regional restore"
            // because
            // there is no other failure notification mechanism in the master hooks
            // ultimately these should get removed anyways in favor of the operator coordinators

            MasterHooks.restoreMasterHooks(
                    masterHooks,
                    latest.getMasterHookStates(),
                    latest.getCheckpointID(),
                    allowNonRestoredState,
                    LOG);

            if (operatorCoordinatorRestoreBehavior != OperatorCoordinatorRestoreBehavior.SKIP) {
                //
                restoreStateToCoordinators(latest.getCheckpointID(), operatorStates);
            }

            return OptionalLong.of(latest.getCheckpointID());
        }
    }

    private Map<OperatorID, OperatorState> extractOperatorStates(CompletedCheckpoint checkpoint) {
        Map<OperatorID, OperatorState> originalOperatorStates = checkpoint.getOperatorStates();

        if (checkpoint.getCheckpointID() != checkpointIdOfIgnoredInFlightData) {
            // Don't do any changes if it is not required.
            return originalOperatorStates;
        }

        HashMap<OperatorID, OperatorState> newStates = new HashMap<>();
        // Create the new operator states without in-flight data.
        for (OperatorState originalOperatorState : originalOperatorStates.values()) {
            newStates.put(
                    originalOperatorState.getOperatorID(),
                    originalOperatorState.copyAndDiscardInFlightData());
        }

        return newStates;
    }

    /**
     * Restore the state with given savepoint.
     *
     * @param restoreSettings Settings for a snapshot to restore from. Includes the path and
     *     parameters for the restore process.
     * @param tasks Map of job vertices to restore. State for these vertices is restored via {@link
     *     Execution#setInitialState(JobManagerTaskRestore)}.
     * @param userClassLoader The class loader to resolve serialized classes in legacy savepoint
     *     versions.
     */
    public boolean restoreSavepoint(
            SavepointRestoreSettings restoreSettings,
            Map<JobVertexID, ExecutionJobVertex> tasks,
            ClassLoader userClassLoader)
            throws Exception {

        final String savepointPointer = restoreSettings.getRestorePath();
        final boolean allowNonRestored = restoreSettings.allowNonRestoredState();
        Preconditions.checkNotNull(savepointPointer, "The savepoint path cannot be null.");

        LOG.info(
                "Starting job {} from savepoint {} ({})",
                job,
                savepointPointer,
                (allowNonRestored ? "allowing non restored state" : ""));

        final CompletedCheckpointStorageLocation checkpointLocation =
                checkpointStorageView.resolveCheckpoint(savepointPointer);

        // convert to checkpoint so the system can fall back to it
        final CheckpointProperties checkpointProperties;
        switch (restoreSettings.getRecoveryClaimMode()) {
            case CLAIM:
                checkpointProperties = this.checkpointProperties;
                break;
            case LEGACY:
                checkpointProperties =
                        CheckpointProperties.forSavepoint(
                                false,
                                // we do not care about the format when restoring, the format is
                                // necessary when triggering a savepoint
                                SavepointFormatType.CANONICAL);
                break;
            case NO_CLAIM:
                checkpointProperties = CheckpointProperties.forUnclaimedSnapshot();
                break;
            default:
                throw new IllegalArgumentException("Unknown snapshot claim mode");
        }

        // Load the savepoint as a checkpoint into the system
        CompletedCheckpoint savepoint =
                Checkpoints.loadAndValidateCheckpoint(
                        job,
                        tasks,
                        checkpointLocation,
                        userClassLoader,
                        allowNonRestored,
                        checkpointProperties);

        // register shared state - even before adding the checkpoint to the store
        // because the latter might trigger subsumption so the ref counts must be up-to-date
        savepoint.registerSharedStatesAfterRestored(
                completedCheckpointStore.getSharedStateRegistry(),
                restoreSettings.getRecoveryClaimMode());

        completedCheckpointStore.addCheckpointAndSubsumeOldestOne(
                savepoint, checkpointsCleaner, this::scheduleTriggerRequest);

        // Reset the checkpoint ID counter
        long nextCheckpointId = savepoint.getCheckpointID() + 1;
        checkpointIdCounter.setCount(nextCheckpointId);

        LOG.info("Reset the checkpoint ID of job {} to {}.", job, nextCheckpointId);

        final OptionalLong restoredCheckpointId =
                restoreLatestCheckpointedStateInternal(
                        new HashSet<>(tasks.values()),
                        OperatorCoordinatorRestoreBehavior.RESTORE_IF_CHECKPOINT_PRESENT,
                        true,
                        allowNonRestored,
                        true);

        return restoredCheckpointId.isPresent();
    }

    // ------------------------------------------------------------------------
    //  Accessors
    // ------------------------------------------------------------------------

    public int getNumberOfPendingCheckpoints() {
        synchronized (lock) {
            return this.pendingCheckpoints.size();
        }
    }

    public int getNumberOfRetainedSuccessfulCheckpoints() {
        synchronized (lock) {
            return completedCheckpointStore.getNumberOfRetainedCheckpoints();
        }
    }

    public Map<Long, PendingCheckpoint> getPendingCheckpoints() {
        synchronized (lock) {
            return new HashMap<>(this.pendingCheckpoints);
        }
    }

    public List<CompletedCheckpoint> getSuccessfulCheckpoints() throws Exception {
        synchronized (lock) {
            return completedCheckpointStore.getAllCheckpoints();
        }
    }

    @VisibleForTesting
    public ArrayDeque<Long> getRecentExpiredCheckpoints() {
        return recentExpiredCheckpoints;
    }

    public CheckpointStorageCoordinatorView getCheckpointStorage() {
        return checkpointStorageView;
    }

    public CompletedCheckpointStore getCheckpointStore() {
        return completedCheckpointStore;
    }

    /**
     * Gets the checkpoint interval. Its value might vary depending on whether there is processing
     * backlog.
     */
    private long getCurrentCheckpointInterval() {
        return backlogOperators.isEmpty() ? baseInterval : baseIntervalDuringBacklog;
    }

    public long getCheckpointTimeout() {
        return checkpointTimeout;
    }

    /**
     * @deprecated use {@link #getNumQueuedRequests()}
     */
    @Deprecated
    @VisibleForTesting
    PriorityQueue<CheckpointTriggerRequest> getTriggerRequestQueue() {
        synchronized (lock) {
            return requestDecider.getTriggerRequestQueue();
        }
    }

    public boolean isTriggering() {
        return isTriggering;
    }

    @VisibleForTesting
    boolean isCurrentPeriodicTriggerAvailable() {
        return currentPeriodicTrigger != null;
    }

    /**
     * Returns whether periodic checkpointing has been configured.
     *
     * @return <code>true</code> if periodic checkpoints have been configured.
     */
    public boolean isPeriodicCheckpointingConfigured() {
        return baseInterval != CheckpointCoordinatorConfiguration.DISABLED_CHECKPOINT_INTERVAL;
    }

    // --------------------------------------------------------------------------------------------
    //  Periodic scheduling of checkpoints
    // --------------------------------------------------------------------------------------------

    public void startCheckpointScheduler() {
        synchronized (lock) {
            if (shutdown) {
                throw new IllegalArgumentException("Checkpoint coordinator is shut down");
            }
            Preconditions.checkState(
                    isPeriodicCheckpointingConfigured(),
                    "Can not start checkpoint scheduler, if no periodic checkpointing is configured");

            if (isPeriodicCheckpointingStarted()) {
                // cancel previously scheduled checkpoints and spare savepoints.
                // TODO: Introduce a more general solution to the race condition
                //  between different checkpoint scheduling triggers.
                //  https://issues.apache.org/jira/browse/FLINK-34519
                stopCheckpointScheduler();
            }

            periodicScheduling = true;
            scheduleTriggerWithDelay(clock.relativeTimeMillis(), getRandomInitDelay());//
        }
    }

    public void stopCheckpointScheduler() {
        synchronized (lock) {
            periodicScheduling = false;

            cancelPeriodicTrigger();

            final CheckpointException reason =
                    new CheckpointException(CheckpointFailureReason.CHECKPOINT_COORDINATOR_SUSPEND);
            abortPendingAndQueuedCheckpoints(reason);
        }
    }

    public boolean isPeriodicCheckpointingStarted() {
        return periodicScheduling;
    }

    /**
     * Aborts all the pending checkpoints due to en exception.
     *
     * @param exception The exception.
     */
    public void abortPendingCheckpoints(CheckpointException exception) {
        synchronized (lock) {
            abortPendingCheckpoints(ignored -> true, exception);
        }
    }

    private void abortPendingCheckpoints(
            Predicate<PendingCheckpoint> checkpointToFailPredicate, CheckpointException exception) {

        assert Thread.holdsLock(lock);

        final PendingCheckpoint[] pendingCheckpointsToFail =
                pendingCheckpoints.values().stream()
                        .filter(checkpointToFailPredicate)
                        .toArray(PendingCheckpoint[]::new);

        // do not traverse pendingCheckpoints directly, because it might be changed during
        // traversing
        for (PendingCheckpoint pendingCheckpoint : pendingCheckpointsToFail) {
            abortPendingCheckpoint(pendingCheckpoint, exception);
        }
    }

    private void rescheduleTrigger(long currentTimeMillis, long tillNextMillis) {
        cancelPeriodicTrigger();
        scheduleTriggerWithDelay(currentTimeMillis, tillNextMillis);
    }

    private void cancelPeriodicTrigger() {
        if (currentPeriodicTrigger != null) {
            nextCheckpointTriggeringRelativeTime = Long.MAX_VALUE;
            currentPeriodicTriggerFuture.cancel(false);
            currentPeriodicTrigger = null;
            currentPeriodicTriggerFuture = null;
        }
    }

    private long getRandomInitDelay() {
        return ThreadLocalRandom.current().nextLong(minPauseBetweenCheckpoints, baseInterval + 1L);
    }

    private void scheduleTriggerWithDelay(long currentRelativeTime, long initDelay) {//
        nextCheckpointTriggeringRelativeTime = currentRelativeTime + initDelay;
        //
        currentPeriodicTrigger = new ScheduledTrigger();//
        currentPeriodicTriggerFuture = timer.schedule(currentPeriodicTrigger, initDelay, TimeUnit.MILLISECONDS);
    }

    private void restoreStateToCoordinators(
            final long checkpointId, final Map<OperatorID, OperatorState> operatorStates)
            throws Exception {

        for (OperatorCoordinatorCheckpointContext coordContext : coordinatorsToCheckpoint) {
            final OperatorState state = operatorStates.get(coordContext.operatorId());
            final ByteStreamStateHandle coordinatorState = state == null ? null : state.getCoordinatorState();
            final byte[] bytes = coordinatorState == null ? null : coordinatorState.getData();
            coordContext.resetToCheckpoint(checkpointId, bytes);
        }
    }

    // ------------------------------------------------------------------------
    //  job status listener that schedules / cancels periodic checkpoints
    // ------------------------------------------------------------------------

    public JobStatusListener createActivatorDeactivator(boolean allTasksOutputNonBlocking) {
        synchronized (lock) {
            if (shutdown) {
                throw new IllegalArgumentException("Checkpoint coordinator is shut down");
            }

            if (jobStatusListener == null) {
                jobStatusListener =
                        new CheckpointCoordinatorDeActivator(this, allTasksOutputNonBlocking);
            }

            return jobStatusListener;
        }
    }

    int getNumQueuedRequests() {
        synchronized (lock) {
            return requestDecider.getNumQueuedRequests();
        }
    }

    public void reportCheckpointMetrics(
            long id, ExecutionAttemptID attemptId, CheckpointMetrics metrics) {
        statsTracker.reportIncompleteStats(id, attemptId, metrics);
    }

    public void reportInitializationMetrics(
            ExecutionAttemptID executionAttemptID,
            SubTaskInitializationMetrics initializationMetrics) {
        statsTracker.reportInitializationMetrics(executionAttemptID, initializationMetrics);
    }

    // ------------------------------------------------------------------------

    final class ScheduledTrigger implements Runnable {

        @Override
        public void run() {
            synchronized (lock) {
                if (currentPeriodicTrigger != this) {
                    // Another periodic trigger has been scheduled but this one
                    // has not been force cancelled yet.
                    return;
                }

                long checkpointInterval = getCurrentCheckpointInterval();
                if (checkpointInterval != CheckpointCoordinatorConfiguration.DISABLED_CHECKPOINT_INTERVAL) {//true
                    nextCheckpointTriggeringRelativeTime += checkpointInterval;
                    currentPeriodicTriggerFuture =
                            timer.schedule(
                                    this,
                                    Math.max(
                                            0,
                                            nextCheckpointTriggeringRelativeTime - clock.relativeTimeMillis()),
                                    TimeUnit.MILLISECONDS);
                } else {
                    nextCheckpointTriggeringRelativeTime = Long.MAX_VALUE;
                    currentPeriodicTrigger = null;
                    currentPeriodicTriggerFuture = null;
                }
            }

            try {
                //触发Checkpoint
                triggerCheckpoint(checkpointProperties, null, true);//
            } catch (Exception e) {
                LOG.error("Exception while triggering checkpoint for job {}.", job, e);
            }
        }
    }

    /**
     * Discards the given state object asynchronously belonging to the given job, execution attempt
     * id and checkpoint id.
     *
     * @param jobId identifying the job to which the state object belongs
     * @param executionAttemptID identifying the task to which the state object belongs
     * @param checkpointId of the state object
     * @param subtaskState to discard asynchronously
     */
    private void discardSubtaskState(
            final JobID jobId,
            final ExecutionAttemptID executionAttemptID,
            final long checkpointId,
            final TaskStateSnapshot subtaskState) {

        if (subtaskState != null) {
            executor.execute(
                    new Runnable() {
                        @Override
                        public void run() {

                            try {
                                subtaskState.discardState();
                            } catch (Throwable t2) {
                                LOG.warn(
                                        "Could not properly discard state object of checkpoint {} "
                                                + "belonging to task {} of job {}.",
                                        checkpointId,
                                        executionAttemptID,
                                        jobId,
                                        t2);
                            }
                        }
                    });
        }
    }

    private void abortPendingCheckpoint(
            PendingCheckpoint pendingCheckpoint, CheckpointException exception) {

        abortPendingCheckpoint(pendingCheckpoint, exception, null);
    }

    private void abortPendingCheckpoint(
            PendingCheckpoint pendingCheckpoint,
            CheckpointException exception,
            @Nullable final ExecutionAttemptID executionAttemptID) {

        assert (Thread.holdsLock(lock));

        if (!pendingCheckpoint.isDisposed()) {
            try {
                // release resource here
                pendingCheckpoint.abort(
                        exception.getCheckpointFailureReason(),
                        exception.getCause(),
                        checkpointsCleaner,
                        this::scheduleTriggerRequest,
                        executor,
                        statsTracker);

                failureManager.handleCheckpointException(
                        pendingCheckpoint,
                        pendingCheckpoint.getProps(),
                        exception,
                        executionAttemptID,
                        job,
                        getStatsCallback(pendingCheckpoint),
                        statsTracker);
            } finally {
                sendAbortedMessages(
                        pendingCheckpoint.getCheckpointPlan().getTasksToCommitTo(),
                        pendingCheckpoint.getCheckpointID(),
                        pendingCheckpoint.getCheckpointTimestamp());
                pendingCheckpoints.remove(pendingCheckpoint.getCheckpointID());
                if (exception
                        .getCheckpointFailureReason()
                        .equals(CheckpointFailureReason.CHECKPOINT_EXPIRED)) {
                    rememberRecentExpiredCheckpointId(pendingCheckpoint.getCheckpointID());
                }
                scheduleTriggerRequest();
            }
        }
    }

    private void preCheckGlobalState(boolean isPeriodic) throws CheckpointException {
        // abort if the coordinator has been shutdown in the meantime
        if (shutdown) {
            throw new CheckpointException(CheckpointFailureReason.CHECKPOINT_COORDINATOR_SHUTDOWN);
        }

        // Don't allow periodic checkpoint if scheduling has been disabled
        if (isPeriodic && !periodicScheduling) {
            throw new CheckpointException(CheckpointFailureReason.PERIODIC_SCHEDULER_SHUTDOWN);
        }
    }

    private void abortPendingAndQueuedCheckpoints(CheckpointException exception) {
        assert (Thread.holdsLock(lock));
        requestDecider.abortAll(exception);
        abortPendingCheckpoints(exception);
    }

    /**
     * The canceller of checkpoint. The checkpoint might be cancelled if it doesn't finish in a
     * configured period.
     */
    class CheckpointCanceller implements Runnable {

        private final PendingCheckpoint pendingCheckpoint;

        private CheckpointCanceller(PendingCheckpoint pendingCheckpoint) {
            this.pendingCheckpoint = checkNotNull(pendingCheckpoint);
        }

        @Override
        public void run() {
            synchronized (lock) {
                // only do the work if the checkpoint is not discarded anyways
                // note that checkpoint completion discards the pending checkpoint object
                if (!pendingCheckpoint.isDisposed()) {
                    LOG.info(
                            "Checkpoint {} of job {} expired before completing.",
                            pendingCheckpoint.getCheckpointID(),
                            job);

                    abortPendingCheckpoint(
                            pendingCheckpoint,
                            new CheckpointException(CheckpointFailureReason.CHECKPOINT_EXPIRED));
                }
            }
        }
    }

    private static CheckpointException getCheckpointException(
            CheckpointFailureReason defaultReason, Throwable throwable) {

        final Optional<IOException> ioExceptionOptional =
                findThrowable(throwable, IOException.class);
        if (ioExceptionOptional.isPresent()) {
            return new CheckpointException(CheckpointFailureReason.IO_EXCEPTION, throwable);
        } else {
            final Optional<CheckpointException> checkpointExceptionOptional =
                    findThrowable(throwable, CheckpointException.class);
            return checkpointExceptionOptional.orElseGet(
                    () -> new CheckpointException(defaultReason, throwable));
        }
    }

    static class CheckpointTriggerRequest {
        final long timestamp;
        final CheckpointProperties props;
        final @Nullable String externalSavepointLocation;
        final boolean isPeriodic;
        private final CompletableFuture<CompletedCheckpoint> onCompletionPromise =
                new CompletableFuture<>();

        CheckpointTriggerRequest(
                CheckpointProperties props,
                @Nullable String externalSavepointLocation,
                boolean isPeriodic) {

            this.timestamp = System.currentTimeMillis();
            this.props = checkNotNull(props);
            this.externalSavepointLocation = externalSavepointLocation;
            this.isPeriodic = isPeriodic;
        }

        CompletableFuture<CompletedCheckpoint> getOnCompletionFuture() {
            return onCompletionPromise;
        }

        public void completeExceptionally(CheckpointException exception) {
            onCompletionPromise.completeExceptionally(exception);
        }

        public boolean isForce() {
            return props.forceCheckpoint();
        }
    }

    private enum OperatorCoordinatorRestoreBehavior {

        /** Coordinators are always restored. If there is no checkpoint, they are restored empty. */
        RESTORE_OR_RESET,

        /** Coordinators are restored if there was a checkpoint. */
        RESTORE_IF_CHECKPOINT_PRESENT,

        /** Coordinators are not restored during this checkpoint restore. */
        SKIP;
    }

    private PendingCheckpointStats trackPendingCheckpointStats(
            long checkpointId,
            CheckpointPlan checkpointPlan,
            CheckpointProperties props,
            long checkpointTimestamp) {
        Map<JobVertexID, Integer> vertices =
                Stream.concat(
                                checkpointPlan.getTasksToWaitFor().stream(),
                                checkpointPlan.getFinishedTasks().stream())
                        .map(Execution::getVertex)
                        .map(ExecutionVertex::getJobVertex)
                        .distinct()
                        .collect(
                                toMap(
                                        ExecutionJobVertex::getJobVertexId,
                                        ExecutionJobVertex::getParallelism));

        PendingCheckpointStats pendingCheckpointStats =
                statsTracker.reportPendingCheckpoint(
                        checkpointId, checkpointTimestamp, props, vertices);

        reportFinishedTasks(pendingCheckpointStats, checkpointPlan.getFinishedTasks());

        return pendingCheckpointStats;
    }

    private void reportFinishedTasks(
            @Nullable PendingCheckpointStats pendingCheckpointStats,
            List<Execution> finishedTasks) {
        if (pendingCheckpointStats == null) {
            return;
        }

        long now = System.currentTimeMillis();
        finishedTasks.forEach(
                execution ->
                        pendingCheckpointStats.reportSubtaskStats(
                                execution.getVertex().getJobvertexId(),
                                new SubtaskStateStats(execution.getParallelSubtaskIndex(), now)));
    }

    @Nullable
    private PendingCheckpointStats getStatsCallback(PendingCheckpoint pendingCheckpoint) {
        return statsTracker.getPendingCheckpointStats(pendingCheckpoint.getCheckpointID());
    }
}
