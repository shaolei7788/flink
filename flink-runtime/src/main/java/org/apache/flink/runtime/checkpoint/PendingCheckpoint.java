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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorInfo;
import org.apache.flink.runtime.state.CheckpointMetadataOutputStream;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.FutureUtils.ConjunctFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A pending checkpoint is a checkpoint that has been started, but has not been acknowledged by all
 * tasks that need to acknowledge it. Once all tasks have acknowledged it, it becomes a {@link
 * CompletedCheckpoint}.
 *
 * <p>Note that the pending checkpoint, as well as the successful checkpoint keep the state handles
 * always as serialized values, never as actual values.
 */
//进行中的检查点
//它表示一个已经触发（Started）但尚未收集齐所有任务（Tasks）确认应答（ACK）的检查点
@NotThreadSafe
public class PendingCheckpoint implements Checkpoint {

    /** Result of the {@link PendingCheckpoint#acknowledgedTasks} method. */
    public enum TaskAcknowledgeResult {
        SUCCESS, // successful acknowledge of the task
        DUPLICATE, // acknowledge message is a duplicate
        UNKNOWN, // unknown task acknowledged
        DISCARDED // pending checkpoint has been discarded
    }

    // ------------------------------------------------------------------------

    /** The PendingCheckpoint logs to the same logger as the CheckpointCoordinator. */
    private static final Logger LOG = LoggerFactory.getLogger(CheckpointCoordinator.class);

    private final Object lock = new Object();

    private final JobID jobId;

    //该检查点的全局唯一、递增 ID 从1开始
    private final long checkpointId;

    //检查点被触发时的系统时间戳
    private final long checkpointTimestamp;

    //作用：核心数据收集仓。存放已经完成 ACK 的各个算子（Operator）的状态元数据句柄（OperatorState）。设计意图：当一个 TaskManager 节点成功持久化状态并 ACK 后，
    //它上报的 TaskStateSnapshot 会被解析并按算子 ID（OperatorID）归类合并到这个 Map 中。当快照转正时，这个 Map 就是 CompletedCheckpoint 的核心内容
    private final Map<OperatorID, OperatorState> operatorStates;

    //它记录了在触发那一刻，整个拓扑中哪些 Task、哪些 Coordinator 需要参与本次快照。
    //它为后面的 ACK 集合初始化提供了依据，解耦了动态拓扑变化（如正在流转的挂起状态）对快照逻辑的干扰
    private final CheckpointPlan checkpointPlan;

    //还未汇报 ACK 的 Task 剩余清单
    private final Map<ExecutionAttemptID, ExecutionVertex> notYetAcknowledgedTasks;

    //还未确认的控制面算子协调器（OperatorCoordinator）清单。设计意图：对应 isFullyAcknowledged() 中的第二维。只有当这个 Set 变为空时，控制面的快照才算完成
    private final Set<OperatorID> notYetAcknowledgedOperatorCoordinators;

    //作用：存放已经收集完毕的 MasterHook 全局外部状态列表。
    //设计意图：当主控节点的自定义钩子（MasterHook）成功执行并返回快照数据后，序列化后的字节数据会暂存在这里，最终一起写入元数据元文件（_metadata）
    private final List<MasterState> masterStates;

    //还未返回的 MasterHook（由字符串标识）清单。设计意图：对应 isFullyAcknowledged() 中的第三维。跟踪主控节点全局钩子的异步执行状态
    private final Set<String> notYetAcknowledgedMasterStates;

    /** Set of acknowledged tasks. */
    //已经成功汇报 ACK 的 Task 清单。设计意图：与notYetAcknowledgedTasks配合使用。防止由于网络波动、RPC 重试导致同一个 Task 重复提交 ACK 引起计数错乱（幂等去重）
    private final Set<ExecutionAttemptID> acknowledgedTasks;

    //定义该检查点的行为行为特征（如：是自动触发的 Checkpoint，还是用户手动触发的 Savepoint？失败时是容错报错还是直接跳过？）
    /** The checkpoint properties. */
    private final CheckpointProperties props;

    /**
     * The promise to fulfill once the checkpoint has been completed. Note that it will be completed
     * only after the checkpoint is successfully added to CompletedCheckpointStore.
     */
    //作用：转正承诺（Promise）。
    // 设计意图：异步编程的核心。上层代码（如调度器或触发线程）不需要同步等待 Checkpoint 完成。它们只需监听这个 Future。
    // 当 PendingCheckpoint 真正成功转换并写入持久化存储（CompletedCheckpointStore）后，该 Future 会被 complete(completedCheckpoint) 激活
    private final CompletableFuture<CompletedCheckpoint> onCompletionPromise;

    //作用：监控指标快照收集器。设计意图：它是 Flink Web UI 上的数据源。
    //当 PendingCheckpoint 内部的 ACK 状态发生变更时，会同步更新到这个 Stats 对象中，
    //这样用户就能实时在前端看到“Checkpoint 进行到 85%，还剩 3 个 Task 没 ACK”的动态画面
    @Nullable private final PendingCheckpointStats pendingCheckpointStats;

    //作用：Master 侧触发阶段完成的承诺。
    // 设计意图：专门用于协调。在分布式 Barrier 发出之前，
    // Master 侧需要先初始化存储、触发 MasterHooks。这个 Future 标志着“Master 准备工作已完毕，可以开始等 TaskManager 的 ACK 了”
    private final CompletableFuture<Void> masterTriggerCompletionPromise;

    /** Target storage location to persist the checkpoint metadata to. */
    //本次快照元数据的最终持久化物理路径
    @Nullable
    private CheckpointStorageLocation targetLocation;

    //已确认的 Task 计数器
    private int numAcknowledgedTasks;

    //表示该对象内部的垃圾回收逻辑是否已经开始执行
    private boolean disposed;

    //表示底层的物理文件（存储在 HDFS/S3 里的孤儿数据）是否已经被彻底删除干净
    private boolean discarded;

    //作用：超时炸弹的引信句柄。设计意图：在触发 Checkpoint 的同时，会向线程池提交一个定时取消任务（Timeout Task）。该属性持有这个定时任务的句柄。
    // 一旦 Checkpoint 提前成功或因其他原因失败，必须调用 cancellerHandle.cancel(false) 把这个定时炸弹拆除，否则会引发不必要的二次销毁
    private volatile ScheduledFuture<?> cancellerHandle;

    //作用：记录导致该检查点死掉的最终死因（如：CheckpointExpiredException、JobFailoverException）。
    //设计意图：用于追溯和可观测性。当这个 PendingCheckpoint 被废弃时，它会把死因填充进这个字段，并最终反馈给 Web UI 或系统日志，方便架构师排查究竟是哪个 Task 拖垮了检查点
    private CheckpointException failureCause;

    // --------------------------------------------------------------------------------------------

    public PendingCheckpoint(
            JobID jobId,
            long checkpointId,
            long checkpointTimestamp,
            CheckpointPlan checkpointPlan,
            Collection<OperatorID> operatorCoordinatorsToConfirm,
            Collection<String> masterStateIdentifiers,
            CheckpointProperties props,
            CompletableFuture<CompletedCheckpoint> onCompletionPromise,
            @Nullable PendingCheckpointStats pendingCheckpointStats,
            CompletableFuture<Void> masterTriggerCompletionPromise) {
        checkArgument(
                checkpointPlan.getTasksToWaitFor().size() > 0,
                "Checkpoint needs at least one vertex that commits the checkpoint");

        this.jobId = checkNotNull(jobId);
        this.checkpointId = checkpointId;
        this.checkpointTimestamp = checkpointTimestamp;
        this.checkpointPlan = checkNotNull(checkpointPlan);

        this.notYetAcknowledgedTasks =
                CollectionUtil.newHashMapWithExpectedSize(
                        checkpointPlan.getTasksToWaitFor().size());
        for (Execution execution : checkpointPlan.getTasksToWaitFor()) {
            notYetAcknowledgedTasks.put(execution.getAttemptId(), execution.getVertex());
        }

        this.props = checkNotNull(props);

        this.operatorStates = new HashMap<>();
        this.masterStates = new ArrayList<>(masterStateIdentifiers.size());
        this.notYetAcknowledgedMasterStates =
                masterStateIdentifiers.isEmpty()
                        ? Collections.emptySet()
                        : new HashSet<>(masterStateIdentifiers);
        this.notYetAcknowledgedOperatorCoordinators =
                operatorCoordinatorsToConfirm.isEmpty()
                        ? Collections.emptySet()
                        : new HashSet<>(operatorCoordinatorsToConfirm);
        this.acknowledgedTasks =
                CollectionUtil.newHashSetWithExpectedSize(
                        checkpointPlan.getTasksToWaitFor().size());
        this.onCompletionPromise = checkNotNull(onCompletionPromise);
        this.pendingCheckpointStats = pendingCheckpointStats;
        this.masterTriggerCompletionPromise = checkNotNull(masterTriggerCompletionPromise);
    }

    // --------------------------------------------------------------------------------------------

    // ------------------------------------------------------------------------
    //  Properties
    // ------------------------------------------------------------------------

    public JobID getJobId() {
        return jobId;
    }

    @Override
    public long getCheckpointID() {
        return checkpointId;
    }

    public void setCheckpointTargetLocation(CheckpointStorageLocation targetLocation) {
        this.targetLocation = targetLocation;
    }

    public CheckpointStorageLocation getCheckpointStorageLocation() {
        return targetLocation;
    }

    public long getCheckpointTimestamp() {
        return checkpointTimestamp;
    }

    public int getNumberOfNonAcknowledgedTasks() {
        return notYetAcknowledgedTasks.size();
    }

    public int getNumberOfNonAcknowledgedOperatorCoordinators() {
        return notYetAcknowledgedOperatorCoordinators.size();
    }

    public CheckpointPlan getCheckpointPlan() {
        return checkpointPlan;
    }

    public int getNumberOfAcknowledgedTasks() {
        return numAcknowledgedTasks;
    }

    public Map<OperatorID, OperatorState> getOperatorStates() {
        return operatorStates;
    }

    public List<MasterState> getMasterStates() {
        return masterStates;
    }

    public boolean isFullyAcknowledged() {
        return
                //[数据面] 所有 TaskManager 上的 Task 均已 ACK
                areTasksFullyAcknowledged()
                //[控制面] 所有 OperatorCoordinator 均已 ACK
                && areCoordinatorsFullyAcknowledged()
                //[主控面] 所有 MasterHook 状态均已安全保存
                && areMasterStatesFullyAcknowledged();
    }

    boolean areMasterStatesFullyAcknowledged() {
        return notYetAcknowledgedMasterStates.isEmpty() && !disposed;
    }

    boolean areCoordinatorsFullyAcknowledged() {
        return notYetAcknowledgedOperatorCoordinators.isEmpty() && !disposed;
    }

    boolean areTasksFullyAcknowledged() {
        return notYetAcknowledgedTasks.isEmpty() && !disposed;
    }

    public boolean isAcknowledgedBy(ExecutionAttemptID executionAttemptId) {
        return !notYetAcknowledgedTasks.containsKey(executionAttemptId);
    }

    public boolean isDisposed() {
        return disposed;
    }

    /**
     * Checks whether this checkpoint can be subsumed or whether it should always continue,
     * regardless of newer checkpoints in progress.
     *
     * @return True if the checkpoint can be subsumed, false otherwise.
     */
    public boolean canBeSubsumed() {
        // If the checkpoint is forced, it cannot be subsumed.
        return !props.isSavepoint();
    }

    CheckpointProperties getProps() {
        return props;
    }

    /**
     * Sets the handle for the canceller to this pending checkpoint. This method fails with an
     * exception if a handle has already been set.
     *
     * @return true, if the handle was set, false, if the checkpoint is already disposed;
     */
    public boolean setCancellerHandle(ScheduledFuture<?> cancellerHandle) {
        synchronized (lock) {
            if (this.cancellerHandle == null) {
                if (!disposed) {
                    this.cancellerHandle = cancellerHandle;
                    return true;
                } else {
                    return false;
                }
            } else {
                throw new IllegalStateException("A canceller handle was already set");
            }
        }
    }

    public CheckpointException getFailureCause() {
        return failureCause;
    }

    // ------------------------------------------------------------------------
    //  Progress and Completion
    // ------------------------------------------------------------------------

    /**
     * Returns the completion future.
     *
     * @return A future to the completed checkpoint
     */
    public CompletableFuture<CompletedCheckpoint> getCompletionFuture() {
        return onCompletionPromise;
    }

    public CompletedCheckpoint finalizeCheckpoint(
            CheckpointsCleaner checkpointsCleaner, Runnable postCleanup, Executor executor)
            throws IOException {

        synchronized (lock) {
            checkState(!isDisposed(), "checkpoint is discarded");
            checkState(
                    isFullyAcknowledged(),
                    "Pending checkpoint has not been fully acknowledged yet");

            // make sure we fulfill the promise with an exception if something fails
            try {
                checkpointPlan.fulfillFinishedTaskStatus(operatorStates);

                // write out the metadata
                final CheckpointMetadata savepoint =
                        new CheckpointMetadata(
                                checkpointId, operatorStates.values(), masterStates, props);
                final CompletedCheckpointStorageLocation finalizedLocation;

                try (CheckpointMetadataOutputStream out =
                        targetLocation.createMetadataOutputStream()) {
                    Checkpoints.storeCheckpointMetadata(savepoint, out);
                    finalizedLocation = out.closeAndFinalizeCheckpoint();
                }

                CompletedCheckpoint completed =
                        new CompletedCheckpoint(
                                jobId,
                                checkpointId,
                                checkpointTimestamp,
                                System.currentTimeMillis(),
                                operatorStates,
                                masterStates,
                                props,
                                finalizedLocation,
                                toCompletedCheckpointStats(finalizedLocation));

                // mark this pending checkpoint as disposed, but do NOT drop the state
                dispose(false, checkpointsCleaner, postCleanup, executor);

                return completed;
            } catch (Throwable t) {
                onCompletionPromise.completeExceptionally(t);
                ExceptionUtils.rethrowIOException(t);
                return null; // silence the compiler
            }
        }
    }

    @Nullable
    private CompletedCheckpointStats toCompletedCheckpointStats(
            CompletedCheckpointStorageLocation finalizedLocation) {
        return pendingCheckpointStats != null
                ? pendingCheckpointStats.toCompletedCheckpointStats(
                        finalizedLocation.getExternalPointer(),
                        finalizedLocation.getMetadataHandle().getStateSize())
                : null;
    }

    /**
     * Acknowledges the task with the given execution attempt id and the given subtask state.
     *
     * @param executionAttemptId of the acknowledged task
     * @param operatorSubtaskStates of the acknowledged task
     * @param metrics Checkpoint metrics for the stats
     * @return TaskAcknowledgeResult of the operation
     */
    public TaskAcknowledgeResult acknowledgeTask(
            ExecutionAttemptID executionAttemptId,
            TaskStateSnapshot operatorSubtaskStates,
            CheckpointMetrics metrics) {

        synchronized (lock) {
            if (disposed) {
                return TaskAcknowledgeResult.DISCARDED;
            }
            //
            final ExecutionVertex vertex = notYetAcknowledgedTasks.remove(executionAttemptId);

            if (vertex == null) {
                if (acknowledgedTasks.contains(executionAttemptId)) {
                    return TaskAcknowledgeResult.DUPLICATE;
                } else {
                    return TaskAcknowledgeResult.UNKNOWN;
                }
            } else {
                acknowledgedTasks.add(executionAttemptId);
            }

            long ackTimestamp = System.currentTimeMillis();
            if (operatorSubtaskStates != null && operatorSubtaskStates.isTaskDeployedAsFinished()) {
                checkpointPlan.reportTaskFinishedOnRestore(vertex);
            } else {
                for (OperatorIDPair operatorIDPair : vertex.getJobVertex().getOperatorIDs()) {
                    updateOperatorState(vertex, operatorSubtaskStates, operatorIDPair);
                }

                if (operatorSubtaskStates != null && operatorSubtaskStates.isTaskFinished()) {
                    checkpointPlan.reportTaskHasFinishedOperators(vertex);
                }
            }

            ++numAcknowledgedTasks;

            // publish the checkpoint statistics
            // to prevent null-pointers from concurrent modification, copy reference onto stack
            if (pendingCheckpointStats != null) {
                // Do this in millis because the web frontend works with them
                long alignmentDurationMillis = metrics.getAlignmentDurationNanos() / 1_000_000;
                long checkpointStartDelayMillis =
                        metrics.getCheckpointStartDelayNanos() / 1_000_000;

                SubtaskStateStats subtaskStateStats =
                        new SubtaskStateStats(
                                vertex.getParallelSubtaskIndex(),
                                ackTimestamp,
                                metrics.getBytesPersistedOfThisCheckpoint(),
                                metrics.getTotalBytesPersisted(),
                                metrics.getSyncDurationMillis(),
                                metrics.getAsyncDurationMillis(),
                                metrics.getBytesProcessedDuringAlignment(),
                                metrics.getBytesPersistedDuringAlignment(),
                                alignmentDurationMillis,
                                checkpointStartDelayMillis,
                                metrics.getUnalignedCheckpoint(),
                                true);

                LOG.trace(
                        "Checkpoint {} stats for {}: size={}Kb, duration={}ms, sync part={}ms, async part={}ms",
                        checkpointId,
                        vertex.getTaskNameWithSubtaskIndex(),
                        subtaskStateStats.getStateSize() == 0
                                ? 0
                                : subtaskStateStats.getStateSize() / 1024,
                        subtaskStateStats.getEndToEndDuration(
                                pendingCheckpointStats.getTriggerTimestamp()),
                        subtaskStateStats.getSyncCheckpointDuration(),
                        subtaskStateStats.getAsyncCheckpointDuration());
                pendingCheckpointStats.reportSubtaskStats(
                        vertex.getJobvertexId(), subtaskStateStats);
            }

            return TaskAcknowledgeResult.SUCCESS;
        }
    }

    private void updateOperatorState(
            ExecutionVertex vertex,
            TaskStateSnapshot operatorSubtaskStates,
            OperatorIDPair operatorIDPair) {
        OperatorState operatorState = operatorStates.get(operatorIDPair.getGeneratedOperatorID());

        if (operatorState == null) {
            operatorState =
                    new OperatorState(
                            operatorIDPair.getUserDefinedOperatorName(),
                            operatorIDPair.getUserDefinedOperatorUid(),
                            operatorIDPair.getGeneratedOperatorID(),
                            vertex.getTotalNumberOfParallelSubtasks(),
                            vertex.getMaxParallelism());
            operatorStates.put(operatorIDPair.getGeneratedOperatorID(), operatorState);
        } else {
            operatorState.setOperatorName(operatorIDPair.getUserDefinedOperatorName());
            operatorState.setOperatorUid(operatorIDPair.getUserDefinedOperatorUid());
        }
        OperatorSubtaskState operatorSubtaskState =
                operatorSubtaskStates == null
                        ? null
                        : operatorSubtaskStates.getSubtaskStateByOperatorID(
                                operatorIDPair.getGeneratedOperatorID());

        if (operatorSubtaskState != null) {
            operatorState.putState(vertex.getParallelSubtaskIndex(), operatorSubtaskState);
        }
    }

    public TaskAcknowledgeResult acknowledgeCoordinatorState(
            OperatorInfo coordinatorInfo, @Nullable ByteStreamStateHandle stateHandle) {

        synchronized (lock) {
            if (disposed) {
                return TaskAcknowledgeResult.DISCARDED;
            }

            final OperatorID operatorId = coordinatorInfo.operatorId();
            OperatorState operatorState = operatorStates.get(operatorId);

            // sanity check for better error reporting
            if (!notYetAcknowledgedOperatorCoordinators.remove(operatorId)) {
                return operatorState != null && operatorState.getCoordinatorState() != null
                        ? TaskAcknowledgeResult.DUPLICATE
                        : TaskAcknowledgeResult.UNKNOWN;
            }

            if (operatorState == null) {
                operatorState =
                        new OperatorState(
                                null,
                                null,
                                operatorId,
                                coordinatorInfo.currentParallelism(),
                                coordinatorInfo.maxParallelism());
                operatorStates.put(operatorId, operatorState);
            }
            if (stateHandle != null) {
                operatorState.setCoordinatorState(stateHandle);
            }

            return TaskAcknowledgeResult.SUCCESS;
        }
    }

    /**
     * Acknowledges a master state (state generated on the checkpoint coordinator) to the pending
     * checkpoint.
     *
     * @param identifier The identifier of the master state
     * @param state The state to acknowledge
     */
    public void acknowledgeMasterState(String identifier, @Nullable MasterState state) {

        synchronized (lock) {
            if (!disposed) {
                if (notYetAcknowledgedMasterStates.remove(identifier) && state != null) {
                    masterStates.add(state);
                }
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Cancellation
    // ------------------------------------------------------------------------

    /** Aborts a checkpoint with reason and cause. */
    public void abort(
            CheckpointFailureReason reason,
            @Nullable Throwable cause,
            CheckpointsCleaner checkpointsCleaner,
            Runnable postCleanup,
            Executor executor,
            CheckpointStatsTracker statsTracker) {
        try {
            failureCause = new CheckpointException(reason, cause);
            onCompletionPromise.completeExceptionally(failureCause);
            masterTriggerCompletionPromise.completeExceptionally(failureCause);
            assertAbortSubsumedForced(reason);
        } finally {
            dispose(true, checkpointsCleaner, postCleanup, executor);
        }
    }

    private void assertAbortSubsumedForced(CheckpointFailureReason reason) {
        if (props.isSavepoint() && reason == CheckpointFailureReason.CHECKPOINT_SUBSUMED) {
            throw new IllegalStateException(
                    "Bug: savepoints must never be subsumed, "
                            + "the abort reason is : "
                            + reason.message());
        }
    }

    private void dispose(
            boolean releaseState,
            CheckpointsCleaner checkpointsCleaner,
            Runnable postCleanup,
            Executor executor) {

        synchronized (lock) {
            try {
                numAcknowledgedTasks = -1;
                checkpointsCleaner.cleanCheckpoint(this, releaseState, postCleanup, executor);
            } finally {
                disposed = true;
                notYetAcknowledgedTasks.clear();
                acknowledgedTasks.clear();
                cancelCanceller();
            }
        }
    }

    @Override
    public DiscardObject markAsDiscarded() {
        return new PendingCheckpointDiscardObject();
    }

    private void cancelCanceller() {
        try {
            final ScheduledFuture<?> canceller = this.cancellerHandle;
            if (canceller != null) {
                canceller.cancel(false);
            }
        } catch (Exception e) {
            // this code should not throw exceptions
            LOG.warn("Error while cancelling checkpoint timeout task", e);
        }
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return String.format(
                "Pending Checkpoint %d @ %d - confirmed=%d, pending=%d",
                checkpointId,
                checkpointTimestamp,
                getNumberOfAcknowledgedTasks(),
                getNumberOfNonAcknowledgedTasks());
    }

    /**
     * Implementation of {@link org.apache.flink.runtime.checkpoint.Checkpoint.DiscardObject} for
     * {@link PendingCheckpoint}.
     */
    public class PendingCheckpointDiscardObject implements DiscardObject {
        /**
         * Discard state. Must be called after {@link #dispose(boolean, CheckpointsCleaner,
         * Runnable, Executor) dispose}.
         */
        @Override
        public void discard() {
            synchronized (lock) {
                if (discarded) {
                    Preconditions.checkState(
                            disposed, "Checkpoint should be disposed before being discarded");
                    return;
                } else {
                    discarded = true;
                }
            }
            // discard the private states.
            // unregistered shared states are still considered private at this point.
            try {
                StateUtil.bestEffortDiscardAllStateObjects(operatorStates.values());
                if (targetLocation != null) {
                    targetLocation.disposeOnFailure();
                }
            } catch (Throwable t) {
                LOG.warn(
                        "Could not properly dispose the private states in the pending checkpoint {} of job {}.",
                        checkpointId,
                        jobId,
                        t);
            } finally {
                operatorStates.clear();
            }
        }

        @Override
        public CompletableFuture<Void> discardAsync(Executor ioExecutor) {
            synchronized (lock) {
                if (discarded) {
                    Preconditions.checkState(
                            disposed, "Checkpoint should be disposed before being discarded");
                } else {
                    discarded = true;
                }
            }
            List<StateObject> discardables =
                    operatorStates.values().stream()
                            .flatMap(op -> op.getDiscardables().stream())
                            .collect(Collectors.toList());

            ConjunctFuture<Void> discardStates =
                    FutureUtils.completeAll(
                            discardables.stream()
                                    .map(
                                            item ->
                                                    FutureUtils.runAsync(
                                                            item::discardState, ioExecutor))
                                    .collect(Collectors.toList()));

            return FutureUtils.runAfterwards(
                    discardStates,
                    () -> {
                        operatorStates.clear();
                        if (targetLocation != null) {
                            targetLocation.disposeOnFailure();
                        }
                    });
        }
    }
}
