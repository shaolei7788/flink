/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetricsBuilder;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter.ChannelStateWriteResult;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriterImpl;
import org.apache.flink.runtime.checkpoint.filemerging.FileMergingSnapshotManager;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.CheckpointStateToolset;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CheckpointStorageWorkerView;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.filesystem.FsMergingCheckpointStorageLocation;
import org.apache.flink.runtime.taskmanager.AsyncExceptionHandler;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.runtime.io.checkpointing.BarrierAlignmentUtil;
import org.apache.flink.streaming.runtime.io.checkpointing.BarrierAlignmentUtil.Cancellable;
import org.apache.flink.streaming.runtime.io.checkpointing.BarrierAlignmentUtil.DelayableTimer;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;
import org.apache.flink.util.function.BiFunctionWithException;
import org.apache.flink.util.function.SupplierWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.apache.flink.util.IOUtils.closeQuietly;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

class SubtaskCheckpointCoordinatorImpl implements SubtaskCheckpointCoordinator {

    private static final Logger LOG =
            LoggerFactory.getLogger(SubtaskCheckpointCoordinatorImpl.class);

    private static final int CHECKPOINT_EXECUTION_DELAY_LOG_THRESHOLD_MS = 30_000;

    private final boolean enableCheckpointAfterTasksFinished;

    private final CachingCheckpointStorageWorkerView checkpointStorage;
    private final String taskName;
    private final ExecutorService asyncOperationsThreadPool;
    private final Environment env;
    private final AsyncExceptionHandler asyncExceptionHandler;
    private final ChannelStateWriter channelStateWriter;
    private final StreamTaskActionExecutor actionExecutor;
    private final BiFunctionWithException<
                    ChannelStateWriter, Long, CompletableFuture<Void>, CheckpointException>
            prepareInputSnapshot;

    /** The IDs of the checkpoint for which we are notified aborted. */
    private final Set<Long> abortedCheckpointIds;

    private final int maxRecordAbortedCheckpoints;

    private long maxAbortedCheckpointId = 0;

    private long lastCheckpointId;

    /** Lock that guards state of AsyncCheckpointRunnable registry. * */
    private final Object lock;

    @GuardedBy("lock")
    private final Map<Long, AsyncCheckpointRunnable> checkpoints;

    /** Indicates if this registry is closed. */
    @GuardedBy("lock")
    private boolean closed;

    private final DelayableTimer registerTimer;

    private final Clock clock;

    /** It always be called in Task Thread. */
    private Cancellable alignmentTimer;

    /**
     * It is the checkpointId corresponding to alignmentTimer. And It should be always updated with
     * {@link #alignmentTimer}.
     */
    private long alignmentCheckpointId;

    @Nullable private final FileMergingSnapshotManager fileMergingSnapshotManager;

    @VisibleForTesting
    SubtaskCheckpointCoordinatorImpl(
            CheckpointStorageWorkerView checkpointStorage,
            String taskName,
            StreamTaskActionExecutor actionExecutor,
            ExecutorService asyncOperationsThreadPool,
            Environment env,
            AsyncExceptionHandler asyncExceptionHandler,
            BiFunctionWithException<
                            ChannelStateWriter, Long, CompletableFuture<Void>, CheckpointException>
                    prepareInputSnapshot,
            int maxRecordAbortedCheckpoints,
            ChannelStateWriter channelStateWriter,
            boolean enableCheckpointAfterTasksFinished,
            DelayableTimer registerTimer) {
        this(
                checkpointStorage,
                taskName,
                actionExecutor,
                asyncOperationsThreadPool,
                env,
                asyncExceptionHandler,
                prepareInputSnapshot,
                maxRecordAbortedCheckpoints,
                channelStateWriter,
                enableCheckpointAfterTasksFinished,
                registerTimer,
                null);
    }

    SubtaskCheckpointCoordinatorImpl(
            CheckpointStorageWorkerView checkpointStorage,
            String taskName,
            StreamTaskActionExecutor actionExecutor,
            ExecutorService asyncOperationsThreadPool,
            Environment env,
            AsyncExceptionHandler asyncExceptionHandler,
            BiFunctionWithException<
                            ChannelStateWriter, Long, CompletableFuture<Void>, CheckpointException>
                    prepareInputSnapshot,
            int maxRecordAbortedCheckpoints,
            ChannelStateWriter channelStateWriter,
            boolean enableCheckpointAfterTasksFinished,
            DelayableTimer registerTimer,
            FileMergingSnapshotManager fileMergingSnapshotManager) {
        this.checkpointStorage =
                new CachingCheckpointStorageWorkerView(checkNotNull(checkpointStorage));
        this.taskName = checkNotNull(taskName);
        this.checkpoints = new HashMap<>();
        this.lock = new Object();
        this.asyncOperationsThreadPool = checkNotNull(asyncOperationsThreadPool);
        this.env = checkNotNull(env);
        this.asyncExceptionHandler = checkNotNull(asyncExceptionHandler);
        this.actionExecutor = checkNotNull(actionExecutor);
        this.channelStateWriter = checkNotNull(channelStateWriter);
        this.prepareInputSnapshot = prepareInputSnapshot;
        this.abortedCheckpointIds =
                createAbortedCheckpointSetWithLimitSize(maxRecordAbortedCheckpoints);
        this.maxRecordAbortedCheckpoints = maxRecordAbortedCheckpoints;
        this.lastCheckpointId = -1L;
        this.closed = false;
        this.enableCheckpointAfterTasksFinished = enableCheckpointAfterTasksFinished;
        this.registerTimer = registerTimer;
        this.clock = SystemClock.getInstance();
        this.fileMergingSnapshotManager = fileMergingSnapshotManager;
    }

    public static ChannelStateWriter openChannelStateWriter(
            String taskName,
            SupplierWithException<CheckpointStorageWorkerView, ? extends IOException>
                    checkpointStorageWorkerView,
            Environment env,
            int maxSubtasksPerChannelStateFile) {
        return new ChannelStateWriterImpl(
                env.getJobVertexId(),
                taskName,
                env.getTaskInfo().getIndexOfThisSubtask(),
                checkpointStorageWorkerView,
                env.getChannelStateExecutorFactory(),
                maxSubtasksPerChannelStateFile);
    }

    @Override
    public void abortCheckpointOnBarrier(
            long checkpointId, CheckpointException cause, OperatorChain<?, ?> operatorChain)
            throws IOException {
        LOG.debug("Aborting checkpoint via cancel-barrier {} for task {}", checkpointId, taskName);
        lastCheckpointId = Math.max(lastCheckpointId, checkpointId);
        Iterator<Long> iterator = abortedCheckpointIds.iterator();
        while (iterator.hasNext()) {
            long next = iterator.next();
            if (next < lastCheckpointId) {
                iterator.remove();
            } else {
                break;
            }
        }

        checkpointStorage.clearCacheFor(checkpointId);

        channelStateWriter.abort(checkpointId, cause, true);

        // notify the coordinator that we decline this checkpoint
        env.declineCheckpoint(checkpointId, cause);

        actionExecutor.runThrowing(
                () -> {
                    if (checkpointId == alignmentCheckpointId) {
                        cancelAlignmentTimer();
                    }
                    // notify all downstream operators that they should not wait for a barrier from
                    // us and abort checkpoint.
                    operatorChain.abortCheckpoint(checkpointId, cause);
                    operatorChain.broadcastEvent(new CancelCheckpointMarker(checkpointId));
                });
    }

    private void cancelAlignmentTimer() {
        if (alignmentTimer == null) {
            return;
        }
        alignmentTimer.cancel();
        alignmentTimer = null;
    }

    @Override
    public CheckpointStorageWorkerView getCheckpointStorage() {
        return checkpointStorage;
    }

    @Override
    public ChannelStateWriter getChannelStateWriter() {
        return channelStateWriter;
    }

    @Override
    public void checkpointState(//
            CheckpointMetaData metadata,
            CheckpointOptions options,
            CheckpointMetricsBuilder metrics,
            OperatorChain<?, ?> operatorChain,
            boolean isTaskFinished,
            Supplier<Boolean> isRunning)
            throws Exception {

        checkNotNull(options);
        checkNotNull(metrics);

        // All of the following steps happen as an atomic step from the perspective of barriers and
        // records/watermarks/timers/callbacks.
        // We generally try to emit the checkpoint barrier as soon as possible to not affect
        // downstream
        // checkpoint alignments

        if (lastCheckpointId >= metadata.getCheckpointId()) {
            //防止过期或乱序的 Barrier 触发无意义的快照。如果在触发本地快照的同时，JobManager 已经宣告这次 Checkpoint 取消了（比如通过 RPC 提前收到了通知），Subtask 会立刻停手，
            //并且向更下游广播一个 CancelCheckpointMarker。这样能防止下游算子因为一直在死等这个 Barrier 对齐而导致严重的反压（Back-pressure）
            LOG.info(
                    "Out of order checkpoint barrier (aborted previously?): {} >= {}",
                    lastCheckpointId,
                    metadata.getCheckpointId());
            channelStateWriter.abort(metadata.getCheckpointId(), new CancellationException(), true);
            checkAndClearAbortedStatus(metadata.getCheckpointId());
            return;
        }

        logCheckpointProcessingDelay(metadata);

        // Step (0): Record the last triggered checkpointId and abort the sync phase of checkpoint
        // if necessary.
        lastCheckpointId = metadata.getCheckpointId();
        if (checkAndClearAbortedStatus(metadata.getCheckpointId())) {
            // broadcast cancel checkpoint marker to avoid downstream back-pressure due to
            // checkpoint barrier align.
            operatorChain.broadcastEvent(new CancelCheckpointMarker(metadata.getCheckpointId()));
            channelStateWriter.abort(
                    metadata.getCheckpointId(),
                    new CancellationException("checkpoint aborted via notification"),
                    true);
            LOG.info(
                    "Checkpoint {} has been notified as aborted, would not trigger any checkpoint.",
                    metadata.getCheckpointId());
            return;
        }

        if (fileMergingSnapshotManager != null) {
            // notify file merging snapshot manager for managed dir lifecycle management
            fileMergingSnapshotManager.notifyCheckpointStart(
                    FileMergingSnapshotManager.SubtaskKey.of(env), metadata.getCheckpointId());
        }

        // if checkpoint has been previously unaligned, but was forced to be aligned (pointwise
        // connection), revert it here so that it can jump over output data
        if (options.getAlignment() == CheckpointOptions.AlignmentType.FORCED_ALIGNED) {//false
            //如果是由于点对点连接（Pointwise，如 rescale/forward）在非对齐模式下被强转为了对齐（Forced Aligned），
            //在这里将其还原并重新初始化输入端的 Checkpoint 行为，确保其可以“飞跃”输出数据
            options = options.withUnalignedSupported();
            initInputsCheckpoint(metadata.getCheckpointId(), options);
        }

        // Step (1): Prepare the checkpoint, allow operators to do some pre-barrier work.
        //           The pre-barrier work should be nothing or minimal in the common case.
        operatorChain.prepareSnapshotPreBarrier(metadata.getCheckpointId());

        // Step (2): Send the checkpoint barrier downstream
        LOG.debug(
                "Task {} broadcastEvent at {}, triggerTime {}, passed time {}",
                taskName,
                System.currentTimeMillis(),
                metadata.getTimestamp(),
                System.currentTimeMillis() - metadata.getTimestamp());
        CheckpointBarrier checkpointBarrier = new CheckpointBarrier(metadata.getCheckpointId(), metadata.getTimestamp(), options);
        //【重点】
        //如果 options.isUnalignedCheckpoint() 为 false（对齐模式）：这个 Barrier 作为一个普通事件，老老实实地被塞入到下游 PipelinedSubpartition 的 Buffer 队列尾部排队。
        // 如果为 true（非对齐模式）：这就是前几轮讨论的“插队”逻辑。它会作为 PriorityEvent，绕过还在序列化器里排队的用户数据，直接强行塞入到输出 Buffer 队列的最头部
        operatorChain.broadcastEvent(checkpointBarrier, options.isUnalignedCheckpoint());//

        // Step (3): Register alignment timer to timeout aligned barrier to unaligned barrier
        //注册对齐超时定时器
        //如果用户配置了对齐超时时间，这里会注册一个定时任务。
        //如果 Barrier 在下游网络中被堵住、超时未完成对齐，该定时器就会触发，将当前的对齐 Checkpoint 现场就地降级/切换为非对齐 Checkpoint，以应对严重的反压
        registerAlignmentTimer(metadata.getCheckpointId(), operatorChain, checkpointBarrier);

        // Step (4): Prepare to spill the in-flight buffers for input and output
        if (options.needsChannelState()) {
            // output data already written while broadcasting event
            //在非对齐模式下，当上面的步骤 (2) 成功把 Barrier 发送出去（广播完成）的瞬间，所有已经在输出管道里排队的普通数据 Buffer，在物理上都已经被定格了。
            //此时调用 finishOutput，告诉 ChannelStateWriter：“我已经成功把 Barrier 塞到它们前面发出去了，现在可以把这些卡在输出端（Output）的积压数据 Buffer 打包持久化到 HDFS 了”
            channelStateWriter.finishOutput(metadata.getCheckpointId());
        }

        // Step (5): Take the state snapshot. This should be largely asynchronous, to not impact
        // progress of the
        // streaming topology
        Map<OperatorID, OperatorSnapshotFutures> snapshotFutures = CollectionUtil.newHashMapWithExpectedSize(operatorChain.getNumberOfOperators());
        try {
            //主线程执行。遍历 operatorChain 中的所有算子，
            //让它们把内存里/RocksDB 里的当前状态（如 WordCount 计数）做个内存浅拷贝或建立硬链接。这一步必须在 Mailbox 主线程内同步做完，因为此时不能有新的数据流入改变状态
            if (takeSnapshotSync(snapshotFutures, metadata, metrics, options, operatorChain, isRunning)) {
                //同步阶段成功建立指针/副本后，主线程立刻解放，继续去处理业务数据流。该方法会把 snapshotFutures 丢给异步线程池，
                //让异步线程慢慢把状态数据或者 RocksDB 的 SST 文件真正传输到远端的 HDFS/S3。传输完成后，异步线程会向 JobManager 发送 ACK
                finishAndReportAsync(
                        snapshotFutures,
                        metadata,
                        metrics,
                        operatorChain.isTaskDeployedAsFinished(),
                        isTaskFinished,
                        isRunning);
            } else {
                cleanup(snapshotFutures, metadata, metrics, new Exception("Checkpoint declined"));
            }
        } catch (Exception ex) {
            cleanup(snapshotFutures, metadata, metrics, ex);
            throw ex;
        }
    }

    private void registerAlignmentTimer(
            long checkpointId,
            OperatorChain<?, ?> operatorChain,
            CheckpointBarrier checkpointBarrier) {
        // The timer isn't triggered when the checkpoint completes quickly, so cancel timer here.
        cancelAlignmentTimer();
        if (!checkpointBarrier.getCheckpointOptions().isTimeoutable()) {
            return;
        }

        long timerDelay = BarrierAlignmentUtil.getTimerDelay(clock, checkpointBarrier);

        alignmentTimer = registerTimer.registerTask(
                        () -> {
                            try {
                                //
                                operatorChain.alignedBarrierTimeout(checkpointId);
                            } catch (Exception e) {
                                ExceptionUtils.rethrowIOException(e);
                            }
                            alignmentTimer = null;
                            return null;
                        },
                        Duration.ofMillis(timerDelay));
        alignmentCheckpointId = checkpointId;
    }

    @Override
    public void notifyCheckpointComplete(
            long checkpointId, OperatorChain<?, ?> operatorChain, Supplier<Boolean> isRunning)
            throws Exception {

        notifyCheckpoint(
                checkpointId, operatorChain, isRunning, Task.NotifyCheckpointOperation.COMPLETE);
    }

    @Override
    public void notifyCheckpointAborted(
            long checkpointId, OperatorChain<?, ?> operatorChain, Supplier<Boolean> isRunning)
            throws Exception {

        notifyCheckpoint(
                checkpointId, operatorChain, isRunning, Task.NotifyCheckpointOperation.ABORT);
    }

    @Override
    public void notifyCheckpointSubsumed(
            long checkpointId, OperatorChain<?, ?> operatorChain, Supplier<Boolean> isRunning)
            throws Exception {

        notifyCheckpoint(
                checkpointId, operatorChain, isRunning, Task.NotifyCheckpointOperation.SUBSUME);
    }

    private void notifyCheckpoint(
            long checkpointId,
            OperatorChain<?, ?> operatorChain,
            Supplier<Boolean> isRunning,
            Task.NotifyCheckpointOperation notifyCheckpointOperation)
            throws Exception {

        Exception previousException = null;
        try {
            if (!isRunning.get()) {
                LOG.debug(
                        "Ignoring notification of checkpoint {} {} for not-running task {}",
                        notifyCheckpointOperation,
                        checkpointId,
                        taskName);
            } else {
                LOG.debug(
                        "Notification of checkpoint {} {} for task {}",
                        notifyCheckpointOperation,
                        checkpointId,
                        taskName);

                if (notifyCheckpointOperation.equals(Task.NotifyCheckpointOperation.ABORT)) {
                    boolean canceled = cancelAsyncCheckpointRunnable(checkpointId);

                    if (!canceled) {
                        if (checkpointId > lastCheckpointId) {
                            // only record checkpoints that have not triggered on task side.
                            abortedCheckpointIds.add(checkpointId);
                            maxAbortedCheckpointId = Math.max(maxAbortedCheckpointId, checkpointId);
                        }
                    }

                    channelStateWriter.abort(
                            checkpointId,
                            new CancellationException("checkpoint aborted via notification"),
                            false);
                }

                try {
                    switch (notifyCheckpointOperation) {
                        case ABORT:
                            operatorChain.notifyCheckpointAborted(checkpointId);
                            break;
                        case COMPLETE:
                            operatorChain.notifyCheckpointComplete(checkpointId);
                            break;
                        case SUBSUME:
                            operatorChain.notifyCheckpointSubsumed(checkpointId);
                    }
                } catch (Exception e) {
                    previousException = ExceptionUtils.firstOrSuppressed(e, previousException);
                }
            }
        } finally {
            try {
                switch (notifyCheckpointOperation) {
                    case ABORT:
                        env.getTaskStateManager().notifyCheckpointAborted(checkpointId);
                        break;
                    case COMPLETE:
                        env.getTaskStateManager().notifyCheckpointComplete(checkpointId);
                }
                notifyFileMergingSnapshotManagerCheckpoint(checkpointId, notifyCheckpointOperation);
            } catch (Exception e) {
                previousException = ExceptionUtils.firstOrSuppressed(e, previousException);
            }
        }

        ExceptionUtils.tryRethrowException(previousException);
    }

    private void notifyFileMergingSnapshotManagerCheckpoint(
            long checkpointId, Task.NotifyCheckpointOperation notifyCheckpointOperation)
            throws Exception {
        if (fileMergingSnapshotManager != null) {
            switch (notifyCheckpointOperation) {
                case ABORT:
                    fileMergingSnapshotManager.notifyCheckpointAborted(
                            FileMergingSnapshotManager.SubtaskKey.of(env), checkpointId);
                    break;
                case COMPLETE:
                    fileMergingSnapshotManager.notifyCheckpointComplete(
                            FileMergingSnapshotManager.SubtaskKey.of(env), checkpointId);
                    break;
                case SUBSUME:
                    fileMergingSnapshotManager.notifyCheckpointSubsumed(
                            FileMergingSnapshotManager.SubtaskKey.of(env), checkpointId);
                    break;
            }
        }
    }

    @Override
    public void initInputsCheckpoint(long id, CheckpointOptions checkpointOptions)
            throws CheckpointException {
        if (checkpointOptions.isUnalignedCheckpoint()) {
            channelStateWriter.start(id, checkpointOptions);

            prepareInflightDataSnapshot(id);
        } else if (checkpointOptions.isTimeoutable()) {
            // The output buffer may need to be snapshotted, so start the channelStateWriter here.
            channelStateWriter.start(id, checkpointOptions);
            channelStateWriter.finishInput(id);
        }
    }

    public void waitForPendingCheckpoints() throws Exception {
        if (!enableCheckpointAfterTasksFinished) {
            return;
        }

        List<AsyncCheckpointRunnable> asyncCheckpointRunnables;
        synchronized (lock) {
            asyncCheckpointRunnables = new ArrayList<>(checkpoints.values());
        }

        // Waits for each checkpoint independently.
        asyncCheckpointRunnables.forEach(
                ar -> {
                    try {
                        ar.getFinishedFuture().get();
                    } catch (Exception e) {
                        LOG.debug(
                                "Async runnable for checkpoint "
                                        + ar.getCheckpointId()
                                        + " throws exception and exit",
                                e);
                    }
                });
    }

    @Override
    public void close() throws IOException {
        cancelAlignmentTimer();
        cancel();
    }

    public void cancel() throws IOException {
        List<AsyncCheckpointRunnable> asyncCheckpointRunnables = null;
        synchronized (lock) {
            if (!closed) {
                closed = true;
                asyncCheckpointRunnables = new ArrayList<>(checkpoints.values());
                checkpoints.clear();
            }
        }
        IOUtils.closeAllQuietly(asyncCheckpointRunnables);
        channelStateWriter.close();
    }

    @VisibleForTesting
    int getAsyncCheckpointRunnableSize() {
        synchronized (lock) {
            return checkpoints.size();
        }
    }

    @VisibleForTesting
    int getAbortedCheckpointSize() {
        return abortedCheckpointIds.size();
    }

    private boolean checkAndClearAbortedStatus(long checkpointId) {
        return abortedCheckpointIds.remove(checkpointId)
                || checkpointId + maxRecordAbortedCheckpoints < maxAbortedCheckpointId;
    }

    private void registerAsyncCheckpointRunnable(
            long checkpointId, AsyncCheckpointRunnable asyncCheckpointRunnable) throws IOException {
        synchronized (lock) {
            if (closed) {
                LOG.debug(
                        "Cannot register Closeable, this subtaskCheckpointCoordinator is already closed. Closing argument.");
                closeQuietly(asyncCheckpointRunnable);
                checkState(
                        !checkpoints.containsKey(checkpointId),
                        "SubtaskCheckpointCoordinator was closed without releasing asyncCheckpointRunnable for checkpoint %s",
                        checkpointId);
            } else if (checkpoints.containsKey(checkpointId)) {
                closeQuietly(asyncCheckpointRunnable);
                throw new IOException(
                        String.format(
                                "Cannot register Closeable, async checkpoint %d runnable has been register. Closing argument.",
                                checkpointId));
            } else {
                checkpoints.put(checkpointId, asyncCheckpointRunnable);
            }
        }
    }

    private boolean unregisterAsyncCheckpointRunnable(long checkpointId) {
        synchronized (lock) {
            return checkpoints.remove(checkpointId) != null;
        }
    }

    /**
     * Cancel the async checkpoint runnable with given checkpoint id. If given checkpoint id is not
     * registered, return false, otherwise return true.
     */
    private boolean cancelAsyncCheckpointRunnable(long checkpointId) {
        AsyncCheckpointRunnable asyncCheckpointRunnable;
        synchronized (lock) {
            asyncCheckpointRunnable = checkpoints.remove(checkpointId);
        }
        if (asyncCheckpointRunnable != null) {
            asyncOperationsThreadPool.execute(() -> closeQuietly(asyncCheckpointRunnable));
        }
        return asyncCheckpointRunnable != null;
    }

    private void cleanup(
            Map<OperatorID, OperatorSnapshotFutures> operatorSnapshotsInProgress,
            CheckpointMetaData metadata,
            CheckpointMetricsBuilder metrics,
            Exception ex) {

        channelStateWriter.abort(metadata.getCheckpointId(), ex, true);
        for (OperatorSnapshotFutures operatorSnapshotResult :
                operatorSnapshotsInProgress.values()) {
            if (operatorSnapshotResult != null) {
                try {
                    operatorSnapshotResult.cancel();
                } catch (Exception e) {
                    LOG.warn("Could not properly cancel an operator snapshot result.", e);
                }
            }
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "{} - did NOT finish synchronous part of checkpoint {}. Alignment duration: {} ms, snapshot duration {} ms",
                    taskName,
                    metadata.getCheckpointId(),
                    metrics.getAlignmentDurationNanosOrDefault() / 1_000_000,
                    metrics.getSyncDurationMillis());
        }
    }

    private void prepareInflightDataSnapshot(long checkpointId) throws CheckpointException {
        prepareInputSnapshot
                .apply(channelStateWriter, checkpointId)
                .whenComplete(
                        (unused, ex) -> {
                            if (ex != null) {
                                channelStateWriter.abort(
                                        checkpointId,
                                        ex,
                                        false /* result is needed and cleaned by getWriteResult */);
                            } else {
                                channelStateWriter.finishInput(checkpointId);
                            }
                        });
    }

    private void finishAndReportAsync(
            Map<OperatorID, OperatorSnapshotFutures> snapshotFutures,
            CheckpointMetaData metadata,
            CheckpointMetricsBuilder metrics,
            boolean isTaskDeployedAsFinished,
            boolean isTaskFinished,
            Supplier<Boolean> isRunning)
            throws IOException {
        AsyncCheckpointRunnable asyncCheckpointRunnable =
                new AsyncCheckpointRunnable(
                        snapshotFutures,
                        metadata,
                        metrics,
                        System.nanoTime(),
                        taskName,
                        unregisterConsumer(),
                        env,
                        asyncExceptionHandler,
                        isTaskDeployedAsFinished,
                        isTaskFinished,
                        isRunning);

        registerAsyncCheckpointRunnable(
                asyncCheckpointRunnable.getCheckpointId(), asyncCheckpointRunnable);

        // we are transferring ownership over snapshotInProgressList for cleanup to the thread,
        // active on submit
        asyncOperationsThreadPool.execute(asyncCheckpointRunnable);
    }

    private Consumer<AsyncCheckpointRunnable> unregisterConsumer() {
        return asyncCheckpointRunnable ->
                unregisterAsyncCheckpointRunnable(asyncCheckpointRunnable.getCheckpointId());
    }

    private boolean takeSnapshotSync(
            Map<OperatorID, OperatorSnapshotFutures> operatorSnapshotsInProgress,
            CheckpointMetaData checkpointMetaData,
            CheckpointMetricsBuilder checkpointMetrics,
            CheckpointOptions checkpointOptions,
            OperatorChain<?, ?> operatorChain,
            Supplier<Boolean> isRunning)
            throws Exception {

        checkState(
                !operatorChain.isClosed(),
                "OperatorChain and Task should never be closed at this point");

        long checkpointId = checkpointMetaData.getCheckpointId();
        long started = System.nanoTime();

        //提取非对齐模式下的网络状态
        //如果是对齐（Aligned）**检查点，不需要记录通道状态，直接返回 EMPTY
        //如果是非对齐（Unaligned）**检查点，在执行此方法前，
        //网络层已经通过 channelStateWriter 开始把输入/输出端积压的 Buffer 往持久化存储里导出了。这里通过 getAndRemoveWriteResult 将这批正在写入的通道数据的 Future 结果集 捞出来
        ChannelStateWriteResult channelStateWriteResult =
                checkpointOptions.needsChannelState()
                        ? channelStateWriter.getAndRemoveWriteResult(checkpointId)
                        : ChannelStateWriteResult.EMPTY;

        //负责解析本次 Checkpoint 应该写到哪（是写到全局默认的 HDFS 路径，还是用户手动指定的 Savepoint 专属目录）
        CheckpointStreamFactory storage =
                checkpointStorage.resolveCheckpointStorageLocation(checkpointId, checkpointOptions.getTargetLocation());
        //让多个并发子任务能够将状态写进同一个物理大文件中，极大地减小了对 HDFS NameNode 的元数据压力
        storage = applyFileMergingCheckpoint(storage, checkpointOptions);

        try {
            //RegularOperatorChain#snapshotState
            operatorChain.snapshotState(
                    operatorSnapshotsInProgress,
                    checkpointMetaData,
                    checkpointOptions,
                    isRunning,
                    channelStateWriteResult,
                    storage);

        } finally {
            checkpointStorage.clearCacheFor(checkpointId);
        }

        checkpointMetrics.setSyncDurationMillis((System.nanoTime() - started) / 1_000_000);

        LOG.debug(
                "{} - finished synchronous part of checkpoint {}. Alignment duration: {} ms, snapshot duration {} ms, is unaligned checkpoint : {}",
                taskName,
                checkpointId,
                checkpointMetrics.getAlignmentDurationNanosOrDefault() / 1_000_000,
                checkpointMetrics.getSyncDurationMillis(),
                checkpointOptions.isUnalignedCheckpoint());

        return true;
    }

    private CheckpointStreamFactory applyFileMergingCheckpoint(
            CheckpointStreamFactory storage, CheckpointOptions checkpointOptions) {
        if (storage instanceof FsMergingCheckpointStorageLocation
                && checkpointOptions.getCheckpointType().isSavepoint()) {
            // fall back to non-fileMerging if it is a savepoint
            return ((FsMergingCheckpointStorageLocation) storage).toNonFileMerging();
        } else {
            return storage;
        }
    }

    private Set<Long> createAbortedCheckpointSetWithLimitSize(int maxRecordAbortedCheckpoints) {
        return Collections.newSetFromMap(
                new LinkedHashMap<Long, Boolean>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    protected boolean removeEldestEntry(Map.Entry<Long, Boolean> eldest) {
                        return size() > maxRecordAbortedCheckpoints;
                    }
                });
    }

    // Caches checkpoint output stream factories to prevent multiple output stream per checkpoint.
    // This could result from requesting output stream by different entities (this and
    // channelStateWriter)
    // We can't just pass a stream to the channelStateWriter because it can receive checkpoint call
    // earlier than this class
    // in some unaligned checkpoints scenarios
    private static class CachingCheckpointStorageWorkerView implements CheckpointStorageWorkerView {
        private final Map<Long, CheckpointStreamFactory> cache = new ConcurrentHashMap<>();
        private final CheckpointStorageWorkerView delegate;

        private CachingCheckpointStorageWorkerView(CheckpointStorageWorkerView delegate) {
            this.delegate = delegate;
        }

        void clearCacheFor(long checkpointId) {
            cache.remove(checkpointId);
        }

        @Override
        public CheckpointStreamFactory resolveCheckpointStorageLocation(
                long checkpointId, CheckpointStorageLocationReference reference) {
            return cache.computeIfAbsent(
                    checkpointId,
                    id -> {
                        try {
                            return delegate.resolveCheckpointStorageLocation(
                                    checkpointId, reference);
                        } catch (IOException e) {
                            throw new FlinkRuntimeException(e);
                        }
                    });
        }

        @Override
        public CheckpointStateOutputStream createTaskOwnedStateStream() throws IOException {
            return delegate.createTaskOwnedStateStream();
        }

        @Override
        public CheckpointStateToolset createTaskOwnedCheckpointStateToolset() {
            return delegate.createTaskOwnedCheckpointStateToolset();
        }
    }

    private static void logCheckpointProcessingDelay(CheckpointMetaData checkpointMetaData) {
        long delay = System.currentTimeMillis() - checkpointMetaData.getReceiveTimestamp();
        if (delay >= CHECKPOINT_EXECUTION_DELAY_LOG_THRESHOLD_MS) {
            LOG.warn(
                    "Time from receiving all checkpoint barriers/RPC for checkpoint {} to executing it exceeded threshold: {}ms",
                    checkpointMetaData.getCheckpointId(),
                    delay);
        }
    }
}
