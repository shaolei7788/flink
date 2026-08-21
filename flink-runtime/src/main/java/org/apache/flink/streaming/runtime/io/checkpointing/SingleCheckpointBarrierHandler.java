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

package org.apache.flink.streaming.runtime.io.checkpointing;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.partition.consumer.CheckpointableInput;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointableTask;
import org.apache.flink.streaming.runtime.io.checkpointing.BarrierAlignmentUtil.Cancellable;
import org.apache.flink.streaming.runtime.io.checkpointing.BarrierAlignmentUtil.DelayableTimer;
import org.apache.flink.streaming.runtime.tasks.SubtaskCheckpointCoordinator;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.FunctionWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.CHECKPOINT_DECLINED_INPUT_END_OF_STREAM;
import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.CHECKPOINT_DECLINED_SUBSUMED;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link SingleCheckpointBarrierHandler} is used for triggering checkpoint while reading the first
 * barrier and keeping track of the number of received barriers and consumed barriers. It can
 * handle/track just single checkpoint at a time. The behaviour when to actually trigger the
 * checkpoint and what the {@link CheckpointableInput} should do is controlled by {@link
 * BarrierHandlerState}.
 */
//输入端（Input 侧）拦截 Barrier 并编排对齐逻辑的核心大脑
@Internal
@NotThreadSafe
public class SingleCheckpointBarrierHandler extends CheckpointBarrierHandler {

    private static final Logger LOG = LoggerFactory.getLogger(SingleCheckpointBarrierHandler.class);

    private final String taskName;

    //作用：当前 Handler 的物理大管家（上下文控制器）。
    //内幕：它持有外部 CheckpointedInputGate 和当前 Task 的控制权。当状态机判定 Barrier 已经齐备，或者需要非对齐强行向更下游插队广播时，
    //状态机自己没有网络发射权限，必须通过调用 context.triggerGlobalCheckpoint(...) 或 context.initInputsCheckpoint(...) 来驱动外界的网络和算子层工作
    private final ControllerImpl context;
    //作用：Flink 网络层专用的高级延迟定时器处理器。
    //内幕：它用来跟操作系统的底层时间轮（Time Wheel）或流处理的 Mailbox 线程定时器对接。我们在前面代码里看到的“注册对齐超时定时器”，底层就是委托它去安排一个延时任务
    private final DelayableTimer registerTimer;
    //当 Handler 辛辛苦苦把所有通道的 Barrier 全对齐、宣告大功告成后，它会直接调用这个引用，
    //触发 checkpointState 方法，开启真正的本地快照（Snapshot）和异步写盘流程
    private final SubtaskCheckpointCoordinator subTaskCheckpointCoordinator;
    //作用：当前 Task 所管理的所有可检查点输入源（管道）数组。内幕：每一个元素代表一个真实的输入网关或通道。当非对齐启动、宣布升级为 UC 时，
    //Handler 需要遍历这个数组，逐个调用 input.checkpointStarted(...)，命令它们立刻定格并抓取各自内部正在排队的在途数据 Buffer
    private final CheckpointableInput[] inputs;

    /**
     * The checkpoint id to guarantee that we would trigger only one checkpoint when reading the
     * same barrier from different channels.
     */
    //当前正在等待、收集或对齐的 Checkpoint 唯一的全局 ID
    private long currentCheckpointId = -1L;

    /**
     * The checkpoint barrier of the current pending checkpoint. It is to allow us to access the
     * checkpoint options when processing {@code EndOfPartitionEvent}.
     */
    //当前正在挂起/等待对齐的 CheckpointBarrier 物理对象缓存
    //内幕：它用来临时保存 Barrier 里的元数据配置（CheckpointOptions）。
    //在极特殊的边界场景下（例如突然收到了 EndOfPartitionEvent，代表某个上游通道突然挂了或结束了），
    //Task 必须能在没有物理 Barrier 的情况下，通过这个挂起的缓存去读取本次检查点的属性（如判定当前到底是对齐还是非对齐连接），从而安全地平摊剔除该通道
    @Nullable
    private CheckpointBarrier pendingCheckpointBarrier;

    //已对齐（已签到）的通道
    //Handler 会拿 alignedChannels.size() == targetChannelCount 作为对齐的结果
    private final Set<InputChannelInfo> alignedChannels = new HashSet<>();

    //本轮检查点预期必须收集满的通道总目标数
    private int targetChannelCount;

    //历史分界线 ID。记录最近一次成功完结、或者被 Master 明确宣告取消的 Checkpoint ID
    private long lastCancelledOrCompletedCheckpointId = -1L;

    //当前仍然存活、未关闭的物理通道数量
    private int numOpenChannels;

    //作用：异步事件通知代理 —— 全网对齐的终极捷报。内幕：Flink 2.x 的驱动引擎是 Mailbox 线程模型。
    // 当 Task 在处理数据时，主循环如果因为等对齐被阻塞了，就会监听这个 Future。一旦所有通道对齐（Set 塞满），
    // 代码执行 allBarriersReceivedFuture.complete(null)，Mailbox 线程就像触电一样立刻被唤醒，继续推进业务流水线
    private CompletableFuture<Void> allBarriersReceivedFuture = new CompletableFuture<>();

    //作用：状态机模式下的当前活动状态实例（Current State）。内幕：它指向具体的子类。如果当前还安全，它可能是闲置状态（RestingState）；如果开始等对齐，变为 CollectingBarriersState；
    //如果超时切换，变为你上一问贴出的 AlternatingWaitingForFirstBarrierUnaligned。它决定了收到 Barrier 时到底走对齐阻塞还是非对齐超车的业务逻辑
    private BarrierHandlerState currentState;

    //作用：当前正在滴答走时的对齐超时定时器句柄（句柄引用）。内幕：当开启了自适应切换、第一个 Barrier 签到时，
    // 系统开启 10 秒倒计时，并把定时器的句柄赋给这个变量。如果在 10 秒内所有通道提前对齐通关了，
    //Handler 会调用 currentAlignmentTimer.cancel() 强行按死这个定时器，防止它在下一轮无故引发非对齐降级；如果 10 秒没到齐，定时器到期引爆，触发降级升级为 UC
    private Cancellable currentAlignmentTimer;

    //作用：全局静态配置标志位 —— 是否开启了“对齐与非对齐自适应交替”模式。内幕：直接对应你在代码里写没写 setAlignedCheckpointTimeout。
    //如果为 true，状态机就会在对齐和非对齐之间反复横跳，完美平衡延迟与存储；  【默认】
    //如果为 false，作业要么是纯对齐（等到死），要么是纯非对齐（一进来就插队）
    private final boolean alternating;

    @VisibleForTesting
    public static SingleCheckpointBarrierHandler createUnalignedCheckpointBarrierHandler(
            SubtaskCheckpointCoordinator checkpointCoordinator,
            String taskName,
            CheckpointableTask toNotifyOnCheckpoint,
            Clock clock,
            boolean enableCheckpointsAfterTasksFinish,
            CheckpointableInput... inputs) {
        return unaligned(
                taskName,
                toNotifyOnCheckpoint,
                checkpointCoordinator,
                clock,
                (int)
                        Arrays.stream(inputs)
                                .flatMap(gate -> gate.getChannelInfos().stream())
                                .count(),
                (callable, duration) -> {
                    throw new IllegalStateException(
                            "Strictly unaligned checkpoints should never register any callbacks");
                },
                enableCheckpointsAfterTasksFinish,
                inputs);
    }

    public static SingleCheckpointBarrierHandler unaligned(
            String taskName,
            CheckpointableTask toNotifyOnCheckpoint,
            SubtaskCheckpointCoordinator checkpointCoordinator,
            Clock clock,
            int numOpenChannels,
            DelayableTimer registerTimer,
            boolean enableCheckpointAfterTasksFinished,
            CheckpointableInput... inputs) {
        return new SingleCheckpointBarrierHandler(
                taskName,
                toNotifyOnCheckpoint,
                checkpointCoordinator,
                clock,
                numOpenChannels,
                new AlternatingWaitingForFirstBarrierUnaligned(false, new ChannelState(inputs)),
                false,
                registerTimer,
                inputs,
                enableCheckpointAfterTasksFinished);
    }

    public static SingleCheckpointBarrierHandler aligned(
            String taskName,
            CheckpointableTask toNotifyOnCheckpoint,
            Clock clock,
            int numOpenChannels,
            DelayableTimer registerTimer,
            boolean enableCheckpointAfterTasksFinished,
            CheckpointableInput... inputs) {
        return new SingleCheckpointBarrierHandler(
                taskName,
                toNotifyOnCheckpoint,
                null,
                clock,
                numOpenChannels,
                new WaitingForFirstBarrier(inputs),
                false,
                registerTimer,
                inputs,
                enableCheckpointAfterTasksFinished);
    }

    public static SingleCheckpointBarrierHandler alternating(
            String taskName,
            CheckpointableTask toNotifyOnCheckpoint,
            SubtaskCheckpointCoordinator checkpointCoordinator,
            Clock clock,
            int numOpenChannels,
            DelayableTimer registerTimer,
            boolean enableCheckpointAfterTasksFinished,
            CheckpointableInput... inputs) {
        return new SingleCheckpointBarrierHandler(//
                taskName,
                toNotifyOnCheckpoint,
                checkpointCoordinator,
                clock,
                numOpenChannels,
                new AlternatingWaitingForFirstBarrier(new ChannelState(inputs)),
                true,
                registerTimer,
                inputs,
                enableCheckpointAfterTasksFinished);
    }

    private SingleCheckpointBarrierHandler(
            String taskName,
            CheckpointableTask toNotifyOnCheckpoint,
            @Nullable SubtaskCheckpointCoordinator subTaskCheckpointCoordinator,
            Clock clock,
            int numOpenChannels,
            BarrierHandlerState currentState,
            boolean alternating,
            DelayableTimer registerTimer,
            CheckpointableInput[] inputs,
            boolean enableCheckpointAfterTasksFinished) {
        super(toNotifyOnCheckpoint, clock, enableCheckpointAfterTasksFinished);

        this.taskName = taskName;
        this.numOpenChannels = numOpenChannels;
        this.currentState = currentState;
        this.alternating = alternating;//true
        this.registerTimer = registerTimer;
        this.subTaskCheckpointCoordinator = subTaskCheckpointCoordinator;
        this.context = new ControllerImpl();
        this.inputs = inputs;
    }

    @Override
    public void processBarrier(CheckpointBarrier barrier, InputChannelInfo channelInfo, boolean isRpcTriggered) throws IOException {//
        long barrierId = barrier.getId();
        LOG.debug("{}: Received barrier from channel {} @ {}.", taskName, channelInfo, barrierId);

        //情况 A (currentCheckpointId > barrierId)：这是一个迟到的旧屏障
        //情况 B (currentCheckpointId == barrierId && !isCheckpointPending())：这是一个重复的屏障，或者当前 Checkpoint 已经在本算子做完了
        if (currentCheckpointId > barrierId || (currentCheckpointId == barrierId && !isCheckpointPending())) {
            if (!barrier.getCheckpointOptions().isUnalignedCheckpoint()) {
                inputs[channelInfo.getGateIdx()].resumeConsumption(channelInfo);
            }
            return;
        }
        //当一个全新的、比当前 currentCheckpointId 还要大的 Checkpoint ID 第一次到达时，
        //这个方法会被触发。它负责在 Handler 内部开辟全新的上下文，把旧的对齐通道列表清空，重置计时器，并向系统宣告：“属于新检查点的时代开始了”
        checkNewCheckpoint(barrier);
        checkState(currentCheckpointId == barrierId);
        FunctionWithException<BarrierHandlerState, BarrierHandlerState, Exception> stateTransformer = new FunctionWithException<>() {

            @Override
            public BarrierHandlerState apply(BarrierHandlerState state) throws Exception {
                //state = AlternatingWaitingForFirstBarrier
                //AbstractAlternatingAlignedBarrierHandlerState#barrierReceived
                return state.barrierReceived(context, channelInfo, barrier, !isRpcTriggered);
            }
        };
        markCheckpointAlignedAndTransformState(channelInfo, barrier, stateTransformer);
    }

    protected void markCheckpointAlignedAndTransformState(
            InputChannelInfo alignedChannel,
            CheckpointBarrier barrier,
            FunctionWithException<BarrierHandlerState, BarrierHandlerState, Exception> stateTransformer)
            throws IOException {

        alignedChannels.add(alignedChannel);
        if (alignedChannels.size() == 1) {
            if (targetChannelCount == 1) {
                //当前 Task 总共就只有一个上游通道（比如并行度为 1 且没有 KeyBy 重新分区）。此时第一个 Barrier 到达，既是启动也是结束
                markAlignmentStartAndEnd(barrier.getId(), barrier.getTimestamp());
            } else {
                //Flink 会记录下当前的时间戳，开始计时对齐时间（Alignment Duration）。这个指标会作为 Metrics 汇报到 Flink Web UI 上，如果这个时间很长，说明作业出现了严重的网络反压
                markAlignmentStart(barrier.getId(), barrier.getTimestamp());
            }
        }

        // we must mark alignment end before calling currentState.barrierReceived which might
        // trigger a checkpoint with unfinished future for alignment duration
        //当目前登记的已对齐通道数量（alignedChannels.size()）正好等于预期的总通道数（targetChannelCount）时，说明最后那张多米诺骨牌也倒下了，所有通道的 Barrier 全部到齐
        if (alignedChannels.size() == targetChannelCount) {
            if (targetChannelCount > 1) {
                markAlignmentEnd();
            }
        }

        try {
            //BarrierHandlerState#barrierReceived
            currentState = stateTransformer.apply(currentState);
        } catch (CheckpointException e) {
            abortInternal(currentCheckpointId, e);
        } catch (Exception e) {
            ExceptionUtils.rethrowIOException(e);
        }

        if (alignedChannels.size() == targetChannelCount) {
            alignedChannels.clear();
            lastCancelledOrCompletedCheckpointId = currentCheckpointId;
            LOG.debug(
                    "{}: All the channels are aligned for checkpoint {}.",
                    taskName,
                    currentCheckpointId);
            resetAlignmentTimer();
            allBarriersReceivedFuture.complete(null);
        }
    }

    private void triggerCheckpoint(CheckpointBarrier trigger) throws IOException {
        LOG.debug(
                "{}: Triggering checkpoint {} on the barrier announcement at {}.",
                taskName,
                trigger.getId(),
                trigger.getTimestamp());
        //
        notifyCheckpoint(trigger);
    }

    @Override
    public void processBarrierAnnouncement(
            CheckpointBarrier announcedBarrier, int sequenceNumber, InputChannelInfo channelInfo)
            throws IOException {
        checkNewCheckpoint(announcedBarrier);

        long barrierId = announcedBarrier.getId();
        if (currentCheckpointId > barrierId
                || (currentCheckpointId == barrierId && !isCheckpointPending())) {
            LOG.debug(
                    "{}: Obsolete announcement of checkpoint {} for channel {}.",
                    taskName,
                    barrierId,
                    channelInfo);
            return;
        }

        currentState = currentState.announcementReceived(context, channelInfo, sequenceNumber);
    }

    private void registerAlignmentTimer(CheckpointBarrier announcedBarrier) {
        long timerDelay = BarrierAlignmentUtil.getTimerDelay(getClock(), announcedBarrier);

        this.currentAlignmentTimer =
                registerTimer.registerTask(
                        () -> {
                            long barrierId = announcedBarrier.getId();
                            try {
                                if (currentCheckpointId == barrierId && !getAllBarriersReceivedFuture(barrierId).isDone()) {
                                    currentState = currentState.alignedCheckpointTimeout(context, announcedBarrier);
                                }
                            } catch (CheckpointException ex) {
                                this.abortInternal(barrierId, ex);
                            } catch (Exception e) {
                                ExceptionUtils.rethrowIOException(e);
                            }
                            currentAlignmentTimer = null;
                            return null;
                        },
                        Duration.ofMillis(timerDelay));
    }

    //清算上一轮、拒绝过期信号、开辟新一轮上下文、并为自适应超时切换（Alternating）安上全新的倒计时时钟
    private void checkNewCheckpoint(CheckpointBarrier barrier) throws IOException {
        long barrierId = barrier.getId();
        if (currentCheckpointId >= barrierId) {
            return; // This barrier is not the first for this checkpoint.
        }

        if (isCheckpointPending()) {
            //意味着当前 Task 还在苦苦对齐或等待上一轮（如 Checkpoint 20）的 Barrier，结果旧的还没等齐，
            //新一轮（Checkpoint 21）的第一个 Barrier 居然已经迎面撞进来了
            //这说明上一轮 Checkpoint 在全局层面上已经被认定为超时或失败了。
            //Flink 遵循最新原则（Subsume 机制）：只要新时代的号角响起，旧时代的残党就必须被无情抹杀
            cancelSubsumedCheckpoint(barrierId);
        }
        // 1. 升级全局 ID 标识
        currentCheckpointId = barrierId;
        // 2. 缓存本次物理 Barrier 的属性配置
        pendingCheckpointBarrier = barrier;
        // 3. 【最关键】清空已对齐通道的登记簿
        alignedChannels.clear();
        // 4. 重新锚定本轮必须收齐的通道总数
        targetChannelCount = numOpenChannels;
        // 5. 实例化全新的异步通知代理
        allBarriersReceivedFuture = new CompletableFuture<>();
        //alternating 对齐模式 默认false， 非对齐模式默认为true
        if (alternating && barrier.getCheckpointOptions().isTimeoutable()) {
            //注册对齐超时定时器，超过时间没完成checkpoint就升级为非对齐checkpoint
            registerAlignmentTimer(barrier);
        }
    }

    @Override
    public void processCancellationBarrier(
            CancelCheckpointMarker cancelBarrier, InputChannelInfo channelInfo) throws IOException {
        final long cancelledId = cancelBarrier.getCheckpointId();
        if (cancelledId > currentCheckpointId
                || (cancelledId == currentCheckpointId && alignedChannels.size() > 0)) {
            LOG.debug("{}: Received cancellation {}.", taskName, cancelledId);
            abortInternal(
                    cancelledId,
                    new CheckpointException(
                            CheckpointFailureReason.CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER));
        }
    }

    private void abortInternal(long cancelledId, CheckpointFailureReason reason)
            throws IOException {
        abortInternal(cancelledId, new CheckpointException(reason));
    }

    private void abortInternal(long cancelledId, CheckpointException exception) throws IOException {
        LOG.debug(
                "{}: Aborting checkpoint {} after exception {}.",
                taskName,
                currentCheckpointId,
                exception);
        // by setting the currentCheckpointId to this checkpoint while keeping the numBarriers
        // at zero means that no checkpoint barrier can start a new alignment
        currentCheckpointId = Math.max(cancelledId, currentCheckpointId);
        lastCancelledOrCompletedCheckpointId =
                Math.max(lastCancelledOrCompletedCheckpointId, cancelledId);
        pendingCheckpointBarrier = null;
        alignedChannels.clear();
        targetChannelCount = 0;
        resetAlignmentTimer();
        currentState = currentState.abort(cancelledId);
        if (cancelledId == currentCheckpointId) {
            resetAlignment();
        }
        notifyAbort(cancelledId, exception);
        allBarriersReceivedFuture.completeExceptionally(exception);
    }

    private void resetAlignmentTimer() {
        if (currentAlignmentTimer != null) {
            currentAlignmentTimer.cancel();
            currentAlignmentTimer = null;
        }
    }

    @Override
    public void processEndOfPartition(InputChannelInfo channelInfo) throws IOException {
        numOpenChannels--;

        if (!isCheckpointAfterTasksFinishedEnabled()) {
            if (isCheckpointPending()) {
                LOG.warn(
                        "{}: Received EndOfPartition(-1) before completing current checkpoint {}. Skipping current checkpoint.",
                        taskName,
                        currentCheckpointId);
                abortInternal(currentCheckpointId, CHECKPOINT_DECLINED_INPUT_END_OF_STREAM);
            }
        } else {
            if (!isCheckpointPending()) {
                return;
            }

            checkState(
                    pendingCheckpointBarrier != null,
                    "pending checkpoint barrier should not be null when"
                            + " there is pending checkpoint.");

            markCheckpointAlignedAndTransformState(
                    channelInfo,
                    pendingCheckpointBarrier,
                    state -> state.endOfPartitionReceived(context, channelInfo));
        }
    }

    @Override
    public long getLatestCheckpointId() {
        return currentCheckpointId;
    }

    @Override
    public void close() throws IOException {
        resetAlignmentTimer();
        allBarriersReceivedFuture.cancel(false);
        super.close();
    }

    @Override
    protected boolean isCheckpointPending() {
        return currentCheckpointId != lastCancelledOrCompletedCheckpointId
                && currentCheckpointId >= 0;
    }

    private void cancelSubsumedCheckpoint(long barrierId) throws IOException {
        LOG.warn(
                "{}: Received checkpoint barrier for checkpoint {} before completing current checkpoint {}. "
                        + "Skipping current checkpoint.",
                taskName,
                barrierId,
                currentCheckpointId);
        abortInternal(currentCheckpointId, CHECKPOINT_DECLINED_SUBSUMED);
    }

    public CompletableFuture<Void> getAllBarriersReceivedFuture(long checkpointId) {
        if (checkpointId < currentCheckpointId || numOpenChannels == 0) {
            return FutureUtils.completedVoidFuture();
        }
        if (checkpointId > currentCheckpointId) {
            throw new IllegalStateException(
                    "Checkpoint " + checkpointId + " has not been started at all");
        }
        return allBarriersReceivedFuture;
    }

    @VisibleForTesting
    int getNumOpenChannels() {
        return numOpenChannels;
    }

    @Override
    public String toString() {
        return String.format(
                "%s: current checkpoint: %d, current aligned channels: %d, target channel count: %d",
                taskName, currentCheckpointId, alignedChannels.size(), targetChannelCount);
    }

    private final class ControllerImpl implements BarrierHandlerState.Controller {
        @Override
        public void triggerGlobalCheckpoint(CheckpointBarrier checkpointBarrier)
                throws IOException {
            //
            SingleCheckpointBarrierHandler.this.triggerCheckpoint(checkpointBarrier);
        }

        @Override
        public boolean isTimedOut(CheckpointBarrier barrier) {
            return barrier.getCheckpointOptions().isTimeoutable()
                    && barrier.getId() <= currentCheckpointId
                    && barrier.getCheckpointOptions().getAlignedCheckpointTimeout()
                            < (getClock().absoluteTimeMillis() - barrier.getTimestamp());
        }

        @Override
        public boolean allBarriersReceived() {
            return alignedChannels.size() == targetChannelCount;
        }

        @Nullable
        @Override
        public CheckpointBarrier getPendingCheckpointBarrier() {
            return pendingCheckpointBarrier;
        }

        @Override
        public void initInputsCheckpoint(CheckpointBarrier checkpointBarrier)
                throws CheckpointException {
            checkState(subTaskCheckpointCoordinator != null);
            long barrierId = checkpointBarrier.getId();
            subTaskCheckpointCoordinator.initInputsCheckpoint(
                    barrierId, checkpointBarrier.getCheckpointOptions());
        }
    }
}
