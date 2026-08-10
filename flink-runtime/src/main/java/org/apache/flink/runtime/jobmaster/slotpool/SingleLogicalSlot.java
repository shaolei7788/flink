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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotContext;
import org.apache.flink.runtime.jobmaster.SlotOwner;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/** Implementation of the {@link LogicalSlot}. */
public class SingleLogicalSlot implements LogicalSlot, PhysicalSlot.Payload {

    private static final AtomicReferenceFieldUpdater<SingleLogicalSlot, Payload> PAYLOAD_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(
                    SingleLogicalSlot.class, Payload.class, "payload");

    private static final AtomicReferenceFieldUpdater<SingleLogicalSlot, State> STATE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(SingleLogicalSlot.class, State.class, "state");

    //身份标识：本次逻辑槽位申请请求的 唯一流水号（UUID）
    private final SlotRequestId slotRequestId;

    //该逻辑槽位所依附的底层物理槽位的上下文信息（是它的“物理真身”）。
    // 核心作用：通过它，任务才能获取到真正的物理硬件信息。它里面包裹了：
    // TaskManagerLocation：该槽位在哪台机器上（IP 地址、数据中心、机架信息）。
    // AllocationID：底层物理资源的绝对分配 ID。
    // ResourceProfile：该槽位的物理资源规格（有多少 CPU、多少内存、多少网络带宽）
    private final SlotContext slotContext;

    // locality of this slot wrt the requested preferred locations
    //描述当前分配的物理位置与任务期望的物理位置之间的亲和度
    //核心作用：Flink 在调度时追求“数据本地化”（Data Locality），
    // 尽量让 Task 运行在它要消费的数据所在的机器上（比如同一个机架、甚至同一个机器）。Locality 是个枚举值，记录了本次分配的结果：
    // LOCAL：完美命中，就在期望的机器上。
    // HOST_LOCAL：在同一个主机上，但可能是不同的容器/TaskManager。
    // NON_LOCAL：远程节点，需要走跨网络传输
    private final Locality locality;

    //定义了当这个逻辑槽位被释放（销毁）时，应该把物理资源归还给谁
    //核心作用：它是一个回调接口。通常在槽位共享模式下，这个 slotOwner 指向的是父级的 SharedSlot。当当前逻辑槽位生命周期结束执行 releaseSlot 时，
    // 它会调用 slotOwner.returnLogicalSlot(this)，通知父级容器：“我用完了，房间空出来了，你可以安排给别人或者退租了”
    // owner of this slot to which it is returned upon release
    private final SlotOwner slotOwner;

    //一个用于监听该槽位是否已经彻底释放的异步 Future。核心作用：外部组件（如调度器或监控模块）如果想在“槽位被销毁时”触发某些清理动作，不需要去轮询状态，
    // 只需要在 releaseFuture.thenAccept(...) 上挂载回调即可。当槽位真正完成释放逻辑时，该 Future 会被 complete(null)
    private final CompletableFuture<Void> releaseFuture;

    //当前逻辑槽位的生命周期状态（使用了 volatile 保证多线程可见性）。
    // 核心作用：控制槽位的状态流转，防止并发重复操作。典型状态包括：
    // ALLOCATED（已分配，但还没绑定任务）
    // ASSIGNED（已绑定任务）
    // RELEASED（已释放/已销毁）
    private volatile State state;

    //当前在这个逻辑槽位上真正运行的具体业务对象。核心作用：
    // Payload 是一个接口，在实际运行中，它的实现类通常就是 Execution（任务执行实例）。
    // 当槽位分配给某个任务时，通过 tryAssignPayload(Payload) 把 Execution 塞进这个变量，
    // 两者正式合体。如果任务运行过程中发生异常失败，槽位可以通过 payload.fail(throwable) 反向去触发任务的状态机变更
    // LogicalSlot.Payload of this slot
    private volatile Payload payload;

    //标记这个逻辑槽位是否会被无限期（持久）地占用
    //对于流处理作业（Streaming），任务启动后会一直运行（除非作业停止），因此该值为 true。这会告诉底层资源管理器：“不要妄想短期内回收这个 Slot”。
    // 对于批处理作业（Batch），任务往往运行几秒或几分钟就结束并释放资源，该值为 false。这有助于 Flink 的资源管理器进行更动态、更大胆的资源按需申请与缩容优化
    /** Whether this logical slot will be occupied indefinitely. */
    private boolean willBeOccupiedIndefinitely;

    @VisibleForTesting
    public SingleLogicalSlot(
            SlotRequestId slotRequestId,
            SlotContext slotContext,
            Locality locality,
            SlotOwner slotOwner) {

        this(slotRequestId, slotContext, locality, slotOwner, true);
    }

    public SingleLogicalSlot(
            SlotRequestId slotRequestId,
            SlotContext slotContext,
            Locality locality,
            SlotOwner slotOwner,
            boolean willBeOccupiedIndefinitely) {
        this.slotRequestId = Preconditions.checkNotNull(slotRequestId);

        this.slotContext = Preconditions.checkNotNull(slotContext);
        this.locality = Preconditions.checkNotNull(locality);
        // slotOwner = SharedSlot
        this.slotOwner = Preconditions.checkNotNull(slotOwner);
        this.willBeOccupiedIndefinitely = willBeOccupiedIndefinitely;
        this.releaseFuture = new CompletableFuture<>();

        this.state = State.ALIVE;
        this.payload = null;
    }

    @Override
    public TaskManagerLocation getTaskManagerLocation() {
        return slotContext.getTaskManagerLocation();
    }

    @Override
    public TaskManagerGateway getTaskManagerGateway() {
        return slotContext.getTaskManagerGateway();
    }

    @Override
    public Locality getLocality() {
        return locality;
    }

    @Override
    public boolean isAlive() {
        return state == State.ALIVE;
    }

    @Override
    public boolean tryAssignPayload(Payload payload) {
        return PAYLOAD_UPDATER.compareAndSet(this, null, payload);
    }

    @Nullable
    @Override
    public Payload getPayload() {
        return payload;
    }

    @Override
    public CompletableFuture<?> releaseSlot(@Nullable Throwable cause) {
        if (STATE_UPDATER.compareAndSet(this, State.ALIVE, State.RELEASING)) {
            signalPayloadRelease(cause);
            returnSlotToOwner(payload.getTerminalStateFuture());
        }

        return releaseFuture;
    }

    @Override
    public AllocationID getAllocationId() {
        return slotContext.getAllocationId();
    }

    @Override
    public SlotRequestId getSlotRequestId() {
        return slotRequestId;
    }

    public static SingleLogicalSlot allocateFromPhysicalSlot(
            final SlotRequestId slotRequestId,
            final PhysicalSlot physicalSlot,
            final Locality locality,
            final SlotOwner slotOwner,
            final boolean slotWillBeOccupiedIndefinitely) {

        final SingleLogicalSlot singleTaskSlot =
                new SingleLogicalSlot(
                        slotRequestId,
                        physicalSlot,
                        locality,
                        slotOwner,
                        slotWillBeOccupiedIndefinitely);

        if (physicalSlot.tryAssignPayload(singleTaskSlot)) {
            return singleTaskSlot;
        } else {
            throw new IllegalStateException(
                    "BUG: Unexpected physical slot payload assignment failure!");
        }
    }

    // -------------------------------------------------------------------------
    // AllocatedSlot.Payload implementation
    // -------------------------------------------------------------------------

    /**
     * A release of the payload by the {@link AllocatedSlot} triggers a release of the payload of
     * the logical slot.
     *
     * @param cause of the payload release
     */
    @Override
    public void release(Throwable cause) {
        if (STATE_UPDATER.compareAndSet(this, State.ALIVE, State.RELEASING)) {
            signalPayloadRelease(cause);
        }
        markReleased();
        releaseFuture.complete(null);
    }

    @Override
    public boolean willOccupySlotIndefinitely() {
        return willBeOccupiedIndefinitely;
    }

    private void signalPayloadRelease(Throwable cause) {
        tryAssignPayload(TERMINATED_PAYLOAD);
        payload.fail(cause);
    }

    private void returnSlotToOwner(CompletableFuture<?> terminalStateFuture) {
        FutureUtils.assertNoException(
                terminalStateFuture.thenRun(
                        () -> {
                            if (state == State.RELEASING) {
                                slotOwner.returnLogicalSlot(this);
                            }

                            markReleased();

                            releaseFuture.complete(null);
                        }));
    }

    private void markReleased() {
        state = State.RELEASED;
    }

    // -------------------------------------------------------------------------
    // Internal classes
    // -------------------------------------------------------------------------

    enum State {
        ALIVE,
        RELEASING,
        RELEASED
    }
}
