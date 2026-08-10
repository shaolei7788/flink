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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotProvider;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotRequest;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotRequestBulkChecker;
import org.apache.flink.runtime.scheduler.SharedSlotProfileRetriever.SharedSlotProfileRetrieverFactory;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Allocates {@link LogicalSlot}s from physical shared slots.
 *
 * <p>The allocator maintains a shared slot for each {@link ExecutionSlotSharingGroup}. It allocates
 * a physical slot for the shared slot and then allocates logical slots from it for scheduled tasks.
 * The physical slot is lazily allocated for a shared slot, upon any hosted subtask asking for the
 * shared slot. Each subsequent sharing subtask allocates a logical slot from the existing shared
 * slot. The shared/physical slot can be released only if all the requested logical slots are
 * released or canceled.
 */
class SlotSharingExecutionSlotAllocator implements ExecutionSlotAllocator {
    /** 记录物理 Slot、共享 Slot 和逻辑 Slot 生命周期相关的调试日志。 */
    private static final Logger LOG =
            LoggerFactory.getLogger(SlotSharingExecutionSlotAllocator.class);

    /** 向 Slot 池申请和取消物理 Slot 的入口。 */
    private final PhysicalSlotProvider slotProvider;

    /** 是否将分配出的物理 Slot 标记为长期占用，直到其所属任务明确释放。 */
    private final boolean slotWillBeOccupiedIndefinitely;

    /** 根据执行顶点查找其共享组的策略。 */
    private final SlotSharingStrategy slotSharingStrategy;

    /** 当前仍在使用的共享 Slot；每个共享组最多对应一个共享 Slot。 */
    private final Map<ExecutionSlotSharingGroup, SharedSlot> sharedSlots;

    /** 为一批执行顶点创建共享 Slot 请求画像的工厂。 */
    private final SharedSlotProfileRetrieverFactory sharedSlotProfileRetrieverFactory;

    /** 检查一批物理 Slot 请求是否仍然可满足，并在超时时取消请求。 */
    private final PhysicalSlotRequestBulkChecker bulkChecker;

    /** 一批 Slot 请求允许保持 pending 的最长时间。 */
    private final Duration allocationTimeout;

    /** 共享组未提供显式资源画像时，按执行顶点获取资源需求。 */
    private final Function<ExecutionVertexID, ResourceProfile> resourceProfileRetriever;

    /** 初始化分配器，并关闭 SlotProvider 自带的批量请求超时检查。 */
    SlotSharingExecutionSlotAllocator(
            PhysicalSlotProvider slotProvider,
            boolean slotWillBeOccupiedIndefinitely,
            SlotSharingStrategy slotSharingStrategy,
            SharedSlotProfileRetrieverFactory sharedSlotProfileRetrieverFactory,
            PhysicalSlotRequestBulkChecker bulkChecker,
            Duration allocationTimeout,
            Function<ExecutionVertexID, ResourceProfile> resourceProfileRetriever) {
        this.slotProvider = checkNotNull(slotProvider);
        this.slotWillBeOccupiedIndefinitely = slotWillBeOccupiedIndefinitely;
        this.slotSharingStrategy = checkNotNull(slotSharingStrategy);
        this.sharedSlotProfileRetrieverFactory = checkNotNull(sharedSlotProfileRetrieverFactory);
        this.bulkChecker = checkNotNull(bulkChecker);
        this.allocationTimeout = checkNotNull(allocationTimeout);
        this.resourceProfileRetriever = checkNotNull(resourceProfileRetriever);
        this.sharedSlots = new IdentityHashMap<>();

        this.slotProvider.disableBatchSlotRequestTimeoutCheck();
    }

    /**
     * 为执行尝试分配逻辑 Slot。
     *
     * <p>方法先将执行尝试转换为执行顶点，再由共享组完成物理 Slot 的复用，最后将结果映射回
     * 调用方传入的 {@link ExecutionAttemptID}。同一个执行顶点不能同时存在多个执行尝试。
     */
    @Override
    public Map<ExecutionAttemptID, ExecutionSlotAssignment> allocateSlotsFor(
            List<ExecutionAttemptID> executionAttemptIds) {

        final Map<ExecutionVertexID, ExecutionAttemptID> vertexIdToExecutionId = new HashMap<>();
        executionAttemptIds.forEach(
                executionId ->
                        vertexIdToExecutionId.put(executionId.getExecutionVertexId(), executionId));

        checkState(
                vertexIdToExecutionId.size() == executionAttemptIds.size(),
                "SlotSharingExecutionSlotAllocator does not support one execution vertex to have multiple concurrent executions");

        final List<ExecutionVertexID> vertexIds =
                executionAttemptIds.stream()
                        .map(ExecutionAttemptID::getExecutionVertexId)
                        .collect(Collectors.toList());
        //
        List<SlotExecutionVertexAssignment> slot = allocateSlotsForVertices(vertexIds);
        return slot.stream()
                .collect(
                        Collectors.toMap(
                                vertexAssignment ->
                                        vertexIdToExecutionId.get(
                                                vertexAssignment.getExecutionVertexId()),
                                vertexAssignment ->
                                        new ExecutionSlotAssignment(
                                                vertexIdToExecutionId.get(
                                                        vertexAssignment.getExecutionVertexId()),
                                                vertexAssignment.getLogicalSlotFuture())));
    }

    /**
     * Creates logical {@link SlotExecutionVertexAssignment}s from physical shared slots.
     *
     * <p>The allocation has the following steps:
     *
     * <ol>
     *   <li>Map the executions to {@link ExecutionSlotSharingGroup}s using {@link
     *       SlotSharingStrategy}
     *   <li>Check which {@link ExecutionSlotSharingGroup}s already have shared slot
     *   <li>For all involved {@link ExecutionSlotSharingGroup}s which do not have a shared slot
     *       yet:
     *   <li>Create a {@link SlotProfile} future using {@link SharedSlotProfileRetriever} and then
     *   <li>Allocate a physical slot from the {@link PhysicalSlotProvider}
     *   <li>Create a shared slot based on the returned physical slot futures
     *   <li>Allocate logical slot futures for the executions from all corresponding shared slots.
     *   <li>If a physical slot request fails, associated logical slot requests are canceled within
     *       the shared slot
     *   <li>Generate {@link SlotExecutionVertexAssignment}s based on the logical slot futures and
     *       returns the results.
     * </ol>
     *
     * @param executionVertexIds Execution vertices to allocate slots for
     */
    private List<SlotExecutionVertexAssignment> allocateSlotsForVertices(List<ExecutionVertexID> executionVertexIds) {

        SharedSlotProfileRetriever sharedSlotProfileRetriever =
                sharedSlotProfileRetrieverFactory.createFromBulk(new HashSet<>(executionVertexIds));
        // 同一共享组中的多个执行顶点只需要申请一个物理 Slot。
        Map<ExecutionSlotSharingGroup, List<ExecutionVertexID>> executionsByGroup =
                executionVertexIds.stream()
                        .collect(
                                Collectors.groupingBy(
                                        slotSharingStrategy::getExecutionSlotSharingGroup));

        Map<ExecutionSlotSharingGroup, SharedSlot> slots = new HashMap<>(executionsByGroup.size());
        Set<ExecutionSlotSharingGroup> groupsToAssign = new HashSet<>(executionsByGroup.keySet());
        //尝试找到已存在的共享slot
        Map<ExecutionSlotSharingGroup, SharedSlot> assignedSlots = tryAssignExistingSharedSlots(groupsToAssign);
        slots.putAll(assignedSlots);
        //减去上步找到slot的共享组
        groupsToAssign.removeAll(assignedSlots.keySet());

        if (!groupsToAssign.isEmpty()) {
            // 还要slot要申请
            //为尚未复用共享 Slot 的共享组批量申请物理 Slot
            Map<ExecutionSlotSharingGroup, SharedSlot> allocatedSlots = allocateSharedSlots(groupsToAssign, sharedSlotProfileRetriever);
            slots.putAll(allocatedSlots);
            groupsToAssign.removeAll(allocatedSlots.keySet());
            Preconditions.checkState(groupsToAssign.isEmpty());
        }
        //按照各个共享组（ExecutionSlotSharingGroup）内部的具体任务清单，切割并派发成一个个供具体 Task（ExecutionVertexID）使用的“逻辑独立槽位”（LogicalSlot）
        Map<ExecutionVertexID, SlotExecutionVertexAssignment> assignments = allocateLogicalSlotsFromSharedSlots(slots, executionsByGroup);

        // we need to pass the slots map to the createBulk method instead of using the allocator's
        // 'sharedSlots'
        // because if any physical slots have already failed, their shared slots have been removed
        // from the allocator's 'sharedSlots' by failed logical slots.
        SharingPhysicalSlotRequestBulk bulk = createBulk(slots, executionsByGroup);
        bulkChecker.schedulePendingRequestBulkTimeoutCheck(bulk, allocationTimeout);

        return executionVertexIds.stream().map(assignments::get).collect(Collectors.toList());
    }

    /** 取消指定执行尝试尚未完成的逻辑 Slot 请求。 */
    @Override
    public void cancel(ExecutionAttemptID executionAttemptId) {
        cancelLogicalSlotRequest(executionAttemptId.getExecutionVertexId(), null);
    }

    /** 将取消操作转发给执行顶点所属的共享 Slot。 */
    private void cancelLogicalSlotRequest(ExecutionVertexID executionVertexId, Throwable cause) {
        ExecutionSlotSharingGroup executionSlotSharingGroup =
                slotSharingStrategy.getExecutionSlotSharingGroup(executionVertexId);
        checkNotNull(
                executionSlotSharingGroup,
                "There is no ExecutionSlotSharingGroup for ExecutionVertexID " + executionVertexId);
        SharedSlot slot = sharedSlots.get(executionSlotSharingGroup);
        if (slot != null) {
            slot.cancelLogicalSlotRequest(executionVertexId, cause);
        } else {
            LOG.debug(
                    "There is no SharedSlot for ExecutionSlotSharingGroup of ExecutionVertexID {}",
                    executionVertexId);
        }
    }

    /** 从每个共享 Slot 中为组内执行顶点创建逻辑 Slot 分配结果。 */
    private static Map<ExecutionVertexID, SlotExecutionVertexAssignment>
            allocateLogicalSlotsFromSharedSlots(
                    Map<ExecutionSlotSharingGroup, SharedSlot> slots,
                    Map<ExecutionSlotSharingGroup, List<ExecutionVertexID>> executionsByGroup) {

        Map<ExecutionVertexID, SlotExecutionVertexAssignment> assignments = new HashMap<>();
        //通过这个双层循环，一个 SharedSlot（比如对应的 groupA）可以同时为 executionId_1（Source）、executionId_2（FlatMap）、executionId_3（Sink）分别开出 3 个 LogicalSlot。
        // 它们在物理上挤在同一个 TaskManager 的 JVM 进程/线程池里，但在调度层面被看作 3 个独立的逻辑槽位
        for (Map.Entry<ExecutionSlotSharingGroup, List<ExecutionVertexID>> entry : executionsByGroup.entrySet()) {
            // executionsByGroup size 为2
            ExecutionSlotSharingGroup group = entry.getKey();
            List<ExecutionVertexID> executionIds = entry.getValue();

            for (ExecutionVertexID executionId : executionIds) {
                //关键动作：从对应的物理槽位中，为当前任务抠出一个逻辑槽位的 Future 凭证
                CompletableFuture<LogicalSlot> logicalSlotFuture = slots.get(group).allocateLogicalSlot(executionId);//
                // 将任务 ID 与其对应的逻辑槽位 Future 绑定封装
                SlotExecutionVertexAssignment assignment = new SlotExecutionVertexAssignment(executionId, logicalSlotFuture);
                assignments.put(executionId, assignment);
            }
        }

        return assignments;
    }

    /** 查找本次请求涉及的、已经存在于分配器中的共享 Slot。 */
    private Map<ExecutionSlotSharingGroup, SharedSlot> tryAssignExistingSharedSlots(
            Set<ExecutionSlotSharingGroup> executionSlotSharingGroups) {
        // 优先复用已有共享 Slot，避免同一共享组在多次调度中重复申请物理 Slot。
        Map<ExecutionSlotSharingGroup, SharedSlot> assignedSlots = new HashMap<>(executionSlotSharingGroups.size());
        for (ExecutionSlotSharingGroup group : executionSlotSharingGroups) {
            SharedSlot sharedSlot = sharedSlots.get(group);
            if (sharedSlot != null) {
                assignedSlots.put(group, sharedSlot);
            }
        }
        return assignedSlots;
    }

    /** 为尚未复用共享 Slot 的共享组批量申请物理 Slot，并建立共享 Slot。 */
    private Map<ExecutionSlotSharingGroup, SharedSlot> allocateSharedSlots(
            Set<ExecutionSlotSharingGroup> executionSlotSharingGroups,
            SharedSlotProfileRetriever sharedSlotProfileRetriever) {
        // 一个共享组对应一个物理请求；请求完成后包装成可复用的 SharedSlot。

        List<PhysicalSlotRequest> slotRequests = new ArrayList<>();
        Map<ExecutionSlotSharingGroup, SharedSlot> allocatedSlots = new HashMap<>();

        Map<SlotRequestId, ExecutionSlotSharingGroup> requestToGroup = new HashMap<>();
        Map<SlotRequestId, ResourceProfile> requestToPhysicalResources = new HashMap<>();

        for (ExecutionSlotSharingGroup group : executionSlotSharingGroups) {
            SlotRequestId physicalSlotRequestId = new SlotRequestId();
            ResourceProfile physicalSlotResourceProfile = getPhysicalSlotResourceProfile(group);
            SlotProfile slotProfile = sharedSlotProfileRetriever.getSlotProfile(group, physicalSlotResourceProfile);
            PhysicalSlotRequest request = new PhysicalSlotRequest(
                            physicalSlotRequestId,
                            slotProfile,
                            group.getLoading(),
                            slotWillBeOccupiedIndefinitely);
            slotRequests.add(request);
            requestToGroup.put(physicalSlotRequestId, group);
            requestToPhysicalResources.put(physicalSlotRequestId, physicalSlotResourceProfile);
        }

        Map<SlotRequestId, CompletableFuture<PhysicalSlotRequest.Result>> allocateResult =
                // 分配物理slot
                //PhysicalSlotProviderImpl#allocatePhysicalSlots
                slotProvider.allocatePhysicalSlots(slotRequests);

        allocateResult.forEach(
                (slotRequestId, resultCompletableFuture) -> {
                    ExecutionSlotSharingGroup group = requestToGroup.get(slotRequestId);
                    CompletableFuture<PhysicalSlot> physicalSlotFuture =
                            resultCompletableFuture.thenApply(PhysicalSlotRequest.Result::getPhysicalSlot);
                    SharedSlot slot = new SharedSlot(
                                    slotRequestId,
                                    requestToPhysicalResources.get(slotRequestId),
                                    group,
                                    physicalSlotFuture,
                                    slotWillBeOccupiedIndefinitely,
                                    this::releaseSharedSlot);
                    allocatedSlots.put(group, slot);
                    Preconditions.checkState(!sharedSlots.containsKey(group));
                    sharedSlots.put(group, slot);
                });
        return allocatedSlots;
    }

    /** 在共享 Slot 为空时移除它，并取消对应的物理 Slot 请求。 */
    private void releaseSharedSlot(ExecutionSlotSharingGroup executionSlotSharingGroup) {
        // SharedSlot 在所有逻辑 Slot 都释放后回调这里，随后取消底层物理请求。
        SharedSlot slot = sharedSlots.remove(executionSlotSharingGroup);
        Preconditions.checkNotNull(slot);
        Preconditions.checkState(
                slot.isEmpty(),
                "Trying to remove a shared slot with physical request id %s which has assigned logical slots",
                slot.getPhysicalSlotRequestId());
        slotProvider.cancelSlotRequest(
                slot.getPhysicalSlotRequestId(),
                new FlinkException(
                        "Slot is being returned from SlotSharingExecutionSlotAllocator."));
    }

    /** 计算共享组对应物理 Slot 所需的资源画像。 */
    private ResourceProfile getPhysicalSlotResourceProfile(
            ExecutionSlotSharingGroup executionSlotSharingGroup) {
        // 组有显式资源画像时直接使用；否则合并组内各执行顶点的资源需求。
        if (!executionSlotSharingGroup.getResourceProfile().equals(ResourceProfile.UNKNOWN)) {
            return executionSlotSharingGroup.getResourceProfile();
        } else {
            return executionSlotSharingGroup.getExecutionVertexIds().stream()
                    .reduce(
                            ResourceProfile.ZERO,
                            (r, e) -> r.merge(resourceProfileRetriever.apply(e)),
                            ResourceProfile::merge);
        }
    }

    /** 创建并初始化用于超时/可满足性检查的物理 Slot 请求批次。 */
    private SharingPhysicalSlotRequestBulk createBulk(
            Map<ExecutionSlotSharingGroup, SharedSlot> slots,
            Map<ExecutionSlotSharingGroup, List<ExecutionVertexID>> executions) {
        // Bulk 以共享组为粒度跟踪物理请求，并能在超时或失败时取消对应逻辑请求。
        Map<ExecutionSlotSharingGroup, ResourceProfile> pendingRequests =
                executions.keySet().stream()
                        .collect(
                                Collectors.toMap(
                                        group -> group,
                                        group ->
                                                slots.get(group).getPhysicalSlotResourceProfile()));
        SharingPhysicalSlotRequestBulk bulk =
                new SharingPhysicalSlotRequestBulk(
                        executions, pendingRequests, this::cancelLogicalSlotRequest);
        registerPhysicalSlotRequestBulkCallbacks(slots, executions.keySet(), bulk);
        return bulk;
    }

    /** 将每个共享 Slot 的异步结果回调绑定到请求批次。 */
    private static void registerPhysicalSlotRequestBulkCallbacks(
            Map<ExecutionSlotSharingGroup, SharedSlot> slots,
            Iterable<ExecutionSlotSharingGroup> executions,
            SharingPhysicalSlotRequestBulk bulk) {
        // 物理 Slot 成功或失败时同步更新 bulk，避免超时检查误判或继续等待已失败请求。
        for (ExecutionSlotSharingGroup group : executions) {
            CompletableFuture<PhysicalSlot> slotContextFuture =
                    slots.get(group).getSlotContextFuture();
            slotContextFuture.thenAccept(
                    physicalSlot -> bulk.markFulfilled(group, physicalSlot.getAllocationId()));
            slotContextFuture.exceptionally(
                    t -> {
                        // clear the bulk to stop the fulfillability check
                        bulk.clearPendingRequests();
                        return null;
                    });
        }
    }

    /** 保存一个执行顶点及其逻辑 Slot future 的内部中间结果。 */
    private static class SlotExecutionVertexAssignment {

        /** 被分配逻辑 Slot 的执行顶点。 */
        private final ExecutionVertexID executionVertexId;

        /** 物理 Slot 就绪后完成的逻辑 Slot future。 */
        private final CompletableFuture<LogicalSlot> logicalSlotFuture;

        /** 创建执行顶点到逻辑 Slot future 的中间映射。 */
        SlotExecutionVertexAssignment(
                ExecutionVertexID executionVertexId,
                CompletableFuture<LogicalSlot> logicalSlotFuture) {
            this.executionVertexId = checkNotNull(executionVertexId);
            this.logicalSlotFuture = checkNotNull(logicalSlotFuture);
        }

        /** 返回该中间结果对应的执行顶点。 */
        ExecutionVertexID getExecutionVertexId() {
            return executionVertexId;
        }

        /** 返回逻辑 Slot 的异步结果。 */
        CompletableFuture<LogicalSlot> getLogicalSlotFuture() {
            return logicalSlotFuture;
        }
    }
}
