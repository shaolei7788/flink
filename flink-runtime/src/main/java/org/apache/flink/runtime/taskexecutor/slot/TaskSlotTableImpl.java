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

package org.apache.flink.runtime.taskexecutor.slot;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceBudgetManager;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor.DummyComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.exceptions.SlotAllocationException;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

//是管理所有格子的“全局索引/管理表”（对应 TaskManager 上的整个 Slot 资源池）
//一个 TaskManager 上 只有一个（单例，管理所有 Slot）
//主要职责 提供 Slot 的查找、分配、释放接口
//  跟踪所有 Slot 的全局状态映射（SlotIndex ↔ TaskSlot）
//  监听并处理 Slot 的超时与回收
//生命周期
//  随 TaskExecutor 的启动而创建，在整个 TaskManager 崩溃/退出时才销毁


/**
 * 2. 交互逻辑场景
 * 场景 A：JobMaster 申请 Slot 资源
 * JobMaster 向 TaskManager 发送 RequestSlot 请求。
 * TaskExecutor 收到请求，调用 TaskSlotTable.allocateSlot(...)。
 * TaskSlotTable 检查表里是否有空闲的 TaskSlot。如果有，将其标记为 Allocated，并把这个 TaskSlot 分配给对应 Job 的 AllocationID。
 *
 * 场景 B：在 Slot 中部署并运行 Task
 * JobMaster 发送 SubmitTask 指令。
 * TaskExecutor 拿着 AllocationID 询问 TaskSlotTable：“对应的 Slot 是哪个？”
 * TaskSlotTable 查找内部映射关系，返回对应的 TaskSlot。
 * TaskExecutor 调用 TaskSlot.addPayload(task)，将 Task 放入该 Slot 中真正启动执行。
 *
 * 场景 C：Task 执行完成或 Job 结束
 * Task 运行完毕退出，TaskSlot 移除内部的 Task 引用。
 * 当 Slot 内所有 Task 都执行完毕且 JobMaster 释放 Slot 时，TaskSlotTable 收到指令，调用 freeSlot(...) 清理并重置该 TaskSlot，使其重新变为 Free 状态供其他作业使用。
 * 总结
 * TaskSlot 关心的是 “我这个格子现在占用了多少内存、跑了哪几个 Task”（关注具体的计算与内存资源）。
 * TaskSlotTable 关心的是 “整个 TaskManager 一共有多少格子，编号 0 的格子给谁了，编号 1 的格子是不是空闲”（关注资源的全局分配与路由）。
 * TaskManagerServices 在启动时创建并初始化了 TaskSlotTable，而 TaskSlotTable 则在内部实例化并管控着所有的 TaskSlot
 */

/** Default implementation of {@link TaskSlotTable}. */
public class TaskSlotTableImpl<T extends TaskSlotPayload> implements TaskSlotTable<T> {

    private static final Logger LOG = LoggerFactory.getLogger(TaskSlotTableImpl.class);

    /**
     * Number of slots in static slot allocation. If slot is requested with an index, the requested
     * index must within the range of [0, numberSlots). When generating slot report, we should
     * always generate slots with index in [0, numberSlots) even the slot does not exist.
     */
    //TaskManager 拥有的静态 TaskSlot 总数量。定义了该 TaskExecutor 节点上最多可以同时容纳的 Slot 上限
    private final int numberSlots;

    /** Slot resource profile for static slot allocation. */
    //默认 Slot 的资源规格描述（包含 CPU 核数、堆内存、堆外内存等资源量）。当分配未指定具体规格的 Slot 时使用该默认配置
    private final ResourceProfile defaultSlotResourceProfile;

    /** Page size for memory manager. */
    private final int memoryPageSize;

    //Slot 超时定时器服务。主要用于跟踪处于非活动/分配状态的 Slot
    // （例如：Slot 已经被分配给某 Job，但在指定时间内没有 Task 部署进来），超时后触发清理逻辑，防止资源长期处于悬空占有状态
    /** Timer service used to time out allocated slots. */
    private final TimerService<AllocationID> timerService;

    //记录Slot 索引号 (Index) 到 TaskSlot 实例的映射集合
    /** The list of all task slots. */
    private final Map<Integer, TaskSlot<T>> taskSlots;

    //记录分配 ID (AllocationID) 到已分配 TaskSlot 的映射。当 JobMaster 申请资源并分配成功后，在此集合中登记该 AllocationID
    /** Mapping from allocation id to task slot. */
    private final Map<AllocationID, TaskSlot<T>> allocatedSlots;

    //记录具体 Task 执行尝试 ID (ExecutionAttemptID) 到 TaskSlot 的映射。用于查找某个 Task 正在哪个 Slot 中运行，或者在 Task 停止/失败时快速找到对应的 Slot 并释放关联
    /** Mapping from execution attempt id to task and task slot. */
    private final Map<ExecutionAttemptID, TaskSlotMapping<T>> taskSlotMappings;

    //记录作业 ID (JobID) 与其名下所有 AllocationID 集合的映射。用于按 Job 进行统一的资源清理与追踪（例如 Job 结束时释放该 Job 占用的所有 Slot）
    /** Mapping from job id to allocated slots for a job. */
    private final Map<JobID, Set<AllocationID>> slotsPerJob;

    //Slot 状态变更时的回调接口（如超时未激活、超时未能接收任务时），用于向 TaskExecutor 抛出回调动作，触发释放 Slot 或通知 JobMaster 的操作
    /** Interface for slot actions, such as freeing them or timing them out. */
    @Nullable private SlotActions slotActions;
    //表示当前 TaskSlotTable 的生命周期状态（例如 CREATED、RUNNING、CLOSING、CLOSED）。用于拒绝表已关闭后的无效分配操作
    /** The table state. */
    private volatile State state;

    /** Current index for dynamic slot, should always not less than numberSlots */
    private int dynamicSlotIndex;
    //资源预算管理器。负责监控与校验 Slot 动态分配时的总资源使用情况，确保所有分配的 Slot 资源总和不会超出 TaskManager 的物理总预算
    private final ResourceBudgetManager budgetManager;

    //异步关闭的 Future 句柄。当 TaskExecutor 停止或清理 Slot Table 时，用于标识所有 Slot 清理与资源释放动作是否已完全结束
    /** The closing future is completed when all slot are freed and state is closed. */
    private final CompletableFuture<Void> closingFuture;

    //TaskManager 的主线程执行器。确保针对 TaskSlotTable 的所有并发修改和状态变更指令都在 TaskExecutor 的单线程（Main Thread）中顺序执行，避免多线程竞争
    /** {@link ComponentMainThreadExecutor} to schedule internal calls to the main thread. */
    private ComponentMainThreadExecutor mainThreadExecutor =
            new DummyComponentMainThreadExecutor(
                    "TaskSlotTableImpl is not initialized with proper main thread executor, "
                            + "call to TaskSlotTableImpl#start is required");

    /** {@link Executor} for background actions, e.g. verify all managed memory released. */
    private final Executor memoryVerificationExecutor;

    public TaskSlotTableImpl(
            final int numberSlots,
            final ResourceProfile totalAvailableResourceProfile,
            final ResourceProfile defaultSlotResourceProfile,
            final int memoryPageSize,
            final TimerService<AllocationID> timerService,
            final Executor memoryVerificationExecutor) {
        Preconditions.checkArgument(
                0 < numberSlots, "The number of task slots must be greater than 0.");

        this.numberSlots = numberSlots;
        this.dynamicSlotIndex = numberSlots;
        this.defaultSlotResourceProfile = Preconditions.checkNotNull(defaultSlotResourceProfile);
        this.memoryPageSize = memoryPageSize;//32768

        this.taskSlots = CollectionUtil.newHashMapWithExpectedSize(numberSlots);

        this.timerService = Preconditions.checkNotNull(timerService);

        budgetManager =
                new ResourceBudgetManager(
                        Preconditions.checkNotNull(totalAvailableResourceProfile));

        allocatedSlots = CollectionUtil.newHashMapWithExpectedSize(numberSlots);

        taskSlotMappings = CollectionUtil.newHashMapWithExpectedSize(4 * numberSlots);

        slotsPerJob = CollectionUtil.newHashMapWithExpectedSize(4);

        slotActions = null;
        state = State.CREATED;
        closingFuture = new CompletableFuture<>();

        this.memoryVerificationExecutor = memoryVerificationExecutor;
    }

    @Override
    public void start(
            SlotActions initialSlotActions, ComponentMainThreadExecutor mainThreadExecutor) {
        Preconditions.checkState(
                state == State.CREATED,
                "The %s has to be just created before starting",
                TaskSlotTableImpl.class.getSimpleName());
        this.slotActions = Preconditions.checkNotNull(initialSlotActions);
        this.mainThreadExecutor = Preconditions.checkNotNull(mainThreadExecutor);

        timerService.start(this);

        state = State.RUNNING;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        if (state == State.CREATED) {
            state = State.CLOSED;
            closingFuture.complete(null);
        } else if (state == State.RUNNING) {
            state = State.CLOSING;
            final FlinkException cause = new FlinkException("Closing task slot table");
            CompletableFuture<Void> cleanupFuture =
                    FutureUtils.waitForAll(
                                    new ArrayList<>(allocatedSlots.values())
                                            .stream()
                                                    .map(slot -> freeSlotInternal(slot, cause))
                                                    .collect(Collectors.toList()))
                            .thenRunAsync(
                                    () -> {
                                        state = State.CLOSED;
                                        timerService.stop();
                                    },
                                    mainThreadExecutor);
            FutureUtils.forward(cleanupFuture, closingFuture);
        }
        return closingFuture;
    }

    @VisibleForTesting
    public boolean isClosed() {
        return state == State.CLOSED;
    }

    @Override
    public Set<AllocationID> getAllocationIdsPerJob(JobID jobId) {
        final Set<AllocationID> allocationIds = slotsPerJob.get(jobId);

        if (allocationIds == null) {
            return Collections.emptySet();
        } else {
            return Collections.unmodifiableSet(allocationIds);
        }
    }

    @Override
    public Set<AllocationID> getActiveTaskSlotAllocationIds() {
        return createAllocationIdSet(new TaskSlotIterator(TaskSlotState.ACTIVE));
    }

    @Override
    public Set<AllocationID> getActiveTaskSlotAllocationIdsPerJob(JobID jobId) {
        return createAllocationIdSet(new TaskSlotIterator(jobId, TaskSlotState.ACTIVE));
    }

    private Set<AllocationID> createAllocationIdSet(Iterator<TaskSlot<T>> taskSlotIterator) {
        Set<AllocationID> allocationIds = new HashSet<>();
        while (taskSlotIterator.hasNext()) {
            allocationIds.add(taskSlotIterator.next().getAllocationId());
        }

        return allocationIds;
    }

    // ---------------------------------------------------------------------
    // Slot report methods
    // ---------------------------------------------------------------------

    @Override
    public SlotReport createSlotReport(ResourceID resourceId) {
        List<SlotStatus> slotStatuses = new ArrayList<>();

        for (int i = 0; i < numberSlots; i++) {
            SlotID slotId = new SlotID(resourceId, i);
            SlotStatus slotStatus;
            if (taskSlots.containsKey(i)) {
                TaskSlot<T> taskSlot = taskSlots.get(i);

                slotStatus =
                        new SlotStatus(
                                slotId,
                                taskSlot.getResourceProfile(),
                                taskSlot.getJobId(),
                                taskSlot.getAllocationId());
            } else {
                slotStatus = new SlotStatus(slotId, defaultSlotResourceProfile, null, null);
            }

            slotStatuses.add(slotStatus);
        }

        for (TaskSlot<T> taskSlot : allocatedSlots.values()) {
            if (isDynamicIndex(taskSlot.getIndex())) {
                SlotStatus slotStatus =
                        new SlotStatus(
                                new SlotID(resourceId, taskSlot.getIndex()),
                                taskSlot.getResourceProfile(),
                                taskSlot.getJobId(),
                                taskSlot.getAllocationId());
                slotStatuses.add(slotStatus);
            }
        }

        final SlotReport slotReport = new SlotReport(slotStatuses);

        return slotReport;
    }

    // ---------------------------------------------------------------------
    // Slot methods
    // ---------------------------------------------------------------------

    @VisibleForTesting
    @Override
    public void allocateSlot(
            int index, JobID jobId, AllocationID allocationId, Duration slotTimeout)
            throws SlotAllocationException {
        allocateSlot(index, jobId, allocationId, defaultSlotResourceProfile, slotTimeout);
    }

    @Override
    public void allocateSlot(
            int requestedIndex,
            JobID jobId,
            AllocationID allocationId,
            ResourceProfile resourceProfile,
            Duration slotTimeout)
            throws SlotAllocationException {
        checkRunning();

        Preconditions.checkArgument(requestedIndex < numberSlots);

        // The negative requestIndex indicate that the SlotManager allocate a dynamic slot, we
        // transfer the index to an increasing number not less than the numberSlots.
        int index = requestedIndex < 0 ? nextDynamicSlotIndex() : requestedIndex;
        ResourceProfile effectiveResourceProfile =
                resourceProfile.equals(ResourceProfile.UNKNOWN)
                        ? defaultSlotResourceProfile
                        : resourceProfile;
        //未分配任务是为空的 也就是第一次有任务过来 allocatedSlots 是空的
        TaskSlot<T> taskSlot = allocatedSlots.get(allocationId);
        if (taskSlot != null) {
            //之前分配了任务 直接复用
            if (isDuplicatedSlot(taskSlot, jobId, effectiveResourceProfile, index)) {
                LOG.info(
                        "Slot with allocationId {} already exist, with resource profile {}, job id {} and index {}. The required index is {}. No further allocation necessary.",
                        taskSlot.getAllocationId(),
                        taskSlot.getResourceProfile(),
                        taskSlot.getJobId(),
                        taskSlot.getIndex(),
                        index);
                return;
            }

            throw new SlotAllocationException(
                    String.format(
                            "A slot with allocationId %s and resource profile %s is already assigned to job %s with subtask index %d.",
                            taskSlot.getAllocationId(),
                            taskSlot.getResourceProfile(),
                            taskSlot.getJobId(),
                            taskSlot.getIndex()));
        } else if (isIndexAlreadyTaken(index)) {
            throw new SlotAllocationException(
                    String.format(
                            "The slot with index %d is already assigned to another allocation with id %s.",
                            index, taskSlots.get(index).getAllocationId()));
        }

        if (!budgetManager.reserve(effectiveResourceProfile)) {
            throw new SlotAllocationException(
                    String.format(
                            "Cannot allocate the requested resources. Trying to allocate %s, while the currently remaining available resources are %s, total is %s.",
                            effectiveResourceProfile,
                            budgetManager.getAvailableBudget(),
                            budgetManager.getTotalBudget()));
        }
        LOG.info(
                "Allocated slot for {} with resources {}.", allocationId, effectiveResourceProfile);
        // 分配新的TaskSlot
        taskSlot = new TaskSlot<>(
                        index,//1
                        effectiveResourceProfile,
                        memoryPageSize,//32768
                        jobId,
                        allocationId,
                        memoryVerificationExecutor);
        taskSlots.put(index, taskSlot);

        // update the allocation id to task slot map
        allocatedSlots.put(allocationId, taskSlot);

        // register a timeout for this slot since it's in state allocated
        timerService.registerTimeout(allocationId, slotTimeout.toMillis(), TimeUnit.MILLISECONDS);

        // add this slot to the set of job slots
        Set<AllocationID> slots = slotsPerJob.get(jobId);

        if (slots == null) {
            slots = CollectionUtil.newHashSetWithExpectedSize(4);
            slotsPerJob.put(jobId, slots);
        }

        slots.add(allocationId);
    }

    private boolean isDuplicatedSlot(
            TaskSlot taskSlot, JobID jobId, ResourceProfile resourceProfile, int index) {
        return taskSlot.getJobId().equals(jobId)
                && taskSlot.getResourceProfile().equals(resourceProfile)
                && (isDynamicIndex(index) || taskSlot.getIndex() == index);
    }

    private boolean isIndexAlreadyTaken(int index) {
        return taskSlots.get(index) != null;
    }

    private boolean isDynamicIndex(int index) {
        return index >= numberSlots;
    }

    @Override
    public boolean markSlotActive(AllocationID allocationId) throws SlotNotFoundException {
        checkRunning();

        TaskSlot<T> taskSlot = getTaskSlot(allocationId);

        if (taskSlot != null) {
            return markExistingSlotActive(taskSlot);
        } else {
            throw new SlotNotFoundException(allocationId);
        }
    }

    private boolean markExistingSlotActive(TaskSlot<T> taskSlot) {
        if (taskSlot.markActive()) {
            // unregister a potential timeout
            LOG.info("Activate slot {}.", taskSlot.getAllocationId());

            timerService.unregisterTimeout(taskSlot.getAllocationId());

            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean markSlotInactive(AllocationID allocationId, Duration slotTimeout)
            throws SlotNotFoundException {
        checkStarted();

        TaskSlot<T> taskSlot = getTaskSlot(allocationId);

        if (taskSlot != null) {
            if (taskSlot.markInactive()) {
                // register a timeout to free the slot
                timerService.registerTimeout(
                        allocationId, slotTimeout.toMillis(), TimeUnit.MILLISECONDS);

                return true;
            } else {
                return false;
            }
        } else {
            throw new SlotNotFoundException(allocationId);
        }
    }

    @Override
    public int freeSlot(AllocationID allocationId, Throwable cause) throws SlotNotFoundException {
        checkStarted();

        TaskSlot<T> taskSlot = getTaskSlot(allocationId);

        if (taskSlot != null) {
            return freeSlotInternal(taskSlot, cause).isDone() ? taskSlot.getIndex() : -1;
        } else {
            throw new SlotNotFoundException(allocationId);
        }
    }

    private CompletableFuture<Void> freeSlotInternal(TaskSlot<T> taskSlot, Throwable cause) {
        AllocationID allocationId = taskSlot.getAllocationId();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Free slot {}.", taskSlot, cause);
        } else {
            LOG.info("Free slot {}.", taskSlot);
        }

        if (taskSlot.isEmpty()) {
            // remove the allocation id to task slot mapping
            allocatedSlots.remove(allocationId);

            // unregister a potential timeout
            timerService.unregisterTimeout(allocationId);

            JobID jobId = taskSlot.getJobId();
            Set<AllocationID> slots = slotsPerJob.get(jobId);

            if (slots == null) {
                throw new IllegalStateException(
                        "There are no more slots allocated for the job "
                                + jobId
                                + ". This indicates a programming bug.");
            }

            slots.remove(allocationId);

            if (slots.isEmpty()) {
                slotsPerJob.remove(jobId);
            }

            taskSlots.remove(taskSlot.getIndex());
            budgetManager.release(taskSlot.getResourceProfile());
        }
        return taskSlot.closeAsync(cause);
    }

    @Override
    public boolean isValidTimeout(AllocationID allocationId, UUID ticket) {
        checkStarted();

        return state == State.RUNNING && timerService.isValid(allocationId, ticket);
    }

    @Override
    public boolean isAllocated(int index, JobID jobId, AllocationID allocationId) {
        TaskSlot<T> taskSlot = taskSlots.get(index);
        if (taskSlot != null) {
            return taskSlot.isAllocated(jobId, allocationId);
        } else {
            return false;
        }
    }

    @Override
    public boolean tryMarkSlotActive(JobID jobId, AllocationID allocationId) {
        TaskSlot<T> taskSlot = getTaskSlot(allocationId);

        if (taskSlot != null && taskSlot.isAllocated(jobId, allocationId)) {
            return markExistingSlotActive(taskSlot);
        } else {
            return false;
        }
    }

    @Override
    public boolean isSlotFree(int index) {
        return !taskSlots.containsKey(index);
    }

    @Override
    public boolean hasAllocatedSlots(JobID jobId) {
        return getAllocatedSlots(jobId).hasNext();
    }

    @Override
    public Iterator<TaskSlot<T>> getAllocatedSlots(JobID jobId) {
        return new TaskSlotIterator(jobId, TaskSlotState.ALLOCATED);
    }

    @Override
    @Nullable
    public JobID getOwningJob(AllocationID allocationId) {
        final TaskSlot<T> taskSlot = getTaskSlot(allocationId);

        if (taskSlot != null) {
            return taskSlot.getJobId();
        } else {
            return null;
        }
    }

    // ---------------------------------------------------------------------
    // Task methods
    // ---------------------------------------------------------------------

    @Override
    public boolean addTask(T task) throws SlotNotFoundException, SlotNotActiveException {
        checkRunning();
        Preconditions.checkNotNull(task);

        TaskSlot<T> taskSlot = getTaskSlot(task.getAllocationId());

        if (taskSlot != null) {
            if (taskSlot.isActive(task.getJobID(), task.getAllocationId())) {
                if (taskSlot.add(task)) {
                    taskSlotMappings.put(
                            task.getExecutionId(), new TaskSlotMapping<>(task, taskSlot));

                    return true;
                } else {
                    return false;
                }
            } else {
                throw new SlotNotActiveException(task.getJobID(), task.getAllocationId());
            }
        } else {
            throw new SlotNotFoundException(task.getAllocationId());
        }
    }

    @Override
    public T removeTask(ExecutionAttemptID executionAttemptID) {
        checkStarted();

        TaskSlotMapping<T> taskSlotMapping = taskSlotMappings.remove(executionAttemptID);

        if (taskSlotMapping != null) {
            T task = taskSlotMapping.getTask();
            TaskSlot<T> taskSlot = taskSlotMapping.getTaskSlot();

            taskSlot.remove(task.getExecutionId());

            if (taskSlot.isReleasing() && taskSlot.isEmpty()) {
                slotActions.freeSlot(taskSlot.getAllocationId());
            }

            return task;
        } else {
            return null;
        }
    }

    @Override
    public T getTask(ExecutionAttemptID executionAttemptID) {
        TaskSlotMapping<T> taskSlotMapping = taskSlotMappings.get(executionAttemptID);

        if (taskSlotMapping != null) {
            return taskSlotMapping.getTask();
        } else {
            return null;
        }
    }

    @Override
    public Iterator<T> getTasks(JobID jobId) {
        return new PayloadIterator(jobId);
    }

    @Override
    public AllocationID getCurrentAllocation(int index) {
        TaskSlot<T> taskSlot = taskSlots.get(index);
        if (taskSlot == null) {
            return null;
        }
        return taskSlot.getAllocationId();
    }

    @Override
    public MemoryManager getTaskMemoryManager(AllocationID allocationID)
            throws SlotNotFoundException {
        TaskSlot<T> taskSlot = getTaskSlot(allocationID);
        if (taskSlot != null) {
            return taskSlot.getMemoryManager();
        } else {
            throw new SlotNotFoundException(allocationID);
        }
    }

    // ---------------------------------------------------------------------
    // TimeoutListener methods
    // ---------------------------------------------------------------------

    @Override
    public void notifyTimeout(AllocationID key, UUID ticket) {
        checkStarted();

        if (slotActions != null) {
            slotActions.timeoutSlot(key, ticket);
        }
    }

    // ---------------------------------------------------------------------
    // Internal methods
    // ---------------------------------------------------------------------

    @Nullable
    private TaskSlot<T> getTaskSlot(AllocationID allocationId) {
        Preconditions.checkNotNull(allocationId);

        return allocatedSlots.get(allocationId);
    }

    private int nextDynamicSlotIndex() {
        return dynamicSlotIndex++;
    }

    private void checkRunning() {
        Preconditions.checkState(
                state == State.RUNNING,
                "The %s has to be running.",
                TaskSlotTableImpl.class.getSimpleName());
    }

    private void checkStarted() {
        Preconditions.checkState(
                state != State.CREATED,
                "The %s has to be started (not created).",
                TaskSlotTableImpl.class.getSimpleName());
    }

    // ---------------------------------------------------------------------
    // Static utility classes
    // ---------------------------------------------------------------------

    /** Mapping class between a {@link TaskSlotPayload} and a {@link TaskSlot}. */
    private static final class TaskSlotMapping<T extends TaskSlotPayload> {
        private final T task;
        private final TaskSlot<T> taskSlot;

        private TaskSlotMapping(T task, TaskSlot<T> taskSlot) {
            this.task = Preconditions.checkNotNull(task);
            this.taskSlot = Preconditions.checkNotNull(taskSlot);
        }

        public T getTask() {
            return task;
        }

        public TaskSlot<T> getTaskSlot() {
            return taskSlot;
        }
    }

    /**
     * Iterator over {@link TaskSlot} which fulfill a given state condition and belong to the given
     * job.
     */
    private final class TaskSlotIterator implements Iterator<TaskSlot<T>> {
        private final Iterator<AllocationID> allSlots;
        private final TaskSlotState state;

        private TaskSlot<T> currentSlot;

        private TaskSlotIterator(TaskSlotState state) {
            this(
                    slotsPerJob.values().stream()
                            .flatMap(Collection::stream)
                            .collect(Collectors.toSet())
                            .iterator(),
                    state);
        }

        private TaskSlotIterator(JobID jobId, TaskSlotState state) {
            this(
                    slotsPerJob.get(jobId) == null
                            ? Collections.emptyIterator()
                            : slotsPerJob.get(jobId).iterator(),
                    state);
        }

        private TaskSlotIterator(Iterator<AllocationID> allocationIDIterator, TaskSlotState state) {
            this.allSlots = Preconditions.checkNotNull(allocationIDIterator);
            this.state = Preconditions.checkNotNull(state);
            this.currentSlot = null;
        }

        @Override
        public boolean hasNext() {
            while (currentSlot == null && allSlots.hasNext()) {
                AllocationID tempSlot = allSlots.next();

                TaskSlot<T> taskSlot = getTaskSlot(tempSlot);

                if (taskSlot != null && taskSlot.getState() == state) {
                    currentSlot = taskSlot;
                }
            }

            return currentSlot != null;
        }

        @Override
        public TaskSlot<T> next() {
            if (currentSlot != null) {
                TaskSlot<T> result = currentSlot;

                currentSlot = null;

                return result;
            } else {
                while (true) {
                    AllocationID tempSlot;

                    try {
                        tempSlot = allSlots.next();
                    } catch (NoSuchElementException e) {
                        throw new NoSuchElementException("No more task slots.");
                    }

                    TaskSlot<T> taskSlot = getTaskSlot(tempSlot);

                    if (taskSlot != null && taskSlot.getState() == state) {
                        return taskSlot;
                    }
                }
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Cannot remove task slots via this iterator.");
        }
    }

    /** Iterator over all {@link TaskSlotPayload} for a given job. */
    private final class PayloadIterator implements Iterator<T> {
        private final Iterator<TaskSlot<T>> taskSlotIterator;

        private Iterator<T> currentTasks;

        private PayloadIterator(JobID jobId) {
            this.taskSlotIterator = new TaskSlotIterator(jobId, TaskSlotState.ACTIVE);

            this.currentTasks = null;
        }

        @Override
        public boolean hasNext() {
            while ((currentTasks == null || !currentTasks.hasNext())
                    && taskSlotIterator.hasNext()) {
                TaskSlot<T> taskSlot = taskSlotIterator.next();

                currentTasks = taskSlot.getTasks();
            }

            return (currentTasks != null && currentTasks.hasNext());
        }

        @Override
        public T next() {
            while ((currentTasks == null || !currentTasks.hasNext())) {
                TaskSlot<T> taskSlot;

                try {
                    taskSlot = taskSlotIterator.next();
                } catch (NoSuchElementException e) {
                    throw new NoSuchElementException("No more tasks.");
                }

                currentTasks = taskSlot.getTasks();
            }

            return currentTasks.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Cannot remove tasks via this iterator.");
        }
    }

    private enum State {
        CREATED,
        RUNNING,
        CLOSING,
        CLOSED
    }
}
