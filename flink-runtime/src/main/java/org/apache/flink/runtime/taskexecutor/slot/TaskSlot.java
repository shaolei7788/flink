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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * Container for multiple {@link TaskSlotPayload tasks} belonging to the same slot. A {@link
 * TaskSlot} can be in one of the following states:
 *
 * <ul>
 *   <li>Free - The slot is empty and not allocated to a job
 *   <li>Releasing - The slot is about to be freed after it has become empty.
 *   <li>Allocated - The slot has been allocated for a job.
 *   <li>Active - The slot is in active use by a job manager which is the leader of the allocating
 *       job.
 * </ul>
 *
 * <p>A task slot can only be allocated if it is in state free. An allocated task slot can transit
 * to state active.
 *
 * <p>An active slot allows to add tasks from the respective job and with the correct allocation id.
 * An active slot can be marked as inactive which sets the state back to allocated.
 *
 * <p>An allocated or active slot can only be freed if it is empty. If it is not empty, then it's
 * state can be set to releasing indicating that it can be freed once it becomes empty.
 *
 * @param <T> type of the {@link TaskSlotPayload} stored in this slot
 */
// 是单个资源的“物理容器/格子”（对应一个具体的 Slot）
    //一个 TaskManager 上有 多个（由 taskmanager.numberOfTaskSlots 配置）
    //隔离并持有该 Slot 分配到的内存/CPU资源
//
//主要职责
//  记录当前 Slot 内运行了哪些 Task（支持 Task 共享/Slot Sharing）
//  维护 Slot 自身状态（Free, Allocated, Active 等）
//生命周期  随 Slot 的分配与释放动态创建或重置状态

//1. 资源隔离与划分（主要针对内存）一个 TaskManager 是一个独立的 JVM 进程，可以包含一个或多个 TaskSlot。
//      内存均分：TaskManager 启动时会将自己的 【托管内存】（Managed Memory）和【堆内存】均匀平分给每个 TaskSlot。
//      资源保护：通过将内存划分为固定的 Slot，可以防止某个 Task/Subtask 无节制地抢占整个 TaskManager 的内存，从而避免内存溢出（OOM）影响其他 Task 的运行。
//      注意：TaskSlot 目前主要隔离内存，并不对 CPU 进行强隔离（即多个 Slot 共享 TaskManager JVM 进程中的 CPU 核心和 CPU 线程池）。
//2. 决定集群的最大并发能力（Parallelism）
//      TaskSlot 的总数量直接决定了 Flink 集群能同时运行多少个并行任务（Subtask）。
//      如果一个 TaskManager 配置了 taskmanager.numberOfTaskSlots: 3，且集群有 3 个 TaskManager，那么整个集群共有 $3 \times 3 = 9$ 个 TaskSlot。
//      这意味着该集群最多能支持并发度为 9 的作业任务执行。
//3. 任务（Subtask）的执行容器Flink 作业在提交后会被拆分为多个算子子任务（Subtask）。每个 Subtask 最终都需要被调度并分发到一个特定的 TaskSlot 中，作为 TaskManager 进程内的一个线程（Thread）来执行
//4. 槽位共享（Slot Sharing）与资源高效利用Flink 默认支持并鼓励槽位共享（Slot Sharing Group）：
//      多算子共存：同一个作业中、属于不同算子的 Subtask（例如 Source -> Map -> Sink），只要它们位于同一个 Slot Sharing Group 内，就可以共享同一个 TaskSlot。
//      优点：避免资源浪费：轻量级算子（如 Map）与重量级/含状态算子（如 Window / Join）共享 Slot，能大幅提升 CPU 与内存的利用率。
//      降低部署复杂度：计算作业所需的总 Slot 数量仅取决于整个 Job 中最大算子的并行度（Max Parallelism），而不需要把每个算子的并行度累加


//1. 堆内存（Heap Memory）的作用
//堆内存是标准的 JVM 堆空间，由 JVM 的垃圾回收器（GC）自动管理。
//
//核心作用与用途：
//运行 用户自定义代码（UDF）：你在 MapFunction、FlatMapFunction 或 ProcessFunction 中创建的 Java/Scala 对象，全部存在堆内存中。
//
//Flink 框架自身运行：TaskManager 进程内部的数据结构、元数据、RPC 通信组件以及调度逻辑等。
//
//JVM 堆内状态后端（Heap StateBackend / HashMapStateBackend）：
//
//如果使用的是默认的堆内状态后端，所有算子的 Keyed State（如 ValueState、ListState）都会直接以 Java 对象的形式保存在堆内存中。
//
//数据流转的临时缓冲区：某些算子在处理数据时产生的临时对象。
//
//特点与风险：使用简单且读写极快（因为是 native Java 对象），但如果状态极其庞大或频繁创建大量短生命周期对象，容易引发严重或频繁的 JVM GC 停顿（Stop-The-World），甚至导致 OOM。
//
//2. 托管内存（Managed Memory）的作用
//托管内存是 Flink 专门划出来、直接进行内存管理（Memory Management） 的区域（默认占 TaskManager 总内存的 40%左右），绝大部分情况下分配在堆外（Off-Heap）。
//
//Flink 引入托管内存的主要目的就是：摆脱 JVM GC 的限制，并实现超大状态和高性能计算的极致优化。
//
//核心作用与用途：
//① RocksDB 状态后端（EmbeddedRocksDBStateBackend）
//当开启 RocksDB 作为状态后端时，RocksDB 运行在 JVM 之外的 C++ 进程层。
//
//托管内存会被分配给 RocksDB 作为 Block Cache 和 Write Buffer (MemTable)，用来加速状态的读取与写入。
//
//关键优势：由于直接分配给堆外的 C++ 空间，RocksDB 的状态数据完全不占用 JVM 堆，彻底避免了因为大状态导致的 GC 停顿。
//
//② 批处理与内置算子的内存缓存（Batch & Sorting/Hashing）
//针对 Batch 作业（或 Streaming 中的某些排序/窗口算子），Flink 会将数据序列化为二进制字节数组（MemorySegment）存放在托管内存中。
//
//Flink 可以直接对这些二进制数据进行排序、哈希连接（Hash Join）和分组，无需反序列化成 Java 对象。
//
//③ Table API & SQL 运行时
//Flink SQL 运行时的很多内置算子（如流式 Group Aggregation、TopN、Join）都需要消耗托管内存来进行高效的数据缓存和计算。
//
//④ Python API 支持（PyFlink）
//如果作业使用了 Python UDF，托管内存会被划出一部分专门作为 Python 进程与 JVM 进程通信和数据交换的缓冲区。
public class TaskSlot<T extends TaskSlotPayload> implements AutoCloseableAsync {
    private static final Logger LOG = LoggerFactory.getLogger(TaskSlot.class);

    //当前 TaskSlot 在 TaskManager 内部的唯一数字编号（例如 0, 1, 2...）
    /** Index of the task slot. */
    private final int index;


    //作用：定义该 TaskSlot 拥有的具体物理资源大小，包括 托管内存 (Managed Memory)、网络内存 (Network Memory) 以及 CPU 核心数 等。
    //意义：Flink 目前主要利用它实现内存的严格隔离，防止不同 Slot 之间因内存竞争导致 OOM 崩溃
    /** Resource characteristics for this slot. */
    private final ResourceProfile resourceProfile;

    /** Tasks running in this slot. */
    private final Map<ExecutionAttemptID, T> tasks;

    private final MemoryManager memoryManager;

    //作用：标识 Slot 当前的生命周期状态（如 FREE 空闲、ALLOCATED 已分配、ACTIVE 激活运行中）。
    //意义：调度器据此判断该 Slot 是否能接受新的计算任务
    /** State of this slot. */
    private TaskSlotState state;

    //作用：记录当前 Slot 属于哪一个正在运行的 Flink Job。
    //意义：以便在 Job 结束或异常失败时，TaskManager 能够批量释放或回收该 Job 占用的所有槽位
    /** Job id to which the slot has been allocated. */
    private final JobID jobId;

    //作用：由 ResourceManager 分配的全局唯一标识符，用于追踪该 Slot 究竟被分配给了哪一个具体的作业请求。
    //意义：区分当前 Slot 的占用权，确保资源分配的准确性
    /** Allocation id of this slot. */
    private final AllocationID allocationId;

    /** The closing future is completed when the slot is freed and closed. */
    private final CompletableFuture<Void> closingFuture;

    /** {@link Executor} for background actions, e.g. verify all managed memory released. */
    private final Executor asyncExecutor;

    public TaskSlot(
            final int index,
            final ResourceProfile resourceProfile,
            final int memoryPageSize,
            final JobID jobId,
            final AllocationID allocationId,
            final Executor asyncExecutor) {

        this.index = index;
        this.resourceProfile = Preconditions.checkNotNull(resourceProfile);
        this.asyncExecutor = Preconditions.checkNotNull(asyncExecutor);

        this.tasks = CollectionUtil.newHashMapWithExpectedSize(4);
        this.state = TaskSlotState.ALLOCATED;

        this.jobId = jobId;
        this.allocationId = allocationId;
        // resourceProfile = ResourceProfile{taskHeapMemory=1024.000gb (1099511627776 bytes), taskOffHeapMemory=1024.000gb (1099511627776 bytes), managedMemory=128.000mb (134217728 bytes), networkMemory=64.000mb (67108864 bytes)}
        // memoryPageSize = 32768  创建内存管理对象
        this.memoryManager = createMemoryManager(resourceProfile, memoryPageSize);//

        this.closingFuture = new CompletableFuture<>();
    }

    // ----------------------------------------------------------------------------------
    // State accessors
    // ----------------------------------------------------------------------------------

    public int getIndex() {
        return index;
    }

    public ResourceProfile getResourceProfile() {
        return resourceProfile;
    }

    public JobID getJobId() {
        return jobId;
    }

    public AllocationID getAllocationId() {
        return allocationId;
    }

    TaskSlotState getState() {
        return state;
    }

    public boolean isEmpty() {
        return tasks.isEmpty();
    }

    public boolean isActive(JobID activeJobId, AllocationID activeAllocationId) {
        Preconditions.checkNotNull(activeJobId);
        Preconditions.checkNotNull(activeAllocationId);

        return TaskSlotState.ACTIVE == state
                && activeJobId.equals(jobId)
                && activeAllocationId.equals(allocationId);
    }

    public boolean isAllocated(JobID jobIdToCheck, AllocationID allocationIDToCheck) {
        Preconditions.checkNotNull(jobIdToCheck);
        Preconditions.checkNotNull(allocationIDToCheck);

        return jobIdToCheck.equals(jobId)
                && allocationIDToCheck.equals(allocationId)
                && (TaskSlotState.ACTIVE == state || TaskSlotState.ALLOCATED == state);
    }

    public boolean isReleasing() {
        return TaskSlotState.RELEASING == state;
    }

    /**
     * Get all tasks running in this task slot.
     *
     * @return Iterator to all currently contained tasks in this task slot.
     */
    public Iterator<T> getTasks() {
        return tasks.values().iterator();
    }

    public MemoryManager getMemoryManager() {
        return memoryManager;
    }

    // ----------------------------------------------------------------------------------
    // State changing methods
    // ----------------------------------------------------------------------------------

    /**
     * Add the given task to the task slot. This is only possible if there is not already another
     * task with the same execution attempt id added to the task slot. In this case, the method
     * returns true. Otherwise the task slot is left unchanged and false is returned.
     *
     * <p>In case that the task slot state is not active an {@link IllegalStateException} is thrown.
     * In case that the task's job id and allocation id don't match with the job id and allocation
     * id for which the task slot has been allocated, an {@link IllegalArgumentException} is thrown.
     *
     * @param task to be added to the task slot
     * @throws IllegalStateException if the task slot is not in state active
     * @return true if the task was added to the task slot; otherwise false
     */
    public boolean add(T task) {
        // Check that this slot has been assigned to the job sending this task
        Preconditions.checkArgument(
                task.getJobID().equals(jobId),
                "The task's job id does not match the "
                        + "job id for which the slot has been allocated.");
        Preconditions.checkArgument(
                task.getAllocationId().equals(allocationId),
                "The task's allocation "
                        + "id does not match the allocation id for which the slot has been allocated.");
        Preconditions.checkState(
                TaskSlotState.ACTIVE == state, "The task slot is not in state active.");

        T oldTask = tasks.put(task.getExecutionId(), task);

        if (oldTask != null) {
            tasks.put(task.getExecutionId(), oldTask);
            return false;
        } else {
            return true;
        }
    }

    /**
     * Remove the task identified by the given execution attempt id.
     *
     * @param executionAttemptId identifying the task to be removed
     * @return The removed task if there was any; otherwise null.
     */
    public T remove(ExecutionAttemptID executionAttemptId) {
        return tasks.remove(executionAttemptId);
    }

    /** Removes all tasks from this task slot. */
    public void clear() {
        tasks.clear();
    }

    /**
     * Mark this slot as active. A slot can only be marked active if it's in state allocated.
     *
     * <p>The method returns true if the slot was set to active. Otherwise it returns false.
     *
     * @return True if the new state of the slot is active; otherwise false
     */
    public boolean markActive() {
        if (TaskSlotState.ALLOCATED == state || TaskSlotState.ACTIVE == state) {
            state = TaskSlotState.ACTIVE;

            return true;
        } else {
            return false;
        }
    }

    /**
     * Mark the slot as inactive/allocated. A slot can only be marked as inactive/allocated if it's
     * in state allocated or active.
     *
     * @return True if the new state of the slot is allocated; otherwise false
     */
    public boolean markInactive() {
        if (TaskSlotState.ACTIVE == state || TaskSlotState.ALLOCATED == state) {
            state = TaskSlotState.ALLOCATED;

            return true;
        } else {
            return false;
        }
    }

    /**
     * Generate the slot offer from this TaskSlot.
     *
     * @return The sot offer which this task slot can provide
     */
    public SlotOffer generateSlotOffer() {
        Preconditions.checkState(
                TaskSlotState.ACTIVE == state || TaskSlotState.ALLOCATED == state,
                "The task slot is not in state active or allocated.");
        Preconditions.checkState(allocationId != null, "The task slot are not allocated");

        return new SlotOffer(allocationId, index, resourceProfile);
    }

    @Override
    public String toString() {
        return "TaskSlot(index:"
                + index
                + ", state:"
                + state
                + ", resource profile: "
                + resourceProfile
                + ", allocationId: "
                + (allocationId != null ? allocationId.toString() : "none")
                + ", jobId: "
                + (jobId != null ? jobId.toString() : "none")
                + ')';
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return closeAsync(new FlinkException("Closing the slot"));
    }

    /**
     * Close the task slot asynchronously.
     *
     * <p>Slot is moved to {@link TaskSlotState#RELEASING} state and only once. If there are active
     * tasks running in the slot then they are failed. The future of all tasks terminated and slot
     * cleaned up is initiated only once and always returned in case of multiple attempts to close
     * the slot.
     *
     * @param cause cause of closing
     * @return future of all running task if any being done and slot cleaned up.
     */
    CompletableFuture<Void> closeAsync(Throwable cause) {
        if (!isReleasing()) {
            state = TaskSlotState.RELEASING;
            if (!isEmpty()) {
                // we couldn't free the task slot because it still contains task, fail the tasks
                // and set the slot state to releasing so that it gets eventually freed
                tasks.values().forEach(task -> task.failExternally(cause));
            }

            final CompletableFuture<Void> shutdownFuture =
                    FutureUtils.waitForAll(
                                    tasks.values().stream()
                                            .map(TaskSlotPayload::getTerminationFuture)
                                            .collect(Collectors.toList()))
                            .thenRun(memoryManager::shutdown);
            verifyAllManagedMemoryIsReleasedAfter(shutdownFuture);
            FutureUtils.forward(shutdownFuture, closingFuture);
        }
        return closingFuture;
    }

    private void verifyAllManagedMemoryIsReleasedAfter(CompletableFuture<Void> after) {
        after.thenRunAsync(
                () -> {
                    if (!memoryManager.verifyEmpty()) {
                        LOG.warn(
                                "Not all slot managed memory is freed at {}. This usually indicates memory leak. "
                                        + "However, when running an old JVM version it can also be caused by slow garbage collection. "
                                        + "Try to upgrade to Java 8u72 or higher if running on an old Java version.",
                                this);
                    }
                },
                asyncExecutor);
    }

    private static MemoryManager createMemoryManager(
            ResourceProfile resourceProfile, int pageSize) {
        //创建内存管理   resourceProfile.getManagedMemory().getBytes() = 128m
        return MemoryManager.create(resourceProfile.getManagedMemory().getBytes(), pageSize);//
    }
}
