/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.scheduler.loading.LoadingWeight;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.concurrent.FutureUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** {@link SlotPool} implementation which uses the {@link DeclarativeSlotPool} to allocate slots. */
//用于衔接新版声明式资源管理（Declarative Resource Management）与传统命令式 Slot 分配逻辑（Imperative/Allocation-based）的核心桥梁组件
//Flink 在架构演进中引入了声明式资源管理，即 JobMaster 不再逐个向 ResourceManager 申请具体的 Slot，而是声明当前作业总共需要多少资源（Resource Requirements）
public class DeclarativeSlotPoolBridge extends DeclarativeSlotPoolService implements SlotPool {

    /** Helper class to represent the fulfilled allocation infromation. */
    private static final class FulfilledAllocation {
        final AllocationID allocationID;
        final ResourceID taskExecutorID;
        final LoadingWeight loadingWeight;

        FulfilledAllocation(PhysicalSlot slot, LoadingWeight loadingWeight) {
            this.allocationID = Preconditions.checkNotNull(slot.getAllocationId());
            this.taskExecutorID =
                    Preconditions.checkNotNull(slot.getTaskManagerLocation().getResourceID());
            this.loadingWeight = Preconditions.checkNotNull(loadingWeight);
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FulfilledAllocation that = (FulfilledAllocation) o;
            return Objects.equals(allocationID, that.allocationID)
                    && Objects.equals(taskExecutorID, that.taskExecutorID)
                    && Objects.equals(loadingWeight, that.loadingWeight);
        }
    }

    //记录当前处于挂起（等待中）状态的 Slot 申请。当上层调度器（Scheduler）调用 allocateSlot 申请资源，
    // 但目前 Slot 池中没有空闲的物理 Slot 时，这个请求就会被封装成 PendingRequest 存入该 Map 中。一旦后续有新 Slot 加入，会从这里取出请求进行匹配
    private final Map<SlotRequestId, PendingRequest> pendingRequests;
    //记录当前**已经成功分配（已满足）**的 Slot 请求映射关系。当一个挂起的请求成功匹配到了物理 Slot，或者直接从空闲池中拿到了 Slot，
    // 该记录就会从 pendingRequests 移入 fulfilledRequests。它用于在作业运行期间，追踪哪个 SlotRequestId 正在占用哪一个具体的物理 Slot
    private final Map<SlotRequestId, FulfilledAllocation> fulfilledRequests;
    //空闲 Slot 的超时释放时间。当某个物理 Slot 变为空闲状态（例如 Task 执行完毕释放了 Slot），且当前作业的整体资源需求不需要它时，它不会立刻归还给集群。
    // DeclarativeSlotPoolBridge 会启动一个定时器，如果该 Slot 在此时间内一直未被再次复用，就会被正式释放并归还给 ResourceManager
    private final Duration idleSlotTimeout;

    //当有新的物理 Slot 供给（Offer）进来，或者有多个挂起请求和空闲 Slot 需要撮合时，
    // 该策略决定了“哪一个请求优先分配到哪一个物理 Slot”**。常见的策略比如优先考虑本地性（Locality，即计算节点和数据节点在同一台机器）、或者优先填满已有的 TaskManager
    private final RequestSlotMatchingStrategy requestSlotMatchingStrategy;

    //批处理（Batch）模式下 Slot 请求的超时时间。在流处理中，如果拿不到 Slot 通常会触发 NoResourceAvailableException 导致作业失败；
    // 而在批处理或有限数据集场景下，资源可能是轮询复用的。这个参数定义了批处理任务在抛出超时异常前，最多可以等待资源的最长时间
    private final Duration batchSlotTimeout;
    //是否禁用批处理 Slot 超时检查的开关。
    // 在某些特定场景下（例如处于某些动态调度的中间状态，或者用户显式配置了不超时），通过该布尔值可以临时或全局关闭 batchSlotTimeout 的定时检查机制，防止误判超时
    private boolean isBatchSlotRequestTimeoutCheckDisabled;

    //标记当前作业是否正在重启中。当作业因为异常触发 Failover（故障转移）或正常的全局重启时，该状态会被置为 true。
    // 此时，DeclarativeSlotPoolBridge 在处理 Slot 释放、保留或者重新申请时会采取不同的逻辑，防止在重启的混乱过渡期将不该释放的 Slot 错误地退还给 ResourceManager
    private boolean isJobRestarting = false;

    //是否开启延迟（推迟）Slot 分配。这是一个优化开关。如果设为 true，当调度器发出申请时，
    // 它不会立刻去强行绑定和分配 Slot，而是会稍微“等一等”或者将分配时机推迟，以便集齐更多的 Slot 供给或更全的拓扑信息，从而做出全局更优的 Locality（本地性）匹配选择
    private final boolean deferSlotAllocation;

    public DeclarativeSlotPoolBridge(
            JobID jobId,
            DeclarativeSlotPoolFactory declarativeSlotPoolFactory,
            Clock clock,
            Duration rpcTimeout,
            Duration idleSlotTimeout,
            Duration batchSlotTimeout,
            RequestSlotMatchingStrategy requestSlotMatchingStrategy,
            Duration slotRequestMaxInterval,
            boolean deferSlotAllocation,
            @Nonnull ComponentMainThreadExecutor componentMainThreadExecutor) {
        super(
                jobId,
                declarativeSlotPoolFactory,
                clock,
                idleSlotTimeout,
                rpcTimeout,
                slotRequestMaxInterval,
                componentMainThreadExecutor);

        this.idleSlotTimeout = idleSlotTimeout;
        this.batchSlotTimeout = Preconditions.checkNotNull(batchSlotTimeout);

        log.debug(
                "Using the request slot matching strategy: {}",
                requestSlotMatchingStrategy.getClass().getSimpleName());
        this.requestSlotMatchingStrategy = requestSlotMatchingStrategy;
        this.deferSlotAllocation = deferSlotAllocation;

        this.isBatchSlotRequestTimeoutCheckDisabled = false;

        this.pendingRequests = new LinkedHashMap<>();
        this.fulfilledRequests = new HashMap<>();
    }

    @Override
    public <T> Optional<T> castInto(Class<T> clazz) {
        if (clazz.isAssignableFrom(getClass())) {
            return Optional.of(clazz.cast(this));
        }

        return Optional.empty();
    }

    @Override
    protected void onStart() {
        //注册获取新slot的监听器
        // DefaultDeclarativeSlotPool#registerNewSlotsListener
        getDeclarativeSlotPool().registerNewSlotsListener(this::newSlotsAreAvailable);//
        if (deferSlotAllocation) {
            getDeclarativeSlotPool()
                    .registerResourceRequestStableListener(
                            () -> {
                                if (!pendingRequests.isEmpty()) {
                                    componentMainThreadExecutor.schedule(
                                            this::newSlotsAvailableForDeferAllocation,
                                            0L,
                                            TimeUnit.MILLISECONDS);
                                }
                            });
        }

        componentMainThreadExecutor.schedule(
                this::checkIdleSlotTimeout, idleSlotTimeout.toMillis(), TimeUnit.MILLISECONDS);
        componentMainThreadExecutor.schedule(
                this::checkBatchSlotTimeout, batchSlotTimeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    protected void onClose() {
        final FlinkException cause = new FlinkException("Closing slot pool");
        cancelPendingRequests(request -> true, cause);
    }

    /**
     * To set whether the underlying is currently restarting or not. In the former case the slot
     * pool bridge will accept all incoming slot offers.
     *
     * @param isJobRestarting whether this is restarting or not
     */
    @Override
    public void setIsJobRestarting(boolean isJobRestarting) {
        this.isJobRestarting = isJobRestarting;
    }

    //接收ResourceManager提供的slot
    @Override
    public Collection<SlotOffer> offerSlots(
            TaskManagerLocation taskManagerLocation,
            TaskManagerGateway taskManagerGateway,
            // wordcount offers size = 2
            // SlotOffer{allocationId=7ed7e8da86faede05a7662e2272b5d6d, slotIndex=3, resourceProfile=ResourceProfile{taskHeapMemory=512.000gb (549755813888 bytes), taskOffHeapMemory=512.000gb (549755813888 bytes), managedMemory=64.000mb (67108864 bytes), networkMemory=32.000mb (33554432 bytes)}}
            Collection<SlotOffer> offers) {
        //断言DeclarativeSlotPoolService 已经启动
        assertHasBeenStarted();

        if (!isTaskManagerRegistered(taskManagerLocation.getResourceID())) {
            // 忽略 没注册提供slot 的 RM
            log.debug(
                    "Ignoring offered slots from unknown task manager {}.",
                    taskManagerLocation.getResourceID());
            return Collections.emptyList();
        }

        if (isJobRestarting) {
            //当前作业是否正在重启中
            return getDeclarativeSlotPool()
                    .registerSlots(
                            offers,
                            taskManagerLocation,
                            taskManagerGateway,
                            getRelativeTimeMillis());

        } else {
            //DefaultDeclarativeSlotPool#offerSlots
            //将SlotOffer 封装成 AllocatedSlot
            return getDeclarativeSlotPool().offerSlots(//
                            offers,
                            taskManagerLocation,
                            taskManagerGateway,
                            getRelativeTimeMillis());
        }
    }

    private void cancelPendingRequests(
            Predicate<PendingRequest> requestPredicate, FlinkException cancelCause) {

        ResourceCounter decreasedResourceRequirements = ResourceCounter.empty();

        // need a copy since failing a request could trigger another request to be issued
        final Iterable<PendingRequest> pendingRequestsToFail =
                new ArrayList<>(pendingRequests.values());
        pendingRequests.clear();

        for (PendingRequest pendingRequest : pendingRequestsToFail) {
            if (requestPredicate.test(pendingRequest)) {
                pendingRequest.failRequest(cancelCause);
                decreasedResourceRequirements =
                        decreasedResourceRequirements.add(pendingRequest.getResourceProfile(), 1);
            } else {
                pendingRequests.put(pendingRequest.getSlotRequestId(), pendingRequest);
            }
        }

        getDeclarativeSlotPool().decreaseResourceRequirementsBy(decreasedResourceRequirements);
    }

    @Override
    protected void onReleaseTaskManager(ResourceCounter previouslyFulfilledRequirement) {
        getDeclarativeSlotPool().decreaseResourceRequirementsBy(previouslyFulfilledRequirement);
    }

    //被DefaultDeclarativeSlotPool#internalOfferSlots 方法的
    // newSlotsListener.notifyNewSlotsAreAvailable(acceptedSlots)调用
    @VisibleForTesting
    void newSlotsAreAvailable(Collection<? extends PhysicalSlot> newSlots) {
        log.debug("Received new available slots: {}", newSlots);

        if (pendingRequests.isEmpty()) {
            return;
        }

        if (deferSlotAllocation) {
            if (getDeclarativeSlotPool().isResourceRequestStable()) {
                newSlotsAvailableForDeferAllocation();
            }
        } else {
            //
            newSlotsAvailableForDirectlyAllocation(newSlots);//
        }
    }

    private Map<ResourceID, LoadingWeight> getTaskExecutorsLoadingView() {
        final Map<ResourceID, LoadingWeight> result = new HashMap<>();
        Collection<FulfilledAllocation> fulfilledAllocations = fulfilledRequests.values();
        for (FulfilledAllocation allocation : fulfilledAllocations) {
            result.compute(
                    allocation.taskExecutorID,
                    (ignoredID, oldLoading) ->
                            Objects.isNull(oldLoading)
                                    ? allocation.loadingWeight
                                    : oldLoading.merge(allocation.loadingWeight));
        }
        return result;
    }

    private void newSlotsAvailableForDeferAllocation() {
        final Collection<PhysicalSlot> freeSlots =
                getDeclarativeSlotPool().getFreeSlotTracker().getFreeSlotsInformation();

        if (freeSlots.size() < pendingRequests.size()) {
            // Do nothing and waiting slots.
            log.debug(
                    "The number of available slots: {}, the required number of slots: {}, waiting for more available slots.",
                    freeSlots.size(),
                    pendingRequests.size());
            return;
        }

        final Collection<RequestSlotMatchingStrategy.RequestSlotMatch> requestSlotMatches =
                requestSlotMatchingStrategy.matchRequestsAndSlots(
                        freeSlots, pendingRequests.values(), getTaskExecutorsLoadingView());
        if (requestSlotMatches.size() == pendingRequests.size()) {
            reserveAndFulfillMatchedFreeSlots(requestSlotMatches);
        } else if (requestSlotMatches.size() < pendingRequests.size()) {
            // Do nothing and waiting slots.
            log.debug(
                    "Ignored the matched results: {}, pendingRequests: {}, waiting for more available slots.",
                    requestSlotMatches,
                    pendingRequests);
        } else {
            // For requestSlotMatches.size() > pendingRequests.size()
            throw new IllegalStateException(
                    "The number of matched slots is not equals to the pendingRequests.");
        }
    }

    private void newSlotsAvailableForDirectlyAllocation(
            Collection<? extends PhysicalSlot> newSlots) {
        //SimpleRequestSlotMatchingStrategy#matchRequestsAndSlots
        final Collection<RequestSlotMatchingStrategy.RequestSlotMatch> requestSlotMatches =
                requestSlotMatchingStrategy.matchRequestsAndSlots(newSlots, pendingRequests.values(), new HashMap<>());//
        reserveAndFulfillMatchedFreeSlots(requestSlotMatches);//
    }

    private void reserveAndFulfillMatchedFreeSlots(
            Collection<RequestSlotMatchingStrategy.RequestSlotMatch> requestSlotMatches) {
        for (RequestSlotMatchingStrategy.RequestSlotMatch match : requestSlotMatches) {
            final PendingRequest pendingRequest = match.getPendingRequest();
            final PhysicalSlot slot = match.getSlot();

            log.debug("Matched pending request {} with slot {}.", pendingRequest, slot);

            Preconditions.checkNotNull(
                    pendingRequests.remove(pendingRequest.getSlotRequestId()),
                    "Cannot fulfill a non existing pending slot request.");
            //
            reserveFreeSlot(slot.getAllocationId(), pendingRequest);
        }

        // we have to first reserve all matching slots before fulfilling the requests
        // otherwise it can happen that the scheduler reserves one of the new slots
        // for a request which has been triggered by fulfilling a pending request
        for (RequestSlotMatchingStrategy.RequestSlotMatch requestSlotMatch : requestSlotMatches) {
            final PendingRequest pendingRequest = requestSlotMatch.getPendingRequest();
            final PhysicalSlot slot = requestSlotMatch.getSlot();

            Preconditions.checkState(
                    pendingRequest.fulfill(slot), "Pending requests must be fulfillable.");
        }
    }

    @VisibleForTesting
    Collection<PhysicalSlot> getFreeSlotsInformation() {
        return getDeclarativeSlotPool().getFreeSlotTracker().getFreeSlotsInformation();
    }

    //将一个原本处于闲置状态（Free）的物理槽位正式锁定，分配给指定的任务请求，并在内存中记录这次绑定关系
    private PhysicalSlot reserveFreeSlot(AllocationID allocationId, PendingRequest pendingRequest) {
        SlotRequestId slotRequestId = pendingRequest.getSlotRequestId();
        log.debug("Reserve slot {} for slot request id {}", allocationId, slotRequestId);
        //底层会将该 Slot 的状态从闲置（Free）更改为已分配/已锁定（Allocated/Reserved），确保该资源不会再被其他请求抢占
        //DefaultDeclarativeSlotPool#reserveFreeSlot
        final PhysicalSlot slot = getDeclarativeSlotPool().reserveFreeSlot(allocationId, pendingRequest.getResourceProfile());
        fulfilledRequests.put(slotRequestId, new FulfilledAllocation(slot, pendingRequest.getLoading()));
        return slot;
    }

    @Override
    public Optional<PhysicalSlot> allocateAvailableSlot(
            AllocationID allocationID, PhysicalSlotRequest physicalSlotRequest) {
        assertRunningInMainThread();

        ResourceProfile requiredResourceProfile =
                physicalSlotRequest.getPhysicalSlotResourceProfile();
        Preconditions.checkNotNull(
                requiredResourceProfile, "The requiredResourceProfile must not be null.");

        SlotRequestId slotRequestId = physicalSlotRequest.getSlotRequestId();
        log.debug(
                "Reserving free slot {} for slot request id {} and profile {}.",
                allocationID,
                slotRequestId,
                requiredResourceProfile);

        return Optional.of(
                reserveFreeSlotForResource(allocationID, physicalSlotRequest.toPendingRequest()));
    }

    private PhysicalSlot reserveFreeSlotForResource(
            AllocationID allocationId, PendingRequest pendingRequest) {

        ResourceProfile requiredResourceProfile = pendingRequest.getResourceProfile();

        getDeclarativeSlotPool()
                .increaseResourceRequirementsBy(
                        ResourceCounter.withResource(requiredResourceProfile, 1));

        return reserveFreeSlot(allocationId, pendingRequest);
    }

    @Override
    public CompletableFuture<PhysicalSlot> requestNewAllocatedSlot(
            PhysicalSlotRequest physicalSlotRequest, @Nullable Duration timeout) {
        assertRunningInMainThread();

        log.debug(
                "Request new allocated slot with slot request id {} and resource profile {}",
                physicalSlotRequest.getSlotRequestId(),
                physicalSlotRequest.getPhysicalSlotResourceProfile());

        return internalRequestNewSlot(physicalSlotRequest.toPendingRequest(), timeout);//
    }

    @Override
    public CompletableFuture<PhysicalSlot> requestNewAllocatedBatchSlot(
            PhysicalSlotRequest physicalSlotRequest) {
        assertRunningInMainThread();

        log.debug(
                "Request new allocated batch slot with slot request id {} and resource profile {}",
                physicalSlotRequest.getSlotRequestId(),
                physicalSlotRequest.getPhysicalSlotResourceProfile());

        return internalRequestNewSlot(physicalSlotRequest.toPendingRequest(), null);
    }

    private CompletableFuture<PhysicalSlot> internalRequestNewSlot(
            PendingRequest pendingRequest, @Nullable Duration timeout) {
        //
        internalRequestNewAllocatedSlot(pendingRequest);//

        if (timeout == null) {
            return pendingRequest.getSlotFuture();
        } else {
            return FutureUtils.orTimeout(
                            pendingRequest.getSlotFuture(),
                            timeout.toMillis(),
                            TimeUnit.MILLISECONDS,
                            componentMainThreadExecutor,
                            String.format(
                                    "Pending slot request %s timed out after %d ms.",
                                    pendingRequest.getSlotRequestId(), timeout.toMillis()))
                    .whenComplete(
                            (physicalSlot, throwable) -> {
                                if (throwable instanceof TimeoutException) {
                                    timeoutPendingSlotRequest(pendingRequest.getSlotRequestId());
                                }
                            });
        }
    }

    private void timeoutPendingSlotRequest(SlotRequestId slotRequestId) {
        releaseSlot(
                slotRequestId,
                new TimeoutException("Pending slot request timed out in slot pool."));
    }

    //
    private void internalRequestNewAllocatedSlot(PendingRequest pendingRequest) {
        //加入集合中
        pendingRequests.put(pendingRequest.getSlotRequestId(), pendingRequest);
        ResourceCounter resourceCounter = ResourceCounter.withResource(
                pendingRequest.getResourceProfile(),
                1);
        //DefaultDeclarativeSlotPool#increaseResourceRequirementsBy
        getDeclarativeSlotPool().increaseResourceRequirementsBy(resourceCounter);
    }

    @Override
    protected void onFailAllocation(ResourceCounter previouslyFulfilledRequirements) {
        getDeclarativeSlotPool().decreaseResourceRequirementsBy(previouslyFulfilledRequirements);
    }

    @Override
    public void releaseSlot(@Nonnull SlotRequestId slotRequestId, @Nullable Throwable cause) {
        log.debug("Release slot with slot request id {}", slotRequestId);
        assertRunningInMainThread();

        final PendingRequest pendingRequest = pendingRequests.remove(slotRequestId);

        if (pendingRequest != null) {
            getDeclarativeSlotPool()
                    .decreaseResourceRequirementsBy(
                            ResourceCounter.withResource(pendingRequest.getResourceProfile(), 1));
            pendingRequest.failRequest(
                    new FlinkException(
                            String.format(
                                    "Pending slot request with %s has been released.",
                                    pendingRequest.getSlotRequestId()),
                            cause));
        } else {
            final FulfilledAllocation fulfilledAllocation = fulfilledRequests.remove(slotRequestId);

            if (fulfilledAllocation != null) {
                ResourceCounter previouslyFulfilledRequirement =
                        getDeclarativeSlotPool()
                                .freeReservedSlot(
                                        fulfilledAllocation.allocationID,
                                        cause,
                                        getRelativeTimeMillis());
                getDeclarativeSlotPool()
                        .decreaseResourceRequirementsBy(previouslyFulfilledRequirement);
            } else {
                log.debug(
                        "Could not find slot which has fulfilled slot request {}. Ignoring the release operation.",
                        slotRequestId);
            }
        }
    }

    @Override
    public void notifyNotEnoughResourcesAvailable(
            Collection<ResourceRequirement> acquiredResources) {
        assertRunningInMainThread();

        failPendingRequests(acquiredResources);
    }

    private void failPendingRequests(Collection<ResourceRequirement> acquiredResources) {
        // only fails streaming requests because batch jobs do not require all resources
        // requirements to be fullfilled at the same time
        Predicate<PendingRequest> predicate = request -> !request.isBatchRequest();
        if (pendingRequests.values().stream().anyMatch(predicate)) {
            log.warn(
                    "Could not acquire the minimum required resources, failing slot requests. Acquired: {}. Current slot pool status: {}",
                    acquiredResources,
                    getSlotServiceStatus());
            cancelPendingRequests(
                    predicate,
                    NoResourceAvailableException.withoutStackTrace(
                            "Could not acquire the minimum required resources."));
        }
    }

    @Override
    public Collection<SlotInfo> getAllocatedSlotsInformation() {
        assertRunningInMainThread();

        final Collection<? extends SlotInfo> allSlotsInformation =
                getDeclarativeSlotPool().getAllSlotsInformation();
        final Set<AllocationID> freeSlots =
                getDeclarativeSlotPool().getFreeSlotTracker().getAvailableSlots();

        return allSlotsInformation.stream()
                .filter(slotInfo -> !freeSlots.contains(slotInfo.getAllocationId()))
                .collect(Collectors.toList());
    }

    @Override
    public FreeSlotTracker getFreeSlotTracker() {
        assertRunningInMainThread();

        return getDeclarativeSlotPool().getFreeSlotTracker();
    }

    @Override
    public void disableBatchSlotRequestTimeoutCheck() {
        isBatchSlotRequestTimeoutCheckDisabled = true;
    }

    private void assertRunningInMainThread() {
        if (componentMainThreadExecutor != null) {
            componentMainThreadExecutor.assertRunningInMainThread();
        } else {
            throw new IllegalStateException("The FutureSlotPool has not been started yet.");
        }
    }

    private void checkIdleSlotTimeout() {
        getDeclarativeSlotPool().releaseIdleSlots(getRelativeTimeMillis());

        if (componentMainThreadExecutor != null) {
            componentMainThreadExecutor.schedule(
                    this::checkIdleSlotTimeout, idleSlotTimeout.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    void checkBatchSlotTimeout() {
        assertRunningInMainThread();

        if (isBatchSlotRequestTimeoutCheckDisabled) {
            return;
        }

        final Collection<PendingRequest> pendingBatchRequests = getPendingBatchRequests();

        if (!pendingBatchRequests.isEmpty()) {
            final Set<ResourceProfile> allResourceProfiles = getResourceProfilesFromAllSlots();

            final Map<Boolean, List<PendingRequest>> fulfillableAndUnfulfillableRequests =
                    pendingBatchRequests.stream()
                            .collect(
                                    Collectors.partitioningBy(
                                            canBeFulfilledWithAnySlot(allResourceProfiles)));

            final List<PendingRequest> fulfillableRequests =
                    fulfillableAndUnfulfillableRequests.get(true);
            final List<PendingRequest> unfulfillableRequests =
                    fulfillableAndUnfulfillableRequests.get(false);

            final long currentTimestamp = getRelativeTimeMillis();

            for (PendingRequest fulfillableRequest : fulfillableRequests) {
                fulfillableRequest.markFulfillable();
            }

            for (PendingRequest unfulfillableRequest : unfulfillableRequests) {
                unfulfillableRequest.markUnfulfillable(currentTimestamp);

                if (unfulfillableRequest.getUnfulfillableSince() + batchSlotTimeout.toMillis()
                        <= currentTimestamp) {
                    timeoutPendingSlotRequest(unfulfillableRequest.getSlotRequestId());
                }
            }
        }

        if (componentMainThreadExecutor != null) {
            componentMainThreadExecutor.schedule(
                    this::checkBatchSlotTimeout,
                    batchSlotTimeout.toMillis(),
                    TimeUnit.MILLISECONDS);
        }
    }

    private Set<ResourceProfile> getResourceProfilesFromAllSlots() {
        return Stream.concat(
                        getFreeSlotTracker().getFreeSlotsInformation().stream(),
                        getAllocatedSlotsInformation().stream())
                .map(SlotInfo::getResourceProfile)
                .collect(Collectors.toSet());
    }

    private Collection<PendingRequest> getPendingBatchRequests() {
        return pendingRequests.values().stream()
                .filter(PendingRequest::isBatchRequest)
                .collect(Collectors.toList());
    }

    private static Predicate<PendingRequest> canBeFulfilledWithAnySlot(
            Set<ResourceProfile> allocatedResourceProfiles) {
        return pendingRequest -> {
            for (ResourceProfile allocatedResourceProfile : allocatedResourceProfiles) {
                if (allocatedResourceProfile.isMatching(pendingRequest.getResourceProfile())) {
                    return true;
                }
            }

            return false;
        };
    }

    @VisibleForTesting
    public int getNumPendingRequests() {
        return pendingRequests.size();
    }

    @VisibleForTesting
    void increaseResourceRequirementsBy(ResourceCounter increment) {
        getDeclarativeSlotPool().increaseResourceRequirementsBy(increment);
    }

    @VisibleForTesting
    boolean isBatchSlotRequestTimeoutCheckEnabled() {
        return !isBatchSlotRequestTimeoutCheckDisabled;
    }
}
