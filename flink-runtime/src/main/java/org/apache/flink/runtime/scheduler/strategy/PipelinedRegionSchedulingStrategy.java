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

package org.apache.flink.runtime.scheduler.strategy;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.SchedulerOperations;
import org.apache.flink.util.IterableUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link SchedulingStrategy} instance which schedules tasks in granularity of pipelined regions.
 */
//基于流水线区域的调度策略 是 Flink 调度引擎（Scheduler）的核心调度大脑之一
//决定在一个作业中，哪些 Task（任务）应该在什么时候被一起调度启动
//为了实现高效的资源利用和避免死锁，它采取的策略是：以 “Pipelined Region”（流水线区域）作为最小的原子单位进行整体调度
//
//Flink 根据 Task 之间的数据传输方式（数据流边 ResultPartitionType），将整个复杂的作业图切分成多个不同的“区域”：Pipelined 边（流式传输）：
// 数据通过网络内存直接管道化传输（如 FORWARD 或 REBALANCE）。上游生产数据的同时，下游必须立刻消费。
//    结果：由 Pipelined 边连接的上下游 Task 必须同时运行。它们被强绑定在同一个 Pipelined Region 中。Blocking 边（批式落盘）：上游产生的数据先全部写入磁盘，上游完全结束后，
// 下游才启动并从磁盘读取数据（常用于 Batch 批处理）。
//    结果：Blocking 边会切断连接。上下游 Task 属于不同的 Pipelined Region

//引入基于 Region 的调度策略带来了巨大的优势：
// 防止分布式死锁（Deadlock Prevention）如果按单个 Task 调度，假设资源不足，上游 Task 启动了，下游 Task 因为没资源启动而卡住。
//      由于是 Pipelined 传输，上游的内存缓冲区很快会被填满导致阻塞，下游又永远拿不到资源，从而引发永久死锁。
//      PipelinedRegionSchedulingStrategy 确保一个 Region 内的 Task 要么全部一起启动，要么一个都不启动，彻底避免了这种死锁。
// 统一流与批（Unified Streaming and Batch）它是 Flink 实现流批一体调度的关键。对于 Streaming 作业，
// 因为全是 Pipelined 边，整个作业就是一个巨大的 Region，该策略会一次性拉起所有 Task（效果等同于以前的 Eager 调度）；
// 对于 Batch 作业，它会根据  Blocking 边自动切分成多个 Region，实现类似 MapReduce 的拓扑阶段顺次调度（效果等同于 LazyFromSource）


public class PipelinedRegionSchedulingStrategy implements SchedulingStrategy {

    private final SchedulerOperations schedulerOperations;

    private final SchedulingTopology schedulingTopology;

    /** External consumer regions of each ConsumedPartitionGroup. */
    private final Map<ConsumedPartitionGroup, Set<SchedulingPipelinedRegion>> partitionGroupConsumerRegions = new IdentityHashMap<>();

    private final Map<SchedulingPipelinedRegion, List<ExecutionVertexID>> regionVerticesSorted = new IdentityHashMap<>();

    /** All produced partition groups of one schedulingPipelinedRegion. */
    private final Map<SchedulingPipelinedRegion, Set<ConsumedPartitionGroup>>
            producedPartitionGroupsOfRegion = new IdentityHashMap<>();

    /** The ConsumedPartitionGroups which are produced by multiple regions. */
    private final Set<ConsumedPartitionGroup> crossRegionConsumedPartitionGroups =
            Collections.newSetFromMap(new IdentityHashMap<>());

    private final Set<SchedulingPipelinedRegion> scheduledRegions = Collections.newSetFromMap(new IdentityHashMap<>());

    public PipelinedRegionSchedulingStrategy(
            final SchedulerOperations schedulerOperations,
            final SchedulingTopology schedulingTopology) {

        this.schedulerOperations = checkNotNull(schedulerOperations);
        this.schedulingTopology = checkNotNull(schedulingTopology);

        init();
    }

    private void init() {

        initCrossRegionConsumedPartitionGroups();

        initPartitionGroupConsumerRegions();

        initProducedPartitionGroupsOfRegion();

        for (SchedulingExecutionVertex vertex : schedulingTopology.getVertices()) {
            final SchedulingPipelinedRegion region = schedulingTopology.getPipelinedRegionOfVertex(vertex.getId());
            regionVerticesSorted
                    .computeIfAbsent(region, r -> new ArrayList<>())
                    .add(vertex.getId());
        }
    }

    private void initProducedPartitionGroupsOfRegion() {
        for (SchedulingPipelinedRegion region : schedulingTopology.getAllPipelinedRegions()) {
            Set<ConsumedPartitionGroup> producedPartitionGroupsSetOfRegion = new HashSet<>();
            for (SchedulingExecutionVertex executionVertex : region.getVertices()) {
                producedPartitionGroupsSetOfRegion.addAll(
                        IterableUtils.toStream(executionVertex.getProducedResults())
                                .flatMap(
                                        partition ->
                                                partition.getConsumedPartitionGroups().stream())
                                .collect(Collectors.toSet()));
            }
            producedPartitionGroupsOfRegion.put(region, producedPartitionGroupsSetOfRegion);
        }
    }

    private void initCrossRegionConsumedPartitionGroups() {
        final Map<ConsumedPartitionGroup, Set<SchedulingPipelinedRegion>>
                producerRegionsByConsumedPartitionGroup = new IdentityHashMap<>();

        for (SchedulingPipelinedRegion pipelinedRegion :
                schedulingTopology.getAllPipelinedRegions()) {
            for (ConsumedPartitionGroup consumedPartitionGroup :
                    pipelinedRegion.getAllNonPipelinedConsumedPartitionGroups()) {
                producerRegionsByConsumedPartitionGroup.computeIfAbsent(
                        consumedPartitionGroup, this::getProducerRegionsForConsumedPartitionGroup);
            }
        }

        for (SchedulingPipelinedRegion pipelinedRegion :
                schedulingTopology.getAllPipelinedRegions()) {
            for (ConsumedPartitionGroup consumedPartitionGroup :
                    pipelinedRegion.getAllNonPipelinedConsumedPartitionGroups()) {
                final Set<SchedulingPipelinedRegion> producerRegions =
                        producerRegionsByConsumedPartitionGroup.get(consumedPartitionGroup);
                if (producerRegions.size() > 1 && producerRegions.contains(pipelinedRegion)) {
                    crossRegionConsumedPartitionGroups.add(consumedPartitionGroup);
                }
            }
        }
    }

    private Set<SchedulingPipelinedRegion> getProducerRegionsForConsumedPartitionGroup(
            ConsumedPartitionGroup consumedPartitionGroup) {
        final Set<SchedulingPipelinedRegion> producerRegions =
                Collections.newSetFromMap(new IdentityHashMap<>());
        for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
            producerRegions.add(getProducerRegion(partitionId));
        }
        return producerRegions;
    }

    private SchedulingPipelinedRegion getProducerRegion(IntermediateResultPartitionID partitionId) {
        return schedulingTopology.getPipelinedRegionOfVertex(
                schedulingTopology.getResultPartition(partitionId).getProducer().getId());
    }

    private void initPartitionGroupConsumerRegions() {
        for (SchedulingPipelinedRegion region : schedulingTopology.getAllPipelinedRegions()) {
            for (ConsumedPartitionGroup consumedPartitionGroup :
                    region.getAllNonPipelinedConsumedPartitionGroups()) {
                if (crossRegionConsumedPartitionGroups.contains(consumedPartitionGroup)
                        || isExternalConsumedPartitionGroup(consumedPartitionGroup, region)) {
                    partitionGroupConsumerRegions
                            .computeIfAbsent(consumedPartitionGroup, group -> new HashSet<>())
                            .add(region);
                }
            }
        }
    }

    private Set<SchedulingPipelinedRegion> getBlockingDownstreamRegionsOfVertex(
            SchedulingExecutionVertex executionVertex) {
        return IterableUtils.toStream(executionVertex.getProducedResults())
                .filter(partition -> !partition.getResultType().canBePipelinedConsumed())
                .flatMap(partition -> partition.getConsumedPartitionGroups().stream())
                .filter(
                        group ->
                                crossRegionConsumedPartitionGroups.contains(group)
                                        || group.areAllPartitionsFinished())
                .flatMap(
                        partitionGroup ->
                                partitionGroupConsumerRegions
                                        .getOrDefault(partitionGroup, Collections.emptySet())
                                        .stream())
                .collect(Collectors.toSet());
    }

    //【前置知识】
    //对于一个标准的 WordCount 流作业（Streaming Job），
    // 在没有任何特殊配置的情况下，整个作业都属于同一个 Pipelined Region。
    // 这意味着，WordCount 作业中的所有 Task 必须同时被调度，一次性全部启动

    //根据 PipelinedRegionSchedulingStrategy 的规则：
    // 只要两个 Task 之间是通过 Pipelined 边连接，它们就必须被划分为同一个 Region。因此，环环相扣，整个 WordCount 从头到尾都在一个 Region 内部

    //触发初始调度
    //作业刚启动时，该策略会遍历整个执行图（ExecutionGraph），
    //找到所有没有任何上游依赖、或者上游依赖已经是 Blocking 且准备就绪的 Pipelined Region，并将这些 Region 内的所有 Task 一次性批量调度（Schedule）起来
    @Override
    public void startScheduling() {
        //DefaultSchedulingPipelinedRegion
        // 获取所有的 Pipelined Region  wordcount 为例 size = 1
        Iterable<? extends SchedulingPipelinedRegion> allPipelinedRegions = schedulingTopology.getAllPipelinedRegions();
        //过滤出“源头区域”（Source Regions）
        final Set<SchedulingPipelinedRegion> sourceRegions = IterableUtils.toStream(allPipelinedRegions)
                        .filter(this::isSourceRegion)
                        .collect(Collectors.toSet());
        maybeScheduleRegions(sourceRegions);//
    }

    private boolean isSourceRegion(SchedulingPipelinedRegion region) {
        for (ConsumedPartitionGroup consumedPartitionGroup : region.getAllNonPipelinedConsumedPartitionGroups()) {
            if (crossRegionConsumedPartitionGroups.contains(consumedPartitionGroup)
                    || isExternalConsumedPartitionGroup(consumedPartitionGroup, region)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void restartTasks(final Set<ExecutionVertexID> verticesToRestart) {
        final Set<SchedulingPipelinedRegion> regionsToRestart =
                verticesToRestart.stream()
                        .map(schedulingTopology::getPipelinedRegionOfVertex)
                        .collect(Collectors.toSet());
        scheduledRegions.removeAll(regionsToRestart);
        maybeScheduleRegions(regionsToRestart);
    }

    //当作业运行中某个 Task 的状态发生变化时，该策略会捕获这些事件
    //当下游可以启动时：当一个 Blocking 边的上游 Region 全部执行完毕，
    //且产生的中间结果数据（IntermediateResultPartition）全部处于可消费状态（Consumable）时，该策略会激活并调度下一个下游的 Pipelined Region
    @Override
    public void onExecutionStateChange(
            final ExecutionVertexID executionVertexId, final ExecutionState executionState) {
        if (executionState == ExecutionState.FINISHED) {
            maybeScheduleRegions(
                    getBlockingDownstreamRegionsOfVertex(
                            schedulingTopology.getVertex(executionVertexId)));
        }
    }


    @Override
    public void onPartitionConsumable(final IntermediateResultPartitionID resultPartitionId) {}

    // 入参是 DefaultSchedulingPipelinedRegion集合
    private void maybeScheduleRegions(final Set<SchedulingPipelinedRegion> regions) {//
         // size = 1
        final Set<SchedulingPipelinedRegion> regionsToSchedule = new HashSet<>();
        Set<SchedulingPipelinedRegion> nextRegions = regions;
        while (!nextRegions.isEmpty()) {
            // 返回是空
            nextRegions = addSchedulableAndGetNextRegions(nextRegions, regionsToSchedule);
        }
        // size = 1
        List<SchedulingPipelinedRegion> schedulingPipelinedRegions =
                SchedulingStrategyUtils.sortPipelinedRegionsInTopologicalOrder(
                schedulingTopology,
                regionsToSchedule);
        // schedule regions in topological order.
        // scheduleRegion 会调度作业
        schedulingPipelinedRegions.forEach(this::scheduleRegion);
    }

    private Set<SchedulingPipelinedRegion> addSchedulableAndGetNextRegions(
            Set<SchedulingPipelinedRegion> currentRegions,//当前正在检查的 Region 集合
            Set<SchedulingPipelinedRegion> regionsToSchedule) {//最终决定要调度的 Region 收集桶
        Set<SchedulingPipelinedRegion> nextRegions = new HashSet<>();
        // cache consumedPartitionGroup's consumable status to avoid compute repeatedly.
        final Map<ConsumedPartitionGroup, Boolean> consumableStatusCache = new HashMap<>();
        final Set<ConsumedPartitionGroup> visitedConsumedPartitionGroups = new HashSet<>();

        for (SchedulingPipelinedRegion currentRegion : currentRegions) {
            //判断该 Region 的所有上游输入数据源，是否都已经达到“可消费（Consumable）”的状态
            if (isRegionSchedulable(currentRegion, consumableStatusCache, regionsToSchedule)) {
                //加入集合
                regionsToSchedule.add(currentRegion);
                producedPartitionGroupsOfRegion
                        .getOrDefault(currentRegion, Collections.emptySet())
                        .forEach(
                                (producedPartitionGroup) -> {
                                    if (!producedPartitionGroup
                                            .getResultPartitionType()
                                            .canBePipelinedConsumed()) {//
                                        return;
                                    }
                                    // If this group has been visited, there is no need
                                    // to repeat the determination.
                                    if (visitedConsumedPartitionGroups.contains(producedPartitionGroup)) {
                                        return;
                                    }
                                    visitedConsumedPartitionGroups.add(producedPartitionGroup);
                                    //
                                    nextRegions.addAll(
                                            partitionGroupConsumerRegions.getOrDefault(
                                                    producedPartitionGroup,
                                                    Collections.emptySet()));
                                });
            }
        }
        //下一轮需要检查的下游 Region 集合
        return nextRegions;
    }

    //判断该 Region 的所有上游输入数据源，是否都已经达到“可消费（Consumable）”的状态
    private boolean isRegionSchedulable(
            final SchedulingPipelinedRegion region,
            final Map<ConsumedPartitionGroup, Boolean> consumableStatusCache,
            final Set<SchedulingPipelinedRegion> regionToSchedule) {
        return !regionToSchedule.contains(region)
                && !scheduledRegions.contains(region)
                && areRegionInputsAllConsumable(region, consumableStatusCache, regionToSchedule);
    }

    private void scheduleRegion(final SchedulingPipelinedRegion region) {
        checkState(
                areRegionVerticesAllInCreatedState(region),
                "BUG: trying to schedule a region which is not in CREATED state");
        scheduledRegions.add(region);
        //DefaultScheduler#allocateSlotsAndDeploy
        schedulerOperations.allocateSlotsAndDeploy(regionVerticesSorted.get(region));
    }

    private boolean areRegionInputsAllConsumable(
            final SchedulingPipelinedRegion region,
            final Map<ConsumedPartitionGroup, Boolean> consumableStatusCache,
            final Set<SchedulingPipelinedRegion> regionToSchedule) {
        for (ConsumedPartitionGroup consumedPartitionGroup : region.getAllNonPipelinedConsumedPartitionGroups()) {
            if (crossRegionConsumedPartitionGroups.contains(consumedPartitionGroup)) {
                if (!isDownstreamOfCrossRegionConsumedPartitionSchedulable(
                        consumedPartitionGroup, region, regionToSchedule)) {
                    return false;
                }
            } else if (isExternalConsumedPartitionGroup(consumedPartitionGroup, region)) {
                if (!consumableStatusCache.computeIfAbsent(
                        consumedPartitionGroup,
                        (group) ->
                                isDownstreamConsumedPartitionGroupSchedulable(
                                        group, regionToSchedule))) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean isDownstreamConsumedPartitionGroupSchedulable(
            final ConsumedPartitionGroup consumedPartitionGroup,
            final Set<SchedulingPipelinedRegion> regionToSchedule) {
        if (consumedPartitionGroup.getResultPartitionType().canBePipelinedConsumed()) {
            for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
                SchedulingPipelinedRegion producerRegion = getProducerRegion(partitionId);
                if (!scheduledRegions.contains(producerRegion)
                        && !regionToSchedule.contains(producerRegion)) {
                    return false;
                }
            }
        } else {
            for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
                if (schedulingTopology.getResultPartition(partitionId).getState()
                        != ResultPartitionState.ALL_DATA_PRODUCED) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean isDownstreamOfCrossRegionConsumedPartitionSchedulable(
            final ConsumedPartitionGroup consumedPartitionGroup,
            final SchedulingPipelinedRegion pipelinedRegion,
            final Set<SchedulingPipelinedRegion> regionToSchedule) {
        if (consumedPartitionGroup.getResultPartitionType().canBePipelinedConsumed()) {
            for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
                if (isExternalConsumedPartition(partitionId, pipelinedRegion)) {
                    SchedulingPipelinedRegion producerRegion = getProducerRegion(partitionId);
                    if (!regionToSchedule.contains(producerRegion)
                            && !scheduledRegions.contains(producerRegion)) {
                        return false;
                    }
                }
            }
        } else {
            for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
                if (isExternalConsumedPartition(partitionId, pipelinedRegion)
                        && schedulingTopology.getResultPartition(partitionId).getState()
                                != ResultPartitionState.ALL_DATA_PRODUCED) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean areRegionVerticesAllInCreatedState(final SchedulingPipelinedRegion region) {
        for (SchedulingExecutionVertex vertex : region.getVertices()) {
            if (vertex.getState() != ExecutionState.CREATED) {
                return false;
            }
        }
        return true;
    }

    private boolean isExternalConsumedPartitionGroup(
            ConsumedPartitionGroup consumedPartitionGroup,
            SchedulingPipelinedRegion pipelinedRegion) {

        return isExternalConsumedPartition(consumedPartitionGroup.getFirst(), pipelinedRegion);
    }

    private boolean isExternalConsumedPartition(
            IntermediateResultPartitionID partitionId, SchedulingPipelinedRegion pipelinedRegion) {
        return !pipelinedRegion.contains(
                schedulingTopology.getResultPartition(partitionId).getProducer().getId());
    }

    @VisibleForTesting
    Set<ConsumedPartitionGroup> getCrossRegionConsumedPartitionGroups() {
        return Collections.unmodifiableSet(crossRegionConsumedPartitionGroups);
    }

    /** The factory for creating {@link PipelinedRegionSchedulingStrategy}. */
    public static class Factory implements SchedulingStrategyFactory {
        @Override
        public SchedulingStrategy createInstance(
                final SchedulerOperations schedulerOperations,
                final SchedulingTopology schedulingTopology) {
            return new PipelinedRegionSchedulingStrategy(schedulerOperations, schedulingTopology);//
        }
    }
}
