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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

//[资源不足/算子排队]
//       │
//       ▼
// 1. 诞生 (Create)：SlotManager 在内存中凭空画大饼，创建一个 PendingTaskManager，
//                  将排队算子挂靠在其账上（预占）。
//       │
//       ▼
// [人工手动或脚本启动物理 TM 进程]
//       │
//       ▼
// 2. 匹配 (Match)：真实 TM 启动成功，调用 `registerTaskManager`。
//                  SlotManager 调用 `findMatchingPendingTaskManager` 精准抓出
//                  这个对应的 Pending 占位符。
//       │
//       ▼
// 3. 兑现与销毁 (Fulfill & Destroy)：
//                  真实 TM 继承该 Pending 对象身上的所有预占算子记录。
//                  随后，该 PendingTaskManager 完成历史使命，被从内存中彻底销毁。

//为什么需要？ 如果没有 新来的算子就会被直接拒绝
//
/** Represents a pending task manager in the {@link SlotManager}. */
public class PendingTaskManager {
    private final PendingTaskManagerId pendingTaskManagerId;
    private final ResourceProfile totalResourceProfile;
    private final ResourceProfile defaultSlotResourceProfile;
    private final int numSlots;

    private ResourceProfile unusedResource;
    Map<JobID, ResourceCounter> pendingSlotAllocationRecords;

    public PendingTaskManager(ResourceProfile totalResourceProfile, int numSlots) {
        this.numSlots = numSlots;
        this.totalResourceProfile = Preconditions.checkNotNull(totalResourceProfile);
        this.defaultSlotResourceProfile = SlotManagerUtils.generateDefaultSlotResourceProfile(totalResourceProfile, numSlots);
        this.pendingTaskManagerId = PendingTaskManagerId.generate();
        this.unusedResource = totalResourceProfile;
        this.pendingSlotAllocationRecords = new HashMap<>();
    }

    public ResourceProfile getTotalResourceProfile() {
        return totalResourceProfile;
    }

    public ResourceProfile getDefaultSlotResourceProfile() {
        return defaultSlotResourceProfile;
    }

    public PendingTaskManagerId getPendingTaskManagerId() {
        return pendingTaskManagerId;
    }

    public int getNumSlots() {
        return numSlots;
    }

    public ResourceProfile getUnusedResource() {
        return unusedResource;
    }

    public Map<JobID, ResourceCounter> getPendingSlotAllocationRecords() {
        return pendingSlotAllocationRecords;
    }

    public void clearAllPendingAllocations() {
        pendingSlotAllocationRecords.clear();
        unusedResource = totalResourceProfile;
    }

    public void replaceAllPendingAllocations(Map<JobID, ResourceCounter> pendingSlotAllocations) {
        pendingSlotAllocationRecords.clear();
        pendingSlotAllocationRecords.putAll(pendingSlotAllocations);
        unusedResource = calculateUnusedResourceProfile();
    }

    public void clearPendingAllocationsOfJob(JobID jobId) {
        Optional.ofNullable(pendingSlotAllocationRecords.remove(jobId))
                .ifPresent(
                        resourceCounter ->
                                unusedResource =
                                        unusedResource.merge(resourceCounter.getTotalResource()));
    }

    private ResourceProfile calculateUnusedResourceProfile() {
        return totalResourceProfile.subtract(
                pendingSlotAllocationRecords.values().stream()
                        .map(ResourceCounter::getTotalResource)
                        .reduce(ResourceProfile.ZERO, ResourceProfile::merge));
    }

    @Override
    public String toString() {
        return String.format(
                "PendingTaskManager{id=%s, totalResourceProfile=%s, defaultSlotResourceProfile=%s}",
                pendingTaskManagerId, totalResourceProfile, defaultSlotResourceProfile);
    }
}
