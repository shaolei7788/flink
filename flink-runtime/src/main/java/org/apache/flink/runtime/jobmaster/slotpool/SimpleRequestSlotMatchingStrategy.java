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

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.scheduler.loading.LoadingWeight;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

/**
 * Simple implementation of the {@link RequestSlotMatchingStrategy} that matches the pending
 * requests in order as long as the resource profile can be fulfilled.
 */
public enum SimpleRequestSlotMatchingStrategy implements RequestSlotMatchingStrategy {
    INSTANCE;

    //为当前等待分配资源的请求（Pending Requests）与可用的物理槽位（Physical Slots）进行基础的资源规格匹配，并建立绑定关系
    @Override
    public Collection<RequestSlotMatch> matchRequestsAndSlots(
            Collection<? extends PhysicalSlot> slots,
            Collection<PendingRequest> pendingRequests,
            Map<ResourceID, LoadingWeight> taskExecutorsLoadingWeight) {
        final Collection<RequestSlotMatch> resultingMatches = new ArrayList<>();

        // if pendingRequests has a special order, then let's preserve it
        final LinkedList<PendingRequest> pendingRequestsIndex = new LinkedList<>(pendingRequests);

        for (PhysicalSlot slot : slots) {
            final Iterator<PendingRequest> pendingRequestIterator = pendingRequestsIndex.iterator();

            while (pendingRequestIterator.hasNext()) {
                //
                final PendingRequest pendingRequest = pendingRequestIterator.next();
                //检查当前 Slot 的资源规格（如 CPU、内存等）是否大于或等于请求所要求的资源规格
                if (slot.getResourceProfile().isMatching(pendingRequest.getResourceProfile())) {
                    // 资源匹配
                    RequestSlotMatch slotMatch = RequestSlotMatch.createFor(pendingRequest, slot);
                    resultingMatches.add(slotMatch);
                    //从等待队列中移除该请求
                    pendingRequestIterator.remove();
                    break;
                }
            }
        }

        return resultingMatches;
    }

    @Override
    public String toString() {
        return SimpleRequestSlotMatchingStrategy.class.getSimpleName();
    }
}
