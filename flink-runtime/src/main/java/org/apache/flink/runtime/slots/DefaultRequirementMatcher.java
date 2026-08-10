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

package org.apache.flink.runtime.slots;

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.util.ResourceCounter;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Default implementation of {@link RequirementMatcher}. This matcher finds the first requirement
 * that a) is not unfulfilled and B) matches the resource profile.
 */
public class DefaultRequirementMatcher implements RequirementMatcher {

    //
    @Override
    public Optional<ResourceProfile> match(
            ResourceProfile resourceProfile,// 当前新到来的【物理 Slot 资源规格】
            ResourceCounter totalRequirements,// 整个作业【声明的总资源需求清单】（规格 -> 所需数量）
            Function<ResourceProfile, Integer> numAssignedResourcesLookup) {// 函数式接口：查询【某规格当前已分配的 Slot 数量】
        // Short-cut for fine-grained resource management. If there is already exactly equal
        // requirement, we can directly match with it.
        // 如果新来的物理资源规格，正好在需求列表中，且该需求还有缺口    精准匹配
        if (totalRequirements.getResourceCount(resourceProfile) > numAssignedResourcesLookup.apply(resourceProfile)) {
            //false
            return Optional.of(resourceProfile);
        }

        //totalRequirements.getResourceCount(resourceProfile)：作业总共需要该规格的个数
        for (Map.Entry<ResourceProfile, Integer> requirementCandidate : totalRequirements.getResourcesWithCount()) {
            ResourceProfile requirementProfile = requirementCandidate.getKey();

            // beware the order when matching resources to requirements, because
            // ResourceProfile.UNKNOWN (which only
            // occurs as a requirement) does not match any resource!
            // 1. 物理资源必须满足或大于需求
            // isMatching 这是 Flink 资源算力的包含性校验。如果物理 Slot 是 4核8G，需求是 2核4G，那么 4核8G.isMatching(2核4G) 就会返回 true（大规格的物理资源可以兼容小规格的任务需求）
            if (resourceProfile.isMatching(requirementProfile)
                    // 2. 该需求得有缺口
                    // requirementCandidate.getValue() = 2   整个作业运行，一共需要该规格的 Slot 数量
                    // numAssignedResourcesLookup.apply(resourceProfile) = 到目前为止，已经分配了多少个满足该规格的物理  第一次进来 = 0 第二次进来就为1
                    //判断当前遍历到的这个“任务资源需求”，是否还有尚未被满足的缺口
                    // 如果还有缺口，代码才会考虑把当前新来的物理 Slot 分配给它；如果该需求已经满了，就直接跳过
                    && requirementCandidate.getValue() > numAssignedResourcesLookup.apply(requirementProfile)) {
                // 如果“总需求 > 已分配”，说明有缺口！直接判定匹配成功，把这个物理 resourceProfile 作为需求规格返回
                return Optional.of(requirementProfile);
            }
        }
        return Optional.empty();
    }
}
