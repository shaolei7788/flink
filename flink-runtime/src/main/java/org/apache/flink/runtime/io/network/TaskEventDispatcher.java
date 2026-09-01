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

package org.apache.flink.runtime.io.network;

import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.api.TaskEventHandler;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.util.event.EventListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The task event dispatcher dispatches events flowing backwards from a consuming task to the task
 * producing the consumed result.
 *
 * <p>Backwards events only work for tasks, which produce pipelined results, where both the
 * producing and consuming task are running at the same time.
 */
//1. 核心作用：建立“分区”与“处理器”的映射
//   键（ResultPartitionID）：唯一标识上游任务（Task）产生的某一个特定的结果分区（Result Partition）。
//   值（TaskEventHandler）：负责处理发送给该分区的事件的处理器。每个正在运行的、需要接收外部事件（如迭代流中的反馈事件、下游的反向控制信号等）的结果分区，
//   都会将其对应的处理器注册到这个 HashMap 中。
//2. 工作机制：反向事件路由在 Flink 中，数据通常是顺着拓扑结构从上游流向下游的。但在某些特殊场景（例如 迭代流 Iteration）中，下游的任务需要将某些控制事件（例如进度通知、迭代终止信号）反向发送给上游的任务。
//    1.注册：当上游任务初始化其 ResultPartition 时，会为该分区创建一个 TaskEventHandler，
//    并调用 TaskEventDispatcher.registerPartition() 将其存入 registeredHandlers。
//    2.触发：当下游任务通过网络传输（如通过 RemoteInputChannel）向上传递一个 TaskEvent 时，网络层会接收到这个事件。
//    3.路由：TaskEventDispatcher 会根据事件携带的 ResultPartitionID，在 registeredHandlers 中查找对应的 TaskEventHandler。
//    4.分发：一旦找到，就会将事件转交给该处理器，由处理器触发上游任务相应的逻辑。
//3. 生命周期管理这个 HashMap 不是静态不变的，它伴随着任务的生命周期动态变化：任务启动/分区创建：调用 registerPartition 写入 Map。
//   任务结束/分区销毁：调用 劈销/注销 方法（如 unregistratePartition）从 Map 中移除，防止内存泄漏。
public class TaskEventDispatcher implements TaskEventPublisher {
    private static final Logger LOG = LoggerFactory.getLogger(TaskEventDispatcher.class);

    private final Map<ResultPartitionID, TaskEventHandler> registeredHandlers = new HashMap<>();

    /**
     * Registers the given partition for incoming task events allowing calls to {@link
     * #subscribeToEvent(ResultPartitionID, EventListener, Class)}.
     *
     * @param partitionId the partition ID
     */
    public void registerPartition(ResultPartitionID partitionId) {
        checkNotNull(partitionId);

        synchronized (registeredHandlers) {
            LOG.debug("registering {}", partitionId);
            if (registeredHandlers.put(partitionId, new TaskEventHandler()) != null) {
                throw new IllegalStateException(
                        "Partition "
                                + partitionId
                                + " already registered at task event dispatcher.");
            }
        }
    }

    /**
     * Removes the given partition from listening to incoming task events, thus forbidding calls to
     * {@link #subscribeToEvent(ResultPartitionID, EventListener, Class)}.
     *
     * @param partitionId the partition ID
     */
    public void unregisterPartition(ResultPartitionID partitionId) {
        checkNotNull(partitionId);

        synchronized (registeredHandlers) {
            LOG.debug("unregistering {}", partitionId);
            // NOTE: tolerate un-registration of non-registered task (unregister is always called
            //       in the cleanup phase of a task even if it never came to the registration - see
            //       Task.java)
            registeredHandlers.remove(partitionId);
        }
    }

    /**
     * Subscribes a listener to this dispatcher for events on a partition.
     *
     * @param partitionId ID of the partition to subscribe for (must be registered via {@link
     *     #registerPartition(ResultPartitionID)} first!)
     * @param eventListener the event listener to subscribe
     * @param eventType event type to subscribe to
     */
    public void subscribeToEvent(
            ResultPartitionID partitionId,
            EventListener<TaskEvent> eventListener,
            Class<? extends TaskEvent> eventType) {
        checkNotNull(partitionId);
        checkNotNull(eventListener);
        checkNotNull(eventType);

        TaskEventHandler taskEventHandler;
        synchronized (registeredHandlers) {
            taskEventHandler = registeredHandlers.get(partitionId);
        }
        if (taskEventHandler == null) {
            throw new IllegalStateException(
                    "Partition " + partitionId + " not registered at task event dispatcher.");
        }
        taskEventHandler.subscribe(eventListener, eventType);
    }

    /**
     * Publishes the event to the registered {@link EventListener} instances.
     *
     * <p>This method is either called directly from a {@link LocalInputChannel} or the network I/O
     * thread on behalf of a {@link RemoteInputChannel}.
     *
     * @return whether the event was published to a registered event handler (initiated via {@link
     *     #registerPartition(ResultPartitionID)}) or not
     */
    @Override
    public boolean publish(ResultPartitionID partitionId, TaskEvent event) {
        checkNotNull(partitionId);
        checkNotNull(event);

        TaskEventHandler taskEventHandler;
        synchronized (registeredHandlers) {
            taskEventHandler = registeredHandlers.get(partitionId);
        }

        if (taskEventHandler != null) {
            taskEventHandler.publish(event);
            return true;
        }

        return false;
    }

    /** Removes all registered event handlers. */
    public void clearAll() {
        synchronized (registeredHandlers) {
            registeredHandlers.clear();
        }
    }
}
