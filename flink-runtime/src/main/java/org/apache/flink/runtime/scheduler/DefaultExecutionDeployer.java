/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Default implementation of {@link ExecutionDeployer}. */
public class DefaultExecutionDeployer implements ExecutionDeployer {

    private final Logger log;

    //核心作用：负责为待部署的 Task 申请和分配物理/逻辑槽位（Slots）。具体应用：
    // 在部署开始前，它负责决定哪个 Task 运行在哪个 TaskManager 的哪个 Slot 上。
    // deploymentHandle.getLogicalSlotFuture() 所对应的底层异步操作，就是由这个分配器触发并驱动的
    private final ExecutionSlotAllocator executionSlotAllocator;

    //核心作用：负责执行 Task 状态机变更的具体动作（操作底层 Execution），它是 Flink 调度层与底层 Task 状态的具体执行桥梁。具体应用：它通常包含诸如 deploy(Execution)、cancel(Execution) 等具体方法。
    // 当资源到位、分区注册完成后，真正向远端 TaskManager 发送 RPC 请求并提交任务（也就是拉起 Task 进程）的动作，会交由这个组件来封装和执行
    private final ExecutionOperations executionOperations;

    //核心作用：负责防止并发冲突和并发局部重试造成的脏写（乐观锁机制）。具体应用：分布式环境下，一个 Task 可能会因为失败被多次重启。
    // executionVertexVersioner 会为每个 ExecutionVertex（执行顶点）维护一个自增的版本号（Version）。
    // 在异步部署完成、准备更新状态时，会校验版本号。如果发现版本号不匹配（说明此期间该任务已经被其他触发器取消或重试了），则直接放弃当前过期的部署操作，避免旧的异步回调覆盖了新的状态
    private final ExecutionVertexVersioner executionVertexVersioner;

    //定义了向 ShuffleMaster 或 JobMaster 注册输出分区（Produced Partitions）的最大等待时间
    private final Duration partitionRegistrationTimeout;

    //核心作用：一个双向消费句柄（BiConsumer），用于在资源分配成功时，向外部/上游同步绑定关系。
    // 具体应用：它的输入参数是 ExecutionVertexID（任务顶点ID）和 AllocationID（物理资源分配ID）。当 executionSlotAllocator 成功为某个任务锁定了一个资源后，
    // 会回调这个函数，通知作业图（JobGraph）或者调度器：“这个任务已经正式预留并绑定到了这个具体的物理槽位上”，用于更新全局的拓扑资源映射表
    private final BiConsumer<ExecutionVertexID, AllocationID> allocationReservationFunc;

    //这是 Flink 架构中最重要的线程安全守护者。它确保所有状态修改操作都串行化在 JobManager 的主线程（Actor 线程）中执行，彻底消除排他锁（Lock）
    private final ComponentMainThreadExecutor mainThreadExecutor;

    private DefaultExecutionDeployer(
            final Logger log,
            final ExecutionSlotAllocator executionSlotAllocator,
            final ExecutionOperations executionOperations,
            final ExecutionVertexVersioner executionVertexVersioner,
            final Duration partitionRegistrationTimeout,
            final BiConsumer<ExecutionVertexID, AllocationID> allocationReservationFunc,
            final ComponentMainThreadExecutor mainThreadExecutor) {

        this.log = checkNotNull(log);
        this.executionSlotAllocator = checkNotNull(executionSlotAllocator);
        this.executionOperations = checkNotNull(executionOperations);
        this.executionVertexVersioner = checkNotNull(executionVertexVersioner);
        this.partitionRegistrationTimeout = checkNotNull(partitionRegistrationTimeout);
        this.allocationReservationFunc = checkNotNull(allocationReservationFunc);
        this.mainThreadExecutor = checkNotNull(mainThreadExecutor);
    }

    //分配slot 并部署
    @Override
    public void allocateSlotsAndDeploy(
            final List<Execution> executionsToDeploy,
            final Map<ExecutionVertexID, ExecutionVertexVersion> requiredVersionByVertex) {
        validateExecutionStates(executionsToDeploy);

        transitionToScheduled(executionsToDeploy);

        // 分配slot 如果共享slot没有就 申请新的slot
        final Map<ExecutionAttemptID, ExecutionSlotAssignment> executionSlotAssignmentMap = allocateSlotsFor(executionsToDeploy);

        final List<ExecutionDeploymentHandle> deploymentHandles =
                //
                createDeploymentHandles(executionsToDeploy, requiredVersionByVertex, executionSlotAssignmentMap);
        //
        waitForAllSlotsAndDeploy(deploymentHandles);
    }

    private void validateExecutionStates(final Collection<Execution> executionsToDeploy) {
        executionsToDeploy.forEach(
                e ->
                        checkState(
                                e.getState() == ExecutionState.CREATED,
                                "Expected execution %s to be in CREATED state, was: %s",
                                e.getAttemptId(),
                                e.getState()));
    }

    private void transitionToScheduled(final List<Execution> executionsToDeploy) {
        executionsToDeploy.forEach(e -> e.transitionState(ExecutionState.SCHEDULED));
    }

    private Map<ExecutionAttemptID, ExecutionSlotAssignment> allocateSlotsFor(final List<Execution> executionsToDeploy) {
        final List<ExecutionAttemptID> executionAttemptIds =
                executionsToDeploy.stream()
                        .map(Execution::getAttemptId)
                        .collect(Collectors.toList());
        //批量申请slot
        return executionSlotAllocator.allocateSlotsFor(executionAttemptIds);
    }

    private List<ExecutionDeploymentHandle> createDeploymentHandles(
            final List<Execution> executionsToDeploy,
            final Map<ExecutionVertexID, ExecutionVertexVersion> requiredVersionByVertex,
            final Map<ExecutionAttemptID, ExecutionSlotAssignment> executionSlotAssignmentMap) {
        checkState(executionsToDeploy.size() == executionSlotAssignmentMap.size());
        final List<ExecutionDeploymentHandle> deploymentHandles = new ArrayList<>(executionsToDeploy.size());
        for (final Execution execution : executionsToDeploy) {
            final ExecutionSlotAssignment assignment = checkNotNull(executionSlotAssignmentMap.get(execution.getAttemptId()));

            final ExecutionVertexID executionVertexId = execution.getVertex().getID();
            //
            final ExecutionDeploymentHandle deploymentHandle =
                    new ExecutionDeploymentHandle(execution, assignment, requiredVersionByVertex.get(executionVertexId));//
            deploymentHandles.add(deploymentHandle);
        }

        return deploymentHandles;
    }

    private void waitForAllSlotsAndDeploy(final List<ExecutionDeploymentHandle> deploymentHandles) {
        FutureUtils.assertNoException(
                //1 先执行 assignAllResourcesAndRegisterProducedPartitions
                assignAllResourcesAndRegisterProducedPartitions(deploymentHandles)
                        // 2 再执行 deployAll 即所有的 Slot 资源全部成功批下来了、分区注册成功了
                        .handle(deployAll(deploymentHandles)));
    }

    private CompletableFuture<Void> assignAllResourcesAndRegisterProducedPartitions(
            final List<ExecutionDeploymentHandle> deploymentHandles) {
        final List<CompletableFuture<Void>> resultFutures = new ArrayList<>();
        // deploymentHandles size = 5
        for (ExecutionDeploymentHandle deploymentHandle : deploymentHandles) {
            final CompletableFuture<Void> resultFuture =
                    deploymentHandle
                            //获取槽位
                            .getLogicalSlotFuture()
                            //分配资源  无论上一步成功还是失败，handle 都会执行。这确保了如果槽位申请失败，assignResource 内部可以进行相应的状态清理或错误转换
                            .handle(assignResource(deploymentHandle))
                            //注册分区 分配好资源后，需要将该任务将要产生的数据分区（Produced Partitions）注册到集群中，以便下游任务消费
                            .thenCompose(registerProducedPartitions(deploymentHandle))
                            .handle(
                                    (ignore, throwable) -> {
                                        if (throwable != null) {
                                            handleTaskDeploymentFailure(
                                                    deploymentHandle.getExecution(), throwable);
                                        }
                                        return null;
                                    });

            resultFutures.add(resultFuture);
        }
        //它会耐心地等待所有 5 个任务全部走完各自的流程（无论成功或失败），才宣告总任务结束
        return FutureUtils.waitForAll(resultFutures);
    }

    private BiFunction<Void, Throwable, Void> deployAll(
            final List<ExecutionDeploymentHandle> deploymentHandles) {
        return (ignored, throwable) -> {
            propagateIfNonNull(throwable);
            for (final ExecutionDeploymentHandle deploymentHandle : deploymentHandles) {
                final CompletableFuture<LogicalSlot> slotAssigned = deploymentHandle.getLogicalSlotFuture();
                checkState(slotAssigned.isDone());

                FutureUtils.assertNoException(
                        //
                        slotAssigned.handle(deployOrHandleError(deploymentHandle)));
            }
            return null;
        };
    }

    private static void propagateIfNonNull(final Throwable throwable) {
        if (throwable != null) {
            throw new CompletionException(throwable);
        }
    }

    //
    private BiFunction<LogicalSlot, Throwable, LogicalSlot> assignResource(
            final ExecutionDeploymentHandle deploymentHandle) {

        return (logicalSlot, throwable) -> {

            final ExecutionVertexVersion requiredVertexVersion = deploymentHandle.getRequiredVertexVersion();
            final Execution execution = deploymentHandle.getExecution();

            if (execution.getState() != ExecutionState.SCHEDULED || executionVertexVersioner.isModified(requiredVertexVersion)) {
                if (throwable == null) {
                    log.debug(
                            "Refusing to assign slot to execution {} because this deployment was "
                                    + "superseded by another deployment",
                            deploymentHandle.getExecutionAttemptId());
                    releaseSlotIfPresent(logicalSlot);
                }
                return null;
            }

            // throw exception only if the execution version is not outdated.
            // this ensures that canceling a pending slot request does not fail
            // a task which is about to cancel.
            if (throwable != null) {
                throw new CompletionException(maybeWrapWithNoResourceAvailableException(throwable));
            }

            if (!execution.tryAssignResource(logicalSlot)) {
                throw new IllegalStateException(
                        "Could not assign resource "
                                + logicalSlot
                                + " to execution "
                                + execution
                                + '.');
            }

            // We only reserve the latest execution of an execution vertex. Because it may cause
            // problems to reserve multiple slots for one execution vertex. Besides that, slot
            // reservation is for local recovery and therefore is only needed by streaming jobs, in
            // which case an execution vertex will have one only current execution.
            allocationReservationFunc.accept(execution.getAttemptId().getExecutionVertexId(), logicalSlot.getAllocationId());

            return logicalSlot;
        };
    }

    private static void releaseSlotIfPresent(@Nullable final LogicalSlot logicalSlot) {
        if (logicalSlot != null) {
            logicalSlot.releaseSlot(null);
        }
    }

    private static Throwable maybeWrapWithNoResourceAvailableException(final Throwable failure) {
        final Throwable strippedThrowable = ExceptionUtils.stripCompletionException(failure);
        if (strippedThrowable instanceof TimeoutException) {
            return new NoResourceAvailableException(
                    "Could not allocate the required slot within slot request timeout. "
                            + "Please make sure that the cluster has enough resources.",
                    failure);
        } else {
            return failure;
        }
    }

    private Function<LogicalSlot, CompletableFuture<Void>> registerProducedPartitions(
            final ExecutionDeploymentHandle deploymentHandle) {

        return logicalSlot -> {
            // a null logicalSlot means the slot assignment is skipped, in which case
            // the produced partition registration process can be skipped as well
            if (logicalSlot != null) {
                final Execution execution = deploymentHandle.getExecution();
                final CompletableFuture<Void> partitionRegistrationFuture =
                        execution.registerProducedPartitions(logicalSlot.getTaskManagerLocation());

                return FutureUtils.orTimeout(
                        partitionRegistrationFuture,
                        partitionRegistrationTimeout.toMillis(),
                        TimeUnit.MILLISECONDS,
                        mainThreadExecutor,
                        String.format(
                                "Registering produced partitions for execution %s timed out after %d ms.",
                                execution.getAttemptId(), partitionRegistrationTimeout.toMillis()));
            } else {
                return FutureUtils.completedVoidFuture();
            }
        };
    }

    private BiFunction<Object, Throwable, Void> deployOrHandleError(//
            final ExecutionDeploymentHandle deploymentHandle) {

        return (ignored, throwable) -> {
            //
            final ExecutionVertexVersion requiredVertexVersion = deploymentHandle.getRequiredVertexVersion();
            final Execution execution = deploymentHandle.getExecution();

            if (execution.getState() != ExecutionState.SCHEDULED || executionVertexVersioner.isModified(requiredVertexVersion)) {
                if (throwable == null) {
                    log.debug(
                            "Refusing to assign slot to execution {} because this deployment was "
                                    + "superseded by another deployment",
                            deploymentHandle.getExecutionAttemptId());
                }
                return null;
            }

            if (throwable == null) {
                //
                deployTaskSafe(execution);//
            } else {
                handleTaskDeploymentFailure(execution, throwable);
            }
            return null;
        };
    }

    private void deployTaskSafe(final Execution execution) {
        try {
            //DefaultExecutionOperations#deploy
            executionOperations.deploy(execution);
        } catch (Throwable e) {
            handleTaskDeploymentFailure(execution, e);
        }
    }

    private void handleTaskDeploymentFailure(final Execution execution, final Throwable error) {
        executionOperations.markFailed(execution, error);
    }

    private static class ExecutionDeploymentHandle {

        //代表什么：当前正在尝试部署的任务执行实例（Task Attempt）。它的职责：它是 Flink 调度状态机的核心。
        //它记录了当前任务的生命周期状态（如 SCHEDULED、DEPLOYING、RUNNING 等），并持有该任务的配置（TaskDeploymentDescriptor）、拓扑关系以及重试次数
        //状态推进：当资源分配成功、分区注册完成后，需要调用 execution.deploy() 真正向 TaskManager 发起 RPC 部署。
        // 错误追踪：如果流水线在中途任何一步失败了（如槽位申请超时），
        // 会把这个 execution 对象传给异常处理器（如 handleTaskDeploymentFailure(execution, throwable)），将其状态变更为 FAILED 并触发容错恢复
        private final Execution execution;
        //代表什么：为当前任务量身定制的槽位分配凭证。它的职责：它是连接调度层与资源层的纽带。它内部的核心就是包裹了一个 CompletableFuture<LogicalSlot>（逻辑槽位 Future）。
        // 在句柄中的作用：异步驱动源：它是整个部署流水线的起点。上一轮代码中 deploymentHandle.getLogicalSlotFuture() 实际上就是去调用了 executionSlotAssignment.getLogicalSlotFuture()。
        // 物理节点寻址：当这个 Future 完成后，从中可以获取到真实的 LogicalSlot。
        // 通过 LogicalSlot，任务才能知道自己被分配到了哪台机器（TaskManagerLocation）、哪个槽位（SlotID），以及该如何建立网络连接
        private final ExecutionSlotAssignment executionSlotAssignment;

        //发起本次部署请求时，任务顶点（ExecutionVertex）的版本快照（乐观锁）。
        // 职责：防止异步多线程带来的并发状态脏写
        private final ExecutionVertexVersion requiredVertexVersion;

        ExecutionDeploymentHandle(
                final Execution execution,
                final ExecutionSlotAssignment executionSlotAssignment,
                final ExecutionVertexVersion requiredVertexVersion) {
            this.execution = checkNotNull(execution);
            this.executionSlotAssignment = checkNotNull(executionSlotAssignment);
            this.requiredVertexVersion = checkNotNull(requiredVertexVersion);
        }

        Execution getExecution() {
            return execution;
        }

        ExecutionAttemptID getExecutionAttemptId() {
            return execution.getAttemptId();
        }

        CompletableFuture<LogicalSlot> getLogicalSlotFuture() {
            //
            return executionSlotAssignment.getLogicalSlotFuture();
        }

        ExecutionVertexVersion getRequiredVertexVersion() {
            return requiredVertexVersion;
        }
    }

    /** Factory to instantiate the {@link DefaultExecutionDeployer}. */
    public static class Factory implements ExecutionDeployer.Factory {

        @Override
        public DefaultExecutionDeployer createInstance(
                Logger log,
                ExecutionSlotAllocator executionSlotAllocator,
                ExecutionOperations executionOperations,
                ExecutionVertexVersioner executionVertexVersioner,
                Duration partitionRegistrationTimeout,
                BiConsumer<ExecutionVertexID, AllocationID> allocationReservationFunc,
                ComponentMainThreadExecutor mainThreadExecutor) {
            return new DefaultExecutionDeployer(
                    log,
                    executionSlotAllocator,
                    executionOperations,
                    executionVertexVersioner,
                    partitionRegistrationTimeout,
                    allocationReservationFunc,
                    mainThreadExecutor);
        }
    }
}
