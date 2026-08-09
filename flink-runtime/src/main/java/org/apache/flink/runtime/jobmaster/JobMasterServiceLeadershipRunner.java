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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.dispatcher.JobCancellationFailedException;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.jobmaster.factories.JobMasterServiceProcessFactory;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElection;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;

/**
 * Leadership runner for the {@link JobMasterServiceProcess}.
 *
 * <p>The responsibility of this component is to manage the leadership of the {@link
 * JobMasterServiceProcess}. This means that the runner will create an instance of the process when
 * it obtains the leadership. The process is stopped once the leadership is revoked.
 *
 * <p>This component only accepts signals (job result completion, initialization failure) as long as
 * it is running and as long as the signals are coming from the current leader process. This ensures
 * that only the current leader can affect this component.
 *
 * <p>All leadership operations are serialized. This means that granting the leadership has to
 * complete before the leadership can be revoked and vice versa.
 *
 * <p>The {@link #resultFuture} can be completed with the following values: * *
 *
 * <ul>
 *   <li>{@link JobManagerRunnerResult} to signal an initialization failure of the {@link
 *       JobMasterService} or the completion of a job
 *   <li>{@link Exception} to signal an unexpected failure
 * </ul>
 */
//当一个作业被提交到 Flink 集群时，Flink 并不会直接启动 JobMaster，而是先启动一个 JobMasterServiceLeadershipRunner。
// 它负责去参与高可用（HA）组件（如 ZooKeeper 或 Kubernetes Lease）的 Leader 选举。
// 只有当它成功抢到 Leader 之后，它才会真正去拉起和初始化底层的 JobMaster 进程
public class JobMasterServiceLeadershipRunner implements JobManagerRunner, LeaderContender {

    private static final Logger LOG =
            LoggerFactory.getLogger(JobMasterServiceLeadershipRunner.class);

    private final Object lock = new Object();

    //JobMaster 服务进程的创建工厂。用于在抢到 Leader 身份后，动态实例化具体的 JobMasterServiceProcess
    private final JobMasterServiceProcessFactory jobMasterServiceProcessFactory;
    //高可用 leader选举
    private final LeaderElection leaderElection;

    //作业运行结果存储器（Flink 近期版本引入的持久化组件）。
    // 当作业执行完成（成功/失败/取消）后，其结果会被写到这里。这是为了防止 JobMaster 挂掉后，新选举出来的 Leader 重复执行已经结束的作业
    private final JobResultStore jobResultStore;

    //类加载器租约。用于管理该作业专属的类加载器（ClassLoader）。只要这个租约没有释放，作业依赖的 JAR 包和类就不会被 JVM 垃圾回收，确保了作业在生命周期内类加载的稳定性
    private final LibraryCacheManager.ClassLoaderLease classLoaderLease;

    //致命错误处理器。当内部发生无法通过重试恢复的系统级灾难（例如：内存溢出、磁盘损坏、无法连接 HA 存储）时，会触发它，通常会导致当前 Flink 进程退出，防止“僵尸节点”影响集群
    private final FatalErrorHandler fatalErrorHandler;

    //终结状态 Future。代表当前整个 JobMasterServiceLeadershipRunner 组件是否已经彻底关闭、资源是否完全释放。上层组件（如 Dispatcher）会监听它来决定何时安全地将该作业从内存管理队列中彻底移除
    private final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();

    //作业最终执行结果的载体。不管作业经历了多少次 Leader 切换和重试，
    // 一旦它最终在某个 Leader 手里成功跑完或者被用户彻底取消，这个 Future 就会被填入结果，通知 Dispatcher 作业已功德圆满
    private final CompletableFuture<JobManagerRunnerResult> resultFuture = new CompletableFuture<>();

    //由内部锁（lock）保护的状态机变量。定义了 Runner 当前所处的阶段（例如：RUNNING 正在运行、SUSPENDED 挂起、STOPPED 停止）。
    // 所有的关键状态切换必须先获取锁，防止在接收到高可用回调的同时用户触发了取消操作导致状态错乱
    @GuardedBy("lock")
    private State state = State.RUNNING;

    //在分布式环境下，底层 HA 可能会在极短时间内连续发送 grantLeadership（授权）和 revokeLeadership（剥夺）。
    // 如果并发处理，可能会导致“先授权的还在初始化，后剥夺的已经执行完了”的严重脑裂问题。
    // Flink 通过将所有 Leadership 相关的操作（如启动、停止）全部 thenCompose 链式挂载到 sequentialOperation 上，迫使所有的领导权变更操作严格按照接收到的先后顺序串行执行
    @GuardedBy("lock")
    private CompletableFuture<Void> sequentialOperation = FutureUtils.completedVoidFuture();

    //真正承载作业调度的底层进程包装器
    @GuardedBy("lock")
    private JobMasterServiceProcess jobMasterServiceProcess = JobMasterServiceProcess.waitingForLeadership();

    //当新的 JobMaster 启动成功并向集群注册好它的 RPC 终点后，
    // 这个 Future 就会被 Complete。外部的 Web UI、REST API 或者 CLI 就可以通过它拿到 Gateway，进而发送诸如“查询作业当前状态”、“触发 Savepoint” 等指令
    @GuardedBy("lock")
    private CompletableFuture<JobMasterGateway> jobMasterGatewayFuture = new CompletableFuture<>();

    //当用户发起取消作业请求，而此时底层正在发生 Leader 切换或者当前的 JobMaster 正在关闭时，
    // 这个标记位可以作为一个全局信号，告诉后续的代码：“不要再尝试去恢复或者重启作业了，因为用户已经明确要求终止它”
    @GuardedBy("lock")
    private boolean hasCurrentLeaderBeenCancelled = false;

    public JobMasterServiceLeadershipRunner(
            JobMasterServiceProcessFactory jobMasterServiceProcessFactory,
            LeaderElection leaderElection,
            JobResultStore jobResultStore,
            LibraryCacheManager.ClassLoaderLease classLoaderLease,
            FatalErrorHandler fatalErrorHandler) {
        this.jobMasterServiceProcessFactory = jobMasterServiceProcessFactory;
        this.leaderElection = leaderElection;
        this.jobResultStore = jobResultStore;
        this.classLoaderLease = classLoaderLease;
        this.fatalErrorHandler = fatalErrorHandler;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        final CompletableFuture<Void> processTerminationFuture;
        synchronized (lock) {
            if (state == State.STOPPED) {
                return terminationFuture;
            }

            state = State.STOPPED;

            LOG.debug("Terminating the leadership runner for job {}.", getJobID());

            jobMasterGatewayFuture.completeExceptionally(
                    new FlinkException(
                            "JobMasterServiceLeadershipRunner is closed. Therefore, the corresponding JobMaster will never acquire the leadership."));

            resultFuture.complete(
                    JobManagerRunnerResult.forSuccess(
                            createExecutionGraphInfoWithJobStatus(JobStatus.SUSPENDED)));

            processTerminationFuture = jobMasterServiceProcess.closeAsync();
        }

        final CompletableFuture<Void> serviceTerminationFuture =
                FutureUtils.runAfterwards(
                        processTerminationFuture,
                        () -> {
                            classLoaderLease.release();
                            leaderElection.close();
                        });

        FutureUtils.forward(serviceTerminationFuture, terminationFuture);

        terminationFuture.whenComplete(
                (unused, throwable) ->
                        LOG.debug("Leadership runner for job {} has been terminated.", getJobID()));
        return terminationFuture;
    }

    //
    @Override
    public void start() throws Exception {
        LOG.debug("Start leadership runner for job {}.", getJobID());
        //开始选举
        leaderElection.startLeaderElection(this);
    }

    //允许 Dispatcher 或 WebMonitorEndpoint 在检测到该作业已经有 Leader 后，获取与其通信的 RPC 句柄，从而实现作业状态查询或取消作业等操作
    @Override
    public CompletableFuture<JobMasterGateway> getJobMasterGateway() {
        synchronized (lock) {
            return jobMasterGatewayFuture;
        }
    }

    //向外暴露出当前作业的最终运行状态 Future，用于供上层组件感知该作业是已经顺利跑完（FINISHED）还是由于错误彻底挂
    @Override
    public CompletableFuture<JobManagerRunnerResult> getResultFuture() {
        return resultFuture;
    }

    @Override
    public JobID getJobID() {
        return jobMasterServiceProcessFactory.getJobId();
    }

    @Override
    public CompletableFuture<Acknowledge> cancel(Duration timeout) {
        synchronized (lock) {
            hasCurrentLeaderBeenCancelled = true;
            return getJobMasterGateway()
                    .thenCompose(jobMasterGateway -> jobMasterGateway.cancel(timeout))
                    .exceptionally(
                            e -> {
                                throw new CompletionException(
                                        new JobCancellationFailedException(
                                                "Cancellation failed.",
                                                ExceptionUtils.stripCompletionException(e)));
                            });
        }
    }

    @Override
    public CompletableFuture<JobStatus> requestJobStatus(Duration timeout) {
        return requestJob(timeout)
                .thenApply(
                        executionGraphInfo ->
                                executionGraphInfo.getArchivedExecutionGraph().getState());
    }

    @Override
    public CompletableFuture<JobDetails> requestJobDetails(Duration timeout) {
        return requestJob(timeout)
                .thenApply(
                        executionGraphInfo ->
                                JobDetails.createDetailsForJob(
                                        executionGraphInfo.getArchivedExecutionGraph()));
    }

    @Override
    public CompletableFuture<ExecutionGraphInfo> requestJob(Duration timeout) {
        synchronized (lock) {
            if (state == State.RUNNING) {
                if (jobMasterServiceProcess.isInitializedAndRunning()) {
                    return getJobMasterGateway()
                            .thenCompose(jobMasterGateway -> jobMasterGateway.requestJob(timeout));
                } else {
                    return CompletableFuture.completedFuture(
                            createExecutionGraphInfoWithJobStatus(
                                    hasCurrentLeaderBeenCancelled
                                            ? JobStatus.CANCELLING
                                            : JobStatus.INITIALIZING));
                }
            } else {
                return resultFuture.thenApply(JobManagerRunnerResult::getExecutionGraphInfo);
            }
        }
    }

    @Override
    public boolean isInitialized() {
        synchronized (lock) {
            return jobMasterServiceProcess.isInitializedAndRunning();
        }
    }

    //当前 Runner 成功被选举为 Leader 时的回调方法
    @Override
    public void grantLeadership(UUID leaderSessionID) {
        runIfStateRunning(
                //
                () -> startJobMasterServiceProcessAsync(leaderSessionID),
                "starting a new JobMasterServiceProcess");
    }

    //异步起动 JobMaster服务
    //当该类成功抢到 Leader 租约并获得新的 leaderSessionId 时，会触发此方法
    // leaderSessionId = 本次抢到 Leader 的唯一会话标识
    @GuardedBy("lock")
    private void startJobMasterServiceProcessAsync(UUID leaderSessionId) {
        sequentialOperation =
                sequentialOperation.thenCompose(
                        unused ->
                                //检查这个作业是否在之前的某个时刻其实已经运行结束（成功、失败或取消）并且结果已经持久化了
                                jobResultStore.hasJobResultEntryAsync(getJobID())
                                        .thenCompose(
                                                hasJobResult -> {
                                                    if (hasJobResult) {
                                                        //如果发现该作业已经结束了
                                                        //既然作业都做完了，就不需要再启动 JobMaster 了。如果当前还是有效 Leader，它会直接通知 Dispatcher 或集群该作业的最终结果，然后优雅地退出释放 Leader 权限
                                                        return handleJobAlreadyDoneIfValidLeader(leaderSessionId);
                                                    } else {
                                                        //todo 正式为该作业创建并启动全新的 JobMasterServiceProcess，真正开始恢复作业拓扑并去请求 Slot 资源
                                                        return createNewJobMasterServiceProcessIfValidLeader(leaderSessionId);//
                                                    }
                                                }));
        handleAsyncOperationError(sequentialOperation, "Could not start the job manager.");
    }

    private CompletableFuture<Void> handleJobAlreadyDoneIfValidLeader(UUID leaderSessionId) {
        return runIfValidLeader(
                leaderSessionId, () -> jobAlreadyDone(leaderSessionId), "check completed job");
    }

    private CompletableFuture<Void> createNewJobMasterServiceProcessIfValidLeader(UUID leaderSessionId) {
        return runIfValidLeader(
                leaderSessionId,
                () ->
                        // the heavy lifting of the JobMasterServiceProcess instantiation is still
                        // done asynchronously (see
                        // DefaultJobMasterServiceFactory#createJobMasterService executing the logic
                        // on the leaderOperation thread in the DefaultLeaderElectionService should
                        // be, therefore, fine
                        ThrowingRunnable.unchecked(
                                        //
                                        () -> createNewJobMasterServiceProcess(leaderSessionId))
                                .run(),
                "create new job master service process");
    }

    private void printLogIfNotValidLeader(String actionDescription, UUID leaderSessionId) {
        LOG.debug(
                "Ignore leader action '{}' because the leadership runner is no longer the valid leader for {}.",
                actionDescription,
                leaderSessionId);
    }

    private ExecutionGraphInfo createExecutionGraphInfoWithJobStatus(JobStatus jobStatus) {
        return new ExecutionGraphInfo(
                jobMasterServiceProcessFactory.createArchivedExecutionGraph(jobStatus, null));
    }

    private void jobAlreadyDone(UUID leaderSessionId) {
        LOG.info(
                "{} for job {} was granted leadership with leader id {}, but job was already done.",
                getClass().getSimpleName(),
                getJobID(),
                leaderSessionId);
        resultFuture.complete(
                JobManagerRunnerResult.forSuccess(
                        new ExecutionGraphInfo(
                                jobMasterServiceProcessFactory.createArchivedExecutionGraph(
                                        JobStatus.FAILED,
                                        new JobAlreadyDoneException(getJobID())))));
    }

    //负责利用工厂创建出作业主节点的执行进程，并为其注册状态监听。一旦 JobMaster 进程抛出错误或运行结束，该方法注册的监听器能够捕获并更新上面的 jobManagerRunnerResultFuture
    @GuardedBy("lock")
    private void createNewJobMasterServiceProcess(UUID leaderSessionId) {
        Preconditions.checkState(jobMasterServiceProcess.closeAsync().isDone());

        LOG.info(
                "{} for job {} was granted leadership with leader id {}. Creating new {}.",
                getClass().getSimpleName(),
                getJobID(),
                leaderSessionId,
                JobMasterServiceProcess.class.getSimpleName());
        //DefaultJobMasterServiceProcess  DefaultJobMasterServiceProcessFactory#create
        jobMasterServiceProcess = jobMasterServiceProcessFactory.create(leaderSessionId);

        forwardIfValidLeader(
                leaderSessionId,
                jobMasterServiceProcess.getJobMasterGatewayFuture(),
                jobMasterGatewayFuture,
                "JobMasterGatewayFuture from JobMasterServiceProcess");
        forwardResultFuture(leaderSessionId, jobMasterServiceProcess.getResultFuture());
        confirmLeadership(leaderSessionId, jobMasterServiceProcess.getLeaderAddressFuture());
    }

    private void confirmLeadership(
            UUID leaderSessionId, CompletableFuture<String> leaderAddressFuture) {
        FutureUtils.assertNoException(
                leaderAddressFuture.thenCompose(
                        address ->
                                callIfRunning(
                                                () -> {
                                                    LOG.debug(
                                                            "Confirm leadership {}.",
                                                            leaderSessionId);
                                                    return leaderElection.confirmLeadershipAsync(
                                                            leaderSessionId, address);
                                                },
                                                "confirming leadership")
                                        .orElse(FutureUtils.completedVoidFuture())));
    }

    private void forwardResultFuture(
            UUID leaderSessionId, CompletableFuture<JobManagerRunnerResult> resultFuture) {
        resultFuture.whenComplete(
                (jobManagerRunnerResult, throwable) ->
                        //
                        runIfValidLeader(
                                leaderSessionId,
                                //
                                () -> onJobCompletion(jobManagerRunnerResult, throwable),
                                "result future forwarding"));
    }

    @GuardedBy("lock")
    private void onJobCompletion(
            JobManagerRunnerResult jobManagerRunnerResult, Throwable throwable) {
        state = State.JOB_COMPLETED;

        LOG.debug("Completing the result for job {}.", getJobID());

        if (throwable != null) {
            resultFuture.completeExceptionally(throwable);
            jobMasterGatewayFuture.completeExceptionally(
                    new FlinkException(
                            "Could not retrieve JobMasterGateway because the JobMaster failed.",
                            throwable));
        } else {
            if (!jobManagerRunnerResult.isSuccess()) {
                jobMasterGatewayFuture.completeExceptionally(
                        new FlinkException(
                                "Could not retrieve JobMasterGateway because the JobMaster initialization failed.",
                                jobManagerRunnerResult.getInitializationFailure()));
            }

            resultFuture.complete(jobManagerRunnerResult);
        }
    }

    //当前 Runner 失去 Leader 权限时的回调方法（例如 ZooKeeper 连接断开、K8s Lease 租约过期续期失败）
    @Override
    public void revokeLeadership() {
        runIfStateRunning(
                this::stopJobMasterServiceProcessAsync,
                "revoke leadership from JobMasterServiceProcess");
    }

    @GuardedBy("lock")
    private void stopJobMasterServiceProcessAsync() {
        sequentialOperation =
                sequentialOperation.thenCompose(
                        ignored ->
                                callIfRunning(
                                                this::stopJobMasterServiceProcess,
                                                "stop leading JobMasterServiceProcess")
                                        .orElse(FutureUtils.completedVoidFuture()));

        handleAsyncOperationError(sequentialOperation, "Could not suspend the job manager.");
    }

    @GuardedBy("lock")
    private CompletableFuture<Void> stopJobMasterServiceProcess() {
        LOG.info(
                "{} for job {} was revoked leadership with leader id {}. Stopping current {}.",
                getClass().getSimpleName(),
                getJobID(),
                jobMasterServiceProcess.getLeaderSessionId(),
                JobMasterServiceProcess.class.getSimpleName());

        jobMasterGatewayFuture.completeExceptionally(
                new FlinkException(
                        "Cannot obtain JobMasterGateway because the JobMaster lost leadership."));
        jobMasterGatewayFuture = new CompletableFuture<>();

        hasCurrentLeaderBeenCancelled = false;

        return jobMasterServiceProcess.closeAsync();
    }

    //当在选举过程中底层发生不可逆的严重错误（如内部高可用服务崩溃）时的回调，该方法通常会调用 FatalErrorHandler 来直接终止当前 JVM 进程或上报集群致命异常
    @Override
    public void handleError(Exception exception) {
        fatalErrorHandler.onFatalError(exception);
    }

    private void handleAsyncOperationError(CompletableFuture<Void> operation, String message) {
        operation.whenComplete(
                (unused, throwable) -> {
                    if (throwable != null) {
                        runIfStateRunning(
                                () ->
                                        handleJobMasterServiceLeadershipRunnerError(
                                                new FlinkException(message, throwable)),
                                "handle JobMasterServiceLeadershipRunner error");
                    }
                });
    }

    private void handleJobMasterServiceLeadershipRunnerError(Throwable cause) {
        if (ExceptionUtils.isJvmFatalError(cause)) {
            fatalErrorHandler.onFatalError(cause);
        } else {
            resultFuture.completeExceptionally(cause);
        }
    }

    private void runIfStateRunning(Runnable action, String actionDescription) {
        synchronized (lock) {
            if (isRunning()) {
                action.run();
            } else {
                LOG.debug(
                        "Ignore '{}' because the leadership runner is no longer running.",
                        actionDescription);
            }
        }
    }

    private <T> Optional<T> callIfRunning(
            Supplier<? extends T> supplier, String supplierDescription) {
        synchronized (lock) {
            if (isRunning()) {
                return Optional.of(supplier.get());
            } else {
                LOG.debug(
                        "Ignore '{}' because the leadership runner is no longer running.",
                        supplierDescription);
                return Optional.empty();
            }
        }
    }

    @GuardedBy("lock")
    private boolean isRunning() {
        return state == State.RUNNING;
    }

    private CompletableFuture<Void> runIfValidLeader(UUID expectedLeaderId, Runnable action, Runnable noLeaderFallback) {
        synchronized (lock) {
            if (isRunning() && leaderElection != null) {
                return leaderElection
                        .hasLeadershipAsync(expectedLeaderId)
                        .thenAccept(
                                hasLeadership -> {
                                    synchronized (lock) {
                                        if (isRunning() && hasLeadership) {
                                            //
                                            action.run();
                                        } else {
                                            noLeaderFallback.run();
                                        }
                                    }
                                });
            } else {
                noLeaderFallback.run();
                return FutureUtils.completedVoidFuture();
            }
        }
    }

    private CompletableFuture<Void> runIfValidLeader(
            UUID expectedLeaderId, Runnable action, String noLeaderFallbackCommandDescription) {
        //
        return runIfValidLeader(
                expectedLeaderId,
                action,
                () ->
                        printLogIfNotValidLeader(
                                noLeaderFallbackCommandDescription, expectedLeaderId));
    }

    private <T> void forwardIfValidLeader(
            UUID expectedLeaderId,
            CompletableFuture<? extends T> source,
            CompletableFuture<T> target,
            String forwardDescription) {
        source.whenComplete(
                (t, throwable) ->
                        runIfValidLeader(
                                expectedLeaderId,
                                () -> {
                                    if (throwable != null) {
                                        target.completeExceptionally(throwable);
                                    } else {
                                        target.complete(t);
                                    }
                                },
                                forwardDescription));
    }

    enum State {
        RUNNING,
        STOPPED,
        JOB_COMPLETED,
    }
}
