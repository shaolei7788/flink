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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobmaster.JMTMRegistrationRejection;
import org.apache.flink.runtime.jobmaster.JMTMRegistrationSuccess;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.TaskManagerRegistrationInformation;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.registration.RegisteredRpcConnection;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.registration.RetryingRegistration;
import org.apache.flink.runtime.registration.RetryingRegistrationConfiguration;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.taskmanager.UnresolvedTaskManagerLocation;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/** Default implementation of {@link JobLeaderService}. */
public class DefaultJobLeaderService implements JobLeaderService {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultJobLeaderService.class);

    /** Self's location, used for the job manager connection. */
    private final UnresolvedTaskManagerLocation ownLocation;

    /** The leader retrieval service and listener for each registered job. */
    private final Map<JobID, Tuple2<LeaderRetrievalService, DefaultJobLeaderService.JobManagerLeaderListener>> jobLeaderServices;

    private final RetryingRegistrationConfiguration retryingRegistrationConfiguration;

    private final UUID taskManagerSession;

    /** Internal state of the service. */
    private volatile DefaultJobLeaderService.State state;

    /**
     * Address of the owner of this service. This address is used for the job manager connection.
     */
    private String ownerAddress;

    /** Rpc service to use for establishing connections. */
    private RpcService rpcService;

    /** High availability services to create the leader retrieval services from. */
    private HighAvailabilityServices highAvailabilityServices;

    /** Job leader listener listening for job leader changes. */
    private JobLeaderListener jobLeaderListener;


    //核心作用是：帮助 TaskExecutor 统一管理、监听多个不同作业的 JobManager（JobMaster）Leader 状态变更，并在确定新的 Leader 后负责与其建立 RPC 通信连接
    public DefaultJobLeaderService(
            UnresolvedTaskManagerLocation location,
            RetryingRegistrationConfiguration retryingRegistrationConfiguration) {
        this.ownLocation = Preconditions.checkNotNull(location);
        this.retryingRegistrationConfiguration =
                Preconditions.checkNotNull(retryingRegistrationConfiguration);
        this.taskManagerSession = UUID.randomUUID();

        // Has to be a concurrent hash map because tests might access this service
        // concurrently via containsJob
        jobLeaderServices = new ConcurrentHashMap<>(4);

        state = DefaultJobLeaderService.State.CREATED;

        ownerAddress = null;
        rpcService = null;
        highAvailabilityServices = null;
        jobLeaderListener = null;
    }

    // -------------------------------------------------------------------------------
    // Methods
    // -------------------------------------------------------------------------------

    @Override
    public void start(
            final String initialOwnerAddress,
            final RpcService initialRpcService,
            final HighAvailabilityServices initialHighAvailabilityServices,
            final JobLeaderListener initialJobLeaderListener) {

        if (DefaultJobLeaderService.State.CREATED != state) {
            throw new IllegalStateException("The service has already been started.");
        } else {
            LOG.info("Start job leader service.");

            this.ownerAddress = Preconditions.checkNotNull(initialOwnerAddress);
            this.rpcService = Preconditions.checkNotNull(initialRpcService);
            this.highAvailabilityServices =
                    Preconditions.checkNotNull(initialHighAvailabilityServices);
            this.jobLeaderListener = Preconditions.checkNotNull(initialJobLeaderListener);
            state = DefaultJobLeaderService.State.STARTED;
        }
    }

    @Override
    public void stop() throws Exception {
        LOG.info("Stop job leader service.");

        if (DefaultJobLeaderService.State.STARTED == state) {

            for (Tuple2<LeaderRetrievalService, DefaultJobLeaderService.JobManagerLeaderListener>
                    leaderRetrievalServiceEntry : jobLeaderServices.values()) {
                LeaderRetrievalService leaderRetrievalService = leaderRetrievalServiceEntry.f0;
                DefaultJobLeaderService.JobManagerLeaderListener jobManagerLeaderListener =
                        leaderRetrievalServiceEntry.f1;

                jobManagerLeaderListener.stop();
                leaderRetrievalService.stop();
            }

            jobLeaderServices.clear();
        }

        state = DefaultJobLeaderService.State.STOPPED;
    }

    @Override
    public void removeJob(JobID jobId) {
        Preconditions.checkState(
                DefaultJobLeaderService.State.STARTED == state,
                "The service is currently not running.");

        Tuple2<LeaderRetrievalService, DefaultJobLeaderService.JobManagerLeaderListener> entry =
                jobLeaderServices.remove(jobId);

        if (entry != null) {
            LOG.info("Remove job {} from job leader monitoring.", jobId);

            LeaderRetrievalService leaderRetrievalService = entry.f0;
            DefaultJobLeaderService.JobManagerLeaderListener jobManagerLeaderListener = entry.f1;

            jobManagerLeaderListener.stop();

            try {
                leaderRetrievalService.stop();
            } catch (Exception e) {
                LOG.info(
                        "Could not properly stop the LeaderRetrievalService for job {}.", jobId, e);
            }
        }
    }

    @Override
    public void addJob(final JobID jobId, final String defaultTargetAddress) throws Exception {
        Preconditions.checkState(
                DefaultJobLeaderService.State.STARTED == state,
                "The service is currently not running.");

        LOG.info("Add job {} for job leader monitoring.", jobId);

        final LeaderRetrievalService leaderRetrievalService =
                highAvailabilityServices.getJobManagerLeaderRetriever(jobId, defaultTargetAddress);
        //
        DefaultJobLeaderService.JobManagerLeaderListener jobManagerLeaderListener = new JobManagerLeaderListener(jobId);

        final Tuple2<LeaderRetrievalService, JobManagerLeaderListener> oldEntry =
                jobLeaderServices.put(
                        jobId, Tuple2.of(leaderRetrievalService, jobManagerLeaderListener));

        if (oldEntry != null) {
            oldEntry.f0.stop();
            oldEntry.f1.stop();
        }

        leaderRetrievalService.start(jobManagerLeaderListener);
    }

    @Override
    public void reconnect(final JobID jobId) {
        Preconditions.checkNotNull(jobId, "JobID must not be null.");

        final Tuple2<LeaderRetrievalService, JobManagerLeaderListener> jobLeaderService =
                jobLeaderServices.get(jobId);

        if (jobLeaderService != null) {
            jobLeaderService.f1.reconnect();
        } else {
            LOG.info("Cannot reconnect to job {} because it is not registered.", jobId);
        }
    }

    /** Leader listener which tries to establish a connection to a newly detected job leader. */
    @ThreadSafe
    private final class JobManagerLeaderListener implements LeaderRetrievalListener {

        private final Object lock = new Object();

        /** Job id identifying the job to look for a leader. */
        private final JobID jobId;

        /** Rpc connection to the job leader. */
        @GuardedBy("lock")
        @Nullable
        private RegisteredRpcConnection<
                        JobMasterId,
                        JobMasterGateway,
                        JMTMRegistrationSuccess,
                        JMTMRegistrationRejection>
                rpcConnection;

        /** Leader id of the current job leader. */
        @GuardedBy("lock")
        @Nullable
        private JobMasterId currentJobMasterId;

        /** State of the listener. */
        private volatile boolean stopped;

        private JobManagerLeaderListener(JobID jobId) {
            this.jobId = Preconditions.checkNotNull(jobId);

            stopped = false;
            rpcConnection = null;
            currentJobMasterId = null;
        }

        private JobMasterId getCurrentJobMasterId() {
            synchronized (lock) {
                return currentJobMasterId;
            }
        }

        public void stop() {
            synchronized (lock) {
                if (!stopped) {
                    stopped = true;

                    closeRpcConnection();
                }
            }
        }

        public void reconnect() {
            synchronized (lock) {
                if (stopped) {
                    LOG.debug(
                            "Cannot reconnect because the JobManagerLeaderListener has already been stopped.");
                } else {
                    if (rpcConnection != null) {
                        Preconditions.checkState(
                                rpcConnection.tryReconnect(),
                                "Illegal concurrent modification of the JobManagerLeaderListener rpc connection.");
                    } else {
                        LOG.debug("Cannot reconnect to an unknown JobMaster.");
                    }
                }
            }
        }

        //被NotifyOfLeaderCall#run 调用
        //是一个极其关键的异步回调方法
        //核心作用是：当底层的分布式协调服务（如 ZooKeeper、Kubernetes ConfigMap）检测到某个组件（如 JobManager、ResourceManager）的 Leader 发生切换或首次上线时，
        // 该方法会被触发，用来接收新 Leader 的网络地址和 Session ID，并驱动本地组件发起 RPC 握手连接
        // leaderAddress  (新 Leader 地址)：新晋升为 Leader 的组件的 Akka/Netty RPC 物理通信 URL
        // leaderAddress = pekko://flink/user/rpc/jobmanager_3
        // leaderId = dd60d21b-46d6-43b0-b445-29cdcb40bd7f
        @Override
        public void notifyLeaderAddress(@Nullable final String leaderAddress, @Nullable final UUID leaderId) {
            Optional<JobMasterId> jobManagerLostLeadership = Optional.empty();

            synchronized (lock) {
                if (stopped) {
                    LOG.debug(
                            "{}'s leader retrieval listener reported a new leader for job {}. "
                                    + "However, the service is no longer running.",
                            DefaultJobLeaderService.class.getSimpleName(),
                            jobId);
                } else {
                    final JobMasterId jobMasterId = JobMasterId.fromUuidOrNull(leaderId);

                    LOG.debug(
                            "New leader information for job {}. Address: {}, leader id: {}.",
                            jobId,
                            leaderAddress,
                            jobMasterId);

                    if (leaderAddress == null || leaderAddress.isEmpty()) {
                        //说明当前没有任何活动的 Leader（旧 Leader 挂了，新 Leader 还没选出来）。此时，方法会触发“失去领导权”的逻辑（如触发 jobManagerLostLeadership），断开现有连接，冻结资源
                        // the leader lost leadership but there is no other leader yet.
                        jobManagerLostLeadership = Optional.ofNullable(currentJobMasterId);
                        closeRpcConnection();
                    } else {
                        // check whether we are already connecting to this leader
                        if (Objects.equals(jobMasterId, currentJobMasterId)) {
                            LOG.debug(
                                    "Ongoing attempt to connect to leader of job {}. Ignoring duplicate leader information.",
                                    jobId);
                        } else {
                            //关闭连接
                            closeRpcConnection();
                            //todo 建立新的连接
                            openRpcConnectionTo(leaderAddress, jobMasterId);
                        }
                    }
                }
            }

            // send callbacks outside of the lock scope
            jobManagerLostLeadership.ifPresent(
                    oldJobMasterId ->
                            jobLeaderListener.jobManagerLostLeadership(jobId, oldJobMasterId));
        }

        @GuardedBy("lock")
        private void openRpcConnectionTo(String leaderAddress, JobMasterId jobMasterId) {
            Preconditions.checkState(
                    currentJobMasterId == null && rpcConnection == null,
                    "Cannot open a new rpc connection if the previous connection has not been closed.");

            currentJobMasterId = jobMasterId;
            // 跟jobmanager注册连接
            // leaderAddress = pekko://flink/user/rpc/jobmanager_3
            // jobMasterId = 8a52e19bb5e66f5925ec8903c0244a9c
            rpcConnection = new JobManagerRegisteredRpcConnection(
                            LOG, leaderAddress, jobMasterId, rpcService.getScheduledExecutor());

            LOG.info(
                    "Try to register at job manager {} with leader id {}.",
                    leaderAddress,
                    jobMasterId.toUUID());
            // 开始向远端 JobManager 拨号
            rpcConnection.start();
        }

        @GuardedBy("lock")
        private void closeRpcConnection() {
            if (rpcConnection != null) {
                rpcConnection.close();
                rpcConnection = null;
                currentJobMasterId = null;
            }
        }

        @Override
        public void handleError(Exception exception) {
            if (stopped) {
                LOG.debug(
                        "{}'s leader retrieval listener reported an exception for job {}. "
                                + "However, the service is no longer running.",
                        DefaultJobLeaderService.class.getSimpleName(),
                        jobId,
                        exception);
            } else {
                jobLeaderListener.handleError(exception);
            }
        }

        /** Rpc connection for the job manager <--> task manager connection. */
        private final class JobManagerRegisteredRpcConnection extends RegisteredRpcConnection<
                        JobMasterId,
                        JobMasterGateway,
                        JMTMRegistrationSuccess,
                        JMTMRegistrationRejection> {

            JobManagerRegisteredRpcConnection(
                    Logger log, String targetAddress, JobMasterId jobMasterId, Executor executor) {
                super(log, targetAddress, jobMasterId, executor);
            }

            @Override
            protected RetryingRegistration<
                            JobMasterId,
                            JobMasterGateway,
                            JMTMRegistrationSuccess,
                            JMTMRegistrationRejection> generateRegistration() {
                return new DefaultJobLeaderService.JobManagerRetryingRegistration(
                        LOG,
                        rpcService,
                        "JobManager",
                        JobMasterGateway.class,
                        getTargetAddress(),
                        getTargetLeaderId(),
                        retryingRegistrationConfiguration,
                        jobId,
                        TaskManagerRegistrationInformation.create(
                                ownerAddress, ownLocation, taskManagerSession));
            }

            @Override
            protected void onRegistrationSuccess(JMTMRegistrationSuccess success) {
                //当收到 JobManager 允许注册的握手信号后，该方法被触发。它会提取出 JobManager 的 Gateway，并通知外部的监听器（JobLeaderListener）
                runIfValidRegistrationAttemptOrElse(
                        () -> {
                            log.info(
                                    "Successful registration at job manager {} for job {}.",
                                    getTargetAddress(),
                                    jobId);
                            //
                            jobLeaderListener.jobManagerGainedLeadership(
                                    jobId, getTargetGateway(), success);
                        },
                        () ->
                                log.debug(
                                        "Encountered obsolete JobManager registration success from {} with leader session ID {}.",
                                        getTargetAddress(),
                                        getTargetLeaderId()));
            }

            @Override
            protected void onRegistrationRejection(JMTMRegistrationRejection rejection) {
                runIfValidRegistrationAttemptOrElse(
                        () -> {
                            log.info(
                                    "Rejected registration at job manager {} for job {}.",
                                    getTargetAddress(),
                                    jobId);
                            //TaskManager 尝试向新 Leader 注册时，被明确拒绝（如标识错误、Job 正在销毁等情况）
                            jobLeaderListener.jobManagerRejectedRegistration(
                                    jobId, getTargetAddress(), rejection);
                        },
                        () ->
                                log.debug(
                                        "Encountered obsolete JobManager registration rejection {} from {} with leader session ID {}.",
                                        rejection,
                                        getTargetAddress(),
                                        getTargetLeaderId()));
            }

            //注册发生致命失败时的回调
            @Override
            protected void onRegistrationFailure(Throwable failure) {
                runIfValidRegistrationAttemptOrElse(
                        () -> {
                            log.info(
                                    "Failed to register at job  manager {} for job {}.",
                                    getTargetAddress(),
                                    jobId);
                            jobLeaderListener.handleError(failure);
                        },
                        () ->
                                log.debug(
                                        "Obsolete JobManager registration failure from {} with leader session ID {}.",
                                        getTargetAddress(),
                                        getTargetLeaderId(),
                                        failure));
            }

            private void runIfValidRegistrationAttemptOrElse(
                    Runnable runIfValid, Runnable runIfInvalid) {
                if (Objects.equals(getTargetLeaderId(), getCurrentJobMasterId())) {
                    runIfValid.run();
                } else {
                    runIfInvalid.run();
                }
            }
        }
    }

    /** Retrying registration for the job manager <--> task manager connection. */
    private static final class JobManagerRetryingRegistration
            extends RetryingRegistration<
                    JobMasterId,
                    JobMasterGateway,
                    JMTMRegistrationSuccess,
                    JMTMRegistrationRejection> {

        private final JobID jobId;

        private final TaskManagerRegistrationInformation taskManagerRegistrationInformation;

        JobManagerRetryingRegistration(
                Logger log,
                RpcService rpcService,
                String targetName,
                Class<JobMasterGateway> targetType,
                String targetAddress,
                JobMasterId jobMasterId,
                RetryingRegistrationConfiguration retryingRegistrationConfiguration,
                JobID jobId,
                TaskManagerRegistrationInformation taskManagerRegistrationInformation) {
            super(
                    log,
                    rpcService,
                    targetName,
                    targetType,
                    targetAddress,
                    jobMasterId,
                    retryingRegistrationConfiguration);

            this.jobId = Preconditions.checkNotNull(jobId);
            this.taskManagerRegistrationInformation = taskManagerRegistrationInformation;
        }

        @Override
        protected CompletableFuture<RegistrationResponse> invokeRegistration(
                JobMasterGateway gateway, JobMasterId fencingToken, long timeoutMillis) {
            return gateway.registerTaskManager(
                    jobId, taskManagerRegistrationInformation, Duration.ofMillis(timeoutMillis));
        }
    }

    /** Internal state of the service. */
    private enum State {
        CREATED,
        STARTED,
        STOPPED
    }

    // -----------------------------------------------------------
    // Testing methods
    // -----------------------------------------------------------

    /**
     * Check whether the service monitors the given job.
     *
     * @param jobId identifying the job
     * @return True if the given job is monitored; otherwise false
     */
    @Override
    @VisibleForTesting
    public boolean containsJob(JobID jobId) {
        Preconditions.checkState(
                DefaultJobLeaderService.State.STARTED == state,
                "The service is currently not running.");

        return jobLeaderServices.containsKey(jobId);
    }
}
