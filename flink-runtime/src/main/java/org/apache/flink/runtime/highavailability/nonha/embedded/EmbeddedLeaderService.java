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

package org.apache.flink.runtime.highavailability.nonha.embedded;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElection;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A simple leader election service, which selects a leader among contenders and notifies listeners.
 *
 * <p>An election service for contenders can be created via {@link #createLeaderElectionService()},
 * a listener service for leader observers can be created via {@link
 * #createLeaderRetrievalService()}.
 */
public class EmbeddedLeaderService {

    private static final Logger LOG = LoggerFactory.getLogger(EmbeddedLeaderService.class);

    //作用：全局并发状态锁。选主涉及多个组件并发注册或退出，由于是非分布式环境，直接使用 JVM 级别的锁来保证竞选、状态变更的线程安全
    private final Object lock = new Object();

    private final Executor notificationExecutor;

    private final Set<EmbeddedLeaderElection> allLeaderContenders;

    //作用：吃瓜群众（监听者）队列。
    //具体意义：那些不需要当老大、但必须要知道老大是谁的组件（例如 TaskExecutor、JobClient）。一旦老大换人，服务会挨个通知这个集合里的所有人
    private final Set<EmbeddedLeaderRetrievalService> listeners;

    /** proposed leader, which has been notified of leadership grant, but has not confirmed. */
    private EmbeddedLeaderElection currentLeaderProposed;

    /** actual leader that has confirmed leadership and of which listeners have been notified. */
    private EmbeddedLeaderElection currentLeaderConfirmed;

    /** fencing UID for the current leader (or proposed leader). */
    private volatile UUID currentLeaderSessionId;

    //作用：当前胜出老大的标记。具体意义：保存当前抢到主节点的组件的 RPC 地址，以及当前任期的唯一会话 ID（Leader Session ID），用于在逻辑上区分每一次选主变更
    /** the cached address of the current leader. */
    private String currentLeaderAddress;

    //生命周期状态。标记当前内存选主服务是否已经被彻底关闭
    /** flag marking the service as terminated. */
    private boolean shutdown;

    // ------------------------------------------------------------------------

    public EmbeddedLeaderService(Executor notificationsDispatcher) {
        this.notificationExecutor = checkNotNull(notificationsDispatcher);
        this.allLeaderContenders = new HashSet<>();
        this.listeners = new HashSet<>();
    }

    // ------------------------------------------------------------------------
    //  shutdown and errors
    // ------------------------------------------------------------------------

    /**
     * Shuts down this leader election service.
     *
     * <p>This method does not perform a clean revocation of the leader status and no notification
     * to any leader listeners. It simply notifies all contenders and listeners that the service is
     * no longer available.
     */
    public void shutdown() {
        synchronized (lock) {
            shutdownInternally(new Exception("Leader election service is shutting down"));
        }
    }

    @VisibleForTesting
    public boolean isShutdown() {
        synchronized (lock) {
            return shutdown;
        }
    }

    private void fatalError(Throwable error) {
        LOG.error(
                "Embedded leader election service encountered a fatal error. Shutting down service.",
                error);

        synchronized (lock) {
            shutdownInternally(
                    new Exception(
                            "Leader election service is shutting down after a fatal error", error));
        }
    }

    @GuardedBy("lock")
    private void shutdownInternally(Exception exceptionForHandlers) {
        Preconditions.checkState(Thread.holdsLock(lock));

        if (!shutdown) {
            // clear all leader status
            currentLeaderProposed = null;
            currentLeaderConfirmed = null;
            currentLeaderSessionId = null;
            currentLeaderAddress = null;

            // fail all registered listeners
            for (EmbeddedLeaderElection leaderElection : allLeaderContenders) {
                leaderElection.shutdown(exceptionForHandlers);
            }
            allLeaderContenders.clear();

            // fail all registered listeners
            for (EmbeddedLeaderRetrievalService service : listeners) {
                service.shutdown(exceptionForHandlers);
            }
            listeners.clear();

            shutdown = true;
        }
    }

    // ------------------------------------------------------------------------
    //  creating contenders and listeners
    // ------------------------------------------------------------------------
    //返回一个 LeaderElectionService 的嵌入式实现类。当 JobMaster 启动时，就会通过这个方法返回的接口，把自己当成一个 LeaderContender 注册进来
    public LeaderElection createLeaderElectionService(String componentId) {
        checkState(!shutdown, "leader election service is shut down");
        return new EmbeddedLeaderElection(componentId);
    }

    //返回一个 LeaderRetrievalService 实现类。TaskExecutor 启动时调用它，就能在内存里源源不断地收到最新老大的地址变动通知
    public LeaderRetrievalService createLeaderRetrievalService() {
        checkState(!shutdown, "leader election service is shut down");
        return new EmbeddedLeaderRetrievalService();
    }

    // ------------------------------------------------------------------------
    //  adding and removing contenders & listeners
    // ------------------------------------------------------------------------

    //添加候选人
    /** Callback from leader contenders when they start their service. */
    private void addContender(
            EmbeddedLeaderElection embeddedLeaderElection, LeaderContender contender) {
        synchronized (lock) {
            checkState(!shutdown, "leader election is shut down");
            checkState(!embeddedLeaderElection.running, "leader election is already started");

            try {
                if (!allLeaderContenders.add(embeddedLeaderElection)) {
                    throw new IllegalStateException(
                            "leader election was added to this service multiple times");
                }

                embeddedLeaderElection.contender = contender;
                embeddedLeaderElection.running = true;

                updateLeader()
                        .whenComplete(
                                (aVoid, throwable) -> {
                                    if (throwable != null) {
                                        fatalError(throwable);
                                    }
                                });
            } catch (Throwable t) {
                fatalError(t);
            }
        }
    }

    /** Callback from leader contenders when they stop their service. */
    private void removeContender(EmbeddedLeaderElection embeddedLeaderElection) {
        synchronized (lock) {
            // if the leader election was not even started, simply do nothing
            if (!embeddedLeaderElection.running || shutdown) {
                return;
            }

            try {
                if (!allLeaderContenders.remove(embeddedLeaderElection)) {
                    throw new IllegalStateException(
                            "leader election does not belong to this service");
                }

                // stop the service
                if (embeddedLeaderElection.isLeader) {
                    embeddedLeaderElection.contender.revokeLeadership();
                }
                embeddedLeaderElection.contender = null;
                embeddedLeaderElection.running = false;
                embeddedLeaderElection.isLeader = false;

                // if that was the current leader, unset its status
                if (currentLeaderConfirmed == embeddedLeaderElection) {
                    currentLeaderConfirmed = null;
                    currentLeaderSessionId = null;
                    currentLeaderAddress = null;
                }
                if (currentLeaderProposed == embeddedLeaderElection) {
                    currentLeaderProposed = null;
                    currentLeaderSessionId = null;
                }

                updateLeader()
                        .whenComplete(
                                (aVoid, throwable) -> {
                                    if (throwable != null) {
                                        fatalError(throwable);
                                    }
                                });
            } catch (Throwable t) {
                fatalError(t);
            }
        }
    }

    /** Callback from leader contenders when they confirm a leader grant. */
    private CompletableFuture<Void> confirmLeader(
            final EmbeddedLeaderElection embeddedLeaderElection,
            final UUID leaderSessionId,
            final String leaderAddress) {
        synchronized (lock) {
            // if the leader election was shut down in the meantime, ignore this confirmation
            if (!embeddedLeaderElection.running || shutdown) {
                return FutureUtils.completedVoidFuture();
            }

            try {
                // check if the confirmation is for the same grant, or whether it is a stale grant
                if (embeddedLeaderElection == currentLeaderProposed
                        && currentLeaderSessionId.equals(leaderSessionId)) {
                    LOG.info(
                            "Received confirmation of leadership for leader {} , session={}",
                            leaderAddress,
                            leaderSessionId);

                    // mark leadership
                    currentLeaderConfirmed = embeddedLeaderElection;
                    currentLeaderAddress = leaderAddress;//pekko://flink/user/rpc/resourcemanager_1
                    currentLeaderProposed = null;

                    //todo 通知所有的监听器  notify all listeners
                    return notifyAllListeners(leaderAddress, leaderSessionId);
                } else {
                    LOG.debug(
                            "Received confirmation of leadership for a stale leadership grant. Ignoring.");
                }
            } catch (Throwable t) {
                fatalError(t);
            }
        }

        return FutureUtils.completedVoidFuture();
    }

    //通知所有监听器
    private CompletableFuture<Void> notifyAllListeners(String address, UUID leaderSessionId) {
        final List<CompletableFuture<Void>> notifyListenerFutures = new ArrayList<>(listeners.size());

        for (EmbeddedLeaderRetrievalService listener : listeners) {
            CompletableFuture<Void> completableFuture = notifyListener(
                    address,
                    leaderSessionId,
                    listener.listener);
            notifyListenerFutures.add(completableFuture);
        }

        return FutureUtils.waitForAll(notifyListenerFutures);
    }

    //抢占检查：当有新的候选人（Contender）加入，或者旧的老大退出时，该方法会被触发。它首先检查当前内存中是否已经有存活的老大。
    //指定胜出者：如果当前没有老大，它会直接去 contenders（竞争者集合）里拿第一个加入的候选人。
    //颁发王冠：一旦确定了谁是第一个，它就会在内存中随机生成一个新的 UUID 作为任期会话 ID（Leader Session ID），然后直接调用
    //负责模拟选主、并在候选人中决定谁来当老大
    @GuardedBy("lock")
    private CompletableFuture<Void> updateLeader() {
        // this must be called under the lock
        Preconditions.checkState(Thread.holdsLock(lock));

        if (currentLeaderConfirmed == null && currentLeaderProposed == null) {
            // we need a new leader
            if (allLeaderContenders.isEmpty()) {
                // no new leader available, tell everyone that there is no leader currently
                return notifyAllListeners(null, null);
            } else {
                // propose a leader and ask it
                final UUID leaderSessionId = UUID.randomUUID();
                //选择第一个人当leader
                EmbeddedLeaderElection embeddedLeaderElection = allLeaderContenders.iterator().next();

                currentLeaderSessionId = leaderSessionId;
                currentLeaderProposed = embeddedLeaderElection;
                currentLeaderProposed.isLeader = true;

                LOG.info(
                        "Proposing leadership to the contender that is registered under component ID '{}'.",
                        embeddedLeaderElection.componentId);
                GrantLeadershipCall grantLeadershipCall = new GrantLeadershipCall(
                        embeddedLeaderElection.contender,
                        leaderSessionId,
                        LOG);
                //告诉该候选人是leader了
                return execute(grantLeadershipCall);
            }
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    private CompletableFuture<Void> notifyListener(
            @Nullable String address,
            @Nullable UUID leaderSessionId,
            LeaderRetrievalListener listener) {
        //
        return CompletableFuture.runAsync(
                //通知leader有变动
                new NotifyOfLeaderCall(address, leaderSessionId, listener, LOG),
                notificationExecutor);
    }

    private void addListener(
            EmbeddedLeaderRetrievalService service, LeaderRetrievalListener listener) {
        synchronized (lock) {
            checkState(!shutdown, "leader election service is shut down");
            checkState(!service.running, "leader retrieval service is already started");

            try {
                if (!listeners.add(service)) {
                    throw new IllegalStateException(
                            "leader retrieval service was added to this service multiple times");
                }

                service.listener = listener;
                service.running = true;

                // if we already have a leader, immediately notify this new listener
                if (currentLeaderConfirmed != null) {
                    //todo
                    notifyListener(currentLeaderAddress, currentLeaderSessionId, listener);
                }
            } catch (Throwable t) {
                fatalError(t);
            }
        }
    }

    //移除监听器
    private void removeListener(EmbeddedLeaderRetrievalService service) {
        synchronized (lock) {
            // if the service was not even started, simply do nothing
            if (!service.running || shutdown) {
                return;
            }

            try {
                if (!listeners.remove(service)) {
                    throw new IllegalStateException(
                            "leader retrieval service does not belong to this service");
                }

                // stop the service
                service.listener = null;
                service.running = false;
            } catch (Throwable t) {
                fatalError(t);
            }
        }
    }

    @VisibleForTesting
    CompletableFuture<Void> grantLeadership() {
        synchronized (lock) {
            if (shutdown) {
                return getShutDownFuture();
            }

            return updateLeader();
        }
    }

    private CompletableFuture<Void> getShutDownFuture() {
        return FutureUtils.completedExceptionally(
                new FlinkException("EmbeddedLeaderService has been shut down."));
    }

    @VisibleForTesting
    CompletableFuture<Void> revokeLeadership() {
        synchronized (lock) {
            if (shutdown) {
                return getShutDownFuture();
            }

            if (currentLeaderProposed != null || currentLeaderConfirmed != null) {
                final EmbeddedLeaderElection embeddedLeaderElection;

                if (currentLeaderConfirmed != null) {
                    embeddedLeaderElection = currentLeaderConfirmed;
                } else {
                    embeddedLeaderElection = currentLeaderProposed;
                }

                LOG.info("Revoking leadership of {}.", embeddedLeaderElection.contender);
                embeddedLeaderElection.isLeader = false;
                CompletableFuture<Void> revokeLeadershipCallFuture =
                        execute(new RevokeLeadershipCall(embeddedLeaderElection.contender));

                CompletableFuture<Void> notifyAllListenersFuture = notifyAllListeners(null, null);

                currentLeaderProposed = null;
                currentLeaderConfirmed = null;
                currentLeaderAddress = null;
                currentLeaderSessionId = null;

                return CompletableFuture.allOf(
                        revokeLeadershipCallFuture, notifyAllListenersFuture);
            } else {
                return CompletableFuture.completedFuture(null);
            }
        }
    }

    private CompletableFuture<Void> execute(Runnable runnable) {
        //指定runnable 在notificationExecutor 运行
        return CompletableFuture.runAsync(runnable, notificationExecutor);
    }

    // ------------------------------------------------------------------------
    //  election and retrieval service implementations
    // ------------------------------------------------------------------------
    //todo standalone 模式不会使用这个类
    private class EmbeddedLeaderElection implements LeaderElection {

        final String componentId;
        volatile LeaderContender contender;

        volatile boolean isLeader;

        volatile boolean running;

        EmbeddedLeaderElection(String componentId) {
            this.componentId = componentId;
        }

        @Override
        public void startLeaderElection(LeaderContender contender) throws Exception {
            checkNotNull(contender);
            addContender(this, contender);
        }

        @Override
        public void close() {
            removeContender(this);
        }

        @Override
        public CompletableFuture<Void> confirmLeadershipAsync(
                UUID leaderSessionID, String leaderAddress) {
            checkNotNull(leaderSessionID);
            checkNotNull(leaderAddress);
            //确认成为leader
            return confirmLeader(this, leaderSessionID, leaderAddress);
        }

        @Override
        public CompletableFuture<Boolean> hasLeadershipAsync(UUID leaderSessionId) {
            return CompletableFuture.completedFuture(
                    isLeader && leaderSessionId.equals(currentLeaderSessionId));
        }

        void shutdown(Exception cause) {
            if (running) {
                running = false;
                isLeader = false;
                contender.revokeLeadership();
                contender = null;
            }
        }
    }

    // ------------------------------------------------------------------------

    private class EmbeddedLeaderRetrievalService implements LeaderRetrievalService {

        volatile LeaderRetrievalListener listener;

        volatile boolean running;
        //会被 startTaskExecutorServices() 的resourceManagerLeaderRetriever.start 调用
        @Override
        public void start(LeaderRetrievalListener listener) throws Exception {
            checkNotNull(listener);
            //todo 添加监听器
            addListener(this, listener);
        }

        @Override
        public void stop() throws Exception {
            removeListener(this);
        }

        public void shutdown(Exception cause) {
            if (running) {
                running = false;
                listener = null;
            }
        }
    }

    // ------------------------------------------------------------------------
    //  asynchronous notifications
    // ------------------------------------------------------------------------

    private static class NotifyOfLeaderCall implements Runnable {

        @Nullable private final String address; // null if leader revoked without new leader
        @Nullable private final UUID leaderSessionId; // null if leader revoked without new leader

        private final LeaderRetrievalListener listener;
        private final Logger logger;

        NotifyOfLeaderCall(
                @Nullable String address,
                @Nullable UUID leaderSessionId,
                LeaderRetrievalListener listener,
                Logger logger) {

            this.address = address;
            this.leaderSessionId = leaderSessionId;
            this.listener = checkNotNull(listener);
            this.logger = checkNotNull(logger);
        }

        @Override
        public void run() {
            try {
                //todo
                // JobMaster$ResourceManagerLeaderListener#notifyLeaderAddress
                // DefaultJobLeaderIdService$JobLeaderIdListener#notifyLeaderAddress
                // DefaultJobLeaderService$JobManagerLeaderListener#notifyLeaderAddress
                listener.notifyLeaderAddress(address, leaderSessionId);//
            } catch (Throwable t) {
                logger.warn("Error notifying leader listener about new leader", t);
                listener.handleError(t instanceof Exception ? (Exception) t : new Exception(t));
            }
        }
    }

    // ------------------------------------------------------------------------

    private static class GrantLeadershipCall implements Runnable {

        private final LeaderContender contender;
        private final UUID leaderSessionId;
        private final Logger logger;

        GrantLeadershipCall(LeaderContender contender, UUID leaderSessionId, Logger logger) {

            this.contender = checkNotNull(contender);
            this.leaderSessionId = checkNotNull(leaderSessionId);
            this.logger = checkNotNull(logger);
        }

        @Override
        public void run() {
            try {
                //赋予leader角色
                // DefaultDispatcherRunner#grantLeadership
                // ResourceManagerServiceImpl#grantLeadership
                System.out.println(contender.getClass().getName()  + "  grantLeadership " + Thread.currentThread().getName());
                contender.grantLeadership(leaderSessionId);
            } catch (Throwable t) {
                logger.warn("Error granting leadership to contender", t);
                contender.handleError(t instanceof Exception ? (Exception) t : new Exception(t));
            }
        }
    }

    private static class RevokeLeadershipCall implements Runnable {

        @Nonnull private final LeaderContender contender;

        RevokeLeadershipCall(@Nonnull LeaderContender contender) {
            this.contender = contender;
        }

        @Override
        public void run() {
            contender.revokeLeadership();
        }
    }
}
