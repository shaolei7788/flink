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

package org.apache.flink.runtime.rpc.pekko;

import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.exceptions.RpcException;
import org.apache.flink.runtime.rpc.pekko.exceptions.UnknownMessageException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import org.apache.pekko.PekkoException;
import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.actor.ChildRestartStats;
import org.apache.pekko.actor.Props;
import org.apache.pekko.actor.Status;
import org.apache.pekko.actor.SupervisorStrategy;
import org.apache.pekko.japi.pf.DeciderBuilder;
import org.apache.pekko.pattern.Patterns;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.function.Function;

import scala.PartialFunction;
import scala.collection.Iterable;

/**
 * Supervisor actor which is responsible for starting {@link PekkoRpcActor} instances and monitoring
 * when the actors have terminated.
 */
//它的生命周期极长，随着 PekkoRpcService 启动而降生。它内部最硬核的逻辑是实现了一套 SupervisorStrategy（监督策略）。
// 当底层的某个 PekkoRpcActor（比如运行中的 JobMaster）因为网络突发故障、或者抛出未知异常（如 NullPointerException）时，这个异常不会直接冲垮 JVM。异常会向上抛给它的父亲 SupervisorActor。
// SupervisorActor 会在底层做出判决：如果是小问题，下达指令让这个 PekkoRpcActor Restart（原地转世/重置内存状态），断点续传；如果是致命问题（如 OOM），下达 Stop 并通知 Flink 彻底自杀切主
//每个 RpcService 内部只有一个 SupervisorActor
class SupervisorActor extends AbstractActor {

    private static final Logger LOG = LoggerFactory.getLogger(SupervisorActor.class);

    private final Executor terminationFutureExecutor;

    private final Map<ActorRef, RpcActorRegistration> registeredRpcActors;

    // 会被 actorSystem.actorOf(supervisorProps, getActorName()) 调用 大概率是通过反射调用
    SupervisorActor(Executor terminationFutureExecutor) {
        this.terminationFutureExecutor = terminationFutureExecutor;
        this.registeredRpcActors = new HashMap<>();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(StartRpcActor.class, this::createStartRpcActorMessage)
                .matchAny(this::handleUnknownMessage)
                .build();
    }

    @Override
    public void postStop() throws Exception {
        LOG.debug("Stopping supervisor actor.");

        super.postStop();

        for (RpcActorRegistration actorRegistration : registeredRpcActors.values()) {
            terminateRpcActorOnStop(actorRegistration);
        }

        registeredRpcActors.clear();
    }

    @Override
    public SupervisorActorSupervisorStrategy supervisorStrategy() {
        return new SupervisorActorSupervisorStrategy();
    }

    private void terminateRpcActorOnStop(RpcActorRegistration rpcActorRegistration) {
        rpcActorRegistration.terminateExceptionally(
                new RpcException(
                        String.format(
                                "Unexpected closing of %s with name %s.",
                                getClass().getSimpleName(), rpcActorRegistration.getEndpointId())),
                terminationFutureExecutor);
    }

    // 处理请求者的请求 根据startRpcActor的信息创建了ActorRef 对象并放入registeredRpcActors里
    private void createStartRpcActorMessage(StartRpcActor startRpcActor) {
        // endpointId = dispatcher_0
        // endpointId = resourcemanager_1
        final String endpointId = startRpcActor.getEndpointId();
        final RpcActorRegistration rpcActorRegistration = new RpcActorRegistration(endpointId);

        //会调用  SupervisorActor.startRpcActor 第二个参数里面的create方法
        final Props rpcActorProps = startRpcActor.getPropsFactory().create(rpcActorRegistration.getInternalTerminationFuture());

        LOG.debug(
                "Starting {} with name {}.",
                rpcActorProps.actorClass().getSimpleName(),
                endpointId);

        try {
            //todo 创建 ActorRef 对象  actorRef = PekkoRpcActor 或 FencedPekkoRpcActor
            final ActorRef actorRef = getContext().actorOf(rpcActorProps, endpointId);

            registeredRpcActors.put(actorRef, rpcActorRegistration);
            //创建一个ActorRegistration 对象 就是封装下actorRef
            ActorRegistration actorRegistration = ActorRegistration.create(actorRef, rpcActorRegistration.getExternalTerminationFuture());
            getSender()
                    // tell 就是响应 无返回值
                    .tell(
                            // 返回的是 StartRpcActorResponse 对象
                            StartRpcActorResponse.success(actorRegistration),
                            getSelf());
        } catch (PekkoException e) {
            //注册失败
            getSender().tell(StartRpcActorResponse.failure(e), getSelf());
        }
    }

    private void rpcActorTerminated(ActorRef actorRef) {
        final RpcActorRegistration actorRegistration = removeAkkaRpcActor(actorRef);

        LOG.debug("RpcActor {} has terminated.", actorRef.path());
        actorRegistration.terminate(terminationFutureExecutor);
    }

    private void rpcActorFailed(ActorRef actorRef, Throwable cause) {
        LOG.warn("RpcActor {} has failed. Shutting it down now.", actorRef.path(), cause);

        for (Map.Entry<ActorRef, RpcActorRegistration> registeredRpcActor :
                registeredRpcActors.entrySet()) {
            final ActorRef otherActorRef = registeredRpcActor.getKey();
            if (otherActorRef.equals(actorRef)) {
                final RpcException error =
                        new RpcException(
                                String.format(
                                        "Stopping actor %s because it failed.", actorRef.path()),
                                cause);
                registeredRpcActor.getValue().markFailed(error);
            } else {
                final RpcException siblingException =
                        new RpcException(
                                String.format(
                                        "Stopping actor %s because its sibling %s has failed.",
                                        otherActorRef.path(), actorRef.path()));
                registeredRpcActor.getValue().markFailed(siblingException);
            }
        }

        getContext().getSystem().terminate();
    }

    private RpcActorRegistration removeAkkaRpcActor(ActorRef actorRef) {
        return Optional.ofNullable(registeredRpcActors.remove(actorRef))
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        String.format(
                                                "Could not find actor %s.", actorRef.path())));
    }

    private void handleUnknownMessage(Object msg) {
        final UnknownMessageException cause =
                new UnknownMessageException(
                        String.format("Cannot handle unknown message %s.", msg));
        getSender().tell(new Status.Failure(cause), getSelf());
        throw cause;
    }

    public static String getActorName() {
        return PekkoRpcServiceUtils.SUPERVISOR_NAME;
    }

    //
    public static ActorRef startSupervisorActor(
            ActorSystem actorSystem, Executor terminationFutureExecutor) {
        final Props supervisorProps = Props.create(SupervisorActor.class, terminationFutureExecutor).withDispatcher("pekko.actor.supervisor-dispatcher");
        // getActorName() = rpc
        //todo 执行完该方法  SupervisorActor 的构造器会被调用
        return actorSystem.actorOf(supervisorProps, getActorName());
    }

    public static StartRpcActorResponse startRpcActor(ActorRef supervisor, StartRpcActor.PropsFactory propsFactory, String endpointId) {
        //创建一个StartRpcActor实体消息
        StartRpcActor startRpcActor = createStartRpcActorMessage(propsFactory, endpointId);
        // 向 supervisor(ActorRef) 发一个消息 startRpcActor 以非阻塞的方式  ask 有返回值
        // createReceive 会接收该消息 会触发 createStartRpcActorMessage 主要是创建一个ActorRef对象
        return Patterns.ask(
                        supervisor,
                        startRpcActor,// 具体的消息
                        RpcUtils.INF_DURATION) //超时时间
                .toCompletableFuture()
                .thenApply(StartRpcActorResponse.class::cast)
                .join();
    }

    public static StartRpcActor createStartRpcActorMessage(
            StartRpcActor.PropsFactory propsFactory, String endpointId) {
        //
        return StartRpcActor.create(propsFactory, endpointId);
    }

    // -----------------------------------------------------------------------------
    // Internal classes
    // -----------------------------------------------------------------------------

    private final class SupervisorActorSupervisorStrategy extends SupervisorStrategy {

        @Override
        public PartialFunction<Throwable, Directive> decider() {
            return DeciderBuilder.match(Exception.class, e -> SupervisorStrategy.stop()).build();
        }

        @Override
        public boolean loggingEnabled() {
            return false;
        }

        @Override
        public void handleChildTerminated(
                org.apache.pekko.actor.ActorContext context,
                ActorRef child,
                Iterable<ActorRef> children) {
            rpcActorTerminated(child);
        }

        @Override
        public void processFailure(
                org.apache.pekko.actor.ActorContext context,
                boolean restart,
                ActorRef child,
                Throwable cause,
                ChildRestartStats stats,
                Iterable<ChildRestartStats> children) {
            Preconditions.checkArgument(
                    !restart, "The supervisor strategy should never restart an actor.");

            rpcActorFailed(child, cause);
        }
    }

    private static final class RpcActorRegistration {
        private final String endpointId;

        private final CompletableFuture<Void> internalTerminationFuture;

        private final CompletableFuture<Void> externalTerminationFuture;

        @Nullable private Throwable errorCause;

        private RpcActorRegistration(String endpointId) {
            this.endpointId = endpointId;
            internalTerminationFuture = new CompletableFuture<>();
            externalTerminationFuture = new CompletableFuture<>();
            errorCause = null;
        }

        private CompletableFuture<Void> getInternalTerminationFuture() {
            return internalTerminationFuture;
        }

        private CompletableFuture<Void> getExternalTerminationFuture() {
            return externalTerminationFuture;
        }

        private String getEndpointId() {
            return endpointId;
        }

        private void terminate(Executor terminationFutureExecutor) {
            CompletableFuture<Void> terminationFuture = internalTerminationFuture;

            if (errorCause != null) {
                if (!internalTerminationFuture.completeExceptionally(errorCause)) {
                    // we have another failure reason -> let's add it
                    terminationFuture =
                            internalTerminationFuture.handle(
                                    (ignored, throwable) -> {
                                        if (throwable != null) {
                                            errorCause.addSuppressed(throwable);
                                        }

                                        throw new CompletionException(errorCause);
                                    });
                }
            } else {
                internalTerminationFuture.completeExceptionally(
                        new RpcException(
                                String.format(
                                        "RpcEndpoint %s did not complete the internal termination future.",
                                        endpointId)));
            }

            FutureUtils.forwardAsync(
                    terminationFuture, externalTerminationFuture, terminationFutureExecutor);
        }

        private void terminateExceptionally(Throwable cause, Executor terminationFutureExecutor) {
            terminationFutureExecutor.execute(
                    () -> externalTerminationFuture.completeExceptionally(cause));
        }

        public void markFailed(Throwable cause) {
            if (errorCause == null) {
                errorCause = cause;
            } else {
                errorCause.addSuppressed(cause);
            }
        }
    }

    // -----------------------------------------------------------------------------
    // Messages
    // -----------------------------------------------------------------------------

    //一个实体消息
    static final class StartRpcActor {
        private final PropsFactory propsFactory;
        private final String endpointId;

        private StartRpcActor(PropsFactory propsFactory, String endpointId) {
            this.propsFactory = propsFactory;
            this.endpointId = endpointId;
        }

        public String getEndpointId() {
            return endpointId;
        }

        public PropsFactory getPropsFactory() {
            return propsFactory;
        }

        private static StartRpcActor create(PropsFactory propsFactory, String endpointId) {
            //
            return new StartRpcActor(propsFactory, endpointId);
        }

        interface PropsFactory {
            // 被 startRpcActor.getPropsFactory().create 调用
            Props create(CompletableFuture<Void> terminationFuture);
        }
    }

    static final class ActorRegistration {
        private final ActorRef actorRef;
        private final CompletableFuture<Void> terminationFuture;

        private ActorRegistration(ActorRef actorRef, CompletableFuture<Void> terminationFuture) {
            this.actorRef = actorRef;
            this.terminationFuture = terminationFuture;
        }

        public ActorRef getActorRef() {
            return actorRef;
        }

        public CompletableFuture<Void> getTerminationFuture() {
            return terminationFuture;
        }

        public static ActorRegistration create(
                ActorRef actorRef, CompletableFuture<Void> terminationFuture) {
            return new ActorRegistration(actorRef, terminationFuture);
        }
    }

    static final class StartRpcActorResponse {
        @Nullable private final ActorRegistration actorRegistration;

        @Nullable private final Throwable error;

        private StartRpcActorResponse(
                @Nullable ActorRegistration actorRegistration, @Nullable Throwable error) {
            this.actorRegistration = actorRegistration;
            this.error = error;
        }

        public <X extends Throwable> ActorRegistration orElseThrow(
                Function<? super Throwable, ? extends X> throwableFunction) throws X {
            if (actorRegistration != null) {
                return actorRegistration;
            } else {
                throw throwableFunction.apply(error);
            }
        }

        public static StartRpcActorResponse success(ActorRegistration actorRegistration) {
            return new StartRpcActorResponse(actorRegistration, null);
        }

        public static StartRpcActorResponse failure(Throwable error) {
            return new StartRpcActorResponse(null, error);
        }
    }
}
