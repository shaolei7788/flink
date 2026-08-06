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

package org.apache.flink.runtime.rpc.pekko;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.concurrent.pekko.ActorSystemScheduledExecutorAdapter;
import org.apache.flink.runtime.concurrent.pekko.ScalaFutureUtils;
import org.apache.flink.runtime.rpc.FencedRpcEndpoint;
import org.apache.flink.runtime.rpc.FencedRpcGateway;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcServer;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.exceptions.RpcConnectionException;
import org.apache.flink.runtime.rpc.exceptions.RpcRuntimeException;
import org.apache.flink.runtime.rpc.messages.HandshakeSuccessMessage;
import org.apache.flink.runtime.rpc.messages.RemoteHandshakeMessage;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSelection;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.actor.Address;
import org.apache.pekko.actor.DeadLetter;
import org.apache.pekko.actor.Props;
import org.apache.pekko.pattern.Patterns;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import scala.Option;
import scala.reflect.ClassTag$;

import static org.apache.flink.runtime.concurrent.ClassLoadingUtils.guardCompletionWithContextClassLoader;
import static org.apache.flink.runtime.concurrent.ClassLoadingUtils.runWithContextClassLoader;
import static org.apache.flink.runtime.concurrent.ClassLoadingUtils.withContextClassLoader;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Pekko based {@link RpcService} implementation. The RPC service starts an actor to receive RPC
 * invocations from a {@link RpcGateway}.
 */
//充当了 Flink 代码与底层 Pekko Actor 模型之间的桥梁
@ThreadSafe
public class PekkoRpcService implements RpcService {

    private static final Logger LOG = LoggerFactory.getLogger(PekkoRpcService.class);

    static final int VERSION = 2;

    private final Object lock = new Object();
    //Flink 所有的 RPC 通信、组件（如 JobMaster、ResourceManager）作为 Actor 启动时，都必须注册并寄生在这个 actorSystem 中
    //这是最核心的属性。它是 Pekko 提供的一个重量级对象，负责管理所有的 Actor（每个 Flink 的 RpcEndpoint 底层都对应一个 Pekko Actor）。
    // 它控制了线程池（Dispatcher）、网络通信（Remoting）以及消息的序列化
    private final ActorSystem actorSystem;
    //RPC 服务的配置项合集。包含了通信的超时时间（timeout）、最大消息体积（maximum framesize，防止大消息打爆网络）、以及心跳间隔等重要参数
    private final PekkoRpcServiceConfiguration configuration;

    private final ClassLoader flinkClassLoader;

    //维护本地运行的物理 Actor（ActorRef）与 Flink 逻辑组件（RpcEndpoint）之间的双向映射关系。用于确保当收到消息时，能精准路由或用于本地状态检查
    @GuardedBy("lock")
    private final Map<ActorRef, RpcEndpoint> actors = CollectionUtil.newHashMapWithExpectedSize(4);

    //当前 RPC 服务的绑定地址和端口
    private final String address;
    private final int port;

    private final boolean captureAskCallstacks;

    //内部的定时任务调度器。由于网络通信经常涉及“超时重试”、“周期性心跳”，这个调度器用来向底层的 Actor 系统提交定时任务
    private final ScheduledExecutor internalScheduledExecutor;

    //一个异步凭证，用于追踪当前 RPC 服务是否已经完全关闭。Flink 很多组件的优雅退出依赖于这个 Future
    private final CompletableFuture<Void> terminationFuture;

    //专门监控该 RpcService 下属的所有子 Actor，一旦它们发生致命崩溃，由该属性对应的 Supervisor 捕获并强制抛出故障（Fail-Fast）
    private final Supervisor supervisor;

    //标记当前 RPC 服务的生命周期状态。用于防止在服务关闭期间重复初始化或接受新的 RPC 调用
    private volatile boolean stopped;

    //没被使用
    @VisibleForTesting
    public PekkoRpcService(final ActorSystem actorSystem, final PekkoRpcServiceConfiguration configuration) {
        this(actorSystem, configuration, PekkoRpcService.class.getClassLoader());
    }

    PekkoRpcService(
            final ActorSystem actorSystem,
            final PekkoRpcServiceConfiguration configuration,
            final ClassLoader flinkClassLoader) {
        this.actorSystem = checkNotNull(actorSystem, "actor system");
        this.configuration = checkNotNull(configuration, "pekko rpc service configuration");
        this.flinkClassLoader = checkNotNull(flinkClassLoader, "flinkClassLoader");
        // actorSystemAddress = pekko.tcp://flink@localhost:6123
        Address actorSystemAddress = PekkoUtils.getAddress(actorSystem);

        if (actorSystemAddress.host().isDefined()) {
            address = actorSystemAddress.host().get();// address = localhost
        } else {
            address = "";
        }

        if (actorSystemAddress.port().isDefined()) {
            port = (Integer) actorSystemAddress.port().get();//6123
        } else {
            port = -1;
        }

        captureAskCallstacks = configuration.captureAskCallStack();

        // Pekko always sets the threads context class loader to the class loader with which it was
        // loaded (i.e., the plugin class loader)
        // we must ensure that the context class loader is set to the Flink class loader when we
        // call into Flink
        // otherwise we could leak the plugin class loader or poison the context class loader of
        // external threads (because they inherit the current threads context class loader)
        internalScheduledExecutor = new ActorSystemScheduledExecutorAdapter(actorSystem, flinkClassLoader);

        terminationFuture = new CompletableFuture<>();

        stopped = false;

        supervisor = startSupervisorActor();//
        startDeadLettersActor();
    }

    private void startDeadLettersActor() {
        final ActorRef deadLettersActor =
                actorSystem.actorOf(DeadLettersActor.getProps(), "deadLettersActor");
        actorSystem.eventStream().subscribe(deadLettersActor, DeadLetter.class);
    }

    private Supervisor startSupervisorActor() {
        final ExecutorService terminationFutureExecutor =
                Executors.newSingleThreadExecutor(
                        new ExecutorThreadFactory(
                                "RpcService-Supervisor-Termination-Future-Executor"));
        final ActorRef actorRef =
                SupervisorActor.startSupervisorActor(
                        actorSystem,
                        withContextClassLoader(terminationFutureExecutor, flinkClassLoader));
        //
        return Supervisor.create(actorRef, terminationFutureExecutor);//
    }

    public ActorSystem getActorSystem() {
        return actorSystem;
    }

    protected int getVersion() {
        return VERSION;
    }

    @Override
    public String getAddress() {
        return address;
    }

    @Override
    public int getPort() {
        return port;
    }

    public <C extends RpcGateway> C getSelfGateway(Class<C> selfGatewayType, RpcServer rpcServer) {
        if (selfGatewayType.isInstance(rpcServer)) {
            @SuppressWarnings("unchecked")
            C selfGateway = ((C) rpcServer);

            return selfGateway;
        } else {
            throw new ClassCastException(
                    "RpcEndpoint does not implement the RpcGateway interface of type "
                            + selfGatewayType
                            + '.');
        }
    }

    //底层逻辑：如果 TaskManager 知道了 JobManager 的地址，就会调用此方法。Pekko 会解析地址，并返回一个 RpcGateway（比如 ResourceManagerGateway）。
    // TaskManager 拿着这个 Gateway 调用方法（比如 registerTaskManager），实际上是在底层把方法名和参数序列化成网络消息发给了 JobManager
    //根据给定的 Pekko 地址（如 pekko.tcp://...），去寻找远端组件的 ActorRef，并基于 JDK 动态代理技术生成对应的 RpcGateway 实例（如 ResourceManagerGateway）
    //PekkoInvocationHandler也实现了RpcGateway、RpcServer
    // this method does not mutate state and is thus thread-safe
    @Override
    public <C extends RpcGateway> CompletableFuture<C> connect(final String address, final Class<C> clazz) {

        return connectInternal(//
                address,
                clazz,
                (ActorRef actorRef) -> {
                    Tuple2<String, String> addressHostname = extractAddressHostname(actorRef);
                    //
                    return new PekkoInvocationHandler(
                            addressHostname.f0,
                            addressHostname.f1,
                            actorRef,
                            configuration.getTimeout(),
                            configuration.getMaximumFramesize(),
                            configuration.isForceRpcInvocationSerialization(),
                            null,
                            captureAskCallstacks,
                            flinkClassLoader);
                });
    }

    //作用：与 connect 类似，但带有 FencingToken（隔离令牌）。
    //底层逻辑：这是 Flink 解决脑裂（Split-Brain）问题的关键。连接高可用（HA）组件时必须带上当前的 Leader Token，如果 Token 不匹配，消息会被直接丢弃
    // this method does not mutate state and is thus thread-safe
    @Override
    public <F extends Serializable, C extends FencedRpcGateway<F>> CompletableFuture<C> connect(String address, F fencingToken, Class<C> clazz) {
        //
        return connectInternal(//
                address,
                clazz,
                // actorRef 会有参数传过来
                (ActorRef actorRef) -> {
                    Tuple2<String, String> addressHostname = extractAddressHostname(actorRef);
                    //
                    return new FencedPekkoInvocationHandler<>(
                            addressHostname.f0,
                            addressHostname.f1,
                            actorRef,
                            configuration.getTimeout(),
                            configuration.getMaximumFramesize(),
                            configuration.isForceRpcInvocationSerialization(),
                            null,
                            () -> fencingToken,
                            captureAskCallstacks,
                            flinkClassLoader);
                });
    }


    //作用：将 Flink 的业务组件（如 TaskManager、ResourceManager）启动为 RPC 服务。
    //底层逻辑：它会把传入的 RpcEndpoint 包装成一个 PekkoRpcActor 并注册到 ActorSystem 中。
    // 随后返回一个 RpcServer（动态代理），组件通过这个代理就可以接收网络消息了
    @Override
    public <C extends RpcEndpoint & RpcGateway> RpcServer startServer(C rpcEndpoint, Map<String, String> loggingContext) {
        checkNotNull(rpcEndpoint, "rpc endpoint");
        //todo ?  它会把传入的 RpcEndpoint 包装成一个 PekkoRpcActor 并注册到 ActorSystem 中
        final SupervisorActor.ActorRegistration actorRegistration = registerRpcActor(rpcEndpoint, loggingContext);
        final ActorRef actorRef = actorRegistration.getActorRef();
        final CompletableFuture<Void> actorTerminationFuture = actorRegistration.getTerminationFuture();

        LOG.info(
                "Starting RPC endpoint for {} at {} .",
                rpcEndpoint.getClass().getName(),
                actorRef.path());
        // address =
        final String address = PekkoUtils.getRpcURL(actorSystem, actorRef);
        final String hostname;
        Option<String> host = actorRef.path().address().host();
        if (host.isEmpty()) {
            hostname = "localhost";
        } else {
            hostname = host.get();
        }

        Set<Class<? extends RpcGateway>> implementedRpcGateways =
                RpcUtils.extractImplementedRpcGateways(rpcEndpoint.getClass());

        implementedRpcGateways.add(RpcServer.class);
        implementedRpcGateways.add(PekkoBasedEndpoint.class);

        final InvocationHandler invocationHandler;
        System.out.println("PekkoRpcService#startServer:" + rpcEndpoint.getClass().getName());
        if (rpcEndpoint instanceof FencedRpcEndpoint) {
            // a FencedRpcEndpoint needs a FencedPekkoInvocationHandler
            // JobMaster
            // Dispatcher
            // ResourceManager
            // StandaloneResourceManager
            // 这四个继承了 FencedRpcEndpoint
            invocationHandler =
                    new FencedPekkoInvocationHandler<>(
                            address,
                            hostname,
                            actorRef,
                            configuration.getTimeout(),
                            configuration.getMaximumFramesize(),
                            configuration.isForceRpcInvocationSerialization(),
                            actorTerminationFuture,
                            ((FencedRpcEndpoint<?>) rpcEndpoint)::getFencingToken,
                            captureAskCallstacks,
                            flinkClassLoader);
        } else {
            // TaskExecutor 没有继承 FencedRpcEndpoint
            invocationHandler =
                    new PekkoInvocationHandler(
                            address,//pekko://flink/user/rpc/taskmanager_0
                            hostname,//localhost
                            actorRef,// Actor[pekko://flink/user/rpc/taskmanager_0#1857661143]
                            configuration.getTimeout(),//300
                            configuration.getMaximumFramesize(),//
                            configuration.isForceRpcInvocationSerialization(),
                            actorTerminationFuture,
                            captureAskCallstacks,//true
                            flinkClassLoader);//
        }

        // Rather than using the System ClassLoader directly, we derive the ClassLoader
        // from this class . That works better in cases where Flink runs embedded and all Flink
        // code is loaded dynamically (for example from an OSGI bundle) through a custom ClassLoader
        ClassLoader classLoader = getClass().getClassLoader();

        @SuppressWarnings("unchecked")
        RpcServer server =
                (RpcServer)
                        Proxy.newProxyInstance(
                                classLoader,
                                implementedRpcGateways.toArray(
                                        new Class<?>[implementedRpcGateways.size()]),
                                invocationHandler);

        return server;
    }

    private <C extends RpcEndpoint & RpcGateway> SupervisorActor.ActorRegistration registerRpcActor(
            C rpcEndpoint, Map<String, String> loggingContext) {
        final Class<? extends AbstractActor> rpcActorType;

        if (rpcEndpoint instanceof FencedRpcEndpoint) {
            rpcActorType = FencedPekkoRpcActor.class;
        } else {
            rpcActorType = PekkoRpcActor.class;
        }

        synchronized (lock) {
            checkState(!stopped, "RpcService is stopped");

            final SupervisorActor.StartRpcActorResponse startRpcActorResponse =
                    SupervisorActor.startRpcActor(
                            supervisor.getActor(),
                            actorTerminationFuture ->
                                    Props.create(
                                            rpcActorType,
                                            rpcEndpoint,
                                            actorTerminationFuture,
                                            getVersion(),
                                            configuration.getMaximumFramesize(),
                                            configuration.isForceRpcInvocationSerialization(),
                                            flinkClassLoader,
                                            loggingContext),
                            rpcEndpoint.getEndpointId());

            final SupervisorActor.ActorRegistration actorRegistration =
                    startRpcActorResponse.orElseThrow(
                            cause ->
                                    new RpcRuntimeException(
                                            String.format(
                                                    "Could not create the %s for %s.",
                                                    PekkoRpcActor.class.getSimpleName(),
                                                    rpcEndpoint.getEndpointId()),
                                            cause));

            actors.put(actorRegistration.getActorRef(), rpcEndpoint);

            return actorRegistration;
        }
    }

    //作用：停止指定的 RPC 服务节点。它会向底层的 Actor 发送毒药消息（PoisonPill 或 stop 指令），让其停止接收新消息并销毁。
    //stopService() / closeAsync()
    //作用：关闭整个 PekkoRpcService。通常在 JVM 退出或 TaskManager/JobManager 进程销毁时调用，它会关闭整个 ActorSystem，释放绑定的端口和网络资源
    @Override
    public void stopServer(RpcServer selfGateway) {
        if (selfGateway instanceof PekkoBasedEndpoint) {
            final PekkoBasedEndpoint client = (PekkoBasedEndpoint) selfGateway;
            final RpcEndpoint rpcEndpoint;

            synchronized (lock) {
                if (stopped) {
                    return;
                } else {
                    rpcEndpoint = actors.remove(client.getActorRef());
                }
            }

            if (rpcEndpoint != null) {
                terminateRpcActor(client.getActorRef(), rpcEndpoint);
            } else {
                LOG.debug(
                        "RPC endpoint {} already stopped or from different RPC service",
                        selfGateway.getAddress());
            }
        }
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        final CompletableFuture<Void> rpcActorsTerminationFuture;

        synchronized (lock) {
            if (stopped) {
                return terminationFuture;
            }

            LOG.info("Stopping Pekko RPC service.");

            stopped = true;

            rpcActorsTerminationFuture = terminateRpcActors();
        }

        final CompletableFuture<Void> supervisorTerminationFuture =
                FutureUtils.composeAfterwards(rpcActorsTerminationFuture, supervisor::closeAsync);

        final CompletableFuture<Void> actorSystemTerminationFuture =
                FutureUtils.composeAfterwards(
                        supervisorTerminationFuture,
                        () -> ScalaFutureUtils.toJava(actorSystem.terminate()));

        actorSystemTerminationFuture.whenComplete(
                (Void ignored, Throwable throwable) -> {
                    runWithContextClassLoader(
                            () -> FutureUtils.doForward(ignored, throwable, terminationFuture),
                            flinkClassLoader);

                    LOG.info("Stopped Pekko RPC service.");
                });

        return terminationFuture;
    }

    @GuardedBy("lock")
    @Nonnull
    private CompletableFuture<Void> terminateRpcActors() {
        final Collection<CompletableFuture<Void>> rpcActorTerminationFutures =
                new ArrayList<>(actors.size());

        for (Map.Entry<ActorRef, RpcEndpoint> actorRefRpcEndpointEntry : actors.entrySet()) {
            rpcActorTerminationFutures.add(
                    terminateRpcActor(
                            actorRefRpcEndpointEntry.getKey(),
                            actorRefRpcEndpointEntry.getValue()));
        }
        actors.clear();

        return FutureUtils.waitForAll(rpcActorTerminationFutures);
    }

    private CompletableFuture<Void> terminateRpcActor(
            ActorRef rpcActorRef, RpcEndpoint rpcEndpoint) {
        rpcActorRef.tell(ControlMessages.TERMINATE, ActorRef.noSender());

        return rpcEndpoint.getTerminationFuture();
    }

    @Override
    public ScheduledExecutor getScheduledExecutor() {
        return internalScheduledExecutor;
    }

    // ---------------------------------------------------------------------------------------
    // Private helper methods
    // ---------------------------------------------------------------------------------------

    private Tuple2<String, String> extractAddressHostname(ActorRef actorRef) {
        final String actorAddress = PekkoUtils.getRpcURL(actorSystem, actorRef);
        final String hostname;
        Option<String> host = actorRef.path().address().host();
        if (host.isEmpty()) {
            hostname = "localhost";
        } else {
            hostname = host.get();
        }

        return Tuple2.of(actorAddress, hostname);
    }

    private <C extends RpcGateway> CompletableFuture<C> connectInternal(
            final String address,
            final Class<C> clazz,
            Function<ActorRef, InvocationHandler> invocationHandlerFactory) {
        checkState(!stopped, "RpcService is stopped");

        LOG.debug(
                "Try to connect to remote RPC endpoint with address {}. Returning a {} gateway.",
                address,
                clazz.getName());
        // 1. 异步获取底层的 Pekko Actor 引用
        final CompletableFuture<ActorRef> actorRefFuture = resolveActorAddress(address);

        final CompletableFuture<HandshakeSuccessMessage> handshakeFuture = actorRefFuture.thenCompose(
                        (ActorRef actorRef) ->
                                ScalaFutureUtils.toJava(
                                        Patterns.ask(
                                                        actorRef,
                                                        new RemoteHandshakeMessage(clazz, getVersion()),
                                                        configuration.getTimeout().toMillis())
                                                .<HandshakeSuccessMessage>mapTo(
                                                        ClassTag$.MODULE$
                                                                .<HandshakeSuccessMessage>apply(
                                                                        HandshakeSuccessMessage
                                                                                .class))));

        final CompletableFuture<C> gatewayFuture = actorRefFuture.thenCombineAsync(
                        handshakeFuture,
                        (ActorRef actorRef, HandshakeSuccessMessage ignored) -> {
                            //会调用 PekkoRpcService#connect connectInternal方法 的 (ActorRef actorRef) -> {}
                            //通过工厂获取 FencedPekkoInvocationHandler 对象
                            InvocationHandler invocationHandler = invocationHandlerFactory.apply(actorRef);

                            // Rather than using the System ClassLoader directly, we derive the
                            // ClassLoader from this class.
                            // That works better in cases where Flink runs embedded and
                            // all Flink code is loaded dynamically
                            // (for example from an OSGI bundle) through a custom ClassLoader
                            ClassLoader classLoader = getClass().getClassLoader();

                            @SuppressWarnings("unchecked")
                            C proxy =
                                    (C)
                                            Proxy.newProxyInstance(
                                                    classLoader,
                                                    new Class<?>[] {clazz},
                                                    invocationHandler);

                            return proxy;
                        },
                        actorSystem.dispatcher());

        return guardCompletionWithContextClassLoader(gatewayFuture, flinkClassLoader);
    }

    private CompletableFuture<ActorRef> resolveActorAddress(String address) {
        final ActorSelection actorSel = actorSystem.actorSelection(address);

        return actorSel.resolveOne(configuration.getTimeout())
                .toCompletableFuture()
                .exceptionally(
                        error -> {
                            throw new CompletionException(
                                    new RpcConnectionException(
                                            String.format(
                                                    "Could not connect to rpc endpoint under address %s.",
                                                    address),
                                            error));
                        });
    }

    // ---------------------------------------------------------------------------------------
    // Private inner classes
    // ---------------------------------------------------------------------------------------

    private static final class Supervisor implements AutoCloseableAsync {

        private final ActorRef actor;

        private final ExecutorService terminationFutureExecutor;

        private Supervisor(ActorRef actor, ExecutorService terminationFutureExecutor) {
            this.actor = actor;
            this.terminationFutureExecutor = terminationFutureExecutor;
        }

        private static Supervisor create(ActorRef actorRef, ExecutorService terminationFutureExecutor) {
            return new Supervisor(actorRef, terminationFutureExecutor);
        }

        public ActorRef getActor() {
            return actor;
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            return ExecutorUtils.nonBlockingShutdown(30L, TimeUnit.SECONDS, terminationFutureExecutor);
        }
    }
}
