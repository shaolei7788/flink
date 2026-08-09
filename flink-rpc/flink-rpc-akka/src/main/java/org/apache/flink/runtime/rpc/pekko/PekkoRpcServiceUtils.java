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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RpcOptions;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.runtime.rpc.AddressResolution;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcSystem;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TemporaryClassLoaderContext;
import org.apache.flink.util.function.TriFunction;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.pekko.actor.ActorSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Optional;

import static org.apache.flink.util.NetUtils.isValidClientPort;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * These RPC utilities contain helper methods around RPC use, such as starting an RPC service, or
 * constructing RPC addresses.
 */
public class PekkoRpcServiceUtils {

    private static final Logger LOG = LoggerFactory.getLogger(PekkoRpcServiceUtils.class);

    private static final String PEKKO_TCP = "pekko.tcp";
    private static final String PEKKO_SSL_TCP = "pekko.ssl.tcp";

    static final String SUPERVISOR_NAME = "rpc";

    private static final String SIMPLE_CONFIG_TEMPLATE =
            "pekko {remote.classic {netty.tcp {maximum-frame-size = %s}}}";

    private static final String MAXIMUM_FRAME_SIZE_PATH =
            "pekko.remote.classic.netty.tcp.maximum-frame-size";

    // ------------------------------------------------------------------------
    //  RPC instantiation
    // ------------------------------------------------------------------------
    //todo 自己加的public
    public static PekkoRpcService createRemoteRpcService(
            Configuration configuration,
            @Nullable String externalAddress,
            String externalPortRange,
            @Nullable String bindAddress,
            @SuppressWarnings("OptionalUsedAsFieldOrParameterType") Optional<Integer> bindPort)
            throws Exception {
        final PekkoRpcServiceBuilder rpcServiceBuilder = PekkoRpcServiceUtils.remoteServiceBuilder(configuration, externalAddress, externalPortRange);

        if (bindAddress != null) {
            rpcServiceBuilder.withBindAddress(bindAddress);
        }

        bindPort.ifPresent(rpcServiceBuilder::withBindPort);
        //standalone 模式不会被调用
        return rpcServiceBuilder.createAndStart();
    }

    static PekkoRpcServiceBuilder remoteServiceBuilder(
            Configuration configuration,
            @Nullable String externalAddress,
            String externalPortRange) {
        return new PekkoRpcServiceBuilder(configuration, LOG, externalAddress, externalPortRange);//
    }

    @VisibleForTesting
    static PekkoRpcServiceBuilder remoteServiceBuilder(
            Configuration configuration, @Nullable String externalAddress, int externalPort) {
        return remoteServiceBuilder(configuration, externalAddress, String.valueOf(externalPort));
    }

    static PekkoRpcServiceBuilder localServiceBuilder(Configuration configuration) {
        return new PekkoRpcServiceBuilder(configuration, LOG);
    }

    // ------------------------------------------------------------------------
    //  RPC endpoint addressing
    // ------------------------------------------------------------------------

    /**
     * @param hostname The hostname or address where the target RPC service is listening.
     * @param port The port where the target RPC service is listening.
     * @param endpointName The name of the RPC endpoint.
     * @param addressResolution Whether to try address resolution of the given hostname or not. This
     *     allows to fail fast in case that the hostname cannot be resolved.
     * @param config The configuration from which to deduce further settings.
     * @return The RPC URL of the specified RPC endpoint.
     */
    public static String getRpcUrl(
            String hostname,
            int port,
            String endpointName,
            AddressResolution addressResolution,
            Configuration config)
            throws UnknownHostException {

        checkNotNull(config, "config is null");

        final boolean sslEnabled = config.get(RpcOptions.SSL_ENABLED) && SecurityOptions.isInternalSSLEnabled(config);

        return getRpcUrl(//
                hostname,
                port,
                endpointName,
                addressResolution,
                sslEnabled ? Protocol.SSL_TCP : Protocol.TCP);
    }

    /**
     * @param hostname The hostname or address where the target RPC service is listening.
     * @param port The port where the target RPC service is listening.
     * @param endpointName The name of the RPC endpoint.
     * @param addressResolution Whether to try address resolution of the given hostname or not. This
     *     allows to fail fast in case that the hostname cannot be resolved.
     * @param protocol True, if security/encryption is enabled, false otherwise.
     * @return The RPC URL of the specified RPC endpoint.
     */
    //获取远程的url
    public static String getRpcUrl(
            String hostname,
            int port,
            String endpointName,
            AddressResolution addressResolution,
            Protocol protocol)
            throws UnknownHostException {

        checkNotNull(hostname, "hostname is null");
        checkNotNull(endpointName, "endpointName is null");
        checkArgument(isValidClientPort(port), "port must be in [1, 65535]");

        if (addressResolution == AddressResolution.TRY_ADDRESS_RESOLUTION) {
            // Fail fast if the hostname cannot be resolved
            //noinspection ResultOfMethodCallIgnored
            InetAddress.getByName(hostname);
        }

        final String hostPort = NetUtils.unresolvedHostAndPortToNormalizedString(hostname, port);
        // pekko.tcp://flink@localhost:6123/user/rpc/resourcemanager_*
        //TaskManager 启动时，必须知道去哪里报到。它会通过读取配置中的 jobmanager.rpc.address 和 jobmanager.rpc.port: 6123，跨网络给这个端口发送 RPC 请求
        //ResourceManager 会在这个端口上监听并接收 TaskManager 的注册信息，随后它们之间后续的所有底层心跳包，全都是通过 6123 这个网络通道进行传输。

        //你可能会奇怪，ResourceManager、Dispatcher、JobMaster 明明是不同的组件，为什么能共用 6123 这一个端口？
        // 这是因为 Flink 底层使用了 Akka / Netty 这一套基于 Actor 模型的 RPC 框架。6123 端口在操作系统层面，只对应一个统一的 RPC Endpoint（网关服务）。
        // 当请求发送到 6123 端口时，Flink 会在数据包里带上目标组件的路径（URL）。
        // 类似于 Web 服务的路由分配：
        //  路径里带有 /resourcemanager_* 就会被分发给 ResourceManager 处理器。
        //  路径里带有 /dispatcher_* 就会分发给 Dispatcher。
        //  路径里带有 /jobmaster_* 就会分发给对应的 JobMaster
        return internalRpcUrl(endpointName, Optional.of(new RemoteAddressInformation(hostPort, protocol)));//
    }

    //获取本地的url  pekko://flink/user/rpc/resourcemanager_*
    public static String getLocalRpcUrl(String endpointName) {
        return internalRpcUrl(endpointName, Optional.empty());
    }

    public static boolean isRecipientTerminatedException(Throwable exception) {
        return exception.getMessage().contains("had already been terminated.");
    }

    private static final class RemoteAddressInformation {
        private final String hostnameAndPort;
        private final Protocol protocol;

        private RemoteAddressInformation(String hostnameAndPort, Protocol protocol) {
            this.hostnameAndPort = hostnameAndPort;
            this.protocol = protocol;
        }

        private String getHostnameAndPort() {
            return hostnameAndPort;
        }

        private Protocol getProtocol() {
            return protocol;
        }
    }

    // pekko url 固定写法
    private static String internalRpcUrl(
            String endpointName, Optional<RemoteAddressInformation> remoteAddressInformation) {
        final String protocolPrefix =
                remoteAddressInformation
                        .map(rai -> protocolToString(rai.getProtocol()))
                        .orElse("pekko");
        final Optional<String> optionalHostnameAndPort =
                remoteAddressInformation.map(RemoteAddressInformation::getHostnameAndPort);
        // url = pekko.tcp://flink
        final StringBuilder url = new StringBuilder(String.format("%s://flink", protocolPrefix));
        // optionalHostnameAndPort = pekko.tcp://flink@localhost:6123
        optionalHostnameAndPort.ifPresent(hostPort -> url.append("@").append(hostPort));
        //
        url.append("/user/").append(SUPERVISOR_NAME).append("/").append(endpointName);
        // protocolPrefix://flink[@hostname:port]/user/rpc/endpointName
        // pekko.tcp://flink@localhost:6123/user/rpc/resourcemanager_*
        return url.toString();
    }

    private static String protocolToString(Protocol protocol) {
        return protocol == Protocol.SSL_TCP ? PEKKO_SSL_TCP : PEKKO_TCP;
    }

    /** Whether to use TCP or encrypted TCP for Pekko. */
    public enum Protocol {
        TCP,
        SSL_TCP
    }

    // ------------------------------------------------------------------------
    //  RPC service configuration
    // ------------------------------------------------------------------------

    public static long extractMaximumFramesize(Configuration configuration) {
        String maxFrameSizeStr = configuration.get(RpcOptions.FRAMESIZE);
        String configStr = String.format(SIMPLE_CONFIG_TEMPLATE, maxFrameSizeStr);
        Config config = ConfigFactory.parseString(configStr);
        return config.getBytes(MAXIMUM_FRAME_SIZE_PATH);
    }

    // ------------------------------------------------------------------------
    //  RPC service builder
    // ------------------------------------------------------------------------

    /** Builder for {@link PekkoRpcService}. */
    static class PekkoRpcServiceBuilder implements RpcSystem.RpcServiceBuilder {

        private final Configuration configuration;
        private final Logger logger;
        @Nullable private final String externalAddress;
        @Nullable private final String externalPortRange;

        private String actorSystemName = PekkoUtils.getFlinkActorSystemName();

        @Nullable private Config actorSystemExecutorConfiguration = null;

        @Nullable private Config customConfig = null;
        private String bindAddress = NetUtils.getWildcardIPAddress();
        @Nullable private Integer bindPort = null;

        /** Builder for creating a remote RPC service. */
        private PekkoRpcServiceBuilder(
                final Configuration configuration,
                final Logger logger,
                @Nullable final String externalAddress,
                final String externalPortRange) {
            this.configuration = Preconditions.checkNotNull(configuration);
            this.logger = Preconditions.checkNotNull(logger);
            this.externalAddress =
                    externalAddress == null
                            ? InetAddress.getLoopbackAddress().getHostAddress()
                            : externalAddress;
            this.externalPortRange = Preconditions.checkNotNull(externalPortRange);
        }

        /** Builder for creating a local RPC service. */
        private PekkoRpcServiceBuilder(final Configuration configuration, final Logger logger) {
            this.configuration = Preconditions.checkNotNull(configuration);
            this.logger = Preconditions.checkNotNull(logger);
            this.externalAddress = null;
            this.externalPortRange = null;
        }

        @Override
        public PekkoRpcServiceBuilder withComponentName(final String actorSystemName) {
            this.actorSystemName = Preconditions.checkNotNull(actorSystemName);
            return this;
        }

        public PekkoRpcServiceBuilder withCustomConfig(final Config customConfig) {
            this.customConfig = customConfig;
            return this;
        }

        @Override
        public PekkoRpcServiceBuilder withBindAddress(final String bindAddress) {
            this.bindAddress = Preconditions.checkNotNull(bindAddress);
            return this;
        }

        @Override
        public PekkoRpcServiceBuilder withBindPort(int bindPort) {
            Preconditions.checkArgument(
                    NetUtils.isValidHostPort(bindPort), "Invalid port number: " + bindPort);
            this.bindPort = bindPort;
            return this;
        }

        @Override
        public RpcSystem.RpcServiceBuilder withExecutorConfiguration(
                RpcSystem.FixedThreadPoolExecutorConfiguration executorConfiguration) {
            this.actorSystemExecutorConfiguration =
                    PekkoUtils.getThreadPoolExecutorConfig(executorConfiguration);
            return this;
        }

        @Override
        public RpcSystem.RpcServiceBuilder withExecutorConfiguration(
                RpcSystem.ForkJoinExecutorConfiguration executorConfiguration) {
            this.actorSystemExecutorConfiguration =
                    PekkoUtils.getForkJoinExecutorConfig(executorConfiguration);
            return this;
        }

        //创建PekkoRpcService 并启动     wordcount 会被MiniCluster#createLocalRpcService 调用
        public PekkoRpcService createAndStart() throws Exception {
            //
            return createAndStart(PekkoRpcService::new);//
        }

        public PekkoRpcService createAndStart(TriFunction<ActorSystem, PekkoRpcServiceConfiguration, ClassLoader, PekkoRpcService> constructor)
                throws Exception {
            if (actorSystemExecutorConfiguration == null) {
                actorSystemExecutorConfiguration =
                        PekkoUtils.getForkJoinExecutorConfig(
                                ActorSystemBootstrapTools.getForkJoinExecutorConfiguration(
                                        configuration));
            }

            final ActorSystem actorSystem;

            // pekko internally caches the context class loader
            // make sure it uses the plugin class loader
            try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(getClass().getClassLoader())) {
                //外部可达地址
                if (externalAddress == null) {
                    // create local actor system  创建本地的ActorSystem
                    actorSystem = ActorSystemBootstrapTools.startLocalActorSystem(
                                    configuration,
                                    actorSystemName,
                                    logger,
                                    actorSystemExecutorConfiguration,
                                    customConfig);
                } else {
                    // create remote actor system  创建远程的ActorSystem
                    actorSystem = ActorSystemBootstrapTools.startRemoteActorSystem(
                                    configuration,
                                    actorSystemName,//flink
                                    externalAddress,//127.0.0.1
                                    externalPortRange,//6123
                                    bindAddress,//0.0.0.0
                                    Optional.ofNullable(bindPort),//
                                    logger,
                                    actorSystemExecutorConfiguration,
                                    customConfig);
                }
            }
            //constructor.apply 会调用 PekkoRpcService::new 即 PekkoRpcService的构造器
            return constructor.apply(
                    actorSystem,
                    PekkoRpcServiceConfiguration.fromConfiguration(configuration),
                    RpcService.class.getClassLoader());
        }
    }

    // ------------------------------------------------------------------------

    /** This class is not meant to be instantiated. */
    private PekkoRpcServiceUtils() {}
}
