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

package org.apache.flink.runtime.webmonitor.retriever.impl;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.rpc.FencedRpcGateway;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.RetryStrategy;

import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * {@link LeaderGatewayRetriever} implementation using the {@link RpcService}.
 *
 * @param <F> type of the fencing token
 * @param <T> type of the fenced gateway to retrieve
 */
//将高可用组件（HA Services）推送过来的、变动频繁的“远程 Leader RPC 字符串地址（String Address）”，
// 自动且线程安全地“翻译并包装”成可以直接调用 Java 方法的“RPC 网关实例（Gateway）
public class RpcGatewayRetriever<F extends Serializable, T extends FencedRpcGateway<F>>
        extends LeaderGatewayRetriever<T> {

    //底层底座服务。用于在得知最新 Leader 地址字符串后，调用其网络连接方法（如 connect）跨网络去抓取并建立真实的 Akka/Netty 远程连接通道
    private final RpcService rpcService;
    //指明当前拦截器具体要把地址翻译成哪种网关类型
    private final Class<T> gatewayType;
    private final Function<UUID, F> fencingTokenMapper;
    private final RetryStrategy retryStrategy;

    public RpcGatewayRetriever(
            RpcService rpcService,
            //DispatcherGateway.class
            //ResourceManagerGateway.class
            Class<T> gatewayType,
            Function<UUID, F> fencingTokenMapper,
            RetryStrategy retryStrategy) {
        this.rpcService = Preconditions.checkNotNull(rpcService);
        this.gatewayType = Preconditions.checkNotNull(gatewayType);
        this.fencingTokenMapper = Preconditions.checkNotNull(fencingTokenMapper);
        this.retryStrategy = Preconditions.checkNotNull(retryStrategy);
    }


    @Override
    protected CompletableFuture<T> createGateway(CompletableFuture<Tuple2<String, UUID>> leaderFuture) {
        //跟指定leader地址建立rpc连接
        return FutureUtils.retryWithDelay(
                () ->
                        leaderFuture.thenCompose(
                                (Tuple2<String, UUID> addressLeaderTuple) ->
                                        //PekkoRpcService#connect
                                        rpcService.connect(
                                                // addressLeaderTuple =
                                                addressLeaderTuple.f0,
                                                fencingTokenMapper.apply(addressLeaderTuple.f1),
                                                gatewayType)),
                retryStrategy,
                rpcService.getScheduledExecutor());
    }
}
