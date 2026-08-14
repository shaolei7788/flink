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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.PartitionRequestClient;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class NettyConnectionManager implements ConnectionManager {

    private final NettyServer server;

    private final NettyClient client;

    private final NettyBufferPool bufferPool;

    private final PartitionRequestClientFactory partitionRequestClientFactory;

    private final NettyProtocol nettyProtocol;

    public NettyConnectionManager(//
            ResultPartitionProvider partitionProvider,
            TaskEventPublisher taskEventPublisher,
            NettyConfig nettyConfig,
            boolean connectionReuseEnabled) {//true
        this(
                new NettyBufferPool(nettyConfig.getNumberOfArenas()),
                partitionProvider,
                taskEventPublisher,
                nettyConfig,
                connectionReuseEnabled);
    }

    @VisibleForTesting
    public NettyConnectionManager(//
            NettyBufferPool bufferPool,
            //ResultPartitionManager
            ResultPartitionProvider partitionProvider,
            //TaskEventDispatcher
            TaskEventPublisher taskEventPublisher,
            NettyConfig nettyConfig,
            boolean connectionReuseEnabled) {//true
        //作为服务端。负责监听网络端口，等待其他 TaskManager（下游）连进来索要数据
        this.server = new NettyServer(nettyConfig);
        //作为客户端。负责主动作出对外连接，连向其他 TaskManager（上游）去拉取/接收数据
        this.client = new NettyClient(nettyConfig);
        this.bufferPool = checkNotNull(bufferPool);
        //背景：如果下游 TaskManager 有 10 个 Task 都要从上游同一个 TaskManager 拉数据，如果建 10 个 TCP 连接，会造成巨大的网络句柄浪费和协议开销
        //当 connectionReuseEnabled 为 true 时，PartitionRequestClientFactory 内部会维护一个连接缓存池。
        // 当不同的 Task 发起请求时，工厂会强行让它们复用同一个底层的 TCP Channel 通道，仅在应用层通过 InputChannelID 进行多路复用
        this.partitionRequestClientFactory = new PartitionRequestClientFactory(client, nettyConfig.getNetworkRetries(), connectionReuseEnabled);

        this.nettyProtocol = new NettyProtocol(checkNotNull(partitionProvider), checkNotNull(taskEventPublisher));//
    }

    @Override
    public int start() throws IOException {
        client.init(nettyProtocol, bufferPool);//

        return server.init(nettyProtocol, bufferPool);//
    }

    @Override
    public PartitionRequestClient createPartitionRequestClient(ConnectionID connectionId)
            throws IOException, InterruptedException {
        return partitionRequestClientFactory.createPartitionRequestClient(connectionId);//
    }

    @Override
    public void closeOpenChannelConnections(ConnectionID connectionId) {
        partitionRequestClientFactory.closeOpenChannelConnections(connectionId);
    }

    @Override
    public int getNumberOfActiveConnections() {
        return partitionRequestClientFactory.getNumberOfActiveClients();
    }

    @Override
    public void shutdown() {
        client.shutdown();
        server.shutdown();
    }

    NettyClient getClient() {
        return client;
    }

    NettyServer getServer() {
        return server;
    }

    NettyBufferPool getBufferPool() {
        return bufferPool;
    }
}
