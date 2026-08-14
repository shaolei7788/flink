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

import org.apache.flink.runtime.io.network.NetworkClientHandler;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;

/** Defines the server and client channel handlers, i.e. the protocol, used by netty. */
public class NettyProtocol {

    //负责将 Flink 内部的高层 Java 对象（即 NettyMessage 的各种子类，
    // 如我们前面看到的 BufferResponse、ErrorResponse、PartitionRequest 等），序列化成网络网卡能直接发送的二进制字节流（ByteBuf）
    private final NettyMessage.NettyMessageEncoder messageEncoder = new NettyMessage.NettyMessageEncoder();

    //它是连接“Netty 网络层”与“Task 线程数据暂存区”的唯一官方接口
    //ResultPartitionManager
    private final ResultPartitionProvider partitionProvider;

    //负责把这个事件安全地路由到正在运行的、目标上游 Task 线程的事件队列中。
    //TaskEventDispatcher
    private final TaskEventPublisher taskEventPublisher;

    NettyProtocol(ResultPartitionProvider partitionProvider, TaskEventPublisher taskEventPublisher) {//
        this.partitionProvider = partitionProvider;
        this.taskEventPublisher = taskEventPublisher;
    }

    /**
     * Returns the server channel handlers.
     *
     * <pre>
     * +-------------------------------------------------------------------+
     * |                        SERVER CHANNEL PIPELINE                    |
     * |                                                                   |
     * |    +----------+----------+ (3) write  +----------------------+    |
     * |    | Queue of queues     +----------->| Message encoder      |    |
     * |    +----------+----------+            +-----------+----------+    |
     * |              /|\                                 \|/              |
     * |               | (2) enqueue                       |               |
     * |    +----------+----------+                        |               |
     * |    | Request handler     |                        |               |
     * |    +----------+----------+                        |               |
     * |              /|\                                  |               |
     * |               |                                   |               |
     * |   +-----------+-----------+                       |               |
     * |   | Message+Frame decoder |                       |               |
     * |   +-----------+-----------+                       |               |
     * |              /|\                                  |               |
     * +---------------+-----------------------------------+---------------+
     * |               | (1) client request               \|/
     * +---------------+-----------------------------------+---------------+
     * |               |                                   |               |
     * |       [ Socket.read() ]                    [ Socket.write() ]     |
     * |                                                                   |
     * |  Netty Internal I/O Threads (Transport Implementation)            |
     * +-------------------------------------------------------------------+
     * </pre>
     *
     * @return channel handlers
     */
    public ChannelHandler[] getServerChannelHandlers() {
        PartitionRequestQueue queueOfPartitionQueues = new PartitionRequestQueue();//

        //负责接收下游发来的控制命令协议
        PartitionRequestServerHandler serverHandler = new PartitionRequestServerHandler(partitionProvider, taskEventPublisher, queueOfPartitionQueues);//
        //场景一：当下游（Client）通过网络发来请求包时（入站 Inbound 流程）
        // 数据包从网卡进来，在 Pipeline 中从前往后依次传递：
        // 1. messageEncoder：它是出站处理器（Outbound），直接跳过。
        // 2. NettyMessageDecoder：将网络上的二进制字节流，解码/拆包成具体的 Flink 协议对象（例如 PartitionRequest 对象）。
        // 3. serverHandler：捕获解码后的请求对象。如果是拉取请求，在此处向 partitionProvider 申请创建 ViewReader，并通过发射 UserEvent 扔给后面的组件。
        // 4. queueOfPartitionQueues：捕获前一步传来的事件（触发 userEventTriggered），单线程无锁地将 ViewReader 排入活跃队列

        // 场景二：当上游（Server）准备往下游发送数据时（出站 Outbound 流程）
        // 当触发数据发送时，由队列尾部的 queueOfPartitionQueues 发起驱动，数据在 Pipeline 中从后往前【反向传递】：
        // 1. queueOfPartitionQueues：在其核心方法中，调用 getNextBuffer() 捞出零拷贝的 BufferAndAvailability，组装成高层的 BufferResponse 业务对象，然后调用 ctx.writeAndFlush(msg)。
        // 2. serverHandler 和 NettyMessageDecoder：它们两个纯粹是入站处理器（Inbound），出站流动时直接无感穿透。
        // 3. messageEncoder：作为出站处理器（Outbound），在数据的最后一站拦截到 BufferResponse 对象，将其序列化为二进制网卡字节，最终彻底刷向物理网卡，发往远端
        //
        // 处理请求 处理顺序是 ChannelHandler 从下表0 到最后一个
        // 发送数据 处理顺序是 ChannelHandler 从最后一个到0
        return new ChannelHandler[] {
            messageEncoder,//对数据编码
            new NettyMessage.NettyMessageDecoder(),//对数据进行解码
            serverHandler,//业务逻辑处理
            //当处理请求 该类的 userEventTriggered 方法被触发
            queueOfPartitionQueues
        };
    }

    /**
     * Returns the client channel handlers.
     *
     * <pre>
     *     +-----------+----------+            +----------------------+
     *     | Remote input channel |            | request client       |
     *     +-----------+----------+            +-----------+----------+
     *                 |                                   | (1) write
     * +---------------+-----------------------------------+---------------+
     * |               |     CLIENT CHANNEL PIPELINE       |               |
     * |               |                                  \|/              |
     * |    +----------+----------+            +----------------------+    |
     * |    | Request handler     +            | Message encoder      |    |
     * |    +----------+----------+            +-----------+----------+    |
     * |              /|\                                 \|/              |
     * |               |                                   |               |
     * |    +----------+------------+                      |               |
     * |    | Message+Frame decoder |                      |               |
     * |    +----------+------------+                      |               |
     * |              /|\                                  |               |
     * +---------------+-----------------------------------+---------------+
     * |               | (3) server response              \|/ (2) client request
     * +---------------+-----------------------------------+---------------+
     * |               |                                   |               |
     * |       [ Socket.read() ]                    [ Socket.write() ]     |
     * |                                                                   |
     * |  Netty Internal I/O Threads (Transport Implementation)            |
     * +-------------------------------------------------------------------+
     * </pre>
     *
     * @return channel handlers
     */
    public ChannelHandler[] getClientChannelHandlers() {
        //核心职责：
        // 处理正常数据：当收到上游发来的普通数据 BufferResponse 时，负责将其剥离，并精准路由分发给下游具体的 RemoteInputChannel（从而喂给具体的解包算子）。
        // 处理异常控制：如果收到上游传来的 ErrorResponse（如之前分析的 PartitionNotFoundException），它会立刻拉响警报，通知下游 Task 触发 Failover 容错重启。
        // 流控核心（Credit 制造者）：它会读取网络包报头里的 backlog（上游积压量），并据此向本地内存池申请对应的浮动 Credit，然后逆流发送给上游
        NetworkClientHandler networkClientHandler = new CreditBasedPartitionRequestClientHandler();//
        // 处理上游发的请求   解码 -> 业务逻辑处理
        // 发送数据给上游    编码
        return new ChannelHandler[] {
            messageEncoder,// 编码
            //在完成解码的瞬间，会直接把解码后的对象委派并推给下一步进行业务处理，效率极高
            new NettyMessageClientDecoderDelegate(networkClientHandler),// 解码
            networkClientHandler //业务逻辑处理
        };
    }
}
