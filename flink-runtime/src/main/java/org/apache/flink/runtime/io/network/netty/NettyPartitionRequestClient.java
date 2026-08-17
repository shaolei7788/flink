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

import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.NetworkClientHandler;
import org.apache.flink.runtime.io.network.PartitionRequestClient;
import org.apache.flink.runtime.io.network.netty.exception.LocalTransportException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFutureListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.io.network.netty.NettyMessage.PartitionRequest;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.TaskEventRequest;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Partition request client for remote partition requests.
 *
 * <p>This client is shared by all remote input channels, which request a partition from the same
 * {@link ConnectionID}.
 */
//代表下游的 Task，通过建立好的 Netty TCP 通道，向上游跨节点的 TaskManager（TM）异步发送数据分区读取请求，
//并作为底层物理连接的抽象，辅助维护基于 Credit 的流量控制（Credit-based Flow Control）
public class NettyPartitionRequestClient implements PartitionRequestClient {

    private static final Logger LOG = LoggerFactory.getLogger(NettyPartitionRequestClient.class);

    //代表了一个真实的 TCP 物理长连接
    //发送 PartitionRequest 请求数据、发送 AddCredit 告知上游本地还有多少空闲缓冲区 最终都是通过调用 tcpChannel.writeAndFlush(msg) 发送给远程上游的
    private final Channel tcpChannel;

    //该连接在 Netty Pipeline 中注册的入站处理器
    //CreditBasedPartitionRequestClientHandler
    private final NetworkClientHandler clientHandler;

    //包含了远程 TaskManager 的网络地址（InetSocketAddress）和连接索引（connectionIndex）
    private final ConnectionID connectionId;

    //当底层的 tcpChannel 因为网络闪断、超时或者对方宕机而关闭时，这个 Client 会通过这个 clientFactory 引用，把自己从全局的连接池缓存中移除，防止其他下游 Task 继续拿到这个坏掉的连接
    private final PartitionRequestClientFactory clientFactory;

    /** If zero, the underlying TCP channel can be safely closed. */
    //一个线程安全的原子计数器，用于记录当前有多少个下游 RemoteInputChannel 正在复用这个物理连接
    private final AtomicInteger closeReferenceCounter = new AtomicInteger(0);

    private final AtomicBoolean closed = new AtomicBoolean(false);

    NettyPartitionRequestClient(//
            Channel tcpChannel,
            NetworkClientHandler clientHandler,
            ConnectionID connectionId,
            PartitionRequestClientFactory clientFactory) {

        this.tcpChannel = checkNotNull(tcpChannel);
        this.clientHandler = checkNotNull(clientHandler);
        this.connectionId = checkNotNull(connectionId);
        this.clientFactory = checkNotNull(clientFactory);
        clientHandler.setConnectionId(connectionId);
    }

    boolean canBeDisposed() {
        return closeReferenceCounter.get() == 0 && !canBeReused();
    }

    /**
     * Validate the client and increment the reference counter.
     *
     * <p>Note: the reference counter has to be incremented before returning the instance of this
     * client to ensure correct closing logic.
     *
     * @return whether this client can be used.
     */
    boolean validateClientAndIncrementReferenceCounter() {
        if (!clientHandler.hasChannelError()) {
            return closeReferenceCounter.incrementAndGet() > 0;
        }
        return false;
    }

    /**
     * Requests a remote intermediate result partition queue.
     *
     * <p>The request goes to the remote producer, for which this partition request client instance
     * has been created.
     */
    //通过底层建立好的 Netty TCP 通道，向上游的 TaskManager 异步发起一个“数据分区消费请求”，并在本地注册监听器，用于后续接收数据或处理网络异常
    @Override
    public void requestSubpartition(
            final ResultPartitionID partitionId,
            final ResultSubpartitionIndexSet subpartitionIndexSet,
            final RemoteInputChannel inputChannel,
            int delayMs)
            throws IOException {

        checkNotClosed();

        LOG.debug(
                "Requesting subpartition {} of partition {} with {} ms delay.",
                subpartitionIndexSet,
                partitionId,
                delayMs);
        //将 InputChannel 挂载到 Handler
        clientHandler.addInputChannel(inputChannel);

        final PartitionRequest request =
                new PartitionRequest(
                        partitionId,//目标中间结果分区
                        subpartitionIndexSet,//[1，1] 表示下游想要消费的子分区索引集合
                        inputChannel.getInputChannelId(),//下游自己的唯一 ID
                        inputChannel.getInitialCredit());//2

        final ChannelFutureListener listener = new ChannelFutureListener(){

            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (!future.isSuccess()) {
                    // 移除注册，防止内存泄漏
                    clientHandler.removeInputChannel(inputChannel);
                    // 通知下游 Channel 报错
                    inputChannel.onError(
                            new LocalTransportException(
                                    String.format(
                                            "Sending the partition request to '%s [%s] (#%d)' failed.",
                                            connectionId.getAddress(),
                                            connectionId
                                                    .getResourceID()
                                                    .getStringWithMetadata(),
                                            connectionId.getConnectionIndex()),
                                    future.channel().localAddress(),
                                    future.cause()));
                    // 发送网络错误消息
                    sendToChannel(
                            new ConnectionErrorMessage(
                                    future.cause() == null
                                            ? new RuntimeException(
                                            "Cannot send partition request.")
                                            : future.cause()));
                }
            };
        };

        if (delayMs == 0) {//true
            //绝大多数常规 Shuffle 的场景
            ChannelFuture f = tcpChannel.writeAndFlush(request);//
            f.addListener(listener);
        } else {
            // 延迟发送逻辑
            //这通常出现在 Flink 的 批处理（Batch Job） 或者 分层存储（Tiered Storage）、或者是某些重试/重连机制中。
            // 当上游节点尚未准备好，或者下游需要等待特定事件时，会利用 Netty 的 EventLoop 定时线程池提交一个延迟任务，等到 delayMs 毫秒后再真正把 PartitionRequest 发出去。
            final ChannelFuture[] f = new ChannelFuture[1];
            tcpChannel
                    .eventLoop()
                    .schedule(
                            () -> {
                                f[0] = tcpChannel.writeAndFlush(request);
                                f[0].addListener(listener);
                            },
                            delayMs,
                            TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Sends a task event backwards to an intermediate result partition producer.
     *
     * <p>Backwards task events flow between readers and writers and therefore will only work when
     * both are running at the same time, which is only guaranteed to be the case when both the
     * respective producer and consumer task run pipelined.
     */
    @Override
    public void sendTaskEvent(
            ResultPartitionID partitionId, TaskEvent event, final RemoteInputChannel inputChannel)
            throws IOException {
        checkNotClosed();

        tcpChannel
                .writeAndFlush(
                        new TaskEventRequest(event, partitionId, inputChannel.getInputChannelId()))
                .addListener(
                        (ChannelFutureListener)
                                future -> {
                                    if (!future.isSuccess()) {
                                        inputChannel.onError(
                                                new LocalTransportException(
                                                        String.format(
                                                                "Sending the task event to '%s [%s] (#%d)' failed.",
                                                                connectionId.getAddress(),
                                                                connectionId
                                                                        .getResourceID()
                                                                        .getStringWithMetadata(),
                                                                connectionId.getConnectionIndex()),
                                                        future.channel().localAddress(),
                                                        future.cause()));
                                        sendToChannel(
                                                new ConnectionErrorMessage(
                                                        future.cause() == null
                                                                ? new RuntimeException(
                                                                        "Cannot send task event.")
                                                                : future.cause()));
                                    }
                                });
    }

    @Override
    public void notifyCreditAvailable(RemoteInputChannel inputChannel) {
        sendToChannel(new AddCreditMessage(inputChannel));
    }

    @Override
    public void notifyNewBufferSize(RemoteInputChannel inputChannel, int bufferSize) {
        sendToChannel(new NewBufferSizeMessage(inputChannel, bufferSize));
    }

    @Override
    public void notifyRequiredSegmentId(
            RemoteInputChannel inputChannel, int subpartitionIndex, int segmentId) {
        sendToChannel(new SegmentIdMessage(inputChannel, subpartitionIndex, segmentId));
    }

    @Override
    public void resumeConsumption(RemoteInputChannel inputChannel) {
        sendToChannel(new ResumeConsumptionMessage(inputChannel));
    }

    @Override
    public void acknowledgeAllRecordsProcessed(RemoteInputChannel inputChannel) {
        sendToChannel(new AcknowledgeAllRecordsProcessedMessage(inputChannel));
    }

    private void sendToChannel(Object message) {
        tcpChannel.eventLoop().execute(() -> tcpChannel.pipeline().fireUserEventTriggered(message));
    }

    @Override
    public void close(RemoteInputChannel inputChannel) throws IOException {

        clientHandler.removeInputChannel(inputChannel);

        if (closeReferenceCounter.updateAndGet(count -> Math.max(count - 1, 0)) == 0
                && !canBeReused()) {
            closeConnection();
        } else {
            clientHandler.cancelRequestFor(inputChannel.getInputChannelId());
        }
    }

    public void closeConnection() {
        Preconditions.checkState(
                canBeDisposed(), "The connection should not be closed before disposed.");
        if (closed.getAndSet(true)) {
            // Do not close connection repeatedly
            return;
        }
        // Close the TCP connection. Send a close request msg to ensure
        // that outstanding backwards task events are not discarded.
        tcpChannel
                .writeAndFlush(new NettyMessage.CloseRequest())
                .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
        // Make sure to remove the client from the factory
        clientFactory.destroyPartitionRequestClient(connectionId, this);
    }

    private boolean canBeReused() {
        return clientFactory.isConnectionReuseEnabled() && !clientHandler.hasChannelError();
    }

    private void checkNotClosed() throws IOException {
        if (closed.get()) {
            final SocketAddress localAddr = tcpChannel.localAddress();
            final SocketAddress remoteAddr = tcpChannel.remoteAddress();
            throw new LocalTransportException(
                    String.format(
                            "Channel to '%s [%s]' closed.",
                            remoteAddr, connectionId.getResourceID().getStringWithMetadata()),
                    localAddr);
        }
    }

    private static class AddCreditMessage extends ClientOutboundMessage {

        private AddCreditMessage(RemoteInputChannel inputChannel) {
            super(checkNotNull(inputChannel));
        }

        @Override
        Object buildMessage() {
            int credits = inputChannel.getAndResetUnannouncedCredit();
            return credits > 0
                    ? new NettyMessage.AddCredit(credits, inputChannel.getInputChannelId())
                    : null;
        }
    }

    private static class NewBufferSizeMessage extends ClientOutboundMessage {
        private final int bufferSize;

        private NewBufferSizeMessage(RemoteInputChannel inputChannel, int bufferSize) {
            super(checkNotNull(inputChannel));
            this.bufferSize = bufferSize;
        }

        @Override
        Object buildMessage() {
            return new NettyMessage.NewBufferSize(bufferSize, inputChannel.getInputChannelId());
        }
    }

    private static class ResumeConsumptionMessage extends ClientOutboundMessage {

        private ResumeConsumptionMessage(RemoteInputChannel inputChannel) {
            super(checkNotNull(inputChannel));
        }

        @Override
        Object buildMessage() {
            return new NettyMessage.ResumeConsumption(inputChannel.getInputChannelId());
        }
    }

    private static class AcknowledgeAllRecordsProcessedMessage extends ClientOutboundMessage {

        private AcknowledgeAllRecordsProcessedMessage(RemoteInputChannel inputChannel) {
            super(checkNotNull(inputChannel));
        }

        @Override
        Object buildMessage() {
            return new NettyMessage.AckAllUserRecordsProcessed(inputChannel.getInputChannelId());
        }
    }

    private static class SegmentIdMessage extends ClientOutboundMessage {

        private final int segmentId;

        private final int subpartitionIndex;

        private SegmentIdMessage(
                RemoteInputChannel inputChannel, int subpartitionIndex, int segmentId) {
            super(checkNotNull(inputChannel));
            this.subpartitionIndex = subpartitionIndex;
            this.segmentId = segmentId;
        }

        @Override
        Object buildMessage() {
            return new NettyMessage.SegmentId(
                    subpartitionIndex, segmentId, inputChannel.getInputChannelId());
        }
    }
}
