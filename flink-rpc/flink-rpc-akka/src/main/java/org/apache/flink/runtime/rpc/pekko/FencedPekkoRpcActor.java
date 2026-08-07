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

import org.apache.flink.runtime.rpc.FencedRpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.exceptions.FencingTokenException;
import org.apache.flink.runtime.rpc.messages.FencedMessage;
import org.apache.flink.runtime.rpc.messages.LocalFencedMessage;
import org.apache.flink.runtime.rpc.pekko.exceptions.UnknownMessageException;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Fenced extension of the {@link PekkoRpcActor}. This actor will be started for {@link
 * FencedRpcEndpoint} and is responsible for filtering out invalid messages with respect to the
 * current fencing token.
 *
 * @param <F> type of the fencing token
 * @param <T> type of the RpcEndpoint
 */
//FencedPekkoRpcActor：带“口令校验”的守卫
//包装的对象：它用于包装 FencedRpcEndpoint。
//
//消息处理机制：它强制要求所有发给它的核心 RPC 消息必须实现 FencedMessage 接口，也就是必须携带一个 FencingToken（隔离令牌，通常是一个 UUID）。
//
//当 FencedPekkoRpcActor 收到消息时，它的底层机制会率先拦截并比对 Token。
//
//如果消息中的 Token 和当前 Actor 维护的 Leader Token 一致，才放行给上层处理。
//
//如果 Token 不匹配，直接把消息丢弃，并可能返回错误。
//
//适用场景：适用于集群中的 Master 节点组件，也就是同一时刻只允许有一个 Leader 的组件。
//
//源码代表：JobMaster、ResourceManager、Dispatcher
public class FencedPekkoRpcActor<
                F extends Serializable, T extends FencedRpcEndpoint<F> & RpcGateway>
        extends PekkoRpcActor<T> {

    //通过是被反射调用
    public FencedPekkoRpcActor(
            T rpcEndpoint,
            CompletableFuture<Boolean> terminationFuture,
            int version,
            final long maximumFramesize,
            final boolean forceSerialization,
            ClassLoader flinkClassLoader,
            final Map<String, String> loggingContext) {
        //
        super(
                rpcEndpoint,
                terminationFuture,
                version,
                maximumFramesize,
                forceSerialization,
                flinkClassLoader,
                loggingContext);
    }

    @Override
    protected void handleRpcMessage(Object message) {
        if (message instanceof FencedMessage) {
            //获取期待的令牌
            final F expectedFencingToken = rpcEndpoint.getFencingToken();

            if (expectedFencingToken == null) {
                //无令牌
                if (log.isDebugEnabled()) {
                    log.debug(
                            "Fencing token not set: Ignoring message {} because the fencing token is null.",
                            message);
                }
                //
                sendErrorIfSender(
                        new FencingTokenException(
                                String.format(
                                        "Fencing token not set: Ignoring message %s sent to %s because the fencing token is null.",
                                        message, rpcEndpoint.getAddress())));
            } else {
                @SuppressWarnings("unchecked")
                FencedMessage<F, ?> fencedMessage = ((FencedMessage<F, ?>) message);

                F fencingToken = fencedMessage.getFencingToken();

                if (Objects.equals(expectedFencingToken, fencingToken)) {
                    //todo  Token 一致，处理消息
                    super.handleRpcMessage(fencedMessage.getPayload());
                } else {
                    // Token 不匹配，丢弃消息，打印 log.debug
                    if (log.isDebugEnabled()) {
                        log.debug(
                                "Fencing token mismatch: Ignoring message {} because the fencing token {} did "
                                        + "not match the expected fencing token {}.",
                                message,
                                fencingToken,
                                expectedFencingToken);
                    }

                    sendErrorIfSender(
                            new FencingTokenException(
                                    "Fencing token mismatch: Ignoring message "
                                            + message
                                            + " because the fencing token "
                                            + fencingToken
                                            + " did not match the expected fencing token "
                                            + expectedFencingToken
                                            + '.'));
                }
            }
        } else {
            if (log.isDebugEnabled()) {
                log.debug(
                        "Unknown message type: Ignoring message {} because it is not of type {}.",
                        message,
                        FencedMessage.class.getSimpleName());
            }

            sendErrorIfSender(
                    new UnknownMessageException(
                            "Unknown message type: Ignoring message "
                                    + message
                                    + " of type "
                                    + message.getClass().getSimpleName()
                                    + " because it is not of type "
                                    + FencedMessage.class.getSimpleName()
                                    + "."));
        }
    }

    @Override
    protected Object envelopeSelfMessage(Object message) {
        final F fencingToken = rpcEndpoint.getFencingToken();

        return new LocalFencedMessage<>(fencingToken, message);
    }
}
