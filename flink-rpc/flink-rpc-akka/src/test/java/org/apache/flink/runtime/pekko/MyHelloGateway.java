package org.apache.flink.runtime.pekko;

import org.apache.flink.runtime.rpc.RpcGateway;

import java.util.concurrent.CompletableFuture;

// 1. 定义 RPC 接口（必须继承 RpcGateway）
public interface MyHelloGateway extends RpcGateway {
    CompletableFuture<String> sayHello(String name);
}
