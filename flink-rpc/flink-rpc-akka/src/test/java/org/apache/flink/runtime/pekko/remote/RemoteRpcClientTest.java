package org.apache.flink.runtime.pekko.remote;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.pekko.MyHelloGateway;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.pekko.PekkoRpcServiceUtils;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class RemoteRpcClientTest {
    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();

        // 1. 客户端也需要启动一个 RpcService 用来处理网络回包（端口设为 0 随机即可）
        RpcService clientRpcService = PekkoRpcServiceUtils.createRemoteRpcService(config, "127.0.0.1", "0", null, Optional.of(9999));

        // 2. 填入 Server 端打印出来的绝对远程 Pekko 地址
        String remoteAddress = "pekko.tcp://flink@127.0.0.1:6123/user/rpc/myHelloEndpoint";

        System.out.println("Client 正在尝试跨网络连接到: " + remoteAddress);

        // 3. 核心：通过跨网络地址连接远端 Server
        CompletableFuture<MyHelloGateway> remoteGatewayFuture = clientRpcService.connect(remoteAddress, MyHelloGateway.class);

        // 4. 获取代理对象并发送网络 RPC 请求
        MyHelloGateway remoteGateway = remoteGatewayFuture.get();
        
        // 这一步在底层会被 PekkoInvocationHandler 拦截，序列化成字节流通过 TCP 发送给 Server
        CompletableFuture<String> response = remoteGateway.sayHello("RemoteWorker");
        // response.get() 是一个异步过程 等待服务端响应
        System.out.println("跨网络 Client 收到远端响应: " + response.get());

        // 5. 关闭客户端
        clientRpcService.closeAsync().get();
    }
}
