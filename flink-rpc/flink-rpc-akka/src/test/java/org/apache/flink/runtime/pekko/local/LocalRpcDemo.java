package org.apache.flink.runtime.pekko.local;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.pekko.MyHelloEndpoint;
import org.apache.flink.runtime.pekko.MyHelloGateway;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.pekko.PekkoRpcServiceUtils;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class LocalRpcDemo {
    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();

        // 1. 创建本地本地 RpcService（通过 Flink 提供的工具类）
        // "localhost" 表示绑定本地，0 表示随机分配可用端口
        // localRpcService = PekkoRpcService
        RpcService localRpcService = PekkoRpcServiceUtils.createRemoteRpcService(
                config, "localhost", "0", null, Optional.of(9999));

        // 2. 启动服务端组件（Server 激活）
        MyHelloEndpoint serverEndpoint = new MyHelloEndpoint(localRpcService);
        serverEndpoint.start();

        // 3. 核心：获取本地连接地址
        // 对于本地组件，地址通常形如：pekko.tcp://flink@localhost:9999/user/rpc/myHelloEndpoint
        String localAddress = serverEndpoint.getAddress();
        System.out.println("Server 本地注册地址: " + localAddress);

        // 4. Client 端连接（筑路并生成动态代理网关）
        CompletableFuture<MyHelloGateway> clientGatewayFuture = localRpcService.connect(localAddress, MyHelloGateway.class);
        // 5. 阻塞等待网关生成，并像调用本地方法一样调用它
        MyHelloGateway clientGateway = clientGatewayFuture.get();
        CompletableFuture<String> response = clientGateway.sayHello("LocalUser");

        System.out.println("Client 收到响应: " + response.get());

        // 6. 优雅关闭
        serverEndpoint.closeAsync().get();
        localRpcService.closeAsync().get();
    }
}
