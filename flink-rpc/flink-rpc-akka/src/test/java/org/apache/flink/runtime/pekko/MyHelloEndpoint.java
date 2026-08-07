package org.apache.flink.runtime.pekko;

import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;

import java.util.concurrent.CompletableFuture;

// 2. 定义服务端业务实现类（必须继承 RpcEndpoint 并实现接口）
public class MyHelloEndpoint extends RpcEndpoint implements MyHelloGateway {

    public MyHelloEndpoint(RpcService rpcService) {
        // 每个 Endpoint 都有一个唯一的 endpointId 
        super(rpcService, "myHelloEndpoint"); 
    }

    @Override
    public CompletableFuture<String> sayHello(String name) {
        // 具体的业务逻辑，返回异步结果
        return CompletableFuture.completedFuture("Hello, " + name + " from Flink Pekko RPC!");
    }
}
