package org.apache.flink.runtime.pekko.remote;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.pekko.MyHelloEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.pekko.PekkoRpcServiceUtils;

import java.util.Optional;

public class RemoteRpcServerTest {
    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();

        // 1. 显式绑定一个固定的 IP 和端口（例如 6123）
        RpcService serverRpcService = PekkoRpcServiceUtils.createRemoteRpcService(
                config, "127.0.0.1", "6123", null, Optional.empty());

        // 2. 启动并发布服务
        MyHelloEndpoint serverEndpoint = new MyHelloEndpoint(serverRpcService);
        // 不调用该方法 rpc服务启动不了
        serverEndpoint.start();

        // 3. 打印出远程连接所需的规范 Akka/Pekko URL 地址
        // 形如：pekko.tcp://flink@127.0.0.1:6123/user/rpc/myHelloEndpoint
        System.out.println("Server 远程监听地址: " + serverEndpoint.getAddress());

        // 让服务器持续运行
        Thread.sleep(Long.MAX_VALUE);
    }
}
