package org.apache.flink.runtime.pekko2;

import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.ActorRef;
import java.util.HashSet;
import java.util.Set;

public class ServerActor extends AbstractActor {
    
    // 🌟 核心资产：在内存中开辟一个“小本本”，动态动态记录所有来报到过的客户端地址
    private final Set<ActorRef> connectedClients = new HashSet<>();

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            // 🌐 场景 1：收到来自客户端的跨网络网络请求消息
            .match(Msg.Request.class, req -> {
                System.out.println("【Server Actor】成功捕获客户端跨网请求: " + req.content);
                
                // 1. 将客户端的回邮地址存入注册表（类似 TaskManager 注册到 ResourceManager）
                connectedClients.add(req.replyTo);
                
                // 2. 顺着回邮地址自动反向自动自动回包响应
                String replyContent = "【Server 自动回包】已收到你的: " + req.content;
                req.replyTo.tell(new Msg.Response(replyContent), getSelf());
            })
            
            // 🌟 场景 2：收到来自本地服务端 main 方法的广播控制控制指令
            .match(Msg.BroadcastCommand.class, cmd -> {
                System.out.println("【Server Actor】收到本地 main 广播指示，开始轰炸全集群... ");
                
                // 遍历注册表，主动、向所有活着的客户端下发控制指令（类似心跳广播或注销命令）
                for (ActorRef clientRef : connectedClients) {
                    clientRef.tell(new Msg.Response("【Server 主动广播】" + cmd.message), getSelf());
                }
            })
            .build();
    }
}
