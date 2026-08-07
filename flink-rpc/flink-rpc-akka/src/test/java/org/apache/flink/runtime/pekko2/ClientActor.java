package org.apache.flink.runtime.pekko2;

import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.ActorSelection;

public class ClientActor extends AbstractActor {
    private final String serverAddress;

    public ClientActor(String serverAddress) {
        this.serverAddress = serverAddress;
    }

    @Override
    public void preStart() throws Exception {
        // 启动时保持绝对静默，绝不自作主张发包
        System.out.println("【Client Actor】已成功孵化降生，保持静默，恭候 main 指示...");
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            // 🌟 场景 1：收到来自本地客户端 main 方法的强制发包发包指令
            .match(Msg.CommandFromMain.class, cmd -> {
                System.out.println("【Client Actor】截获本地 main 指令，开始跨网传输: " + cmd.text);
                
                // 1. 顺着初始化的 URL 地址找到远程服务器门牌
                ActorSelection serverSelection = getContext().actorSelection(serverAddress);
                
                // 2. 执行真正的跨网 tell，并强行把自身的 getSelf() 门牌号作为回邮地址带过去
                serverSelection.tell(new Msg.Request(cmd.text, getSelf()), getSelf());
            })
            
            // 🌐 场景 2：成功接收到来自远程服务端的跨网络网络回包响应
            .match(Msg.Response.class, res -> {
                System.out.println("【Client Actor】🔥 网络回包触达: " + res.result);
            })
            .build();
    }
}
