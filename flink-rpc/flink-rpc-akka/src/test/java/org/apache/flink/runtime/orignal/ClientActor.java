package org.apache.flink.runtime.orignal;

import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.ActorSelection;

public class ClientActor extends AbstractActor {
    private final String serverAddress;

    public ClientActor(String serverAddress) {
        this.serverAddress = serverAddress;
    }

    @Override
    public void preStart() throws Exception {
        // 1. 在 Actor 启动时，根据网络 URL 绝对路径选址并勾连上远程服务器
        ActorSelection serverSelection = getContext().actorSelection(serverAddress);
        
        System.out.println("【Client】正在向服务器发送请求...");
        // 2. 发送请求，并把自身的 getSelf() 作为回邮地址传过去
        serverSelection.tell(new Msg.Request("冲锋鸭！", getSelf()), getSelf());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(Msg.Response.class, res -> {
                // 3. 接收并处理服务器返回的回包
                System.out.println("【Client】成功收到 Server 的回包: " + res.result);
            })
            .build();
    }
}
