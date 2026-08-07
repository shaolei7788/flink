package org.apache.flink.runtime.orignal;

import org.apache.pekko.actor.AbstractActor;

public class ServerActor extends AbstractActor {


    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(Msg.Request.class, req -> {
                System.out.println("【Server】收到来自客户端的消息: " + req.content);
                
                // 处理业务，并顺着请求里带过来的 replyTo 地址把结果打回去
                String replyContent = "你好，我是原生 Pekko Server！已收到你的: " + req.content;
                req.replyTo.tell(new Msg.Response(replyContent), getSelf());
            })
            .build();
    }
}
