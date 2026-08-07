package org.apache.flink.runtime.orignal;

import org.apache.pekko.actor.ActorRef;

import java.io.Serializable;

public class Msg implements Serializable {
    // 客户端发送给服务端的请求消息
    public static class Request implements Serializable {
        public final String content;
        public final ActorRef replyTo; // 原生 Pekko 靠这个属性把客户端的"回邮地址"带给服务端

        public Request(String content, ActorRef replyTo) {
            this.content = content;
            this.replyTo = replyTo;
        }
    }

    // 服务端返回给客户端的响应消息
    public static class Response implements Serializable {
        public final String result;

        public Response(String result) {
            this.result = result;
        }
    }
}
