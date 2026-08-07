package org.apache.flink.runtime.pekko2;

import org.apache.pekko.actor.ActorRef;

import java.io.Serializable;

public class Msg {

    // ==========================================
    // 🌟 本地内部指令（仅在各自进程的 main 方法与 Actor 之间传递，不跨网）
    // ==========================================
    
    // 客户端 main 指挥 客户端 Actor 发包的本地指令
    public static class CommandFromMain {
        public final String text;
        public CommandFromMain(String text) { this.text = text; }
    }

    // 服务端 main 指挥 服务端 Actor 广播的本地指令
    public static class BroadcastCommand {
        public final String message;
        public BroadcastCommand(String message) { this.message = message; }
    }

    // ==========================================
    // 🌐 跨网络网络数据包（必须实现 Serializable 以便跨进程传输）
    // ==========================================
    
    // 客户端 -> 服务端：跨网请求包
    public static class Request implements Serializable {
        private static final long serialVersionUID = 1L;
        public final String content;
        public final ActorRef replyTo; // 🌟 核心：客户端的回邮地址（门牌号）

        public Request(String content, ActorRef replyTo) {
            this.content = content;
            this.replyTo = replyTo;
        }
    }

    // 服务端 -> 客户端：跨网响应包
    public static class Response implements Serializable {
        private static final long serialVersionUID = 1L;
        public final String result;
        public Response(String result) { this.result = result; }
    }
}
