package org.apache.flink.runtime.pekko2;

import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.actor.Props;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class PekkoServerApp {
    public static void main(String[] args) throws Exception {
        // Hocon 网络配置：服务端绑定固定 25520 端口
        String configStr = 
            "pekko {\n" +
            "  actor {\n" +
            "    provider = \"remote\"\n" +
            "    allow-java-serialization = on\n" +
            "    warn-about-java-serialization-usage = off\n" +
            "  }\n" +
            "  remote.artery {\n" +
            "    canonical.hostname = \"127.0.0.1\"\n" +
            "    canonical.port = 25520\n" +
            "  }\n" +
            "}";

        Config config = ConfigFactory.parseString(configStr);
        ActorSystem system = ActorSystem.create("ServerSystem", config);

        // 1. 物理起炉灶：启动 ServerActor，拿到属于外层控制的物理引用 serverRef
        Props props = Props.create(ServerActor.class);
        ActorRef serverRef = system.actorOf(props, "helloServer");

        System.out.println("【Server Main】Pekko 远程服务端已在 25520 端口激活，等待客户端勾连...");

        /*// 2. 模拟分布式常驻常驻心跳检测：每隔 8 秒，服务端 main 线程主动整活广播一次
        while (true) {
            Thread.sleep(8000);
            System.out.println("\n【Server Main】触发定时整活指令，通过 serverRef 派发广播...");
            
            serverRef.tell(
                new Msg.BroadcastCommand("警告：当前集群正在执行 Flink 全局 Checkpoint，请勿下线！"), 
                ActorRef.noSender()
            );
        }*/
    }
}
