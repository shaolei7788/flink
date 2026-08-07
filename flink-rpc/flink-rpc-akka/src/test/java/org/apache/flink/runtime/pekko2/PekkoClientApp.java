package org.apache.flink.runtime.pekko2;

import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.actor.Props;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class PekkoClientApp {
    public static void main(String[] args) throws Exception {
        // Hocon 网络配置：客户端绑定 0 随机端口，用来接收服务端的反向回包
        String configStr = 
            "pekko {\n" +
            "  actor {\n" +
            "    provider = \"remote\"\n" +
            "    allow-java-serialization = on\n" +
            "    warn-about-java-serialization-usage = off\n" +
            "  }\n" +
            "  remote.artery {\n" +
            "    canonical.hostname = \"127.0.0.1\"\n" +
            "    canonical.port = 0\n" +
            "  }\n" +
            "}";

        Config config = ConfigFactory.parseString(configStr);
        ActorSystem system = ActorSystem.create("ClientSystem", config);

        // 远程绝对网络 URL
        String remoteServerAddress = "pekko://ServerSystem@127.0.0.1:25520/user/helloServer";

        // 1. 物理物理起炉灶：启动本地客户端 Actor，拿到属于外层外层指挥的物理引用 actorRef
        Props props = Props.create(ClientActor.class, remoteServerAddress);
        ActorRef actorRef = system.actorOf(props, "myClient");

        // 模拟外部业务等待：等 2 秒钟再下达指令
        Thread.sleep(2000);

        // 🌟 2. 核心大演练：在外面通过 actorRef 随时随地指挥客户端向远端发包！
        System.out.println("\n【Client Main】下达发包政令 1...");
        actorRef.tell(new Msg.CommandFromMain("Flink TaskManager 节点 1 申请报到注册！"), ActorRef.noSender());
        
        Thread.sleep(3000);
        
        System.out.println("\n【Client Main】下达发包政令 2...");
        actorRef.tell(new Msg.CommandFromMain("Flink 细粒度算子 Slot 状态发生变动，上报！"), ActorRef.noSender());
    }
}
