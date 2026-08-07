package org.apache.flink.runtime.orignal;

import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.actor.Props;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class PekkoServerTest {
    public static void main(String[] args) {
        // 使用代码硬编码 Pekko Remote 配置（也可以写在 resources/application.conf 中）
        String configStr = 
            "pekko {\n" +
            "  actor {\n" +
            "    provider = \"remote\"\n" + // 🌟 关键：开启远程远程通信模块
            "    allow-java-serialization = on\n" + // 允许 Java 原生序列化（测试用）
            "    warn-about-java-serialization-usage = off\n" +
            "  }\n" +
            "  remote.artery {\n" +
            "    canonical.hostname = \"127.0.0.1\"\n" +
            "    canonical.port = 25520\n" + // 🌟 服务端监听固定的 25520 端口
            "  }\n" +
            "}";

        Config config = ConfigFactory.parseString(configStr);
        
        // 1. 创建服务端的系统的 ActorSystem
        ActorSystem system = ActorSystem.create("ServerSystem", config);

        // 2. 并在系统内创建并启动 ServerActor，命名为 "helloServer"
        system.actorOf(Props.create(ServerActor.class), "helloServer");

        System.out.println("【Server】Pekko 远程服务端已在 25520 端口拉起，守株待兔中...");
    }
}
