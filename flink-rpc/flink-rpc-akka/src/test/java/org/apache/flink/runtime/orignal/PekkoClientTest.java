package org.apache.flink.runtime.orignal;

import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.actor.Props;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class PekkoClientTest {
    public static void main(String[] args) {
        // 客户端配置
        String configStr = 
            "pekko {\n" +
            "  actor {\n" +
            "    provider = \"remote\"\n" +
            "    allow-java-serialization = on\n" +
            "    warn-about-java-serialization-usage = off\n" +
            "  }\n" +
            "  remote.artery {\n" +
            "    canonical.hostname = \"127.0.0.1\"\n" +
            "    canonical.port = 0\n" + // 🌟 客户端传入 0，使用随机端口接收回包
            "  }\n" +
            "}";

        Config config = ConfigFactory.parseString(configStr);
        ActorSystem system = ActorSystem.create("ClientSystem", config);

        // 🌟 核心：拼装出标准的远程 Pekko 绝对网络路径
        // 格式：pekko://[服务端系统名]@[服务端IP]:[服务端端口]/user/[服务端Actor名]
        String remoteServerAddress = "pekko://ServerSystem@127.0.0.1:25520/user/helloServer";

        // 启动本地客户端 Actor，开始触发连线逻辑
        Props props = Props.create(ClientActor.class, remoteServerAddress);
        system.actorOf(props, "myClient");
    }
}
