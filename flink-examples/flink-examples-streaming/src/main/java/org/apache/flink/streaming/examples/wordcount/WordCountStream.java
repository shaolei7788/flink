package org.apache.flink.streaming.examples.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

// Function(代码逻辑) -> Transformation(逻辑结构) -> Operator(运行时实例)
//      定义：用户业务逻辑的最小承载体。
//      所处阶段：API 开发阶段。
//      特点：通常表现为接口（Interface）或抽象类。例如 MapFunction、FlatMapFunction、FilterFunction。
//      示例：你在代码里写的 new MapFunction<String, Integer>() { ... } 或 Lambda 表达式 str -> str.length() 就是一个 Function
// Function 只管计算数据
// Transformation
//      定义：Flink 内部用来构建 “逻辑拓扑图（StreamGraph）” 的核心对象。
//      所处阶段：Graph 编译阶段（对用户透明）。
//      特点：它记录了数据流是怎么改变的（比如从哪个上游流入、经过什么转换、流向哪个下游）。
//      示例：当你调用 dataStream.map(new MyMapFunction()) 时，Flink 底层会创建一个 OneInputTransformation 对象。它不负责执行，只负责记录依赖关系
// Operator
//      定义：Flink 运行时（Runtime） 真正负责管理状态（State）、生命周期、水位线（Watermark）和数据处理的实体。所处阶段：集群运行阶段。
//      特点：它是 Function 的“包装壳”。Function 只管计算数据，而 Operator 管得更多（比如什么时候调用 Function、怎么备份状态、怎么处理 Checkpoint）。
//      示例：StreamMap、StreamFilter

//算子					function					operator	    transformation				transformations     id
//socketTextStream  	SocketTextStreamFunction	StreamSource	LegacySourceTransformation	    x               1
//faltmap             	FlatMapFunction				StreamFlatMap 	OneInputTransformation          add             2
//map					MapFunction					StreamMap 		OneInputTransformation			add             3
//keyBy																PartitionTransformation         x               4  7
//sum					SumAggregator								ReduceTransformation 			add             5 走到5这里会生成一个虚拟节点7
//print 															LegacySinkTransformation		add				6
//KeyGroup 是 Flink 中一组 Key 的逻辑集合，是状态管理的最小单元。每个 KeyGroup 包含通过哈希算法映射到同一组的多个 Key。
// Flink 在作业启动时就确定了 KeyGroup 的总数，这个数量由maxParallelism参数决定，并在整个作业生命周期内保持不变。
public class WordCountStream {
    public static void main(String[] args) throws Exception {
		// KeyedStream extends DataStream
		// DataStreamSource extends SingleOutputStreamOperator extends DataStream
    	// SingleOutputStreamOperator extends DataStream
        // 1. 先在idea 启动 StandaloneSessionClusterEntrypoint
        // 2. 再 启动 TaskManagerRunner
        //
        //
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		// 开启 checkpoint，并设置间隔 ms
		env.enableCheckpointing(1000 * 30);
		// 模式 Exactly-Once、At-Least-Once
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //使用非对齐checkpoint，需要跟下面的配合使用
        env.getCheckpointConfig().enableUnalignedCheckpoints();
        //设置为对齐模式，checkpoint 超时时间为20s，20s未完成checkpoint，则升级为非对齐
        env.getCheckpointConfig().setAlignedCheckpointTimeout(Duration.ofSeconds(20));
		//两个 checkpoint 之间最小间隔
		env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
		//超时时间
		env.getCheckpointConfig().setCheckpointTimeout(60000);
		//同时执行的 checkpoint 数量（比如上一个还没执行完，下一个已经触发开始了）
		env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
		//当用户取消了作业后，是否保留远程存储上的Checkpoint数据
		//env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
		//StreamGroupedReduceOperator#processElement
		FlatMapFunction<String,String> flatMapFunction = new FlatMapFunction<String,String>() {
			@Override
			public void flatMap(String value, Collector<String> collector) throws Exception {
				for (String word : value.split(" ")) {
					// words = TimestampedCollector
					collector.collect(word);
				}
			}
		};
		// 将Function 转换为 Operator 再转为 Transformation 添加到 List<Transformation<?>> transformations
		SingleOutputStreamOperator<String> wordAndOne = source.flatMap(flatMapFunction);

		MapFunction mapFunction = new MapFunction<String, Tuple2<String,Long>>() {
			@Override
			public Tuple2<String,Long> map(String value) throws Exception {
				return Tuple2.of(value, 1L);
			}
		};
		SingleOutputStreamOperator<Tuple2<String,Long>> singleOutputStreamOperator = wordAndOne.map(mapFunction);

		KeySelector keySelector = new KeySelector<Tuple2<String,Long>, String>() {
			@Override
			public String getKey(Tuple2<String,Long> value) throws Exception {
				return value.f0;
			}
		};
		KeyedStream<Tuple2<String, Long>, String> wordAndOneKS = singleOutputStreamOperator.keyBy(keySelector);
        // 5. 求和  会调用 StreamGroupedReduceOperator#processElement
        SingleOutputStreamOperator<Tuple2<String, Long>> result = wordAndOneKS.sum(1);
        // 6. 打印
        result.print();
        // 7. 执行 LocalStreamEnvironment#execute
        env.execute("WordCountStream");//


    }
}
