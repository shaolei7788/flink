package org.apache.flink.streaming.examples.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


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
        // 1. 创建流式执行环境 flink run -d -t yarn-per-job
		// env =  StreamContextEnvironment 如果是本地 LocalStreamEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(2);

		// 开启 checkpoint，并设置间隔 ms
		//env.enableCheckpointing(1000 * 30);
		// 模式 Exactly-Once、At-Least-Once
		//env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
		// 模式 At-Least-Once
		//env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);

		// 两个 checkpoint 之间最小间隔
		//env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
		// 超时时间
		//env.getCheckpointConfig().setCheckpointTimeout(60000);
		// 同时执行的 checkpoint 数量（比如上一个还没执行完，下一个已经触发开始了）
		// env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
		// 当用户取消了作业后，是否保留远程存储上的Checkpoint数据
		//env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


        DataStreamSource<String> lineDSS = env.socketTextStream("localhost", 9999);
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
		SingleOutputStreamOperator<String> wordAndOne = lineDSS.flatMap(flatMapFunction);

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
