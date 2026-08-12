/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.graph;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.attribute.Attribute;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.InputOutputFormatVertex;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.forwardgroup.ForwardGroup;
import org.apache.flink.runtime.jobgraph.forwardgroup.ForwardGroupComputeUtil;
import org.apache.flink.runtime.jobgraph.forwardgroup.JobVertexForwardGroup;
import org.apache.flink.runtime.jobgraph.tasks.TaskInvokable;
import org.apache.flink.runtime.jobgraph.topology.DefaultLogicalPipelinedRegion;
import org.apache.flink.runtime.jobgraph.topology.DefaultLogicalTopology;
import org.apache.flink.runtime.jobgraph.topology.LogicalVertex;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroupImpl;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.util.Hardware;
import org.apache.flink.runtime.util.config.memory.ManagedMemoryUtils;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.graph.util.ChainedOperatorHashInfo;
import org.apache.flink.streaming.api.graph.util.ChainedSourceInfo;
import org.apache.flink.streaming.api.graph.util.JobVertexBuildContext;
import org.apache.flink.streaming.api.graph.util.OperatorChainInfo;
import org.apache.flink.streaming.api.graph.util.OperatorInfo;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.legacy.YieldingOperatorFactory;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;
import org.apache.flink.streaming.runtime.partitioner.CustomPartitionerWrapper;
import org.apache.flink.streaming.runtime.partitioner.ForwardForConsecutiveHashPartitioner;
import org.apache.flink.streaming.runtime.partitioner.ForwardForUnspecifiedPartitioner;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RescalePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.tasks.StreamIterationHead;
import org.apache.flink.streaming.runtime.tasks.StreamIterationTail;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IterableUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.FutureUtils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** The StreamingJobGraphGenerator converts a {@link StreamGraph} into a {@link JobGraph}. */
@Internal
public class StreamingJobGraphGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(StreamingJobGraphGenerator.class);

    // ------------------------------------------------------------------------

    @VisibleForTesting
    public static JobGraph createJobGraph(StreamGraph streamGraph) {
        return new StreamingJobGraphGenerator(
                        Thread.currentThread().getContextClassLoader(),
                        streamGraph,
                        streamGraph.getJobID(),
                        Runnable::run)
                .createJobGraph();
    }

    public static JobGraph createJobGraph(
            ClassLoader userClassLoader, StreamGraph streamGraph, @Nullable JobID jobID) {
        // TODO Currently, we construct a new thread pool for the compilation of each job. In the
        // future, we may refactor the job submission framework and make it reusable across jobs.
        final ExecutorService serializationExecutor =
                Executors.newFixedThreadPool(
                        Math.max(
                                1,
                                Math.min(
                                        Hardware.getNumberCPUCores(),
                                        streamGraph.getExecutionConfig().getParallelism())),
                        new ExecutorThreadFactory("flink-operator-serialization-io"));
        try {
            StreamingJobGraphGenerator jobGraphGenerator = new StreamingJobGraphGenerator(
                    userClassLoader,
                    streamGraph,
                    jobID,
                    serializationExecutor);
            // 生成JobGraph
            return jobGraphGenerator.createJobGraph();//
        } finally {
            serializationExecutor.shutdown();
        }
    }

    // ------------------------------------------------------------------------

    private final ClassLoader userClassloader;
    private final StreamGraph streamGraph;

    private final JobGraph jobGraph;

    private final StreamGraphHasher defaultStreamGraphHasher;
    private final List<StreamGraphHasher> legacyStreamGraphHashers;

    private final Executor serializationExecutor;

    // We save all the context needed to create the JobVertex in this structure.
    private final JobVertexBuildContext jobVertexBuildContext;

    private StreamingJobGraphGenerator(
            ClassLoader userClassloader,
            StreamGraph streamGraph,
            @Nullable JobID jobID,
            Executor serializationExecutor) {
        this.userClassloader = userClassloader;
        this.streamGraph = streamGraph;
        this.defaultStreamGraphHasher = new StreamGraphHasherV2();
        this.legacyStreamGraphHashers = Arrays.asList(new StreamGraphUserHashHasher());
        this.serializationExecutor = Preconditions.checkNotNull(serializationExecutor);
        jobGraph = createAndInitializeJobGraph(streamGraph, jobID);

        // Generate deterministic hashes for the nodes in order to identify them across
        // submission iff they didn't change.
        final Map<Integer, byte[]> hashes =
                defaultStreamGraphHasher.traverseStreamGraphAndGenerateHashes(streamGraph);

        // Generate legacy version hashes for backwards compatibility
        final List<Map<Integer, byte[]>> legacyHashes =
                new ArrayList<>(legacyStreamGraphHashers.size());
        for (StreamGraphHasher hasher : legacyStreamGraphHashers) {
            legacyHashes.add(hasher.traverseStreamGraphAndGenerateHashes(streamGraph));
        }

        this.jobVertexBuildContext =
                new JobVertexBuildContext(
                        jobGraph,
                        streamGraph,
                        new AtomicBoolean(false),
                        hashes,
                        legacyHashes,
                        new SlotSharingGroup());
    }

    private JobGraph createJobGraph() {
        //作用：在图转换开始前，对全局配置和算子进行兼容性与安全性合规检查。
        // 细节：利用用户类加载器（userClassloader）检查图中所有的用户自定义函数（UDF）、序列化器（Serializer）和状态后端（State Backend）是否能正常加载。
        // 如果存在冲突配置（例如同时开启了不支持的混合模式），在此处直接抛出异常，提前熔断任务
        preValidate(streamGraph, userClassloader);
        //遍历所有的 StreamNode，计算算子链合并，将数个逻辑节点揉成物理顶点 JobVertex，并初步在内存中建立它们之间的依赖树
        setChaining();

        if (jobGraph.isDynamic()) {// false
            setVertexParallelismsForDynamicGraphIfNecessary();
        }

        // Note that we set all the non-chainable outputs configuration here because the
        // "setVertexParallelismsForDynamicGraphIfNecessary" may affect the parallelism of job
        // vertices and partition-reuse
        //作用：为那些无法链化的边缘算子，配置其向外吐出数据的物理输出通道（配置序列化格式、数据缓冲区等）。细节：由于前一步的动态并发度（Parallelism）调整可能会改变顶点和分区的复用策略（Partition-reuse），
        //所以 Flink 2.2 刻意将这一步推迟到动态图判定之后执行。它建立一个映射表，将每个算子和它断链后的物理输出（NonChainedOutput）死死绑定，确保数据能正确流向物理边缘
        final Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputs = new HashMap<>();
        setAllOperatorNonChainedOutputsConfigs(opIntermediateOutputs, jobVertexBuildContext);
        setAllVertexNonChainedOutputsConfigs(opIntermediateOutputs);
        //构建物理连接边
        //将断链边界的逻辑边（StreamEdge）正式实例化为物理世界里的 JobEdge（作业边） 和 IntermediateDataSet（中间数据集）。
        //细节：它会把诸如 keyBy 的 Hash 分区器、rebalance 的轮询分区器等物理传输路由规则，真正写入 JobEdge 的配置项中，以此指导 TaskManager 运行时底层 Netty 组件的网络数据分发
        setPhysicalEdges(jobVertexBuildContext);
        //并发多尝试（Speculative Execution）标记 标记当前图中的算子是否支持推测执行
        //在 Batch 模式下，如果某个 Task 运行过慢（长尾作业），
        //Flink 支持启动一个一模一样的并发 Task 去抢跑。该方法负责检查哪些 JobVertex 满足推测执行的条件（例如不能包含某些不安全的状态或非幂等 Source），并为其打上特殊的“通行证”标记
        markSupportingConcurrentExecutionAttempts(jobVertexBuildContext);
        //批处理混合洗牌（Hybrid Shuffle）安全校验
        //作用：校验 Hybrid Shuffle（混合落盘模式） 的运行合法性。细节：Hybrid Shuffle 是 Flink 批处理兼顾吞吐与低延迟的黑科技（数据既可以留在内存直推，也可以落盘）。
        // 此步骤严格检查在 BATCH 模式下，生成的 JobVertex 结构和数据边是否能绝对安全、完美地支持 Hybrid Shuffle 的内存段隔离机制
        validateHybridShuffleExecuteInBatchMode(jobVertexBuildContext);
        //槽位共享与共宿分配
        //决定哪些 JobVertex 可以挤在同一个 TaskManager 的物理 Slot 线程中。它会遍历所有的顶点，
        // 读取用户的 .slotSharingGroup(...) 配置，并为具有关联性（如 CoGroup 或特异性 State）的算子构建 CoLocationGroup（共宿组），确保它们部署在同一台机器上以消减网络开销
        setSlotSharingAndCoLocation(jobVertexBuildContext);
        //托管内存分配比例计算
        //作用：精准计算每个物理顶点对 托管内存（Managed Memory） 的瓜分占比。
        //细节：Flink 2.2 对堆外托管内存有着极其严苛的管理。如果一个物理链里同时包含 RocksDB 状态后端、批处理排序（Batch Operator）以及 Python UDF 算子，
        // 该方法会精确算出每一类组件在该顶点总托管内存中应当占有的 Fraction（权重比例值），防止运行时 OOM
        setManagedMemoryFraction(jobVertexBuildContext);
        //拓扑序号前缀注入（为了日志与监控可读性）
        addVertexIndexPrefixInVertexName(jobVertexBuildContext, new AtomicInteger(0));
        //作用：为每个 JobVertex 注入详细的物理拓扑描述（JSON 或文本结构）。
        //细节：把这个物理顶点里到底“打包/融合”了哪些具体的逻辑算子名、UID 以及配置详情写进 Description。后续在 Web UI 页面点击该算子节点时，弹出的“详细物理信息面板”正是由此处提供的数据支撑
        setVertexDescription(jobVertexBuildContext);

        // Wait for the serialization of operator coordinators and stream config.
        //为了防止主线程卡顿，它启动了一个专门的线程池 serializationExecutor 异步去执行这一步。将我们在 createSourceChainInfo 中提取出的那些极其沉重、面向分布式大脑的 OperatorCoordinator.Provider（算子协调器） 序列化为字节数组。
        //将所有物理顶点的全量 StreamConfig（包含算子链内部信息）全部持久化、序列化完毕并安全回填到 JobGraph 实例中
        serializeOperatorCoordinatorsAndStreamConfig(serializationExecutor, jobVertexBuildContext);

        return jobGraph;
    }

    public static void serializeOperatorCoordinatorsAndStreamConfig(
            Executor serializationExecutor, JobVertexBuildContext jobVertexBuildContext) {
        try {
            FutureUtils.combineAll(
                            jobVertexBuildContext.getChainInfosInOrder().values().stream()
                                    .flatMap(
                                            chainInfo ->
                                                    serializationOperatorConfigs(
                                                            chainInfo, serializationExecutor))
                                    .collect(Collectors.toList()))
                    .get();

            waitForSerializationFuturesAndUpdateJobVertices(jobVertexBuildContext);
        } catch (Exception e) {
            throw new FlinkRuntimeException("Error in serialization.", e);
        }
    }

    private static Stream<CompletableFuture<StreamConfig>> serializationOperatorConfigs(
            OperatorChainInfo chainInfo, Executor serializationExecutor) {
        return chainInfo.getOperatorInfos().values().stream()
                .map(
                        operatorInfo ->
                                operatorInfo
                                        .getVertexConfig()
                                        .triggerSerializationAndReturnFuture(
                                                serializationExecutor));
    }

    /**
     * Creates an instance of {@link OperatorID} based on the provided operator unique identifier
     * (UID).
     *
     * @param operatorUid the unique identifier of the operator, used to generate the hash
     * @return a new {@link OperatorID} instance generated from the specified operator UID
     */
    public static OperatorID generateOperatorID(String operatorUid) {
        return new OperatorID(StreamGraphHasherV2.generateUserSpecifiedHash(operatorUid));
    }

    private static void waitForSerializationFuturesAndUpdateJobVertices(
            JobVertexBuildContext jobVertexBuildContext)
            throws ExecutionException, InterruptedException {
        for (Map.Entry<
                        JobVertexID,
                        List<CompletableFuture<SerializedValue<OperatorCoordinator.Provider>>>>
                futuresPerJobVertex :
                        jobVertexBuildContext
                                .getCoordinatorSerializationFuturesPerJobVertex()
                                .entrySet()) {
            final JobVertexID jobVertexId = futuresPerJobVertex.getKey();
            final JobVertex jobVertex =
                    jobVertexBuildContext.getJobGraph().findVertexByID(jobVertexId);

            Preconditions.checkState(
                    jobVertex != null,
                    "OperatorCoordinator providers were registered for JobVertexID '%s' but no corresponding JobVertex can be found.",
                    jobVertexId);
            FutureUtils.combineAll(futuresPerJobVertex.getValue())
                    .get()
                    .forEach(jobVertex::addOperatorCoordinator);
        }
    }

    public static void addVertexIndexPrefixInVertexName(
            JobVertexBuildContext jobVertexBuildContext, AtomicInteger vertexIndexId) {
        if (!jobVertexBuildContext.getStreamGraph().isVertexNameIncludeIndexPrefix()) {
            return;
        }
        Set<JobVertexID> jobVertexIds =
                jobVertexBuildContext.getJobVerticesInOrder().values().stream()
                        .map(JobVertex::getID)
                        .collect(Collectors.toSet());
        // JobVertexBuildContext only contains incrementally generated jobVertex instances. The
        // setName method needs to set the names based on the topological order of all jobVertices,
        // so it relies on the job graph to compute a difference set.
        jobVertexBuildContext
                .getJobGraph()
                .getVerticesSortedTopologicallyFromSources()
                .forEach(
                        vertex -> {
                            if (jobVertexIds.contains(vertex.getID())) {
                                vertex.setName(
                                        String.format(
                                                "[vertex-%d]%s",
                                                vertexIndexId.getAndIncrement(), vertex.getName()));
                            }
                        });
    }

    public static void setVertexDescription(JobVertexBuildContext jobVertexBuildContext) {
        final Map<Integer, JobVertex> jobVertices = jobVertexBuildContext.getJobVerticesInOrder();
        final StreamGraph streamGraph = jobVertexBuildContext.getStreamGraph();
        for (Map.Entry<Integer, JobVertex> headOpAndJobVertex : jobVertices.entrySet()) {
            Integer headOpId = headOpAndJobVertex.getKey();
            JobVertex vertex = headOpAndJobVertex.getValue();
            StringBuilder builder = new StringBuilder();
            switch (streamGraph.getVertexDescriptionMode()) {
                case CASCADING:
                    buildCascadingDescription(builder, headOpId, headOpId, jobVertexBuildContext);
                    break;
                case TREE:
                    buildTreeDescription(
                            builder, headOpId, headOpId, "", true, jobVertexBuildContext);
                    break;
                default:
                    throw new IllegalArgumentException(
                            String.format(
                                    "Description mode %s not supported",
                                    streamGraph.getVertexDescriptionMode()));
            }
            vertex.setOperatorPrettyName(builder.toString());
        }
    }

    private static void buildCascadingDescription(
            StringBuilder builder,
            int headOpId,
            int currentOpId,
            JobVertexBuildContext jobVertexBuildContext) {
        StreamNode node = jobVertexBuildContext.getStreamGraph().getStreamNode(currentOpId);
        builder.append(getDescriptionWithChainedSourcesInfo(node, jobVertexBuildContext));

        LinkedList<Integer> chainedOutput =
                getChainedOutputNodes(headOpId, node, jobVertexBuildContext);
        if (chainedOutput.isEmpty()) {
            return;
        }
        builder.append(" -> ");

        boolean multiOutput = chainedOutput.size() > 1;
        if (multiOutput) {
            builder.append("(");
        }
        while (true) {
            Integer outputId = chainedOutput.pollFirst();
            buildCascadingDescription(builder, headOpId, outputId, jobVertexBuildContext);
            if (chainedOutput.isEmpty()) {
                break;
            }
            builder.append(" , ");
        }
        if (multiOutput) {
            builder.append(")");
        }
    }

    private static LinkedList<Integer> getChainedOutputNodes(
            int headOpId, StreamNode node, JobVertexBuildContext jobVertexBuildContext) {
        LinkedList<Integer> chainedOutput = new LinkedList<>();
        Map<Integer, Map<Integer, StreamConfig>> chainedConfigs =
                jobVertexBuildContext.getChainedConfigs();
        if (chainedConfigs.containsKey(headOpId)) {
            for (StreamEdge edge : node.getOutEdges()) {
                int targetId = edge.getTargetId();
                if (chainedConfigs.get(headOpId).containsKey(targetId)) {
                    chainedOutput.add(targetId);
                }
            }
        }
        return chainedOutput;
    }

    private static void buildTreeDescription(
            StringBuilder builder,
            int headOpId,
            int currentOpId,
            String prefix,
            boolean isLast,
            JobVertexBuildContext jobVertexBuildContext) {
        // Replace the '-' in prefix of current node with ' ', keep ':'
        // HeadNode
        // :- Node1
        // :  :- Child1
        // :  +- Child2
        // +- Node2
        //    :- Child3
        //    +- Child4
        String currentNodePrefix = "";
        String childPrefix = "";
        if (currentOpId != headOpId) {
            if (isLast) {
                currentNodePrefix = prefix + "+- ";
                childPrefix = prefix + "   ";
            } else {
                currentNodePrefix = prefix + ":- ";
                childPrefix = prefix + ":  ";
            }
        }

        StreamNode node = jobVertexBuildContext.getStreamGraph().getStreamNode(currentOpId);
        builder.append(currentNodePrefix);
        builder.append(getDescriptionWithChainedSourcesInfo(node, jobVertexBuildContext));
        builder.append("\n");

        LinkedList<Integer> chainedOutput =
                getChainedOutputNodes(headOpId, node, jobVertexBuildContext);
        while (!chainedOutput.isEmpty()) {
            Integer outputId = chainedOutput.pollFirst();
            buildTreeDescription(
                    builder,
                    headOpId,
                    outputId,
                    childPrefix,
                    chainedOutput.isEmpty(),
                    jobVertexBuildContext);
        }
    }

    private static String getDescriptionWithChainedSourcesInfo(
            StreamNode node, JobVertexBuildContext jobVertexBuildContext) {

        List<StreamNode> chainedSources;
        final Map<Integer, Map<Integer, StreamConfig>> chainedConfigs =
                jobVertexBuildContext.getChainedConfigs();
        final StreamGraph streamGraph = jobVertexBuildContext.getStreamGraph();

        if (!chainedConfigs.containsKey(node.getId())) {
            // node is not head operator of a vertex
            chainedSources = Collections.emptyList();
        } else {
            chainedSources =
                    node.getInEdges().stream()
                            .map(StreamEdge::getSourceId)
                            .filter(
                                    id ->
                                            streamGraph.getSourceIDs().contains(id)
                                                    && chainedConfigs
                                                            .get(node.getId())
                                                            .containsKey(id))
                            .map(streamGraph::getStreamNode)
                            .collect(Collectors.toList());
        }
        return chainedSources.isEmpty()
                ? node.getOperatorDescription()
                : String.format(
                        "%s [%s]",
                        node.getOperatorDescription(),
                        chainedSources.stream()
                                .map(StreamNode::getOperatorDescription)
                                .collect(Collectors.joining(", ")));
    }

    @SuppressWarnings("deprecation")
    public static void preValidate(StreamGraph streamGraph, ClassLoader userClassloader) {
        CheckpointConfig checkpointConfig = streamGraph.getCheckpointConfig();

        if (checkpointConfig.isCheckpointingEnabled()) {
            // temporarily forbid checkpointing for iterative jobs
            if (streamGraph.isIterative()
                    && checkpointConfig.isUnalignedCheckpointsEnabled()
                    && !checkpointConfig.isForceUnalignedCheckpoints()) {
                throw new UnsupportedOperationException(
                        "Unaligned Checkpoints are currently not supported for iterative jobs, "
                                + "as rescaling would require alignment (in addition to the reduced checkpointing guarantees)."
                                + "\nThe user can force Unaligned Checkpoints by using 'execution.checkpointing.unaligned.forced'");
            }
            if (checkpointConfig.isUnalignedCheckpointsEnabled()
                    && !checkpointConfig.isForceUnalignedCheckpoints()
                    && streamGraph.getStreamNodes().stream()
                            .anyMatch(StreamingJobGraphGenerator::hasCustomPartitioner)) {
                throw new UnsupportedOperationException(
                        "Unaligned checkpoints are currently not supported for custom partitioners, "
                                + "as rescaling is not guaranteed to work correctly."
                                + "\nThe user can force Unaligned Checkpoints by using 'execution.checkpointing.unaligned.forced'");
            }

            for (StreamNode node : streamGraph.getStreamNodes()) {
                StreamOperatorFactory operatorFactory = node.getOperatorFactory();
                if (operatorFactory != null) {
                    Class<?> operatorClass =
                            operatorFactory.getStreamOperatorClass(userClassloader);
                    if (InputSelectable.class.isAssignableFrom(operatorClass)) {

                        throw new UnsupportedOperationException(
                                "Checkpointing is currently not supported for operators that implement InputSelectable:"
                                        + operatorClass.getName());
                    }
                }
            }
        }
    }

    private static boolean hasCustomPartitioner(StreamNode node) {
        return node.getOutEdges().stream()
                .anyMatch(edge -> edge.getPartitioner() instanceof CustomPartitionerWrapper);
    }

    public static void setPhysicalEdges(JobVertexBuildContext jobVertexBuildContext) {
        Map<Integer, List<StreamEdge>> physicalInEdgesInOrder =
                new HashMap<Integer, List<StreamEdge>>();

        for (StreamEdge edge : jobVertexBuildContext.getPhysicalEdgesInOrder()) {
            int target = edge.getTargetId();

            List<StreamEdge> inEdges =
                    physicalInEdgesInOrder.computeIfAbsent(target, k -> new ArrayList<>());

            inEdges.add(edge);
        }

        for (Map.Entry<Integer, List<StreamEdge>> inEdges : physicalInEdgesInOrder.entrySet()) {
            int vertex = inEdges.getKey();
            List<StreamEdge> edgeList = inEdges.getValue();

            jobVertexBuildContext
                    .getChainInfo(vertex)
                    .getOperatorInfo(vertex)
                    .getVertexConfig()
                    .setInPhysicalEdges(edgeList);
        }
    }

    //扫描整个拓扑图，找出所有能够作为“链化源头”（Chain Head）的入口节点，并为它们建立第一批初始的算子链信息
    private Map<Integer, OperatorChainInfo> buildChainedInputsAndGetHeadInputs() {
        final Map<Integer, OperatorChainInfo> chainEntryPoints = new HashMap<>();

        for (Integer sourceNodeId : streamGraph.getSourceIDs()) {
            //获取StreamNode
            final StreamNode sourceNode = streamGraph.getStreamNode(sourceNodeId);
            //判断是否满足与其下游算子进行 Chaining 合并的物理条件
            if (isChainableSource(sourceNode, streamGraph)) {//
                //可以链化的 Source
                //这个方法会提前分配并开辟一个共同的 OperatorChainInfo（链化容器），把这个 Source 作为这个链的 Head（头部节点） 塞进去
                createSourceChainInfo(sourceNode, chainEntryPoints, jobVertexBuildContext);//
            } else {
                //无法链化的 Source 生成一个独立的 OperatorChainInfo，但它的上下游链条里，只包含它自己这一个孤零零的节点
                chainEntryPoints.put(sourceNodeId, new OperatorChainInfo(sourceNodeId));//
            }
        }

        return chainEntryPoints;
    }

    /**
     * Sets up task chains from the source {@link StreamNode} instances.
     *
     * <p>This will recursively create all {@link JobVertex} instances.
     */
    private void setChaining() {
        // we separate out the sources that run as inputs to another operator (chained inputs)
        // from the sources that needs to run as the main (head) operator.
        final Map<Integer, OperatorChainInfo> chainEntryPoints = buildChainedInputsAndGetHeadInputs();
        final Collection<OperatorChainInfo> initialEntryPoints =
                chainEntryPoints.entrySet().stream()
                        .sorted(Comparator.comparing(Map.Entry::getKey))
                        .map(Map.Entry::getValue)
                        .collect(Collectors.toList());

        // iterate over a copy of the values, because this map gets concurrently modified
        for (OperatorChainInfo info : initialEntryPoints) {
            //
            createChain(
                    info.getStartNodeId(),
                    1, // operators start at position 1 because 0 is for chained source inputs
                    info,
                    chainEntryPoints,
                    true,
                    serializationExecutor,
                    jobVertexBuildContext,
                    null);
        }
    }

    public static List<StreamEdge> createChain(//
            final Integer currentNodeId,
            final int chainIndex,
            final OperatorChainInfo chainInfo,
            final Map<Integer, OperatorChainInfo> chainEntryPoints,
            final boolean canCreateNewChain,
            final Executor serializationExecutor,
            final JobVertexBuildContext jobVertexBuildContext,
            final @Nullable Consumer<Integer> visitedStreamNodeConsumer) {

        Integer startNodeId = chainInfo.getStartNodeId();
        if (!jobVertexBuildContext.getJobVerticesInOrder().containsKey(startNodeId)) {
            StreamGraph streamGraph = jobVertexBuildContext.getStreamGraph();

            // Adaptive graph generator needs to subscribe the visited stream node id to
            // generate hashes for it.
            if (visitedStreamNodeConsumer != null) {//false
                visitedStreamNodeConsumer.accept(currentNodeId);
            }
            //整个算子链最终暴露在外面的、用来连接其他物理顶点的物理出边总集
            List<StreamEdge> transitiveOutEdges = new ArrayList<StreamEdge>();
            //可以和当前算子链化合并的下游边
            List<StreamEdge> chainableOutputs = new ArrayList<StreamEdge>();
            //无法跟当前算子链化的下游边（比如因为遇到了 keyBy 网络分区或用户手动禁用了链化）
            List<StreamEdge> nonChainableOutputs = new ArrayList<StreamEdge>();

            StreamNode currentNode = streamGraph.getStreamNode(currentNodeId);
            Attribute currentNodeAttribute = currentNode.getAttribute();
            boolean isNoOutputUntilEndOfInput =
                    currentNode.isOutputOnlyAfterEndOfStream()
                            || currentNodeAttribute.isNoOutputUntilEndOfInput();
            if (isNoOutputUntilEndOfInput) {//false
                currentNodeAttribute.setNoOutputUntilEndOfInput(true);
            }

            for (StreamEdge outEdge : currentNode.getOutEdges()) {
                if (isChainable(outEdge, streamGraph)) {
                    // 属于当前链的内部延续
                    chainableOutputs.add(outEdge);
                } else {
                    // 链在此处被强行斩断
                    nonChainableOutputs.add(outEdge);
                }
            }

            //顺着 chainableOutputs 递归（壮大当前阵营）：
            for (StreamEdge chainable : chainableOutputs) {
                StreamNode targetNode = streamGraph.getStreamNode(chainable.getTargetId());
                Attribute targetNodeAttribute = targetNode.getAttribute();
                if (isNoOutputUntilEndOfInput) {
                    if (targetNodeAttribute != null) {
                        targetNodeAttribute.setNoOutputUntilEndOfInput(true);
                    }
                }
                transitiveOutEdges.addAll(
                        createChain(
                                chainable.getTargetId(),
                                chainIndex + 1,
                                chainInfo,
                                chainEntryPoints,
                                canCreateNewChain,
                                serializationExecutor,
                                jobVertexBuildContext,
                                visitedStreamNodeConsumer));
                // Mark upstream nodes in the same chain as outputBlocking
                if (targetNodeAttribute != null
                        && targetNodeAttribute.isNoOutputUntilEndOfInput()) {
                    currentNodeAttribute.setNoOutputUntilEndOfInput(true);
                }
            }

            //顺着 nonChainableOutputs 递归（开启新的独立阵营）
            for (StreamEdge nonChainable : nonChainableOutputs) {
                transitiveOutEdges.add(nonChainable);
                // Used to control whether a new chain can be created, this value is true in the
                // full graph generation algorithm and false in the progressive generation
                // algorithm. In the future, this variable can be a boolean type function to adapt
                // to more adaptive scenarios.
                if (canCreateNewChain) {//true
                    createChain(
                            nonChainable.getTargetId(),
                            1, // operators start at position 1 because 0 is for chained source
                            // inputs
                            chainEntryPoints.computeIfAbsent(
                                    nonChainable.getTargetId(),
                                    (k) -> chainInfo.newChain(nonChainable.getTargetId())),
                            chainEntryPoints,
                            canCreateNewChain,
                            serializationExecutor,
                            jobVertexBuildContext,
                            visitedStreamNodeConsumer);
                }
            }

            chainInfo.addChainedName(
                    currentNodeId,
                    createChainedName(
                            currentNodeId,
                            chainableOutputs,
                            Optional.ofNullable(chainEntryPoints.get(currentNodeId)),
                            chainInfo.getChainedNames(),
                            jobVertexBuildContext));

            chainInfo.addChainedMinResources(
                    currentNodeId,
                    createChainedMinResources(
                            currentNodeId, chainableOutputs, chainInfo, jobVertexBuildContext));

            chainInfo.addChainedPreferredResources(
                    currentNodeId,
                    createChainedPreferredResources(
                            currentNodeId, chainableOutputs, chainInfo, jobVertexBuildContext));

            OperatorID currentOperatorId =
                    chainInfo.addNodeToChain(
                            currentNodeId,
                            streamGraph.getStreamNode(currentNodeId).getOperatorName(),
                            jobVertexBuildContext);

            if (currentNode.getInputFormat() != null) {
                chainInfo
                        .getOrCreateFormatContainer()
                        .addInputFormat(currentOperatorId, currentNode.getInputFormat());
            }

            if (currentNode.getOutputFormat() != null) {
                chainInfo
                        .getOrCreateFormatContainer()
                        .addOutputFormat(currentOperatorId, currentNode.getOutputFormat());
            }
            OperatorInfo operatorInfo =
                    chainInfo.createAndGetOperatorInfo(currentNodeId, currentOperatorId);

            StreamConfig config;
            if (currentNodeId.equals(startNodeId)) {
                //证明这整条链的所有算子都已经被遍历并处理完毕了  实例化出核心的物理顶点
                //它会把当前算子链的所有对外交界出边（transitiveOutEdges），全部转换为连接其他物理顶点的 JobEdge（物理边） 和 IntermediateDataSet（中间数据集）
                JobVertex jobVertex = jobVertexBuildContext.getJobVertex(startNodeId);
                if (jobVertex == null) {
                    jobVertex =
                            createJobVertex(
                                    chainInfo, serializationExecutor, jobVertexBuildContext);
                }
                config = new StreamConfig(jobVertex.getConfiguration());
            } else {
                config = new StreamConfig(new Configuration());
            }

            tryConvertPartitionerForDynamicGraph(
                    chainableOutputs, nonChainableOutputs, jobVertexBuildContext);
            config.setAttribute(currentNodeAttribute);
            config.setWatermarkDeclarations(streamGraph.getSerializedWatermarkDeclarations());
            setOperatorConfig(currentNodeId, config, chainInfo, jobVertexBuildContext);

            setOperatorChainedOutputsConfig(config, chainableOutputs, jobVertexBuildContext);
            // we cache the non-chainable outputs here, and set the non-chained config later
            operatorInfo.addNonChainableOutputs(nonChainableOutputs);

            if (currentNodeId.equals(startNodeId)) {
                chainInfo.setTransitiveOutEdges(transitiveOutEdges);
                jobVertexBuildContext.addChainInfo(startNodeId, chainInfo);

                config.setChainStart();
                config.setChainIndex(chainIndex);
                config.setOperatorName(streamGraph.getStreamNode(currentNodeId).getOperatorName());
                config.setTransitiveChainedTaskConfigs(
                        jobVertexBuildContext.getChainedConfigs().get(startNodeId));
            } else {
                config.setChainIndex(chainIndex);
                StreamNode node = streamGraph.getStreamNode(currentNodeId);
                config.setOperatorName(node.getOperatorName());
                jobVertexBuildContext
                        .getOrCreateChainedConfig(startNodeId)
                        .put(currentNodeId, config);
            }

            config.setOperatorID(currentOperatorId);

            if (chainableOutputs.isEmpty()) {
                config.setChainEnd();
            }
            return transitiveOutEdges;

        } else {
            return new ArrayList<>();
        }
    }

    /**
     * This method is used to reset or set job vertices' parallelism for dynamic graph:
     *
     * <p>1. Reset parallelism for job vertices whose parallelism is not configured.
     *
     * <p>2. Set parallelism and maxParallelism for job vertices in forward group, to ensure the
     * parallelism and maxParallelism of vertices in the same forward group to be the same; set the
     * parallelism at early stage if possible, to avoid invalid partition reuse.
     */
    private void setVertexParallelismsForDynamicGraphIfNecessary() {
        final Map<Integer, JobVertex> jobVertices = jobVertexBuildContext.getJobVerticesInOrder();
        // Note that the jobVertices are reverse topological order
        final List<JobVertex> topologicalOrderVertices =
                IterableUtils.toStream(jobVertices.values()).collect(Collectors.toList());
        Collections.reverse(topologicalOrderVertices);

        // reset parallelism for job vertices whose parallelism is not configured
        jobVertices.forEach(
                (startNodeId, jobVertex) -> {
                    final OperatorChainInfo chainInfo =
                            jobVertexBuildContext.getChainInfo(startNodeId);
                    if (!jobVertex.isParallelismConfigured()
                            && streamGraph.isAutoParallelismEnabled()) {
                        jobVertex.setParallelism(ExecutionConfig.PARALLELISM_DEFAULT);
                        chainInfo
                                .getAllChainedNodes()
                                .forEach(
                                        n ->
                                                n.setParallelism(
                                                        ExecutionConfig.PARALLELISM_DEFAULT,
                                                        false));
                    }
                });

        final Map<JobVertex, Set<JobVertex>> forwardProducersByJobVertex = new HashMap<>();
        jobVertices.forEach(
                (startNodeId, jobVertex) -> {
                    Set<JobVertex> forwardConsumers =
                            jobVertexBuildContext
                                    .getChainInfo(startNodeId)
                                    .getTransitiveOutEdges()
                                    .stream()
                                    .filter(
                                            edge ->
                                                    edge.getPartitioner()
                                                            instanceof ForwardPartitioner)
                                    .map(StreamEdge::getTargetId)
                                    .map(jobVertices::get)
                                    .collect(Collectors.toSet());

                    for (JobVertex forwardConsumer : forwardConsumers) {
                        forwardProducersByJobVertex.compute(
                                forwardConsumer,
                                (ignored, producers) -> {
                                    if (producers == null) {
                                        producers = new HashSet<>();
                                    }
                                    producers.add(jobVertex);
                                    return producers;
                                });
                    }
                });

        // compute forward groups
        final Map<JobVertexID, JobVertexForwardGroup> forwardGroupsByJobVertexId =
                ForwardGroupComputeUtil.computeForwardGroups(
                        topologicalOrderVertices,
                        jobVertex ->
                                forwardProducersByJobVertex.getOrDefault(
                                        jobVertex, Collections.emptySet()));

        jobVertices.forEach(
                (startNodeId, jobVertex) -> {
                    ForwardGroup forwardGroup = forwardGroupsByJobVertexId.get(jobVertex.getID());
                    // set parallelism for vertices in forward group
                    if (forwardGroup != null && forwardGroup.isParallelismDecided()) {
                        jobVertex.setParallelism(forwardGroup.getParallelism());
                        jobVertex.setParallelismConfigured(true);
                        jobVertexBuildContext
                                .getChainInfo(startNodeId)
                                .getAllChainedNodes()
                                .forEach(
                                        streamNode ->
                                                streamNode.setParallelism(
                                                        forwardGroup.getParallelism(), true));
                    }

                    // set max parallelism for vertices in forward group
                    if (forwardGroup != null && forwardGroup.isMaxParallelismDecided()) {
                        jobVertex.setMaxParallelism(forwardGroup.getMaxParallelism());
                        jobVertexBuildContext
                                .getChainInfo(startNodeId)
                                .getAllChainedNodes()
                                .forEach(
                                        streamNode ->
                                                streamNode.setMaxParallelism(
                                                        forwardGroup.getMaxParallelism()));
                    }
                });
    }

    public static JobGraph createAndInitializeJobGraph(
            StreamGraph streamGraph, @Nullable JobID jobId) {
        JobGraph jobGraph = new JobGraph(jobId, streamGraph.getJobName());
        jobGraph.setJobType(streamGraph.getJobType());
        jobGraph.setDynamic(streamGraph.isDynamic());

        jobGraph.enableApproximateLocalRecovery(
                streamGraph.getCheckpointConfig().isApproximateLocalRecoveryEnabled());

        jobGraph.setSnapshotSettings(streamGraph.getCheckpointingSettings());
        jobGraph.setSavepointRestoreSettings(streamGraph.getSavepointRestoreSettings());
        for (Map.Entry<String, DistributedCache.DistributedCacheEntry> entry :
                streamGraph.getUserArtifacts().entrySet()) {
            jobGraph.addUserArtifact(entry.getKey(), entry.getValue());
        }

        streamGraph.getUserJarBlobKeys().forEach(jobGraph::addUserJarBlobKey);
        jobGraph.setClasspaths(streamGraph.getClasspath());

        ExecutionConfig executionConfig = streamGraph.getExecutionConfig();
        if (executionConfig == null) {
            jobGraph.setSerializedExecutionConfig(streamGraph.getSerializedExecutionConfig());
        } else {
            try {
                jobGraph.setExecutionConfig(streamGraph.getExecutionConfig());
            } catch (IOException e) {
                throw new IllegalConfigurationException(
                        "Could not serialize the ExecutionConfig."
                                + "This indicates that non-serializable types (like custom serializers) were registered");
            }
        }

        jobGraph.setJobConfiguration(streamGraph.getJobConfiguration());

        if (!streamGraph.getJobStatusHooks().isEmpty()) {
            jobGraph.setJobStatusHooks(streamGraph.getJobStatusHooks());
        }

        return jobGraph;
    }

    public static void createSourceChainInfo(
            StreamNode sourceNode,
            Map<Integer, OperatorChainInfo> chainEntryPoints,
            JobVertexBuildContext jobVertexBuildContext) {
        Integer sourceNodeId = sourceNode.getId();
        StreamEdge sourceOutEdge = sourceNode.getOutEdges().get(0);

        //
        final OperatorChainInfo chainInfo =
                //sourceOutEdge.getTargetId() = 下游算子的 ID
                chainEntryPoints.computeIfAbsent(sourceOutEdge.getTargetId(), (k) -> new OperatorChainInfo(sourceOutEdge.getTargetId()));
        //
        final OperatorID opId = new OperatorID(jobVertexBuildContext.getHash(sourceNodeId));
        final OperatorInfo operatorInfo = chainInfo.createAndGetOperatorInfo(sourceNodeId, opId);
        final StreamConfig.SourceInputConfig inputConfig = new StreamConfig.SourceInputConfig(sourceOutEdge);
        final StreamConfig operatorConfig = new StreamConfig(new Configuration());
        setOperatorConfig(sourceNodeId, operatorConfig, chainInfo, jobVertexBuildContext);
        setOperatorChainedOutputsConfig(
                operatorConfig, Collections.emptyList(), jobVertexBuildContext);
        // we cache the non-chainable outputs here, and set the non-chained config later
        operatorInfo.addNonChainableOutputs(Collections.emptyList());
        //明确 Source 在这个物理算子链（OperatorChain）中的执行顺序
        operatorConfig.setChainIndex(0); // sources are always first
        operatorConfig.setOperatorID(opId);
        operatorConfig.setOperatorName(sourceNode.getOperatorName());

        final SourceOperatorFactory<?> sourceOperatorFactory = (SourceOperatorFactory<?>) Preconditions.checkNotNull(sourceNode.getOperatorFactory());
        final OperatorCoordinator.Provider coord = sourceOperatorFactory.getCoordinatorProvider(sourceNode.getOperatorName(), opId);

        chainInfo.addChainedSource(sourceNode, new ChainedSourceInfo(operatorConfig, inputConfig));
        chainInfo.addCoordinatorProvider(coord);
    }

    private static void checkAndReplaceReusableHybridPartitionType(
            NonChainedOutput reusableOutput) {
        if (reusableOutput.getPartitionType() == ResultPartitionType.HYBRID_SELECTIVE) {
            // for can be reused hybrid output, it can be optimized to always use full
            // spilling strategy to significantly reduce shuffle data writing cost.
            reusableOutput.setPartitionType(ResultPartitionType.HYBRID_FULL);
            LOG.info(
                    "{} result partition has been replaced by {} result partition to support partition reuse,"
                            + " which will reduce shuffle data writing cost.",
                    reusableOutput.getPartitionType().name(),
                    ResultPartitionType.HYBRID_FULL.name());
        }
    }

    public static String createChainedName(
            Integer vertexID,
            List<StreamEdge> chainedOutputs,
            Optional<OperatorChainInfo> operatorChainInfo,
            Map<Integer, String> chainedNames,
            JobVertexBuildContext jobVertexBuildContext) {
        StreamGraph streamGraph = jobVertexBuildContext.getStreamGraph();
        List<ChainedSourceInfo> chainedSourceInfos =
                operatorChainInfo
                        .map(
                                chainInfo ->
                                        getChainedSourcesByVertexId(
                                                vertexID, chainInfo, streamGraph))
                        .orElse(Collections.emptyList());
        final String operatorName =
                nameWithChainedSourcesInfo(
                        streamGraph.getStreamNode(vertexID).getOperatorName(), chainedSourceInfos);
        if (chainedOutputs.size() > 1) {
            List<String> outputChainedNames = new ArrayList<>();
            for (StreamEdge chainable : chainedOutputs) {
                outputChainedNames.add(chainedNames.get(chainable.getTargetId()));
            }
            return operatorName + " -> (" + StringUtils.join(outputChainedNames, ", ") + ")";
        } else if (chainedOutputs.size() == 1) {
            return operatorName + " -> " + chainedNames.get(chainedOutputs.get(0).getTargetId());
        } else {
            return operatorName;
        }
    }

    private static List<ChainedSourceInfo> getChainedSourcesByVertexId(
            Integer vertexId, OperatorChainInfo chainInfo, StreamGraph streamGraph) {
        return streamGraph.getStreamNode(vertexId).getInEdges().stream()
                .map(inEdge -> chainInfo.getChainedSources().get(inEdge.getSourceId()))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    public static ResourceSpec createChainedMinResources(
            Integer vertexID,
            List<StreamEdge> chainedOutputs,
            OperatorChainInfo operatorChainInfo,
            JobVertexBuildContext jobVertexBuildContext) {
        ResourceSpec minResources =
                jobVertexBuildContext.getStreamGraph().getStreamNode(vertexID).getMinResources();
        for (StreamEdge chainable : chainedOutputs) {
            minResources =
                    minResources.merge(
                            operatorChainInfo.getChainedMinResources(chainable.getTargetId()));
        }
        return minResources;
    }

    public static ResourceSpec createChainedPreferredResources(
            Integer vertexID,
            List<StreamEdge> chainedOutputs,
            OperatorChainInfo operatorChainInfo,
            JobVertexBuildContext jobVertexBuildContext) {
        ResourceSpec preferredResources =
                jobVertexBuildContext
                        .getStreamGraph()
                        .getStreamNode(vertexID)
                        .getPreferredResources();
        for (StreamEdge chainable : chainedOutputs) {
            preferredResources =
                    preferredResources.merge(
                            operatorChainInfo.getChainedPreferredResources(
                                    chainable.getTargetId()));
        }
        return preferredResources;
    }

    public static JobVertex createJobVertex(
            OperatorChainInfo chainInfo,
            Executor serializationExecutor,
            JobVertexBuildContext jobVertexBuildContext) {

        Integer streamNodeId = chainInfo.getStartNodeId();

        JobVertex jobVertex;
        StreamNode streamNode = jobVertexBuildContext.getStreamGraph().getStreamNode(streamNodeId);

        byte[] hash = jobVertexBuildContext.getHash(streamNodeId);

        if (hash == null) {
            throw new IllegalStateException(
                    "Cannot find node hash. "
                            + "Did you generate them before calling this method?");
        }

        JobVertexID jobVertexId = new JobVertexID(hash);

        List<ChainedOperatorHashInfo> chainedOperators =
                chainInfo.getChainedOperatorHashes(streamNodeId);
        List<OperatorIDPair> operatorIDPairs = new ArrayList<>();
        if (chainedOperators != null) {
            for (ChainedOperatorHashInfo chainedOperator : chainedOperators) {
                OperatorID userDefinedOperatorID =
                        chainedOperator.getUserDefinedOperatorId() == null
                                ? null
                                : new OperatorID(chainedOperator.getUserDefinedOperatorId());
                operatorIDPairs.add(
                        OperatorIDPair.of(
                                new OperatorID(chainedOperator.getGeneratedOperatorId()),
                                userDefinedOperatorID,
                                chainedOperator.getStreamNode().getOperatorName(),
                                chainedOperator.getStreamNode().getTransformationUID()));
            }
        }

        if (chainInfo.hasFormatContainer()) {
            jobVertex =
                    new InputOutputFormatVertex(
                            chainInfo.getChainedName(streamNodeId), jobVertexId, operatorIDPairs);
            chainInfo
                    .getOrCreateFormatContainer()
                    .write(new TaskConfig(jobVertex.getConfiguration()));
        } else {
            jobVertex =
                    new JobVertex(
                            chainInfo.getChainedName(streamNodeId), jobVertexId, operatorIDPairs);
        }

        if (streamNode.getConsumeClusterDatasetId() != null) {
            jobVertex.addIntermediateDataSetIdToConsume(streamNode.getConsumeClusterDatasetId());
        }

        final List<CompletableFuture<SerializedValue<OperatorCoordinator.Provider>>>
                serializationFutures = new ArrayList<>();
        for (OperatorCoordinator.Provider coordinatorProvider :
                chainInfo.getCoordinatorProviders()) {
            serializationFutures.add(
                    CompletableFuture.supplyAsync(
                            () -> {
                                try {
                                    return new SerializedValue<>(coordinatorProvider);
                                } catch (IOException e) {
                                    throw new FlinkRuntimeException(
                                            String.format(
                                                    "Coordinator Provider for node %s is not serializable.",
                                                    chainInfo.getChainedName(streamNodeId)),
                                            e);
                                }
                            },
                            serializationExecutor));
        }
        if (!serializationFutures.isEmpty()) {
            jobVertexBuildContext.putCoordinatorSerializationFutures(
                    jobVertexId, serializationFutures);
        }

        jobVertex.setResources(
                chainInfo.getChainedMinResources(streamNodeId),
                chainInfo.getChainedPreferredResources(streamNodeId));

        jobVertex.setInvokableClass(streamNode.getJobVertexClass());

        int parallelism = streamNode.getParallelism();

        if (parallelism > 0) {
            jobVertex.setParallelism(parallelism);
        } else {
            parallelism = jobVertex.getParallelism();
        }

        jobVertex.setMaxParallelism(streamNode.getMaxParallelism());

        if (LOG.isDebugEnabled()) {
            LOG.debug("Parallelism set: {} for {}", parallelism, streamNodeId);
        }

        jobVertexBuildContext.addJobVertex(streamNodeId, jobVertex);
        jobVertexBuildContext.getJobGraph().addVertex(jobVertex);

        jobVertex.setParallelismConfigured(
                chainInfo.getAllChainedNodes().stream()
                        .anyMatch(StreamNode::isParallelismConfigured));

        return jobVertex;
    }

    public static void setOperatorConfig(
            Integer vertexId,
            StreamConfig config,
            OperatorChainInfo chainInfo,
            JobVertexBuildContext jobVertexBuildContext) {
        StreamGraph streamGraph = jobVertexBuildContext.getStreamGraph();
        OperatorInfo operatorInfo = chainInfo.getOperatorInfo(vertexId);
        Map<Integer, ChainedSourceInfo> chainedSources = chainInfo.getChainedSources();
        StreamNode vertex = streamGraph.getStreamNode(vertexId);

        config.setVertexID(vertexId);

        // build the inputs as a combination of source and network inputs
        final List<StreamEdge> inEdges = vertex.getInEdges();
        final TypeSerializer<?>[] inputSerializers = vertex.getTypeSerializersIn();

        final StreamConfig.InputConfig[] inputConfigs =
                new StreamConfig.InputConfig[inputSerializers.length];

        int inputGateCount = 0;
        for (final StreamEdge inEdge : inEdges) {
            final ChainedSourceInfo chainedSource = chainedSources.get(inEdge.getSourceId());

            final int inputIndex =
                    inEdge.getTypeNumber() == 0
                            ? 0 // single input operator
                            : inEdge.getTypeNumber() - 1; // in case of 2 or more inputs

            Preconditions.checkState(
                    inputIndex < inputSerializers.length,
                    "Could not find valid input serializers when creating job graph for edge: %s",
                    inEdge);

            if (chainedSource != null) {
                // chained source is the input
                if (inputConfigs[inputIndex] != null) {
                    throw new IllegalStateException(
                            "Trying to union a chained source with another input.");
                }
                inputConfigs[inputIndex] = chainedSource.getInputConfig();
                jobVertexBuildContext
                        .getOrCreateChainedConfig(vertexId)
                        .put(inEdge.getSourceId(), chainedSource.getOperatorConfig());
            } else {
                // network input. null if we move to a new input, non-null if this is a further edge
                // that is union-ed into the same input
                if (inputConfigs[inputIndex] == null) {
                    // PASS_THROUGH is a sensible default for streaming jobs. Only for BATCH
                    // execution can we have sorted inputs
                    StreamConfig.InputRequirement inputRequirement =
                            vertex.getInputRequirements()
                                    .getOrDefault(
                                            inputIndex, StreamConfig.InputRequirement.PASS_THROUGH);
                    inputConfigs[inputIndex] =
                            new StreamConfig.NetworkInputConfig(
                                    inputSerializers[inputIndex],
                                    inputGateCount++,
                                    inputRequirement);
                }
            }
        }

        // set the input config of the vertex if it consumes from cached intermediate dataset.
        if (vertex.getConsumeClusterDatasetId() != null) {
            config.setNumberOfNetworkInputs(1);
            inputConfigs[0] = new StreamConfig.NetworkInputConfig(inputSerializers[0], 0);
        }

        config.setInputs(inputConfigs);

        config.setTypeSerializerOut(vertex.getTypeSerializerOut());

        config.setStreamOperatorFactory(vertex.getOperatorFactory());

        final CheckpointConfig checkpointCfg = streamGraph.getCheckpointConfig();

        config.setSerializedStateBackend(
                streamGraph.getCheckpointingSettings().getDefaultStateBackend(),
                Boolean.TRUE.equals(
                        streamGraph
                                .getCheckpointingSettings()
                                .isStateBackendUseManagedMemory()
                                .getAsBoolean()));
        config.setSerializedCheckpointStorage(
                streamGraph.getCheckpointingSettings().getDefaultCheckpointStorage());
        config.setGraphContainingLoops(streamGraph.isIterative());
        config.setTimerServiceProvider(streamGraph.getTimerServiceProvider());
        config.getConfiguration()
                .set(
                        CheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH,
                        streamGraph.isEnableCheckpointsAfterTasksFinish());

        for (int i = 0; i < vertex.getStatePartitioners().length; i++) {
            config.setStatePartitioner(i, vertex.getStatePartitioners()[i]);
        }
        config.setStateKeySerializer(vertex.getStateKeySerializer());
        config.setAdditionalMetricVariables(vertex.getAdditionalMetricVariables());

        Class<? extends TaskInvokable> vertexClass = vertex.getJobVertexClass();

        if (vertexClass.equals(StreamIterationHead.class)
                || vertexClass.equals(StreamIterationTail.class)) {
            config.setIterationId(streamGraph.getBrokerID(vertexId));
            config.setIterationWaitTime(streamGraph.getLoopTimeout(vertexId));
        }

        operatorInfo.setVertexConfig(config);
    }

    public static void setOperatorChainedOutputsConfig(
            StreamConfig config,
            List<StreamEdge> chainableOutputs,
            JobVertexBuildContext jobVertexBuildContext) {
        // iterate edges, find sideOutput edges create and save serializers for each outputTag type
        for (StreamEdge edge : chainableOutputs) {
            if (edge.getOutputTag() != null) {
                config.setTypeSerializerSideOut(
                        edge.getOutputTag(),
                        edge.getOutputTag()
                                .getTypeInfo()
                                .createSerializer(
                                        jobVertexBuildContext
                                                .getStreamGraph()
                                                .getExecutionConfig()
                                                .getSerializerConfig()));
            }
        }
        config.setChainedOutputs(chainableOutputs);
    }

    private static void setOperatorNonChainedOutputsConfig(
            JobVertexID jobVertexId,
            Integer streamNodeId,
            StreamConfig config,
            List<StreamEdge> nonChainableOutputs,
            Map<StreamEdge, NonChainedOutput> outputsConsumedByEdge,
            JobVertexBuildContext jobVertexBuildContext) {
        // iterate edges, find sideOutput edges create and save serializers for each outputTag type
        for (StreamEdge edge : nonChainableOutputs) {
            if (edge.getOutputTag() != null) {
                config.setTypeSerializerSideOut(
                        edge.getOutputTag(),
                        edge.getOutputTag()
                                .getTypeInfo()
                                .createSerializer(
                                        jobVertexBuildContext
                                                .getStreamGraph()
                                                .getExecutionConfig()
                                                .getSerializerConfig()));
            }
        }

        List<NonChainedOutput> deduplicatedOutputs =
                mayReuseNonChainedOutputs(
                        jobVertexId,
                        streamNodeId,
                        nonChainableOutputs,
                        outputsConsumedByEdge,
                        jobVertexBuildContext);
        config.setNumberOfOutputs(deduplicatedOutputs.size());
        config.setOperatorNonChainedOutputs(deduplicatedOutputs);
    }

    private void setVertexNonChainedOutputsConfig(
            Integer startNodeId,
            StreamConfig config,
            List<StreamEdge> transitiveOutEdges,
            final Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputs) {

        LinkedHashSet<NonChainedOutput> transitiveOutputs = new LinkedHashSet<>();
        for (StreamEdge edge : transitiveOutEdges) {
            NonChainedOutput output = opIntermediateOutputs.get(edge.getSourceId()).get(edge);
            transitiveOutputs.add(output);
            connect(
                            startNodeId,
                            edge,
                            output,
                            jobVertexBuildContext.getJobVerticesInOrder(),
                            jobVertexBuildContext)
                    .increaseNumJobEdgesToCreate();
        }

        config.setVertexNonChainedOutputs(new ArrayList<>(transitiveOutputs));
    }

    public static void setAllOperatorNonChainedOutputsConfigs(
            final Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputs,
            JobVertexBuildContext jobVertexBuildContext) {
        // set non chainable output config
        jobVertexBuildContext
                .getChainInfosInOrder()
                .forEach(
                        (startNodeId, chainInfo) -> {
                            JobVertexID jobVertexId =
                                    jobVertexBuildContext.getJobVertex(startNodeId).getID();

                            setOperatorNonChainedOutputsConfigs(
                                    opIntermediateOutputs,
                                    jobVertexBuildContext,
                                    chainInfo,
                                    jobVertexId);
                        });
    }

    private static void setOperatorNonChainedOutputsConfigs(
            Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputs,
            JobVertexBuildContext jobVertexBuildContext,
            OperatorChainInfo chainInfo,
            JobVertexID jobVertexId) {
        chainInfo
                .getOperatorInfos()
                .forEach(
                        (streamNodeId, operatorInfo) -> {
                            Map<StreamEdge, NonChainedOutput> outputsConsumedByEdge =
                                    opIntermediateOutputs.computeIfAbsent(
                                            streamNodeId, ignored -> new HashMap<>());
                            setOperatorNonChainedOutputsConfig(
                                    jobVertexId,
                                    streamNodeId,
                                    operatorInfo.getVertexConfig(),
                                    operatorInfo.getNonChainableOutputs(),
                                    outputsConsumedByEdge,
                                    jobVertexBuildContext);
                        });
    }

    private void setAllVertexNonChainedOutputsConfigs(
            final Map<Integer, Map<StreamEdge, NonChainedOutput>> opIntermediateOutputs) {
        jobVertexBuildContext
                .getJobVerticesInOrder()
                .keySet()
                .forEach(
                        startNodeId ->
                                setVertexNonChainedOutputsConfig(
                                        startNodeId,
                                        jobVertexBuildContext
                                                .getChainInfo(startNodeId)
                                                .getOperatorInfo(startNodeId)
                                                .getVertexConfig(),
                                        jobVertexBuildContext
                                                .getChainInfo(startNodeId)
                                                .getTransitiveOutEdges(),
                                        opIntermediateOutputs));
    }

    private static List<NonChainedOutput> mayReuseNonChainedOutputs(
            JobVertexID jobVertexId,
            int streamNodeId,
            List<StreamEdge> consumerEdges,
            Map<StreamEdge, NonChainedOutput> outputsConsumedByEdge,
            JobVertexBuildContext jobVertexBuildContext) {
        if (consumerEdges.isEmpty()) {
            return new ArrayList<>();
        }
        List<NonChainedOutput> outputs = new ArrayList<>(consumerEdges.size());
        for (StreamEdge consumerEdge : consumerEdges) {
            checkState(
                    streamNodeId == consumerEdge.getSourceId(), "stream node id must be the same.");
            ResultPartitionType partitionType =
                    getResultPartitionType(consumerEdge, jobVertexBuildContext);
            IntermediateDataSetID dataSetId =
                    new IntermediateDataSetID(jobVertexId, consumerEdge.getEdgeId().hashCode());

            boolean isPersistentDataSet =
                    isPersistentIntermediateDataset(partitionType, consumerEdge);
            if (isPersistentDataSet) {
                partitionType = ResultPartitionType.BLOCKING_PERSISTENT;
                dataSetId = consumerEdge.getIntermediateDatasetIdToProduce();
            }

            if (partitionType.isHybridResultPartition()) {
                jobVertexBuildContext.setHasHybridResultPartition(true);
                if (consumerEdge.getPartitioner().isBroadcast()
                        && partitionType == ResultPartitionType.HYBRID_SELECTIVE) {
                    // for broadcast result partition, it can be optimized to always use full
                    // spilling strategy to significantly reduce shuffle data writing cost.
                    LOG.info(
                            "{} result partition has been replaced by {} result partition to support "
                                    + "broadcast optimization, which will reduce shuffle data writing cost.",
                            partitionType.name(),
                            ResultPartitionType.HYBRID_FULL.name());
                    partitionType = ResultPartitionType.HYBRID_FULL;
                }
            }

            createOrReuseOutput(
                    outputs,
                    outputsConsumedByEdge,
                    consumerEdge,
                    isPersistentDataSet,
                    dataSetId,
                    partitionType,
                    jobVertexBuildContext.getStreamGraph());
        }
        return outputs;
    }

    private static void createOrReuseOutput(
            List<NonChainedOutput> outputs,
            Map<StreamEdge, NonChainedOutput> outputsConsumedByEdge,
            StreamEdge consumerEdge,
            boolean isPersistentDataSet,
            IntermediateDataSetID dataSetId,
            ResultPartitionType partitionType,
            StreamGraph streamGraph) {
        int consumerParallelism =
                streamGraph.getStreamNode(consumerEdge.getTargetId()).getParallelism();
        int consumerMaxParallelism =
                streamGraph.getStreamNode(consumerEdge.getTargetId()).getMaxParallelism();
        NonChainedOutput reusableOutput = null;
        if (isPartitionTypeCanBeReuse(partitionType)) {
            for (NonChainedOutput outputCandidate : outputsConsumedByEdge.values()) {
                // Reusing the same output can improve performance. The target output can be reused
                // if meeting the following conditions:
                // 1. all is hybrid partition or are same re-consumable partition.
                // 2. have the same partitioner, consumer parallelism, persistentDataSetId,
                // outputTag.
                if (allHybridOrSameReconsumablePartitionType(
                                outputCandidate.getPartitionType(), partitionType)
                        && consumerParallelism == outputCandidate.getConsumerParallelism()
                        && consumerMaxParallelism == outputCandidate.getConsumerMaxParallelism()
                        && Objects.equals(
                                outputCandidate.getPersistentDataSetId(),
                                consumerEdge.getIntermediateDatasetIdToProduce())
                        && Objects.equals(
                                outputCandidate.getOutputTag(), consumerEdge.getOutputTag())
                        && Objects.equals(
                                consumerEdge.getPartitioner(), outputCandidate.getPartitioner())) {
                    reusableOutput = outputCandidate;
                    outputsConsumedByEdge.put(consumerEdge, reusableOutput);
                    checkAndReplaceReusableHybridPartitionType(reusableOutput);
                    break;
                }
            }
        }
        if (reusableOutput == null) {
            NonChainedOutput output =
                    new NonChainedOutput(
                            consumerEdge.supportsUnalignedCheckpoints(),
                            consumerEdge.getSourceId(),
                            consumerParallelism,
                            consumerMaxParallelism,
                            consumerEdge.getBufferTimeout(),
                            isPersistentDataSet,
                            dataSetId,
                            consumerEdge.getOutputTag(),
                            consumerEdge.getPartitioner(),
                            partitionType);
            outputs.add(output);
            outputsConsumedByEdge.put(consumerEdge, output);
        }
    }

    private static boolean isPartitionTypeCanBeReuse(ResultPartitionType partitionType) {
        // for non-hybrid partition, partition reuse only works when its re-consumable.
        // for hybrid selective partition, it still has the opportunity to be converted to
        // hybrid full partition to support partition reuse.
        return partitionType.isReconsumable() || partitionType.isHybridResultPartition();
    }

    private static boolean allHybridOrSameReconsumablePartitionType(
            ResultPartitionType partitionType1, ResultPartitionType partitionType2) {
        return (partitionType1.isReconsumable() && partitionType1 == partitionType2)
                || (partitionType1.isHybridResultPartition()
                        && partitionType2.isHybridResultPartition());
    }

    public static void tryConvertPartitionerForDynamicGraph(
            List<StreamEdge> chainableOutputs,
            List<StreamEdge> nonChainableOutputs,
            JobVertexBuildContext jobVertexBuildContext) {
        StreamGraph streamGraph = jobVertexBuildContext.getStreamGraph();
        for (StreamEdge edge : chainableOutputs) {
            StreamPartitioner<?> partitioner = edge.getPartitioner();
            if (partitioner instanceof ForwardForConsecutiveHashPartitioner
                    || partitioner instanceof ForwardForUnspecifiedPartitioner) {
                checkState(
                        streamGraph.isDynamic(),
                        String.format(
                                "%s should only be used in dynamic graph.",
                                partitioner.getClass().getSimpleName()));
                edge.setPartitioner(new ForwardPartitioner<>());
            }
        }
        for (StreamEdge edge : nonChainableOutputs) {
            StreamPartitioner<?> partitioner = edge.getPartitioner();
            if (partitioner instanceof ForwardForConsecutiveHashPartitioner) {
                checkState(
                        streamGraph.isDynamic(),
                        "ForwardForConsecutiveHashPartitioner should only be used in dynamic graph.");
                edge.setPartitioner(
                        ((ForwardForConsecutiveHashPartitioner<?>) partitioner)
                                .getHashPartitioner());
            } else if (partitioner instanceof ForwardForUnspecifiedPartitioner) {
                checkState(
                        streamGraph.isDynamic(),
                        "ForwardForUnspecifiedPartitioner should only be used in dynamic graph.");
                edge.setPartitioner(new RescalePartitioner<>());
            }
        }
    }

    public static IntermediateDataSet connect(
            Integer headOfChain,
            StreamEdge edge,
            NonChainedOutput output,
            Map<Integer, JobVertex> jobVertices,
            JobVertexBuildContext jobVertexBuildContext) {

        jobVertexBuildContext.addPhysicalEdgesInOrder(edge);

        Integer downStreamVertexID = edge.getTargetId();

        JobVertex headVertex = jobVertices.get(headOfChain);
        JobVertex downStreamVertex = jobVertices.get(downStreamVertexID);

        StreamConfig downStreamConfig = new StreamConfig(downStreamVertex.getConfiguration());

        downStreamConfig.setNumberOfNetworkInputs(downStreamConfig.getNumberOfNetworkInputs() + 1);

        StreamPartitioner<?> partitioner = output.getPartitioner();
        ResultPartitionType resultPartitionType = output.getPartitionType();

        checkBufferTimeout(resultPartitionType, edge);

        JobEdge jobEdge;
        if (partitioner.isPointwise()) {
            jobEdge =
                    downStreamVertex.connectNewDataSetAsInput(
                            headVertex,
                            DistributionPattern.POINTWISE,
                            resultPartitionType,
                            output.getDataSetId(),
                            partitioner.isBroadcast(),
                            partitioner.getClass().equals(ForwardPartitioner.class),
                            edge.getTypeNumber(),
                            edge.areInterInputsKeysCorrelated(),
                            edge.isIntraInputKeyCorrelated());
        } else {
            jobEdge =
                    downStreamVertex.connectNewDataSetAsInput(
                            headVertex,
                            DistributionPattern.ALL_TO_ALL,
                            resultPartitionType,
                            output.getDataSetId(),
                            partitioner.isBroadcast(),
                            partitioner.getClass().equals(ForwardPartitioner.class),
                            edge.getTypeNumber(),
                            edge.areInterInputsKeysCorrelated(),
                            edge.isIntraInputKeyCorrelated());
        }

        // set strategy name so that web interface can show it.
        jobEdge.setShipStrategyName(partitioner.toString());
        jobEdge.setDownstreamSubtaskStateMapper(partitioner.getDownstreamSubtaskStateMapper());
        jobEdge.setUpstreamSubtaskStateMapper(partitioner.getUpstreamSubtaskStateMapper());

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "CONNECTED: {} - {} -> {}",
                    partitioner.getClass().getSimpleName(),
                    headOfChain,
                    downStreamVertexID);
        }

        return jobEdge.getSource();
    }

    private static boolean isPersistentIntermediateDataset(
            ResultPartitionType resultPartitionType, StreamEdge edge) {
        return resultPartitionType.isBlockingOrBlockingPersistentResultPartition()
                && edge.getIntermediateDatasetIdToProduce() != null;
    }

    private static void checkBufferTimeout(ResultPartitionType type, StreamEdge edge) {
        long bufferTimeout = edge.getBufferTimeout();
        if (!type.canBePipelinedConsumed()
                && bufferTimeout != ExecutionOptions.DISABLED_NETWORK_BUFFER_TIMEOUT) {
            throw new UnsupportedOperationException(
                    "only canBePipelinedConsumed partition support buffer timeout "
                            + bufferTimeout
                            + " for src operator in edge "
                            + edge
                            + ". \nPlease either disable buffer timeout (via -1) or use the canBePipelinedConsumed partition.");
        }
    }

    private static ResultPartitionType getResultPartitionType(
            StreamEdge edge, JobVertexBuildContext jobVertexBuildContext) {
        switch (edge.getExchangeMode()) {
            case PIPELINED:
                return ResultPartitionType.PIPELINED_BOUNDED;
            case BATCH:
                return ResultPartitionType.BLOCKING;
            case HYBRID_FULL:
                return ResultPartitionType.HYBRID_FULL;
            case HYBRID_SELECTIVE:
                return ResultPartitionType.HYBRID_SELECTIVE;
            case UNDEFINED:
                return determineUndefinedResultPartitionType(edge, jobVertexBuildContext);
            default:
                throw new UnsupportedOperationException(
                        "Data exchange mode " + edge.getExchangeMode() + " is not supported yet.");
        }
    }

    public static ResultPartitionType determineUndefinedResultPartitionType(
            StreamEdge edge, JobVertexBuildContext jobVertexBuildContext) {
        StreamGraph streamGraph = jobVertexBuildContext.getStreamGraph();
        Attribute sourceNodeAttribute =
                streamGraph.getStreamNode(edge.getSourceId()).getAttribute();
        if (sourceNodeAttribute.isNoOutputUntilEndOfInput()) {
            edge.setBufferTimeout(ExecutionOptions.DISABLED_NETWORK_BUFFER_TIMEOUT);
            return ResultPartitionType.BLOCKING;
        }

        StreamPartitioner<?> partitioner = edge.getPartitioner();
        switch (streamGraph.getGlobalStreamExchangeMode()) {
            case ALL_EDGES_BLOCKING:
                return ResultPartitionType.BLOCKING;
            case FORWARD_EDGES_PIPELINED:
                if (partitioner instanceof ForwardPartitioner) {
                    return ResultPartitionType.PIPELINED_BOUNDED;
                } else {
                    return ResultPartitionType.BLOCKING;
                }
            case POINTWISE_EDGES_PIPELINED:
                if (partitioner.isPointwise()) {
                    return ResultPartitionType.PIPELINED_BOUNDED;
                } else {
                    return ResultPartitionType.BLOCKING;
                }
            case ALL_EDGES_PIPELINED:
                return ResultPartitionType.PIPELINED_BOUNDED;
            case ALL_EDGES_PIPELINED_APPROXIMATE:
                return ResultPartitionType.PIPELINED_APPROXIMATE;
            case ALL_EDGES_HYBRID_FULL:
                return ResultPartitionType.HYBRID_FULL;
            case ALL_EDGES_HYBRID_SELECTIVE:
                return ResultPartitionType.HYBRID_SELECTIVE;
            default:
                throw new RuntimeException(
                        "Unrecognized global data exchange mode "
                                + streamGraph.getGlobalStreamExchangeMode());
        }
    }

    public static boolean isChainable(StreamEdge edge, StreamGraph streamGraph) {
        return isChainable(edge, streamGraph, false);//
    }

    public static boolean isChainable(
            StreamEdge edge, StreamGraph streamGraph, boolean allowChainWithDefaultParallelism) {
        StreamNode downStreamVertex = streamGraph.getTargetVertex(edge);

        return downStreamVertex.getInEdges().size() == 1 && isChainableInput(edge, streamGraph, allowChainWithDefaultParallelism);
    }

    public static boolean isChainableSource(StreamNode streamNode, StreamGraph streamGraph) {
        //在 Flink 中，新版的 Source API 在底层生成流图时，其算子工厂的类型必定是 SourceOperatorFactory（例如基于 KafkaSource、FileSource 构建的源）。
        // 如果使用的是老版的 SourceFunction（比如旧的 FlinkKafkaConsumer），这里直接返回 false，判定为不可链化
        if (streamNode.getOperatorFactory() == null
                || !(streamNode.getOperatorFactory() instanceof SourceOperatorFactory)
                //Source 的下游输出边必须“有且仅有一条”
                || streamNode.getOutEdges().size() != 1) {
            //source.map(...)
            //source.filter(...)
            // source 进行两个操作 导致 outEdges.size() > 1 那么这个 Source 必须独立存在，不能和任意一个下游链化
            return false;
        }
        final StreamEdge sourceOutEdge = streamNode.getOutEdges().get(0);
        final StreamNode target = streamGraph.getStreamNode(sourceOutEdge.getTargetId());
        //根据Operator工厂获取链式策略
        final ChainingStrategy targetChainingStrategy = Preconditions.checkNotNull(target.getOperatorFactory()).getChainingStrategy();
        return targetChainingStrategy == ChainingStrategy.HEAD_WITH_SOURCES
                && isChainableInput(sourceOutEdge, streamGraph, false);//
    }

    //它必须要确认上下游算子处于同一个资源组、具备完全相同的并行度、使用直通（Forward）的传输方式、且下游不是多路 Union 的缝合怪
    private static boolean isChainableInput(
            StreamEdge edge, StreamGraph streamGraph, boolean allowChainWithDefaultParallelism) {
        //edge的上游
        StreamNode upStreamVertex = streamGraph.getSourceVertex(edge);
        //edge的下游
        StreamNode downStreamVertex = streamGraph.getTargetVertex(edge);

        if (!(
                // 检查作业是否开启了算子链化 默认true
                streamGraph.isChainingEnabled()
                //校验上下游算子是否属于同一个槽位共享组
                && upStreamVertex.isSameSlotSharingGroup(downStreamVertex)
                //深入对比上下游算子本身的并行度、执行模式和链化策略
                && areOperatorsChainable(
                        upStreamVertex,
                        downStreamVertex,
                        streamGraph,
                        allowChainWithDefaultParallelism)
                //传输模式与分区检查 检查数据的物理分发策略和交换模式（ExchangeMode）
                //分区器（Partitioner）：只有 ForwardPartitioner（直通） 或者是特定单并发下的 RebalancePartitioner 才能链化。
                // 如果是 KeyBy（Hash 分区）、Broadcast（广播），意味着数据需要重新打散走网络连接，直接拒绝

                //自适应批优化（streamGraph.isDynamic()）：
                // 在 Flink 2.2 中，如果开启了自适应动态图，数据交换模式（ExchangeMode）如果是阻塞式落盘（BATCH 阻塞）则不能链化，必须是流水线直推（PIPELINED）才行
                && arePartitionerAndExchangeModeChainable(
                        edge.getPartitioner(), edge.getExchangeMode(), streamGraph.isDynamic()))) {

            return false;
        }

        // check that we do not have a union operation, because unions currently only work
        // through the network/byte-channel stack.
        // we check that by testing that each "type" (which means input position) is used only once
        for (StreamEdge inEdge : downStreamVertex.getInEdges()) {
            //遍历下游算子所有的输入边
            //如果发现了另外一条边（inEdge != edge），
            // 而且它的输入位置编号和当前检查的边一模一样（inEdge.getTypeNumber() == edge.getTypeNumber()），这就铁证了这是一个 Union 操作
            //Flink 官方在注释中写得很直白：“因为 Union 目前只能通过网络/字节通道栈（network/byte-channel stack）来工作”
            if (inEdge != edge && inEdge.getTypeNumber() == edge.getTypeNumber()) {
                return false;
            }
        }
        return true;
    }

    @VisibleForTesting
    static boolean arePartitionerAndExchangeModeChainable(
            StreamPartitioner<?> partitioner,
            StreamExchangeMode exchangeMode,
            boolean isDynamicGraph) {
        if (partitioner instanceof ForwardForConsecutiveHashPartitioner) {
            checkState(isDynamicGraph);
            return true;
        } else if ((partitioner instanceof ForwardPartitioner)
                && exchangeMode != StreamExchangeMode.BATCH) {
            return true;
        } else {
            return false;
        }
    }

    @VisibleForTesting
    static boolean areOperatorsChainable(
            StreamNode upStreamVertex,
            StreamNode downStreamVertex,
            StreamGraph streamGraph,
            boolean allowChainWithDefaultParallelism) {
        StreamOperatorFactory<?> upStreamOperator = upStreamVertex.getOperatorFactory();
        StreamOperatorFactory<?> downStreamOperator = downStreamVertex.getOperatorFactory();
        if (downStreamOperator == null || upStreamOperator == null) {
            return false;
        }

        // yielding operators cannot be chained to legacy sources
        // unfortunately the information that vertices have been chained is not preserved at this
        // point
        if (downStreamOperator instanceof YieldingOperatorFactory
                && getHeadOperator(upStreamVertex, streamGraph).isLegacySource()) {
            return false;
        }

        // we use switch/case here to make sure this is exhaustive if ever values are added to the
        // ChainingStrategy enum
        boolean isChainable;
        //上游算子链式策略
        switch (upStreamOperator.getChainingStrategy()) {
            case NEVER:
                isChainable = false;
                break;
            case ALWAYS:
            case HEAD:
            case HEAD_WITH_SOURCES:
                isChainable = true;
                break;
            default:
                throw new RuntimeException(
                        "Unknown chaining strategy: " + upStreamOperator.getChainingStrategy());
        }
        //下游算子链式策略
        switch (downStreamOperator.getChainingStrategy()) {
            case NEVER:
            case HEAD:
                isChainable = false;
                break;
            case ALWAYS:
                // keep the value from upstream
                break;
            case HEAD_WITH_SOURCES:
                // only if upstream is a source
                isChainable &= (upStreamOperator instanceof SourceOperatorFactory);
                break;
            default:
                throw new RuntimeException(
                        "Unknown chaining strategy: " + downStreamOperator.getChainingStrategy());
        }

        // When allowChainWithDefaultParallelism is true, any vertex with default parallelism can
        // be chained, otherwise only vertices with the same parallelism can be chained.
        if (allowChainWithDefaultParallelism) {
            isChainable &=
                    (upStreamVertex.getParallelism() == downStreamVertex.getParallelism()
                            || upStreamVertex.getParallelism()
                                    == ExecutionConfig.PARALLELISM_DEFAULT
                            || downStreamVertex.getParallelism()
                                    == ExecutionConfig.PARALLELISM_DEFAULT);
        } else {
            isChainable &= upStreamVertex.getParallelism() == downStreamVertex.getParallelism();
        }

        if (!streamGraph.isChainingOfOperatorsWithDifferentMaxParallelismEnabled()) {
            isChainable &=
                    upStreamVertex.getMaxParallelism() == downStreamVertex.getMaxParallelism();
        }

        return isChainable;
    }

    /** Backtraces the head of an operator chain. */
    private static StreamOperatorFactory<?> getHeadOperator(
            StreamNode upStreamVertex, StreamGraph streamGraph) {
        if (streamGraph.getHeadOperatorForNodeFromCache(upStreamVertex) == null) {
            if (upStreamVertex.getInEdges().size() == 1
                    && isChainable(upStreamVertex.getInEdges().get(0), streamGraph)) {
                StreamOperatorFactory<?> headOperator =
                        getHeadOperator(
                                streamGraph.getSourceVertex(upStreamVertex.getInEdges().get(0)),
                                streamGraph);
                streamGraph.cacheHeadOperatorForNode(upStreamVertex, headOperator);
            } else {
                Preconditions.checkNotNull(upStreamVertex.getOperatorFactory());
                streamGraph.cacheHeadOperatorForNode(
                        upStreamVertex, upStreamVertex.getOperatorFactory());
            }
        }

        return streamGraph.getHeadOperatorForNodeFromCache(upStreamVertex);
    }

    public static void markSupportingConcurrentExecutionAttempts(
            JobVertexBuildContext jobVertexBuildContext) {
        final Map<Integer, JobVertex> jobVertices = jobVertexBuildContext.getJobVerticesInOrder();
        StreamGraph streamGraph = jobVertexBuildContext.getStreamGraph();

        for (Map.Entry<Integer, JobVertex> entry : jobVertices.entrySet()) {
            final JobVertex jobVertex = entry.getValue();
            final Set<Integer> vertexOperators = new HashSet<>();
            vertexOperators.add(entry.getKey());
            final Map<Integer, StreamConfig> vertexChainedConfigs =
                    jobVertexBuildContext.getChainedConfigs().get(entry.getKey());
            if (vertexChainedConfigs != null) {
                vertexOperators.addAll(vertexChainedConfigs.keySet());
            }

            // disable supportConcurrentExecutionAttempts of job vertex if there is any stream node
            // does not support it
            boolean supportConcurrentExecutionAttempts = true;
            for (int nodeId : vertexOperators) {
                final StreamNode streamNode = streamGraph.getStreamNode(nodeId);
                if (!streamNode.isSupportsConcurrentExecutionAttempts()) {
                    supportConcurrentExecutionAttempts = false;
                    break;
                }
            }
            jobVertex.setSupportsConcurrentExecutionAttempts(supportConcurrentExecutionAttempts);
        }
    }

    public static void setSlotSharingAndCoLocation(JobVertexBuildContext jobVertexBuildContext) {
        setSlotSharing(jobVertexBuildContext);
        setCoLocation(jobVertexBuildContext);
    }

    private static void setSlotSharing(JobVertexBuildContext jobVertexBuildContext) {
        StreamGraph streamGraph = jobVertexBuildContext.getStreamGraph();
        final Map<String, SlotSharingGroup> specifiedSlotSharingGroups = new HashMap<>();
        final Map<JobVertexID, SlotSharingGroup> vertexRegionSlotSharingGroups =
                buildVertexRegionSlotSharingGroups(jobVertexBuildContext);
        final Map<Integer, JobVertex> jobVertices = jobVertexBuildContext.getJobVerticesInOrder();

        for (Map.Entry<Integer, JobVertex> entry : jobVertices.entrySet()) {

            final JobVertex vertex = entry.getValue();
            final String slotSharingGroupKey =
                    streamGraph.getStreamNode(entry.getKey()).getSlotSharingGroup();

            checkNotNull(slotSharingGroupKey, "StreamNode slot sharing group must not be null");

            final SlotSharingGroup effectiveSlotSharingGroup;
            if (slotSharingGroupKey.equals(StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP)) {
                // fallback to the region slot sharing group by default
                effectiveSlotSharingGroup =
                        checkNotNull(vertexRegionSlotSharingGroups.get(vertex.getID()));
            } else {
                checkState(
                        !jobVertexBuildContext.hasHybridResultPartition(),
                        "hybrid shuffle mode currently does not support setting non-default slot sharing group.");

                effectiveSlotSharingGroup =
                        specifiedSlotSharingGroups.computeIfAbsent(
                                slotSharingGroupKey,
                                k -> {
                                    SlotSharingGroup ssg = new SlotSharingGroup();
                                    streamGraph
                                            .getSlotSharingGroupResource(k)
                                            .ifPresent(ssg::setResourceProfile);
                                    return ssg;
                                });
            }

            vertex.setSlotSharingGroup(effectiveSlotSharingGroup);
        }
    }

    public static void validateHybridShuffleExecuteInBatchMode(
            JobVertexBuildContext jobVertexBuildContext) {
        if (jobVertexBuildContext.hasHybridResultPartition()) {
            checkState(
                    jobVertexBuildContext.getStreamGraph().getJobType() == JobType.BATCH,
                    "hybrid shuffle mode only supports batch job, please set %s to %s",
                    ExecutionOptions.RUNTIME_MODE.key(),
                    RuntimeExecutionMode.BATCH.name());
        }
    }

    /**
     * Maps a vertex to its region slot sharing group. If {@link
     * StreamGraph#isAllVerticesInSameSlotSharingGroupByDefault()} returns true, all regions will be
     * in the same slot sharing group.
     */
    private static Map<JobVertexID, SlotSharingGroup> buildVertexRegionSlotSharingGroups(
            JobVertexBuildContext jobVertexBuildContext) {
        StreamGraph streamGraph = jobVertexBuildContext.getStreamGraph();
        final Map<JobVertexID, SlotSharingGroup> vertexRegionSlotSharingGroups = new HashMap<>();
        final SlotSharingGroup defaultSlotSharingGroup =
                jobVertexBuildContext.getDefaultSlotSharingGroup();
        streamGraph
                .getSlotSharingGroupResource(StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP)
                .ifPresent(defaultSlotSharingGroup::setResourceProfile);

        final boolean allRegionsInSameSlotSharingGroup =
                streamGraph.isAllVerticesInSameSlotSharingGroupByDefault();

        final Iterable<DefaultLogicalPipelinedRegion> regions =
                DefaultLogicalTopology.fromJobGraph(jobVertexBuildContext.getJobGraph())
                        .getAllPipelinedRegions();
        for (DefaultLogicalPipelinedRegion region : regions) {
            final SlotSharingGroup regionSlotSharingGroup;
            if (allRegionsInSameSlotSharingGroup) {
                regionSlotSharingGroup = defaultSlotSharingGroup;
            } else {
                regionSlotSharingGroup = new SlotSharingGroup();
                streamGraph
                        .getSlotSharingGroupResource(
                                StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP)
                        .ifPresent(regionSlotSharingGroup::setResourceProfile);
            }

            for (LogicalVertex vertex : region.getVertices()) {
                vertexRegionSlotSharingGroups.put(vertex.getId(), regionSlotSharingGroup);
            }
        }

        return vertexRegionSlotSharingGroups;
    }

    private static void setCoLocation(JobVertexBuildContext jobVertexBuildContext) {
        final Map<String, Tuple2<SlotSharingGroup, CoLocationGroupImpl>> coLocationGroups =
                new HashMap<>();
        final Map<Integer, JobVertex> jobVertices = jobVertexBuildContext.getJobVerticesInOrder();
        final StreamGraph streamGraph = jobVertexBuildContext.getStreamGraph();

        for (Map.Entry<Integer, JobVertex> entry : jobVertices.entrySet()) {

            final StreamNode node = streamGraph.getStreamNode(entry.getKey());
            final JobVertex vertex = entry.getValue();
            final SlotSharingGroup sharingGroup = vertex.getSlotSharingGroup();

            // configure co-location constraint
            final String coLocationGroupKey = node.getCoLocationGroup();
            if (coLocationGroupKey != null) {
                if (sharingGroup == null) {
                    throw new IllegalStateException(
                            "Cannot use a co-location constraint without a slot sharing group");
                }

                Tuple2<SlotSharingGroup, CoLocationGroupImpl> constraint =
                        coLocationGroups.computeIfAbsent(
                                coLocationGroupKey,
                                k -> new Tuple2<>(sharingGroup, new CoLocationGroupImpl()));

                if (constraint.f0 != sharingGroup) {
                    throw new IllegalStateException(
                            "Cannot co-locate operators from different slot sharing groups");
                }

                vertex.updateCoLocationGroup(constraint.f1);
                constraint.f1.addVertex(vertex);
            }
        }
    }

    public static void setManagedMemoryFraction(JobVertexBuildContext jobVertexBuildContext) {

        final Map<Integer, JobVertex> jobVertices = jobVertexBuildContext.getJobVerticesInOrder();

        // all slot sharing groups in this job
        final Set<SlotSharingGroup> slotSharingGroups =
                Collections.newSetFromMap(new IdentityHashMap<>());

        // maps a job vertex ID to its head operator ID
        final Map<JobVertexID, Integer> vertexHeadOperators = new HashMap<>();

        // maps a job vertex ID to IDs of all operators in the vertex
        final Map<JobVertexID, Set<Integer>> vertexOperators = new HashMap<>();

        for (Map.Entry<Integer, JobVertex> entry : jobVertices.entrySet()) {
            final int headOperatorId = entry.getKey();
            final JobVertex jobVertex = entry.getValue();

            final SlotSharingGroup jobVertexSlotSharingGroup = jobVertex.getSlotSharingGroup();

            checkState(
                    jobVertexSlotSharingGroup != null,
                    "JobVertex slot sharing group must not be null");
            slotSharingGroups.add(jobVertexSlotSharingGroup);

            vertexHeadOperators.put(jobVertex.getID(), headOperatorId);

            final Set<Integer> operatorIds = new HashSet<>();
            operatorIds.add(headOperatorId);
            operatorIds.addAll(
                    jobVertexBuildContext
                            .getChainedConfigs()
                            .getOrDefault(headOperatorId, Collections.emptyMap())
                            .keySet());
            vertexOperators.put(jobVertex.getID(), operatorIds);
        }

        for (SlotSharingGroup slotSharingGroup : slotSharingGroups) {
            setManagedMemoryFractionForSlotSharingGroup(
                    slotSharingGroup, vertexHeadOperators, vertexOperators, jobVertexBuildContext);
        }
    }

    private static void setManagedMemoryFractionForSlotSharingGroup(
            final SlotSharingGroup slotSharingGroup,
            final Map<JobVertexID, Integer> vertexHeadOperators,
            final Map<JobVertexID, Set<Integer>> vertexOperators,
            final JobVertexBuildContext jobVertexBuildContext) {
        final StreamGraph streamGraph = jobVertexBuildContext.getStreamGraph();
        final Map<Integer, Map<Integer, StreamConfig>> vertexChainedConfigs =
                jobVertexBuildContext.getChainedConfigs();
        // In the progressive job graph generation algorithm, if the user specified the
        // SlotSharingGroupResource or the AllVerticesInSameSlotSharingGroupByDefault is set to
        // true, job vertices generated in different phase may be assigned to the same
        // slotSharingGroup. Therefore, we need to filter out the job vertices that belong to the
        // current phase.
        final Set<JobVertexID> jobVertexIds =
                slotSharingGroup.getJobVertexIds().stream()
                        .filter(vertexOperators::containsKey)
                        .collect(Collectors.toSet());
        final Set<Integer> groupOperatorIds =
                jobVertexIds.stream()
                        .flatMap((vid) -> vertexOperators.get(vid).stream())
                        .collect(Collectors.toSet());

        final Map<ManagedMemoryUseCase, Integer> groupOperatorScopeUseCaseWeights =
                groupOperatorIds.stream()
                        .flatMap(
                                (oid) ->
                                        streamGraph
                                                .getStreamNode(oid)
                                                .getManagedMemoryOperatorScopeUseCaseWeights()
                                                .entrySet()
                                                .stream())
                        .collect(
                                Collectors.groupingBy(
                                        Map.Entry::getKey,
                                        Collectors.summingInt(Map.Entry::getValue)));

        final Set<ManagedMemoryUseCase> groupSlotScopeUseCases =
                groupOperatorIds.stream()
                        .flatMap(
                                (oid) ->
                                        streamGraph
                                                .getStreamNode(oid)
                                                .getManagedMemorySlotScopeUseCases()
                                                .stream())
                        .collect(Collectors.toSet());

        for (JobVertexID jobVertexID : jobVertexIds) {
            final int headOperatorNodeId = vertexHeadOperators.get(jobVertexID);
            for (int operatorNodeId : vertexOperators.get(jobVertexID)) {
                final StreamConfig operatorConfig =
                        jobVertexBuildContext
                                .getChainInfo(headOperatorNodeId)
                                .getOperatorInfo(operatorNodeId)
                                .getVertexConfig();
                final Map<ManagedMemoryUseCase, Integer> operatorScopeUseCaseWeights =
                        streamGraph
                                .getStreamNode(operatorNodeId)
                                .getManagedMemoryOperatorScopeUseCaseWeights();
                final Set<ManagedMemoryUseCase> slotScopeUseCases =
                        streamGraph
                                .getStreamNode(operatorNodeId)
                                .getManagedMemorySlotScopeUseCases();
                setManagedMemoryFractionForOperator(
                        operatorScopeUseCaseWeights,
                        slotScopeUseCases,
                        groupOperatorScopeUseCaseWeights,
                        groupSlotScopeUseCases,
                        operatorConfig);
            }

            // need to refresh the chained task configs because they are serialized
            final StreamConfig vertexConfig =
                    jobVertexBuildContext
                            .getChainInfo(headOperatorNodeId)
                            .getOperatorInfo(headOperatorNodeId)
                            .getVertexConfig();
            vertexConfig.setTransitiveChainedTaskConfigs(
                    vertexChainedConfigs.get(headOperatorNodeId));
        }
    }

    private static void setManagedMemoryFractionForOperator(
            final Map<ManagedMemoryUseCase, Integer> operatorScopeUseCaseWeights,
            final Set<ManagedMemoryUseCase> slotScopeUseCases,
            final Map<ManagedMemoryUseCase, Integer> groupManagedMemoryWeights,
            final Set<ManagedMemoryUseCase> groupSlotScopeUseCases,
            final StreamConfig operatorConfig) {

        // For each operator, make sure fractions are set for all use cases in the group, even if
        // the operator does not have the use case (set the fraction to 0.0). This allows us to
        // learn which use cases exist in the group from either one of the stream configs.
        for (Map.Entry<ManagedMemoryUseCase, Integer> entry :
                groupManagedMemoryWeights.entrySet()) {
            final ManagedMemoryUseCase useCase = entry.getKey();
            final int groupWeight = entry.getValue();
            final int operatorWeight = operatorScopeUseCaseWeights.getOrDefault(useCase, 0);
            operatorConfig.setManagedMemoryFractionOperatorOfUseCase(
                    useCase,
                    operatorWeight > 0
                            ? ManagedMemoryUtils.getFractionRoundedDown(operatorWeight, groupWeight)
                            : 0.0);
        }
        for (ManagedMemoryUseCase useCase : groupSlotScopeUseCases) {
            operatorConfig.setManagedMemoryFractionOperatorOfUseCase(
                    useCase, slotScopeUseCases.contains(useCase) ? 1.0 : 0.0);
        }
    }

    private static String nameWithChainedSourcesInfo(
            String operatorName, Collection<ChainedSourceInfo> chainedSourceInfos) {
        return chainedSourceInfos.isEmpty()
                ? operatorName
                : String.format(
                        "%s [%s]",
                        operatorName,
                        chainedSourceInfos.stream()
                                .map(
                                        chainedSourceInfo ->
                                                chainedSourceInfo
                                                        .getOperatorConfig()
                                                        .getOperatorName())
                                .collect(Collectors.joining(", ")));
    }
}
