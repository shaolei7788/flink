/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.blob.BlobClient;
import org.apache.flink.runtime.client.ClientUtils;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.job.JobSubmitHeaders;
import org.apache.flink.runtime.rest.messages.job.JobSubmitRequestBody;
import org.apache.flink.runtime.rest.messages.job.JobSubmitResponseBody;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.streaming.api.graph.ExecutionPlan;
import org.apache.flink.util.FlinkException;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.ObjectInputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/** This handler can be used to submit jobs to a Flink cluster. */
public final class JobSubmitHandler
        extends AbstractRestHandler<
                DispatcherGateway,
                JobSubmitRequestBody,
                JobSubmitResponseBody,
                EmptyMessageParameters> {

    private static final String FILE_TYPE_EXECUTION_PLAN = "ExecutionPlan";
    private static final String FILE_TYPE_JAR = "Jar";
    private static final String FILE_TYPE_ARTIFACT = "Artifact";

    private final Executor executor;
    private final Configuration configuration;

    public JobSubmitHandler(
            GatewayRetriever<? extends DispatcherGateway> leaderRetriever,
            Duration timeout,//600s
            Map<String, String> headers,
            Executor executor,
            Configuration configuration) {
        //
        super(leaderRetriever, timeout, headers, JobSubmitHeaders.getInstance());
        this.executor = executor;
        this.configuration = configuration;
    }

    //处理作业提交的请求
    @Override
    protected CompletableFuture<JobSubmitResponseBody> handleRequest(
            //包装了当前的 HTTP 请求。通过它可以拿到用户上传的物理文件、由 JSON 请求体反序列化得到的 Java 对象（JobSubmitRequestBody）以及 URL 参数
            @Nonnull HandlerRequest<JobSubmitRequestBody> request,
            //当前集群处于 Leader 状态的 Dispatcher 的 RPC 代理（Gateway）。通过它可以向集群核心组件发送 RPC 命名控制指令（如 submitJob）
            @Nonnull DispatcherGateway gateway)
            throws RestHandlerException {
        //从 Netty 的 Multipart HTTP 请求中提取用户本次上传的所有临时本地文件（包括作业图文件、JAR 包、依赖的 Artifact 文件等）
        final Collection<File> uploadedFiles = request.getUploadedFiles();
        //使用 Java Stream 流将文件集合转换成一个 Map 映射
        final Map<String, Path> nameToFile =
                uploadedFiles.stream()
                        .collect(Collectors.toMap(File::getName, Path::fromLocalFile));

        if (uploadedFiles.size() != nameToFile.size()) {
            // 同名文件去重与冲突校验。如果 uploadedFiles 的数量和 Map 的数量不一致，说明用户上传了重名的文件（导致 Collectors.toMap 时发生了覆盖或冲突）
            //防止前端错误或多线程上传时引发文件覆盖，确保上传的文件一一对应。如果不相等，直接向客户端返回 HTTP 400 (Bad Request)
            throw new RestHandlerException(
                    String.format(
                            "The number of uploaded files was %s than the expected count. Expected: %s Actual %s",
                            uploadedFiles.size() < nameToFile.size() ? "lower" : "higher",
                            nameToFile.size(),
                            uploadedFiles.size()),
                    HttpResponseStatus.BAD_REQUEST);
        }
        //获取 HTTP 请求体中携带的 JSON 元数据（包含了执行计划文件名、关联的 JAR 包列表、分布式缓存分布式文件等信息）
        final JobSubmitRequestBody requestBody = request.getRequestBody();

        if (requestBody.executionPlanFileName == null) {
            throw new RestHandlerException(
                    String.format(
                            "The %s field must not be omitted or be null.",
                            JobSubmitRequestBody.FIELD_NAME_JOB_GRAPH),
                    HttpResponseStatus.BAD_REQUEST);
        }
        // 根据 requestBody.executionPlanFileName 去刚才的 nameToFile 映射中找到对应的磁盘临时文件。读取该文件，
        // 并将其反序列化（Deserialization）**为内存中的 Flink ExecutionPlan（即传统意义上的 JobGraph）对象
        CompletableFuture<ExecutionPlan> executionPlanFuture = loadExecutionPlan(requestBody, nameToFile);
        // 从用户上传的文件大盘（nameToFile）中，根据请求体里指定的 JAR 包名称列表（jarFileNames），筛选出本作业真正需要用到的、属于用户代码的 JAR 包文件集合
        Collection<Path> jarFiles = getJarFilesToUpload(requestBody.jarFileNames, nameToFile);
        // 处理分布式缓存文件。提取用户随作业一同提交的其他辅助依赖资产（如文本配置、机器学习模型、字典文件等），并以 (注册名, 临时文件路径) 的元组（Tuple2）形式封存
        Collection<Tuple2<String, Path>> artifacts = getArtifactFilesToUpload(requestBody.artifactFileNames, nameToFile);

        //将作业的依赖文件真正上传到集群的 BlobServer 注册中心
        //它等待 executionPlanFuture 反序列化完成后，将获取到的 jarFiles 和 artifacts 通过 RPC 上传到 Flink 的中央文件服务（BlobServer）。
        // BlobServer 会返回唯一的 BlobKey，随后这些 Key 会被注入、回写到 ExecutionPlan 对象的配置信息中，从而得到一个资源就绪、具备完整运行时上下文的“最终版执行计划”（finalizedExecutionPlan）
        CompletableFuture<ExecutionPlan> finalizedExecutionPlanFuture =
                uploadExecutionPlanFiles(gateway, executionPlanFuture, jarFiles, artifacts, configuration);
        // 正式向集群提交作业（核心 RPC 交互）
        //当上面的文件全部上传完毕、执行计划最终定型后，解开 Future 包裹拿到 executionPlan
        CompletableFuture<Acknowledge> jobSubmissionFuture =
                finalizedExecutionPlanFuture.thenCompose(
                        //提交作业
                        //会通过Pekko 将作业正式递交给 Dispatcher
                        // Dispatcher#submitJob。  600s
                        executionPlan -> gateway.submitJob(executionPlan, timeout));
        //使用 thenCombine 将 jobSubmissionFuture（代表提交成功）和最开始的 executionPlanFuture（为了拿作业 ID）进行合并组合
        //当且仅当上述的所有异步链路（反序列化 \(\rightarrow \) 文件上传 \(\rightarrow \) RPC 提交成功）全部顺利完成时，该方法最终返回一个 JobSubmitResponseBody。
        // 其内部包含该作业的监控路由路径（例如 /jobs/4a3b2c...），前端（Web UI）拿到该路径后，便可以立即跳转到该作业的详情页
        return jobSubmissionFuture.thenCombine(
                executionPlanFuture,
                (ack, executionPlan) ->
                        //
                        new JobSubmitResponseBody("/jobs/" + executionPlan.getJobID()));
    }

    private CompletableFuture<ExecutionPlan> loadExecutionPlan(
            JobSubmitRequestBody requestBody, Map<String, Path> nameToFile)
            throws MissingFileException {
        final Path executionPlanFile =
                getPathAndAssertUpload(
                        requestBody.executionPlanFileName, FILE_TYPE_EXECUTION_PLAN, nameToFile);

        return CompletableFuture.supplyAsync(
                () -> {
                    ExecutionPlan executionPlan;
                    try (ObjectInputStream objectIn =
                            new ObjectInputStream(
                                    executionPlanFile.getFileSystem().open(executionPlanFile))) {
                        executionPlan = (ExecutionPlan) objectIn.readObject();
                    } catch (Exception e) {
                        throw new CompletionException(
                                new RestHandlerException(
                                        "Failed to deserialize ExecutionPlan.",
                                        HttpResponseStatus.BAD_REQUEST,
                                        e));
                    }
                    return executionPlan;
                },
                executor);
    }

    private static Collection<Path> getJarFilesToUpload(
            Collection<String> jarFileNames, Map<String, Path> nameToFileMap)
            throws MissingFileException {
        Collection<Path> jarFiles = new ArrayList<>(jarFileNames.size());
        for (String jarFileName : jarFileNames) {
            Path jarFile = getPathAndAssertUpload(jarFileName, FILE_TYPE_JAR, nameToFileMap);
            jarFiles.add(new Path(jarFile.toString()));
        }
        return jarFiles;
    }

    private static Collection<Tuple2<String, Path>> getArtifactFilesToUpload(
            Collection<JobSubmitRequestBody.DistributedCacheFile> artifactEntries,
            Map<String, Path> nameToFileMap)
            throws MissingFileException {
        Collection<Tuple2<String, Path>> artifacts = new ArrayList<>(artifactEntries.size());
        for (JobSubmitRequestBody.DistributedCacheFile artifactFileName : artifactEntries) {
            Path artifactFile =
                    getPathAndAssertUpload(
                            artifactFileName.fileName, FILE_TYPE_ARTIFACT, nameToFileMap);
            artifacts.add(Tuple2.of(artifactFileName.entryName, new Path(artifactFile.toString())));
        }
        return artifacts;
    }

    private CompletableFuture<ExecutionPlan> uploadExecutionPlanFiles(
            DispatcherGateway gateway,
            CompletableFuture<ExecutionPlan> executionPlanFuture,
            Collection<Path> jarFiles,
            Collection<Tuple2<String, Path>> artifacts,
            Configuration configuration) {
        CompletableFuture<Integer> blobServerPortFuture = gateway.getBlobServerPort(timeout);
        CompletableFuture<InetAddress> blobServerAddressFuture =
                gateway.getBlobServerAddress(timeout);

        return executionPlanFuture.thenCombine(
                blobServerPortFuture.thenCombine(
                        blobServerAddressFuture,
                        (blobServerPort, blobServerAddress) ->
                                new InetSocketAddress(
                                        blobServerAddress.getHostName(), blobServerPort)),
                (ExecutionPlan executionPlan, InetSocketAddress blobSocketAddress) -> {
                    try {
                        ClientUtils.uploadExecutionPlanFiles(
                                executionPlan,
                                jarFiles,
                                artifacts,
                                () -> new BlobClient(blobSocketAddress, configuration));
                    } catch (FlinkException e) {
                        throw new CompletionException(
                                new RestHandlerException(
                                        "Could not upload job files.",
                                        HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                        e));
                    }
                    return executionPlan;
                });
    }

    private static Path getPathAndAssertUpload(
            String fileName, String type, Map<String, Path> uploadedFiles)
            throws MissingFileException {
        final Path file = uploadedFiles.get(fileName);
        if (file == null) {
            throw new MissingFileException(type, fileName);
        }
        return file;
    }

    private static final class MissingFileException extends RestHandlerException {

        private static final long serialVersionUID = -7954810495610194965L;

        MissingFileException(String type, String fileName) {
            super(
                    type + " file " + fileName + " could not be found on the server.",
                    HttpResponseStatus.BAD_REQUEST);
        }
    }
}
