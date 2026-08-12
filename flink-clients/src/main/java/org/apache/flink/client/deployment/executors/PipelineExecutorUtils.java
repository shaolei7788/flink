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

package org.apache.flink.client.deployment.executors;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.cli.ClientOptions;
import org.apache.flink.client.cli.ExecutionConfigAccessor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.core.execution.JobStatusChangedListener;
import org.apache.flink.streaming.api.graph.ExecutionPlan;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.runtime.execution.DefaultJobCreatedEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Utility class with method related to job execution. */
public class PipelineExecutorUtils {
    private static final Logger LOG = LoggerFactory.getLogger(PipelineExecutorUtils.class);

    /**
     * Notify the {@link DefaultJobCreatedEvent} to job status changed listeners.
     *
     * @param pipeline the pipeline that contains lineage graph information.
     * @param executionPlan executionPlan that contains job basic info
     * @param listeners the list of job status changed listeners
     */
    public static void notifyJobStatusListeners(
            @Nonnull final Pipeline pipeline,
            @Nonnull final ExecutionPlan executionPlan,
            List<JobStatusChangedListener> listeners) {
        RuntimeExecutionMode executionMode =
                executionPlan.getJobConfiguration().get(ExecutionOptions.RUNTIME_MODE);
        listeners.forEach(
                listener -> {
                    try {
                        listener.onEvent(
                                new DefaultJobCreatedEvent(
                                        executionPlan.getJobID(),
                                        executionPlan.getName(),
                                        ((StreamGraph) pipeline).getLineageGraph(),
                                        executionMode));
                    } catch (Throwable e) {
                        LOG.error(
                                "Fail to notify job status changed listener {}",
                                listener.getClass().getName(),
                                e);
                    }
                });
    }

    public static StreamGraph getStreamGraph(
            @Nonnull final Pipeline pipeline, @Nonnull final Configuration configuration)
            throws Exception {
        checkNotNull(pipeline);
        checkNotNull(configuration);
        checkState(pipeline instanceof StreamGraph);

        StreamGraph streamGraph = (StreamGraph) pipeline;

        final ExecutionConfigAccessor executionConfigAccessor =
                ExecutionConfigAccessor.fromConfiguration(configuration);

        configuration
                .getOptional(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID)
                // 设置JobId
                .ifPresent(strJobID -> streamGraph.setJobId(JobID.fromHexString(strJobID)));

        if (configuration.get(DeploymentOptions.ATTACHED)
                && configuration.get(DeploymentOptions.SHUTDOWN_IF_ATTACHED)) {
            //默认模式
            //命令示例：./bin/flink run my-job.jar（不加 -d）行为表现：
            // 当你敲下提交命令后，你的终端（Terminal）窗口会被阻塞（Block）。客户端进程（JVM）会一直保持存活并挂在前台，实时拉取并打印集群的作业状态、日志或进度
            streamGraph.setInitialClientHeartbeatTimeout(configuration.get(ClientOptions.CLIENT_HEARTBEAT_TIMEOUT).toMillis());
        }
        //DETACHED 模式
        //命令示例：./bin/flink run -d my-job.jar（使用 -d 或 -detached 参数）
        // 行为表现：客户端进程只需把编译好的 JobGraph（或 2.2 的 ExecutionPlan）通过网络成功推送到 JobManager 的 REST 端口，并在控制台打印出生成的 JobID 之后，本地客户端进程就会立刻退出。
        // 生命周期：作业完全脱离客户端的肉眼控制，在集群后台（Background）安静地独立运行。后续你需要通过 Web UI 或者 flink list 命令去查看它

        streamGraph.addJars(executionConfigAccessor.getJars());
        streamGraph.setClasspath(executionConfigAccessor.getClasspaths());
        streamGraph.setSavepointRestoreSettings(executionConfigAccessor.getSavepointRestoreSettings());

        return streamGraph;
    }
}
