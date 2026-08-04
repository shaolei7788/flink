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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.blob.PermanentBlobService;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory.ShuffleDescriptorGroup;
import org.apache.flink.runtime.entrypoint.WorkingDirectory;
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.SharedResources;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.shuffle.ShuffleEnvironmentContext;
import org.apache.flink.runtime.shuffle.ShuffleServiceLoader;
import org.apache.flink.runtime.state.TaskExecutorChannelStateExecutorFactoryManager;
import org.apache.flink.runtime.state.TaskExecutorFileMergingManager;
import org.apache.flink.runtime.state.TaskExecutorLocalStateStoresManager;
import org.apache.flink.runtime.state.TaskExecutorStateChangelogStoragesManager;
import org.apache.flink.runtime.taskexecutor.slot.DefaultTimerService;
import org.apache.flink.runtime.taskexecutor.slot.FileSlotAllocationSnapshotPersistenceService;
import org.apache.flink.runtime.taskexecutor.slot.NoOpSlotAllocationSnapshotPersistenceService;
import org.apache.flink.runtime.taskexecutor.slot.SlotAllocationSnapshotPersistenceService;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotTable;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotTableImpl;
import org.apache.flink.runtime.taskexecutor.slot.TimerService;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.UnresolvedTaskManagerLocation;
import org.apache.flink.runtime.util.DefaultGroupCache;
import org.apache.flink.runtime.util.GroupCache;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Container for {@link TaskExecutor} services such as the {@link MemoryManager}, {@link IOManager},
 * {@link ShuffleEnvironment}. All services are exclusive to a single {@link TaskExecutor}.
 * Consequently, the respective {@link TaskExecutor} is responsible for closing them.
 */
public class TaskManagerServices {
    private static final Logger LOG = LoggerFactory.getLogger(TaskManagerServices.class);

    /** TaskManager services. */
    private final UnresolvedTaskManagerLocation unresolvedTaskManagerLocation;

    private final long managedMemorySize;
    private final IOManager ioManager;
    private final ShuffleEnvironment<?, ?> shuffleEnvironment;
    private final KvStateService kvStateService;
    private final BroadcastVariableManager broadcastVariableManager;
    private final TaskSlotTable<Task> taskSlotTable;
    private final JobTable jobTable;
    private final JobLeaderService jobLeaderService;
    private final TaskExecutorLocalStateStoresManager taskManagerStateStore;
    private final TaskExecutorFileMergingManager taskManagerFileMergingManager;
    private final TaskExecutorStateChangelogStoragesManager taskManagerChangelogManager;
    private final TaskExecutorChannelStateExecutorFactoryManager taskManagerChannelStateManager;
    private final TaskEventDispatcher taskEventDispatcher;
    private final ExecutorService ioExecutor;
    private final LibraryCacheManager libraryCacheManager;
    private final SlotAllocationSnapshotPersistenceService slotAllocationSnapshotPersistenceService;
    private final SharedResources sharedResources;
    private final GroupCache<JobID, PermanentBlobKey, JobInformation> jobInformationCache;
    private final GroupCache<JobID, PermanentBlobKey, TaskInformation> taskInformationCache;
    private final GroupCache<JobID, PermanentBlobKey, ShuffleDescriptorGroup>
            shuffleDescriptorsCache;

    TaskManagerServices(
            UnresolvedTaskManagerLocation unresolvedTaskManagerLocation,
            long managedMemorySize,
            IOManager ioManager,
            ShuffleEnvironment<?, ?> shuffleEnvironment,
            KvStateService kvStateService,
            BroadcastVariableManager broadcastVariableManager,
            TaskSlotTable<Task> taskSlotTable,
            JobTable jobTable,
            JobLeaderService jobLeaderService,
            TaskExecutorLocalStateStoresManager taskManagerStateStore,
            TaskExecutorFileMergingManager taskManagerFileMergingManager,
            TaskExecutorStateChangelogStoragesManager taskManagerChangelogManager,
            TaskExecutorChannelStateExecutorFactoryManager taskManagerChannelStateManager,
            TaskEventDispatcher taskEventDispatcher,
            ExecutorService ioExecutor,
            LibraryCacheManager libraryCacheManager,
            SlotAllocationSnapshotPersistenceService slotAllocationSnapshotPersistenceService,
            SharedResources sharedResources,
            GroupCache<JobID, PermanentBlobKey, JobInformation> jobInformationCache,
            GroupCache<JobID, PermanentBlobKey, TaskInformation> taskInformationCache,
            GroupCache<JobID, PermanentBlobKey, ShuffleDescriptorGroup> shuffleDescriptorsCache) {

        this.unresolvedTaskManagerLocation =
                Preconditions.checkNotNull(unresolvedTaskManagerLocation);
        this.managedMemorySize = managedMemorySize;
        this.ioManager = Preconditions.checkNotNull(ioManager);
        this.shuffleEnvironment = Preconditions.checkNotNull(shuffleEnvironment);
        this.kvStateService = Preconditions.checkNotNull(kvStateService);
        this.broadcastVariableManager = Preconditions.checkNotNull(broadcastVariableManager);
        this.taskSlotTable = Preconditions.checkNotNull(taskSlotTable);
        this.jobTable = Preconditions.checkNotNull(jobTable);
        this.jobLeaderService = Preconditions.checkNotNull(jobLeaderService);
        this.taskManagerStateStore = Preconditions.checkNotNull(taskManagerStateStore);
        this.taskManagerFileMergingManager =
                Preconditions.checkNotNull(taskManagerFileMergingManager);
        this.taskManagerChangelogManager = Preconditions.checkNotNull(taskManagerChangelogManager);
        this.taskManagerChannelStateManager = taskManagerChannelStateManager;
        this.taskEventDispatcher = Preconditions.checkNotNull(taskEventDispatcher);
        this.ioExecutor = Preconditions.checkNotNull(ioExecutor);
        this.libraryCacheManager = Preconditions.checkNotNull(libraryCacheManager);
        this.slotAllocationSnapshotPersistenceService = slotAllocationSnapshotPersistenceService;
        this.sharedResources = Preconditions.checkNotNull(sharedResources);
        this.jobInformationCache = jobInformationCache;
        this.taskInformationCache = taskInformationCache;
        this.shuffleDescriptorsCache = Preconditions.checkNotNull(shuffleDescriptorsCache);
    }

    // --------------------------------------------------------------------------------------------
    //  Getter/Setter
    // --------------------------------------------------------------------------------------------

    public long getManagedMemorySize() {
        return managedMemorySize;
    }

    public IOManager getIOManager() {
        return ioManager;
    }

    public ShuffleEnvironment<?, ?> getShuffleEnvironment() {
        return shuffleEnvironment;
    }

    public KvStateService getKvStateService() {
        return kvStateService;
    }

    public UnresolvedTaskManagerLocation getUnresolvedTaskManagerLocation() {
        return unresolvedTaskManagerLocation;
    }

    public BroadcastVariableManager getBroadcastVariableManager() {
        return broadcastVariableManager;
    }

    public TaskSlotTable<Task> getTaskSlotTable() {
        return taskSlotTable;
    }

    public JobTable getJobTable() {
        return jobTable;
    }

    public JobLeaderService getJobLeaderService() {
        return jobLeaderService;
    }

    public TaskExecutorLocalStateStoresManager getTaskManagerStateStore() {
        return taskManagerStateStore;
    }

    public TaskExecutorFileMergingManager getTaskManagerFileMergingManager() {
        return taskManagerFileMergingManager;
    }

    public TaskExecutorStateChangelogStoragesManager getTaskManagerChangelogManager() {
        return taskManagerChangelogManager;
    }

    public TaskExecutorChannelStateExecutorFactoryManager getTaskManagerChannelStateManager() {
        return taskManagerChannelStateManager;
    }

    public TaskEventDispatcher getTaskEventDispatcher() {
        return taskEventDispatcher;
    }

    public Executor getIOExecutor() {
        return ioExecutor;
    }

    public LibraryCacheManager getLibraryCacheManager() {
        return libraryCacheManager;
    }

    public SharedResources getSharedResources() {
        return sharedResources;
    }

    public GroupCache<JobID, PermanentBlobKey, JobInformation> getJobInformationCache() {
        return jobInformationCache;
    }

    public GroupCache<JobID, PermanentBlobKey, TaskInformation> getTaskInformationCache() {
        return taskInformationCache;
    }

    public GroupCache<JobID, PermanentBlobKey, ShuffleDescriptorGroup> getShuffleDescriptorCache() {
        return shuffleDescriptorsCache;
    }

    // --------------------------------------------------------------------------------------------
    //  Shut down method
    // --------------------------------------------------------------------------------------------

    /** Shuts the {@link TaskExecutor} services down. */
    public void shutDown() throws FlinkException {

        Exception exception = null;

        try {
            taskManagerStateStore.shutdown();
        } catch (Exception e) {
            exception = e;
        }

        try {
            ioManager.close();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        try {
            shuffleEnvironment.close();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        try {
            kvStateService.shutdown();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        try {
            taskSlotTable.close();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        try {
            jobLeaderService.stop();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        try {
            ioExecutor.shutdown();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        try {
            jobTable.close();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        try {
            libraryCacheManager.shutdown();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        taskEventDispatcher.clearAll();

        if (exception != null) {
            throw new FlinkException(
                    "Could not properly shut down the TaskManager services.", exception);
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Static factory methods for task manager services
    // --------------------------------------------------------------------------------------------

    /**
     * Creates and returns the task manager services.
     *
     * @param taskManagerServicesConfiguration task manager configuration
     * @param permanentBlobService permanentBlobService used by the services
     * @param taskManagerMetricGroup metric group of the task manager
     * @param ioExecutor executor for async IO operations
     * @param scheduledExecutor scheduled executor in rpc service
     * @param fatalErrorHandler to handle class loading OOMs
     * @param workingDirectory the working directory of the process
     * @return task manager components
     * @throws Exception
     */
    public static TaskManagerServices fromConfiguration(
            TaskManagerServicesConfiguration taskManagerServicesConfiguration,
            PermanentBlobService permanentBlobService,
            MetricGroup taskManagerMetricGroup,
            ExecutorService ioExecutor,
            ScheduledExecutor scheduledExecutor,
            FatalErrorHandler fatalErrorHandler,
            WorkingDirectory workingDirectory)
            throws Exception {

        // pre-start checks
        checkTempDirs(taskManagerServicesConfiguration.getTmpDirPaths());
        //任务线程池与执行器
        //作用：管理任务（Task）运行时的线程资源与事件分发。
        //场景：
        //提供 TaskEventDispatcher，用于在上游 Task 和下游 Task 之间反向传递自定义事件（如迭代计算中的控制事件）。
        //初始化异步任务执行器、IO 执行器等线程池
        final TaskEventDispatcher taskEventDispatcher = new TaskEventDispatcher();

        // start the I/O manager, it will create some temp directories.
        //作用：提供高效的本地磁盘读写与临时文件管理服务。
        //场景：
        //当内存中的数据（如大窗口聚合、Sort-Merge 算子）超出容量时，提供异步落盘（Spill to disk）和读取机制。
        //自动清理 Task 运行过程中在本地临时目录产生的垃圾文件
        final IOManager ioManager = new IOManagerAsync(taskManagerServicesConfiguration.getTmpDirPaths(), ioExecutor);

        //作用：构建 TaskManager 节点间以及节点内部进行 Data Shuffle（数据交换）的网络底座。
        //包含组件：
        //NettyShuffleEnvironment：基于 Netty 实现的高性能节点间数据传输网络。
        //NetworkBufferPool：专门管理网络传输缓冲区的内存池（Network Memory）。
        //ResultPartitionManager & SingleInputGate：分别负责输出数据分片管理和输入数据闸门接收
        final ShuffleEnvironment<?, ?> shuffleEnvironment =
                createShuffleEnvironment(
                        taskManagerServicesConfiguration,
                        taskEventDispatcher,
                        taskManagerMetricGroup,
                        ioExecutor,
                        scheduledExecutor);
        final int listeningDataPort = shuffleEnvironment.start();

        LOG.info(
                "TaskManager data connection initialized successfully; listening internally on port: {}",
                listeningDataPort);
        //状态后端与本地恢复
        //作用：提供 Task 级别的本地状态存储与快速恢复机制（Local Recovery）。
        //场景：
        //在 Task 制作 Checkpoint 时，除了将状态持久化到远程分布式存储（如 S3/HDFS），TaskLocalStateStore 会在 TaskManager 本地磁盘同步保存一份副本。
        //当 Task 发生单节点故障重启时，直接从本地磁盘加载状态，避免从远程存储跨网络下载几百 GB 的 Checkpoint 数据，大幅缩短 Failover 时间
        final KvStateService kvStateService =
                KvStateService.fromConfiguration(taskManagerServicesConfiguration);
        kvStateService.start();

        final UnresolvedTaskManagerLocation unresolvedTaskManagerLocation =
                new UnresolvedTaskManagerLocation(
                        taskManagerServicesConfiguration.getResourceID(),
                        taskManagerServicesConfiguration.getExternalAddress(),
                        // we expose the task manager location with the listening port
                        // iff the external data port is not explicitly defined
                        taskManagerServicesConfiguration.getExternalDataPort() > 0
                                ? taskManagerServicesConfiguration.getExternalDataPort()
                                : listeningDataPort,
                        taskManagerServicesConfiguration.getNodeId());

        final BroadcastVariableManager broadcastVariableManager = new BroadcastVariableManager();
        //todo
        final TaskSlotTable<Task> taskSlotTable =
                createTaskSlotTable(
                        taskManagerServicesConfiguration.getNumberOfSlots(),//1
                        // taskManagerServicesConfiguration.getTaskExecutorResourceSpec().getCpuCores() =
                        // taskManagerServicesConfiguration.getTaskExecutorResourceSpec().getTaskHeapSize() = 1099511627776
                        // taskManagerServicesConfiguration.getTaskExecutorResourceSpec().getTaskOffHeapSize() = 1099511627776
                        // taskManagerServicesConfiguration.getTaskExecutorResourceSpec().getNetworkMemSize() = 64m
                        // taskManagerServicesConfiguration.getTaskExecutorResourceSpec().getManagedMemorySize() = 128m
                        // taskManagerServicesConfiguration.getTaskExecutorResourceSpec().getExtendedResources() size = 0
                        taskManagerServicesConfiguration.getTaskExecutorResourceSpec(),
                        taskManagerServicesConfiguration.getTimerServiceShutdownTimeout(),
                        taskManagerServicesConfiguration.getPageSize(),
                        ioExecutor);

        final JobTable jobTable = DefaultJobTable.create();
        //JobLeaderService 是负责 管理 TaskManager 与各个 JobMaster（作业主节点）之间的连接与 Leader 监听 的核心服务
        //帮 TaskManager 盯紧它所参与的所有 Job 的 JobMaster 谁才是真正的“老大”（Leader），并建立/断开对应的 RPC 连接
        /**
         * 1. 动态监听 JobMaster 的 Leader 变更（Leader Retrieval）
         * 当 TaskManager 上分配了某个 Job 的 TaskSlot 时，JobLeaderService 会为该 Job 注册一个 LeaderRetrievalService（如通过 ZooKeeper 或 Kubernetes 的 Leader 选举服务）。
         * 一旦远端 JobMaster 发生选主（例如原来的 JobMaster 挂了，新的 JobMaster 上任），JobLeaderService 会第一时间感知到新 Leader 的 RPC 地址与 Leader Session ID。
         * 2. 建立与维护 RPC 连接（Gateway Connection）
         * 当监听到正确的 JobMaster Leader 地址后，JobLeaderService 会自动通过 RpcService 去连接远端的 JobMaster，获取到该 JobMaster 的 RPC 代理对象（JobMasterGateway）。
         * 随后它会通知 TaskManager 的核心组件（TaskExecutor）：“* Job XXX 的新 Leader 已经连上了，地址是 YYY，你可以向它汇报 Slot 状态或心跳了。*”
         *
         * 3. 处理 Leader 丢失与连接断开（Disconnection & Cleanup）
         * 当 JobMaster 失去 Leader 身份、网络中断或 Job 结束时，JobLeaderService 会收到通知，并触发 JobLeaderListener.onJobLeaderLost 回调。
         * 它会及时清理掉已经失效的 JobMasterGateway，避免 TaskManager 继续向旧的、无效的 JobMaster 发送心跳或汇报数据。
         */
        final JobLeaderService jobLeaderService =
                new DefaultJobLeaderService(
                        unresolvedTaskManagerLocation,
                        taskManagerServicesConfiguration.getRetryingRegistrationConfiguration());

        final TaskExecutorLocalStateStoresManager taskStateManager =
                new TaskExecutorLocalStateStoresManager(
                        taskManagerServicesConfiguration.isLocalRecoveryEnabled(),
                        taskManagerServicesConfiguration.isLocalBackupEnabled(),
                        taskManagerServicesConfiguration.getLocalRecoveryStateDirectories(),
                        ioExecutor);

        final TaskExecutorStateChangelogStoragesManager changelogStoragesManager =
                new TaskExecutorStateChangelogStoragesManager();

        final TaskExecutorChannelStateExecutorFactoryManager channelStateExecutorFactoryManager =
                new TaskExecutorChannelStateExecutorFactoryManager();

        final TaskExecutorFileMergingManager fileMergingManager =
                new TaskExecutorFileMergingManager();

        final boolean failOnJvmMetaspaceOomError =
                taskManagerServicesConfiguration
                        .getConfiguration()
                        .get(CoreOptions.FAIL_ON_USER_CLASS_LOADING_METASPACE_OOM);
        final boolean checkClassLoaderLeak =
                taskManagerServicesConfiguration
                        .getConfiguration()
                        .get(CoreOptions.CHECK_LEAKED_CLASSLOADER);
        final LibraryCacheManager libraryCacheManager =
                new BlobLibraryCacheManager(
                        permanentBlobService,
                        BlobLibraryCacheManager.defaultClassLoaderFactory(
                                taskManagerServicesConfiguration.getClassLoaderResolveOrder(),
                                taskManagerServicesConfiguration
                                        .getAlwaysParentFirstLoaderPatterns(),
                                failOnJvmMetaspaceOomError ? fatalErrorHandler : null,
                                checkClassLoaderLeak),
                        false);

        final SlotAllocationSnapshotPersistenceService slotAllocationSnapshotPersistenceService;

        if (taskManagerServicesConfiguration.isLocalRecoveryEnabled()) {
            slotAllocationSnapshotPersistenceService =
                    new FileSlotAllocationSnapshotPersistenceService(
                            workingDirectory.getSlotAllocationSnapshotDirectory());
        } else {
            slotAllocationSnapshotPersistenceService =
                    NoOpSlotAllocationSnapshotPersistenceService.INSTANCE;
        }

        final GroupCache<JobID, PermanentBlobKey, JobInformation> jobInformationCache =
                new DefaultGroupCache.Factory<JobID, PermanentBlobKey, JobInformation>().create();
        final GroupCache<JobID, PermanentBlobKey, TaskInformation> taskInformationCache =
                new DefaultGroupCache.Factory<JobID, PermanentBlobKey, TaskInformation>().create();

        final GroupCache<JobID, PermanentBlobKey, ShuffleDescriptorGroup> shuffleDescriptorsCache =
                new DefaultGroupCache.Factory<JobID, PermanentBlobKey, ShuffleDescriptorGroup>()
                        .create();

        return new TaskManagerServices(
                unresolvedTaskManagerLocation,
                taskManagerServicesConfiguration.getManagedMemorySize().getBytes(),
                ioManager,
                shuffleEnvironment,
                kvStateService,
                broadcastVariableManager,
                taskSlotTable,
                jobTable,
                jobLeaderService,
                taskStateManager,
                fileMergingManager,
                changelogStoragesManager,
                channelStateExecutorFactoryManager,
                taskEventDispatcher,
                ioExecutor,
                libraryCacheManager,
                slotAllocationSnapshotPersistenceService,
                new SharedResources(),
                jobInformationCache,
                taskInformationCache,
                shuffleDescriptorsCache);
    }

    private static TaskSlotTable<Task> createTaskSlotTable(
            final int numberOfSlots,
            final TaskExecutorResourceSpec taskExecutorResourceSpec,
            final long timerServiceShutdownTimeout,
            final int pageSize,
            final Executor memoryVerificationExecutor) {
        final TimerService<AllocationID> timerService =
                new DefaultTimerService<>(
                        new ScheduledThreadPoolExecutor(1), timerServiceShutdownTimeout);
        //todo
        return new TaskSlotTableImpl<>(
                numberOfSlots,
                TaskExecutorResourceUtils.generateTotalAvailableResourceProfile(
                        taskExecutorResourceSpec),
                TaskExecutorResourceUtils.generateDefaultSlotResourceProfile(
                        taskExecutorResourceSpec, numberOfSlots),
                pageSize,
                timerService,
                memoryVerificationExecutor);
    }

    private static ShuffleEnvironment<?, ?> createShuffleEnvironment(
            TaskManagerServicesConfiguration taskManagerServicesConfiguration,
            TaskEventDispatcher taskEventDispatcher,
            MetricGroup taskManagerMetricGroup,
            Executor ioExecutor,
            ScheduledExecutor scheduledExecutor)
            throws FlinkException {

        final ShuffleEnvironmentContext shuffleEnvironmentContext =
                new ShuffleEnvironmentContext(
                        taskManagerServicesConfiguration.getConfiguration(),
                        taskManagerServicesConfiguration.getResourceID(),
                        taskManagerServicesConfiguration.getNetworkMemorySize(),
                        taskManagerServicesConfiguration.isLocalCommunicationOnly(),
                        taskManagerServicesConfiguration.getBindAddress(),
                        taskManagerServicesConfiguration.getNumberOfSlots(),
                        taskManagerServicesConfiguration.getTmpDirPaths(),
                        taskEventDispatcher,
                        taskManagerMetricGroup,
                        ioExecutor,
                        scheduledExecutor);

        return ShuffleServiceLoader.loadShuffleServiceFactory(
                        taskManagerServicesConfiguration.getConfiguration())
                .createShuffleEnvironment(shuffleEnvironmentContext);
    }

    /**
     * Validates that all the directories denoted by the strings do actually exist or can be
     * created, are proper directories (not files), and are writable.
     *
     * @param tmpDirs The array of directory paths to check.
     * @throws IOException Thrown if any of the directories does not exist and cannot be created or
     *     is not writable or is a file, rather than a directory.
     */
    private static void checkTempDirs(String[] tmpDirs) throws IOException {
        for (String dir : tmpDirs) {
            if (dir != null && !dir.equals("")) {
                File file = new File(dir);
                if (!file.exists()) {
                    if (!file.mkdirs()) {
                        throw new IOException(
                                "Temporary file directory "
                                        + file.getAbsolutePath()
                                        + " does not exist and could not be created.");
                    }
                }
                if (!file.isDirectory()) {
                    throw new IOException(
                            "Temporary file directory "
                                    + file.getAbsolutePath()
                                    + " is not a directory.");
                }
                if (!file.canWrite()) {
                    throw new IOException(
                            "Temporary file directory "
                                    + file.getAbsolutePath()
                                    + " is not writable.");
                }

                if (LOG.isInfoEnabled()) {
                    long totalSpaceGb = file.getTotalSpace() >> 30;
                    long usableSpaceGb = file.getUsableSpace() >> 30;
                    double usablePercentage = (double) usableSpaceGb / totalSpaceGb * 100;
                    String path = file.getAbsolutePath();
                    LOG.info(
                            String.format(
                                    "Temporary file directory '%s': total %d GB, "
                                            + "usable %d GB (%.2f%% usable)",
                                    path, totalSpaceGb, usableSpaceGb, usablePercentage));
                }
            } else {
                throw new IllegalArgumentException("Temporary file directory #$id is null.");
            }
        }
    }

    public SlotAllocationSnapshotPersistenceService getSlotAllocationSnapshotPersistenceService() {
        return slotAllocationSnapshotPersistenceService;
    }
}
