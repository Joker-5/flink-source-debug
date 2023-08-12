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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.blocklist.BlockedNode;
import org.apache.flink.runtime.blocklist.BlocklistContext;
import org.apache.flink.runtime.blocklist.BlocklistHandler;
import org.apache.flink.runtime.blocklist.BlocklistUtils;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.heartbeat.HeartbeatListener;
import org.apache.flink.runtime.heartbeat.HeartbeatManager;
import org.apache.flink.runtime.heartbeat.HeartbeatReceiver;
import org.apache.flink.runtime.heartbeat.HeartbeatSender;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.NoOpHeartbeatManager;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.io.network.partition.PartitionTrackerFactory;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmanager.PartitionProducerDisposedException;
import org.apache.flink.runtime.jobmaster.factories.JobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.jobmaster.slotpool.BlocklistDeclarativeSlotPoolFactory;
import org.apache.flink.runtime.jobmaster.slotpool.DeclarativeSlotPoolFactory;
import org.apache.flink.runtime.jobmaster.slotpool.DefaultDeclarativeSlotPoolFactory;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPoolService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.query.UnknownKvStateLocation;
import org.apache.flink.runtime.registration.RegisteredRpcConnection;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.registration.RetryingRegistration;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.FencedRpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcServiceUtils;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.scheduler.SchedulerNG;
import org.apache.flink.runtime.shuffle.JobShuffleContext;
import org.apache.flink.runtime.shuffle.JobShuffleContextImpl;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutorToJobManagerHeartbeatPayload;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation.ResolutionMode;
import org.apache.flink.runtime.taskmanager.UnresolvedTaskManagerLocation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.checkpoint.TaskStateSnapshot.deserializeTaskStateSnapshot;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * JobMaster implementation. The job master is responsible for the execution of a single {@link
 * JobGraph}.
 *
 * <p>It offers the following methods as part of its rpc interface to interact with the JobMaster
 * remotely:
 *
 * <ul>
 *   <li>{@link #updateTaskExecutionState} updates the task execution state for given task
 * </ul>
 */
public class JobMaster extends FencedRpcEndpoint<JobMasterId>
        implements JobMasterGateway, JobMasterService {

    /** Default names for Flink's distributed components. */
    public static final String JOB_MANAGER_NAME = "jobmanager";

    // ------------------------------------------------------------------------

    private final JobMasterConfiguration jobMasterConfiguration;

    private final ResourceID resourceId;

    // 作业的 JobGraph 信息
    private final JobGraph jobGraph;

    private final Time rpcTimeout;

    // HA 服务，这里主要用于监控 RM Leader，如果 RM Leader 有变换，这里会和新 Leader 建立连接
    private final HighAvailabilityServices highAvailabilityServices;

    // 用于将数据上传到 BlobServer，这里主要上传 JobInformation 和 TaskInformation
    private final BlobWriter blobWriter;

    private final HeartbeatServices heartbeatServices;

    private final ScheduledExecutorService futureExecutor;

    private final Executor ioExecutor;

    private final OnCompletionActions jobCompletionActions;

    private final FatalErrorHandler fatalErrorHandler;

    private final ClassLoader userCodeLoader;

    // 处理 slot 相关内容
    private final SlotPoolService slotPoolService;

    private final long initializationTimestamp;

    private final boolean retrieveTaskManagerHostName;

    // --------- ResourceManager --------

    private final LeaderRetrievalService resourceManagerLeaderRetriever;

    // --------- TaskManagers --------

    private final Map<ResourceID, TaskManagerRegistration> registeredTaskManagers;

    // 用于注册 Intermediate result partition，在作业调度时使用
    private final ShuffleMaster<?> shuffleMaster;

    // --------- Scheduler --------
    // 用于调度作业的 ExecutionGraph
    private final SchedulerNG schedulerNG;

    private final JobManagerJobStatusListener jobStatusListener;

    private final JobManagerJobMetricGroup jobManagerJobMetricGroup;

    // -------- Misc ---------

    private final Map<String, Object> accumulators;

    // 用于追踪 Intermediate result partition 的服务
    private final JobMasterPartitionTracker partitionTracker;

    private final ExecutionDeploymentTracker executionDeploymentTracker;
    private final ExecutionDeploymentReconciler executionDeploymentReconciler;

    // -------- Mutable fields ---------

    @Nullable private ResourceManagerAddress resourceManagerAddress;

    @Nullable private ResourceManagerConnection resourceManagerConnection;

    @Nullable private EstablishedResourceManagerConnection establishedResourceManagerConnection;

    private HeartbeatManager<TaskExecutorToJobManagerHeartbeatPayload, AllocatedSlotReport>
            taskManagerHeartbeatManager;

    private HeartbeatManager<Void, Void> resourceManagerHeartbeatManager;

    private final BlocklistHandler blocklistHandler;

    // ------------------------------------------------------------------------

    public JobMaster(
            RpcService rpcService,
            JobMasterId jobMasterId,
            JobMasterConfiguration jobMasterConfiguration,
            ResourceID resourceId,
            JobGraph jobGraph,
            HighAvailabilityServices highAvailabilityService,
            SlotPoolServiceSchedulerFactory slotPoolServiceSchedulerFactory,
            JobManagerSharedServices jobManagerSharedServices,
            HeartbeatServices heartbeatServices,
            JobManagerJobMetricGroupFactory jobMetricGroupFactory,
            OnCompletionActions jobCompletionActions,
            FatalErrorHandler fatalErrorHandler,
            ClassLoader userCodeLoader,
            ShuffleMaster<?> shuffleMaster,
            PartitionTrackerFactory partitionTrackerFactory,
            ExecutionDeploymentTracker executionDeploymentTracker,
            ExecutionDeploymentReconciler.Factory executionDeploymentReconcilerFactory,
            BlocklistHandler.Factory blocklistHandlerFactory,
            long initializationTimestamp)
            throws Exception {

        super(rpcService, RpcServiceUtils.createRandomName(JOB_MANAGER_NAME), jobMasterId);

        final ExecutionDeploymentReconciliationHandler executionStateReconciliationHandler =
                new ExecutionDeploymentReconciliationHandler() {

                    @Override
                    public void onMissingDeploymentsOf(
                            Collection<ExecutionAttemptID> executionAttemptIds, ResourceID host) {
                        log.debug(
                                "Failing deployments {} due to no longer being deployed.",
                                executionAttemptIds);
                        for (ExecutionAttemptID executionAttemptId : executionAttemptIds) {
                            schedulerNG.updateTaskExecutionState(
                                    new TaskExecutionState(
                                            executionAttemptId,
                                            ExecutionState.FAILED,
                                            new FlinkException(
                                                    String.format(
                                                            "Execution %s is unexpectedly no longer running on task executor %s.",
                                                            executionAttemptId, host))));
                        }
                    }

                    @Override
                    public void onUnknownDeploymentsOf(
                            Collection<ExecutionAttemptID> executionAttemptIds, ResourceID host) {
                        log.debug(
                                "Canceling left-over deployments {} on task executor {}.",
                                executionAttemptIds,
                                host);
                        for (ExecutionAttemptID executionAttemptId : executionAttemptIds) {
                            TaskManagerRegistration taskManagerRegistration =
                                    registeredTaskManagers.get(host);
                            if (taskManagerRegistration != null) {
                                taskManagerRegistration
                                        .getTaskExecutorGateway()
                                        .cancelTask(executionAttemptId, rpcTimeout);
                            }
                        }
                    }
                };

        this.executionDeploymentTracker = executionDeploymentTracker;
        this.executionDeploymentReconciler =
                executionDeploymentReconcilerFactory.create(executionStateReconciliationHandler);

        this.jobMasterConfiguration = checkNotNull(jobMasterConfiguration);
        this.resourceId = checkNotNull(resourceId);
        this.jobGraph = checkNotNull(jobGraph);
        this.rpcTimeout = jobMasterConfiguration.getRpcTimeout();
        this.highAvailabilityServices = checkNotNull(highAvailabilityService);
        this.blobWriter = jobManagerSharedServices.getBlobWriter();
        this.futureExecutor = jobManagerSharedServices.getFutureExecutor();
        this.ioExecutor = jobManagerSharedServices.getIoExecutor();
        this.jobCompletionActions = checkNotNull(jobCompletionActions);
        this.fatalErrorHandler = checkNotNull(fatalErrorHandler);
        this.userCodeLoader = checkNotNull(userCodeLoader);
        this.initializationTimestamp = initializationTimestamp;
        this.retrieveTaskManagerHostName =
                jobMasterConfiguration
                        .getConfiguration()
                        .getBoolean(JobManagerOptions.RETRIEVE_TASK_MANAGER_HOSTNAME);

        final String jobName = jobGraph.getName();
        final JobID jid = jobGraph.getJobID();

        log.info("Initializing job '{}' ({}).", jobName, jid);

        resourceManagerLeaderRetriever =
                highAvailabilityServices.getResourceManagerLeaderRetriever();

        this.registeredTaskManagers = new HashMap<>();
        this.blocklistHandler =
                blocklistHandlerFactory.create(
                        new JobMasterBlocklistContext(),
                        this::getNodeIdOfTaskManager,
                        getMainThreadExecutor(),
                        log);

        this.slotPoolService =
                checkNotNull(slotPoolServiceSchedulerFactory)
                        .createSlotPoolService(
                                jid,
                                createDeclarativeSlotPoolFactory(
                                        jobMasterConfiguration.getConfiguration()));

        this.partitionTracker =
                checkNotNull(partitionTrackerFactory)
                        .create(
                                resourceID -> {
                                    return Optional.ofNullable(
                                                    registeredTaskManagers.get(resourceID))
                                            .map(TaskManagerRegistration::getTaskExecutorGateway);
                                });

        this.shuffleMaster = checkNotNull(shuffleMaster);

        this.jobManagerJobMetricGroup = jobMetricGroupFactory.create(jobGraph);
        this.jobStatusListener = new JobManagerJobStatusListener();
        // 创建 scheduler
        this.schedulerNG =
                createScheduler(
                        slotPoolServiceSchedulerFactory,
                        executionDeploymentTracker,
                        jobManagerJobMetricGroup,
                        jobStatusListener);

        this.heartbeatServices = checkNotNull(heartbeatServices);
        this.taskManagerHeartbeatManager = NoOpHeartbeatManager.getInstance();
        this.resourceManagerHeartbeatManager = NoOpHeartbeatManager.getInstance();

        this.resourceManagerConnection = null;
        this.establishedResourceManagerConnection = null;

        this.accumulators = new HashMap<>();
    }

    private SchedulerNG createScheduler(
            SlotPoolServiceSchedulerFactory slotPoolServiceSchedulerFactory,
            ExecutionDeploymentTracker executionDeploymentTracker,
            JobManagerJobMetricGroup jobManagerJobMetricGroup,
            JobStatusListener jobStatusListener)
            throws Exception {
        final SchedulerNG scheduler =
                slotPoolServiceSchedulerFactory.createScheduler(
                        log,
                        jobGraph,
                        ioExecutor,
                        jobMasterConfiguration.getConfiguration(),
                        slotPoolService,
                        futureExecutor,
                        userCodeLoader,
                        highAvailabilityServices.getCheckpointRecoveryFactory(),
                        rpcTimeout,
                        blobWriter,
                        jobManagerJobMetricGroup,
                        jobMasterConfiguration.getSlotRequestTimeout(),
                        shuffleMaster,
                        partitionTracker,
                        executionDeploymentTracker,
                        initializationTimestamp,
                        getMainThreadExecutor(),
                        fatalErrorHandler,
                        jobStatusListener,
                        blocklistHandler::addNewBlockedNodes);

        return scheduler;
    }

    private HeartbeatManager<Void, Void> createResourceManagerHeartbeatManager(
            HeartbeatServices heartbeatServices) {
        return heartbeatServices.createHeartbeatManager(
                resourceId, new ResourceManagerHeartbeatListener(), getMainThreadExecutor(), log);
    }

    private HeartbeatManager<TaskExecutorToJobManagerHeartbeatPayload, AllocatedSlotReport>
            createTaskManagerHeartbeatManager(HeartbeatServices heartbeatServices) {
        return heartbeatServices.createHeartbeatManagerSender(
                resourceId, new TaskManagerHeartbeatListener(), getMainThreadExecutor(), log);
    }

    private DeclarativeSlotPoolFactory createDeclarativeSlotPoolFactory(
            Configuration configuration) {
        if (BlocklistUtils.isBlocklistEnabled(configuration)) {
            return new BlocklistDeclarativeSlotPoolFactory(blocklistHandler::isBlockedTaskManager);
        } else {
            return new DefaultDeclarativeSlotPoolFactory();
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Lifecycle management
    // ----------------------------------------------------------------------------------------------

    @Override
    protected void onStart() throws JobMasterException {
        try {
            startJobExecution();
        } catch (Exception e) {
            final JobMasterException jobMasterException =
                    new JobMasterException("Could not start the JobMaster.", e);
            handleJobMasterError(jobMasterException);
            throw jobMasterException;
        }
    }

    /** Suspend the job and shutdown all other services including rpc. */
    @Override
    public CompletableFuture<Void> onStop() {
        log.info(
                "Stopping the JobMaster for job '{}' ({}).",
                jobGraph.getName(),
                jobGraph.getJobID());

        // make sure there is a graceful exit
        return stopJobExecution(
                        new FlinkException(
                                String.format(
                                        "Stopping JobMaster for job '%s' (%s).",
                                        jobGraph.getName(), jobGraph.getJobID())))
                .exceptionally(
                        exception -> {
                            throw new CompletionException(
                                    new JobMasterException(
                                            "Could not properly stop the JobMaster.", exception));
                        });
    }

    // ----------------------------------------------------------------------------------------------
    // RPC methods
    // ----------------------------------------------------------------------------------------------

    @Override
    // 取消当前正在执行的作业，如果作业还在调度会执行停止；如果作业正在运行，则会向对应的 TM 发送取消 task 的请求
    public CompletableFuture<Acknowledge> cancel(Time timeout) {
        schedulerNG.cancel();

        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    /**
     * Updates the task execution state for a given task.
     *
     * @param taskExecutionState New task execution state for a given task
     * @return Acknowledge the task execution state update
     */
    @Override
    // 更新 task 的状态信息，这是 TM 主动向 JM 发送的更新请求
    public CompletableFuture<Acknowledge> updateTaskExecutionState(
            final TaskExecutionState taskExecutionState) {
        FlinkException taskExecutionException;
        try {
            checkNotNull(taskExecutionState, "taskExecutionState");

            if (schedulerNG.updateTaskExecutionState(taskExecutionState)) {
                return CompletableFuture.completedFuture(Acknowledge.get());
            } else {
                taskExecutionException =
                        new ExecutionGraphException(
                                "The execution attempt "
                                        + taskExecutionState.getID()
                                        + " was not found.");
            }
        } catch (Exception e) {
            taskExecutionException =
                    new JobMasterException(
                            "Could not update the state of task execution for JobMaster.", e);
            handleJobMasterError(taskExecutionException);
        }
        return FutureUtils.completedExceptionally(taskExecutionException);
    }

    @Override
    public CompletableFuture<SerializedInputSplit> requestNextInputSplit(
            final JobVertexID vertexID, final ExecutionAttemptID executionAttempt) {

        try {
            return CompletableFuture.completedFuture(
                    schedulerNG.requestNextInputSplit(vertexID, executionAttempt));
        } catch (IOException e) {
            log.warn("Error while requesting next input split", e);
            return FutureUtils.completedExceptionally(e);
        }
    }

    @Override
    // 获取指定 Result Partition 对应生产者的 JobVertex 的执行状态
    public CompletableFuture<ExecutionState> requestPartitionState(
            final IntermediateDataSetID intermediateResultId,
            final ResultPartitionID resultPartitionId) {

        try {
            return CompletableFuture.completedFuture(
                    schedulerNG.requestPartitionState(intermediateResultId, resultPartitionId));
        } catch (PartitionProducerDisposedException e) {
            log.info("Error while requesting partition state", e);
            return FutureUtils.completedExceptionally(e);
        }
    }

    @Override
    // TM 心跳超时或作业取消时会调用这个方法，此时 JM 会释放这个 TM 上的所有 slot 资源
    public CompletableFuture<Acknowledge> disconnectTaskManager(
            final ResourceID resourceID, final Exception cause) {
        log.info(
                "Disconnect TaskExecutor {} because: {}",
                resourceID.getStringWithMetadata(),
                cause.getMessage(),
                ExceptionUtils.returnExceptionIfUnexpected(cause.getCause()));
        ExceptionUtils.logExceptionIfExcepted(cause.getCause(), log);

        taskManagerHeartbeatManager.unmonitorTarget(resourceID);
        slotPoolService.releaseTaskManager(resourceID, cause);
        partitionTracker.stopTrackingPartitionsFor(resourceID);

        TaskManagerRegistration taskManagerRegistration = registeredTaskManagers.remove(resourceID);

        if (taskManagerRegistration != null) {
            taskManagerRegistration
                    .getTaskExecutorGateway()
                    .disconnectJobManager(jobGraph.getJobID(), cause);
        }

        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    // TODO: This method needs a leader session ID
    @Override
    // 当一个 task 完成 snapshot 时，会通过这个接口通知 JM 进行相应处理，
    // 如果这个 cp 的所有 task 都已经 ack 了，则意味着 cp 完成
    public void acknowledgeCheckpoint(
            final JobID jobID,
            final ExecutionAttemptID executionAttemptID,
            final long checkpointId,
            final CheckpointMetrics checkpointMetrics,
            @Nullable final SerializedValue<TaskStateSnapshot> checkpointState) {
        schedulerNG.acknowledgeCheckpoint(
                jobID,
                executionAttemptID,
                checkpointId,
                checkpointMetrics,
                deserializeTaskStateSnapshot(checkpointState, getClass().getClassLoader()));
    }

    @Override
    public void reportCheckpointMetrics(
            JobID jobID,
            ExecutionAttemptID executionAttemptID,
            long checkpointId,
            CheckpointMetrics checkpointMetrics) {

        schedulerNG.reportCheckpointMetrics(
                jobID, executionAttemptID, checkpointId, checkpointMetrics);
    }

    // TODO: This method needs a leader session ID
    @Override
    // TM 向 JM 发送此消息，通知 JM 的 Checkpoint Coordinator 这个 checkpoint request 没有响应，
    // 例如：TM 触发 cp 失败，这样 Checkpoint Coordinator 就知道这个 cp 失败，然后进行相应处理
    public void declineCheckpoint(DeclineCheckpoint decline) {
        schedulerNG.declineCheckpoint(decline);
    }

    @Override
    public CompletableFuture<Acknowledge> sendOperatorEventToCoordinator(
            final ExecutionAttemptID task,
            final OperatorID operatorID,
            final SerializedValue<OperatorEvent> serializedEvent) {

        try {
            final OperatorEvent evt = serializedEvent.deserializeValue(userCodeLoader);
            schedulerNG.deliverOperatorEventToCoordinator(task, operatorID, evt);
            return CompletableFuture.completedFuture(Acknowledge.get());
        } catch (Exception e) {
            return FutureUtils.completedExceptionally(e);
        }
    }

    @Override
    public CompletableFuture<CoordinationResponse> sendRequestToCoordinator(
            OperatorID operatorID, SerializedValue<CoordinationRequest> serializedRequest) {
        try {
            final CoordinationRequest request = serializedRequest.deserializeValue(userCodeLoader);
            return schedulerNG.deliverCoordinationRequestToCoordinator(operatorID, request);
        } catch (Exception e) {
            return FutureUtils.completedExceptionally(e);
        }
    }

    @Override
    // 请求某个注册过 registrationName 对应的 KvState 的位置信息
    public CompletableFuture<KvStateLocation> requestKvStateLocation(
            final JobID jobId, final String registrationName) {
        try {
            return CompletableFuture.completedFuture(
                    schedulerNG.requestKvStateLocation(jobId, registrationName));
        } catch (UnknownKvStateLocation | FlinkJobNotFoundException e) {
            log.info("Error while request key-value state location", e);
            return FutureUtils.completedExceptionally(e);
        }
    }

    @Override
    // 当注册一个 KvState 时，会调用此方法，一个 operator 在初始化时会调用这个方法注册一个 KvState
    public CompletableFuture<Acknowledge> notifyKvStateRegistered(
            final JobID jobId,
            final JobVertexID jobVertexId,
            final KeyGroupRange keyGroupRange,
            final String registrationName,
            final KvStateID kvStateId,
            final InetSocketAddress kvStateServerAddress) {

        try {
            schedulerNG.notifyKvStateRegistered(
                    jobId,
                    jobVertexId,
                    keyGroupRange,
                    registrationName,
                    kvStateId,
                    kvStateServerAddress);
            return CompletableFuture.completedFuture(Acknowledge.get());
        } catch (FlinkJobNotFoundException e) {
            log.info("Error while receiving notification about key-value state registration", e);
            return FutureUtils.completedExceptionally(e);
        }
    }

    @Override
    // 取消一个 KvState 的注册，operator 在关闭 state backend 时会调用此方法，
    // 比如 operator 的生命周期结束
    public CompletableFuture<Acknowledge> notifyKvStateUnregistered(
            JobID jobId,
            JobVertexID jobVertexId,
            KeyGroupRange keyGroupRange,
            String registrationName) {
        try {
            schedulerNG.notifyKvStateUnregistered(
                    jobId, jobVertexId, keyGroupRange, registrationName);
            return CompletableFuture.completedFuture(Acknowledge.get());
        } catch (FlinkJobNotFoundException e) {
            log.info("Error while receiving notification about key-value state de-registration", e);
            return FutureUtils.completedExceptionally(e);
        }
    }

    @Override
    // TM 通知 JM 其上面分配到的 slot 列表
    public CompletableFuture<Collection<SlotOffer>> offerSlots(
            final ResourceID taskManagerId, final Collection<SlotOffer> slots, final Time timeout) {

        TaskManagerRegistration taskManagerRegistration = registeredTaskManagers.get(taskManagerId);

        if (taskManagerRegistration == null) {
            return FutureUtils.completedExceptionally(
                    new Exception("Unknown TaskManager " + taskManagerId));
        }

        final RpcTaskManagerGateway rpcTaskManagerGateway =
                new RpcTaskManagerGateway(
                        taskManagerRegistration.getTaskExecutorGateway(), getFencingToken());

        return CompletableFuture.completedFuture(
                slotPoolService.offerSlots(
                        taskManagerRegistration.getTaskManagerLocation(),
                        rpcTaskManagerGateway,
                        slots));
    }

    @Override
    // 当 TM 分配 slot 失败，比如 slot 分配时状态转移失败，会调用这个方法通知 JM
    public void failSlot(
            final ResourceID taskManagerId,
            final AllocationID allocationId,
            final Exception cause) {

        if (registeredTaskManagers.containsKey(taskManagerId)) {
            internalFailAllocation(taskManagerId, allocationId, cause);
        } else {
            log.warn(
                    "Cannot fail slot "
                            + allocationId
                            + " because the TaskManager "
                            + taskManagerId
                            + " is unknown.");
        }
    }

    private void internalFailAllocation(
            @Nullable ResourceID resourceId, AllocationID allocationId, Exception cause) {
        final Optional<ResourceID> resourceIdOptional =
                slotPoolService.failAllocation(resourceId, allocationId, cause);
        resourceIdOptional.ifPresent(
                taskManagerId -> {
                    if (!partitionTracker.isTrackingPartitionsFor(taskManagerId)) {
                        releaseEmptyTaskManager(taskManagerId);
                    }
                });
    }

    private void releaseEmptyTaskManager(ResourceID resourceId) {
        disconnectTaskManager(
                resourceId,
                new FlinkException(
                        String.format(
                                "No more slots registered at JobMaster %s.",
                                resourceId.getStringWithMetadata())));
    }

    @Override
    // 向 JM 注册 TM，并通过心跳监控对应的 TM，
    // 只有注册过的 TM 的 slot 才是有效的，才会进行相应的分配
    public CompletableFuture<RegistrationResponse> registerTaskManager(
            final JobID jobId,
            final TaskManagerRegistrationInformation taskManagerRegistrationInformation,
            final Time timeout) {

        if (!jobGraph.getJobID().equals(jobId)) {
            log.debug(
                    "Rejecting TaskManager registration attempt because of wrong job id {}.",
                    jobId);
            return CompletableFuture.completedFuture(
                    new JMTMRegistrationRejection(
                            String.format(
                                    "The JobManager is not responsible for job %s. Maybe the TaskManager used outdated connection information.",
                                    jobId)));
        }

        final TaskManagerLocation taskManagerLocation;
        try {
            taskManagerLocation =
                    resolveTaskManagerLocation(
                            taskManagerRegistrationInformation.getUnresolvedTaskManagerLocation());
        } catch (FlinkException exception) {
            log.error("Could not accept TaskManager registration.", exception);
            return CompletableFuture.completedFuture(new RegistrationResponse.Failure(exception));
        }

        final ResourceID taskManagerId = taskManagerLocation.getResourceID();
        final UUID sessionId = taskManagerRegistrationInformation.getTaskManagerSession();
        final TaskManagerRegistration taskManagerRegistration =
                registeredTaskManagers.get(taskManagerId);

        if (taskManagerRegistration != null) {
            if (taskManagerRegistration.getSessionId().equals(sessionId)) {
                log.debug(
                        "Ignoring registration attempt of TaskManager {} with the same session id {}.",
                        taskManagerId,
                        sessionId);
                final RegistrationResponse response = new JMTMRegistrationSuccess(resourceId);
                return CompletableFuture.completedFuture(response);
            } else {
                disconnectTaskManager(
                        taskManagerId,
                        new FlinkException(
                                String.format(
                                        "A registered TaskManager %s re-registered with a new session id. This indicates a restart of the TaskManager. Closing the old connection.",
                                        taskManagerId)));
            }
        }

        return getRpcService()
                .connect(
                        taskManagerRegistrationInformation.getTaskManagerRpcAddress(),
                        TaskExecutorGateway.class)
                .handleAsync(
                        (TaskExecutorGateway taskExecutorGateway, Throwable throwable) -> {
                            if (throwable != null) {
                                return new RegistrationResponse.Failure(throwable);
                            }

                            slotPoolService.registerTaskManager(taskManagerId);
                            registeredTaskManagers.put(
                                    taskManagerId,
                                    TaskManagerRegistration.create(
                                            taskManagerLocation, taskExecutorGateway, sessionId));

                            // monitor the task manager as heartbeat target
                            taskManagerHeartbeatManager.monitorTarget(
                                    taskManagerId,
                                    new TaskExecutorHeartbeatSender(taskExecutorGateway));

                            return new JMTMRegistrationSuccess(resourceId);
                        },
                        getMainThreadExecutor());
    }

    @Nonnull
    private TaskManagerLocation resolveTaskManagerLocation(
            UnresolvedTaskManagerLocation unresolvedTaskManagerLocation) throws FlinkException {
        try {
            if (retrieveTaskManagerHostName) {
                return TaskManagerLocation.fromUnresolvedLocation(
                        unresolvedTaskManagerLocation, ResolutionMode.RETRIEVE_HOST_NAME);
            } else {
                return TaskManagerLocation.fromUnresolvedLocation(
                        unresolvedTaskManagerLocation, ResolutionMode.USE_IP_ONLY);
            }
        } catch (Throwable throwable) {
            final String errMsg =
                    String.format(
                            "TaskManager address %s cannot be resolved. %s",
                            unresolvedTaskManagerLocation.getExternalAddress(),
                            throwable.getMessage());
            throw new FlinkException(errMsg, throwable);
        }
    }

    @Override
    // 与 RM 断开连接，有 3 种条件会触发：
    // 1、JM 与 RM 心跳超时
    // 2、作业取消
    // 3、重连 RM 时断开连接，比如 RM leader 切换、RM 心跳超时
    public void disconnectResourceManager(
            final ResourceManagerId resourceManagerId, final Exception cause) {

        if (isConnectingToResourceManager(resourceManagerId)) {
            reconnectToResourceManager(cause);
        }
    }

    private boolean isConnectingToResourceManager(ResourceManagerId resourceManagerId) {
        return resourceManagerAddress != null
                && resourceManagerAddress.getResourceManagerId().equals(resourceManagerId);
    }

    @Override
    // TM 向 JM 发送心跳消息
    public CompletableFuture<Void> heartbeatFromTaskManager(
            final ResourceID resourceID, TaskExecutorToJobManagerHeartbeatPayload payload) {
        return taskManagerHeartbeatManager.receiveHeartbeat(resourceID, payload);
    }

    @Override
    // JM 向 RM 发送心跳消息，RM 会监听 JM 是否超时
    public CompletableFuture<Void> heartbeatFromResourceManager(final ResourceID resourceID) {
        return resourceManagerHeartbeatManager.requestHeartbeat(resourceID, null);
    }

    @Override
    public CompletableFuture<JobDetails> requestJobDetails(Time timeout) {
        return CompletableFuture.completedFuture(schedulerNG.requestJobDetails());
    }

    @Override
    // 请求这个作业的执行状态 JobStatus
    public CompletableFuture<JobStatus> requestJobStatus(Time timeout) {
        return CompletableFuture.completedFuture(schedulerNG.requestJobStatus());
    }

    @Override
    // 请求这个作业的 ArchivedExecutionGraph（ExecutionGraph 序列化后的结果）
    public CompletableFuture<ExecutionGraphInfo> requestJob(Time timeout) {
        return CompletableFuture.completedFuture(schedulerNG.requestJob());
    }

    @Override
    public CompletableFuture<CompletedCheckpoint> triggerCheckpoint(
            final CheckpointType checkpointType, final Time timeout) {
        return schedulerNG.triggerCheckpoint(checkpointType);
    }

    @Override
    // 对作业触发一次 savepoint
    public CompletableFuture<String> triggerSavepoint(
            @Nullable final String targetDirectory,
            final boolean cancelJob,
            final SavepointFormatType formatType,
            final Time timeout) {

        return schedulerNG.triggerSavepoint(targetDirectory, cancelJob, formatType);
    }

    @Override
    // 停止作业前触发一次 savepoint，
    // 比如用户手动停止作业时指定一个 savepoint 路径，这样就会在停止前触发一次 savepoint
    public CompletableFuture<String> stopWithSavepoint(
            @Nullable final String targetDirectory,
            final SavepointFormatType formatType,
            final boolean terminate,
            final Time timeout) {

        return schedulerNG.stopWithSavepoint(targetDirectory, terminate, formatType);
    }

    @Override
    public void notifyNotEnoughResourcesAvailable(
            Collection<ResourceRequirement> acquiredResources) {
        slotPoolService.notifyNotEnoughResourcesAvailable(acquiredResources);
    }

    @Override
    public CompletableFuture<Object> updateGlobalAggregate(
            String aggregateName, Object aggregand, byte[] serializedAggregateFunction) {

        AggregateFunction aggregateFunction = null;
        try {
            aggregateFunction =
                    InstantiationUtil.deserializeObject(
                            serializedAggregateFunction, userCodeLoader);
        } catch (Exception e) {
            log.error("Error while attempting to deserialize user AggregateFunction.");
            return FutureUtils.completedExceptionally(e);
        }

        Object accumulator = accumulators.get(aggregateName);
        if (null == accumulator) {
            accumulator = aggregateFunction.createAccumulator();
        }
        accumulator = aggregateFunction.add(aggregand, accumulator);
        accumulators.put(aggregateName, accumulator);
        return CompletableFuture.completedFuture(aggregateFunction.getResult(accumulator));
    }

    @Override
    public CompletableFuture<CoordinationResponse> deliverCoordinationRequestToCoordinator(
            OperatorID operatorId,
            SerializedValue<CoordinationRequest> serializedRequest,
            Time timeout) {
        return this.sendRequestToCoordinator(operatorId, serializedRequest);
    }

    @Override
    public CompletableFuture<?> stopTrackingAndReleasePartitions(
            Collection<ResultPartitionID> partitionIds) {
        CompletableFuture<?> future = new CompletableFuture<>();
        try {
            partitionTracker.stopTrackingAndReleasePartitions(partitionIds, false);
            future.complete(null);
        } catch (Throwable throwable) {
            future.completeExceptionally(throwable);
        }
        return future;
    }

    @Override
    public CompletableFuture<Acknowledge> notifyNewBlockedNodes(Collection<BlockedNode> newNodes) {
        blocklistHandler.addNewBlockedNodes(newNodes);
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    // ----------------------------------------------------------------------------------------------
    // Internal methods
    // ----------------------------------------------------------------------------------------------

    // -- job starting and stopping
    // -----------------------------------------------------------------

    private void startJobExecution() throws Exception {
        validateRunsInMainThread();

        JobShuffleContext context = new JobShuffleContextImpl(jobGraph.getJobID(), this);
        shuffleMaster.registerJob(context);

        startJobMasterServices();

        log.info(
                "Starting execution of job '{}' ({}) under job master id {}.",
                jobGraph.getName(),
                jobGraph.getJobID(),
                getFencingToken());
        // 调度作业
        startScheduling();
    }

    private void startJobMasterServices() throws Exception {
        try {
            this.taskManagerHeartbeatManager = createTaskManagerHeartbeatManager(heartbeatServices);
            this.resourceManagerHeartbeatManager =
                    createResourceManagerHeartbeatManager(heartbeatServices);

            // start the slot pool make sure the slot pool now accepts messages for this leader
            slotPoolService.start(getFencingToken(), getAddress(), getMainThreadExecutor());

            // job is ready to go, try to establish connection with resource manager
            //   - activate leader retrieval for the resource manager
            //   - on notification of the leader, the connection will be established and
            //     the slot pool will start requesting slots
            resourceManagerLeaderRetriever.start(new ResourceManagerLeaderListener());
        } catch (Exception e) {
            handleStartJobMasterServicesError(e);
        }
    }

    private void handleStartJobMasterServicesError(Exception e) throws Exception {
        try {
            stopJobMasterServices();
        } catch (Exception inner) {
            e.addSuppressed(inner);
        }

        throw e;
    }

    private void stopJobMasterServices() throws Exception {
        Exception resultingException = null;

        try {
            resourceManagerLeaderRetriever.stop();
        } catch (Exception e) {
            resultingException = e;
        }

        // TODO: Distinguish between job termination which should free all slots and a loss of
        // leadership which should keep the slots
        slotPoolService.close();

        stopHeartbeatServices();

        ExceptionUtils.tryRethrowException(resultingException);
    }

    private CompletableFuture<Void> stopJobExecution(final Exception cause) {
        validateRunsInMainThread();

        final CompletableFuture<Void> terminationFuture = stopScheduling();

        return FutureUtils.runAfterwards(
                terminationFuture,
                () -> {
                    shuffleMaster.unregisterJob(jobGraph.getJobID());
                    disconnectTaskManagerResourceManagerConnections(cause);
                    stopJobMasterServices();
                });
    }

    private void disconnectTaskManagerResourceManagerConnections(Exception cause) {
        // disconnect from all registered TaskExecutors
        final Set<ResourceID> taskManagerResourceIds =
                new HashSet<>(registeredTaskManagers.keySet());

        for (ResourceID taskManagerResourceId : taskManagerResourceIds) {
            disconnectTaskManager(taskManagerResourceId, cause);
        }

        // disconnect from resource manager:
        closeResourceManagerConnection(cause);
    }

    private void stopHeartbeatServices() {
        taskManagerHeartbeatManager.stop();
        resourceManagerHeartbeatManager.stop();
    }

    private void startScheduling() {
        schedulerNG.startScheduling();
    }

    private CompletableFuture<Void> stopScheduling() {
        jobManagerJobMetricGroup.close();
        jobStatusListener.stop();

        return schedulerNG.closeAsync();
    }

    // ----------------------------------------------------------------------------------------------

    private void handleJobMasterError(final Throwable cause) {
        if (ExceptionUtils.isJvmFatalError(cause)) {
            log.error("Fatal error occurred on JobManager.", cause);
            // The fatal error handler implementation should make sure that this call is
            // non-blocking
            fatalErrorHandler.onFatalError(cause);
        } else {
            jobCompletionActions.jobMasterFailed(cause);
        }
    }

    private void jobStatusChanged(final JobStatus newJobStatus) {
        validateRunsInMainThread();
        if (newJobStatus.isGloballyTerminalState()) {
            CompletableFuture<Void> partitionPromoteFuture;
            if (newJobStatus == JobStatus.FINISHED) {
                Collection<ResultPartitionID> jobPartitions =
                        partitionTracker.getAllTrackedNonClusterPartitions().stream()
                                .map(d -> d.getShuffleDescriptor().getResultPartitionID())
                                .collect(Collectors.toList());
                partitionTracker.stopTrackingAndReleasePartitions(jobPartitions);
                Collection<ResultPartitionID> clusterPartitions =
                        partitionTracker.getAllTrackedClusterPartitions().stream()
                                .map(d -> d.getShuffleDescriptor().getResultPartitionID())
                                .collect(Collectors.toList());
                partitionPromoteFuture =
                        partitionTracker.stopTrackingAndPromotePartitions(clusterPartitions);
            } else {
                Collection<ResultPartitionID> allTracked =
                        partitionTracker.getAllTrackedPartitions().stream()
                                .map(d -> d.getShuffleDescriptor().getResultPartitionID())
                                .collect(Collectors.toList());
                partitionTracker.stopTrackingAndReleasePartitions(allTracked);
                partitionPromoteFuture = CompletableFuture.completedFuture(null);
            }

            final ExecutionGraphInfo executionGraphInfo = schedulerNG.requestJob();

            futureExecutor.execute(
                    () -> {
                        try {
                            partitionPromoteFuture.get();
                        } catch (Throwable e) {
                            // We do not want to fail the job in case of partition releasing and
                            // promoting fail. The TaskExecutors will release the partitions
                            // eventually when they find out the JobMaster is closed.
                            log.warn("Fail to release or promote partitions", e);
                        }
                        jobCompletionActions.jobReachedGloballyTerminalState(executionGraphInfo);
                    });
        }
    }

    private void notifyOfNewResourceManagerLeader(
            final String newResourceManagerAddress, final ResourceManagerId resourceManagerId) {
        resourceManagerAddress =
                createResourceManagerAddress(newResourceManagerAddress, resourceManagerId);

        reconnectToResourceManager(
                new FlinkException(
                        String.format(
                                "ResourceManager leader changed to new address %s",
                                resourceManagerAddress)));
    }

    @Nullable
    private ResourceManagerAddress createResourceManagerAddress(
            @Nullable String newResourceManagerAddress,
            @Nullable ResourceManagerId resourceManagerId) {
        if (newResourceManagerAddress != null) {
            // the contract is: address == null <=> id == null
            checkNotNull(resourceManagerId);
            return new ResourceManagerAddress(newResourceManagerAddress, resourceManagerId);
        } else {
            return null;
        }
    }

    private void reconnectToResourceManager(Exception cause) {
        closeResourceManagerConnection(cause);
        tryConnectToResourceManager();
    }

    private void tryConnectToResourceManager() {
        if (resourceManagerAddress != null) {
            connectToResourceManager();
        }
    }

    private void connectToResourceManager() {
        assert (resourceManagerAddress != null);
        assert (resourceManagerConnection == null);
        assert (establishedResourceManagerConnection == null);

        log.info("Connecting to ResourceManager {}", resourceManagerAddress);

        resourceManagerConnection =
                new ResourceManagerConnection(
                        log,
                        jobGraph.getJobID(),
                        resourceId,
                        getAddress(),
                        getFencingToken(),
                        resourceManagerAddress.getAddress(),
                        resourceManagerAddress.getResourceManagerId(),
                        futureExecutor);

        resourceManagerConnection.start();
    }

    private void establishResourceManagerConnection(final JobMasterRegistrationSuccess success) {
        final ResourceManagerId resourceManagerId = success.getResourceManagerId();

        // verify the response with current connection
        if (resourceManagerConnection != null
                && Objects.equals(
                        resourceManagerConnection.getTargetLeaderId(), resourceManagerId)) {

            log.info(
                    "JobManager successfully registered at ResourceManager, leader id: {}.",
                    resourceManagerId);

            final ResourceManagerGateway resourceManagerGateway =
                    resourceManagerConnection.getTargetGateway();

            final ResourceID resourceManagerResourceId = success.getResourceManagerResourceId();

            establishedResourceManagerConnection =
                    new EstablishedResourceManagerConnection(
                            resourceManagerGateway, resourceManagerResourceId);

            blocklistHandler.registerBlocklistListener(resourceManagerGateway);
            slotPoolService.connectToResourceManager(resourceManagerGateway);
            partitionTracker.connectToResourceManager(resourceManagerGateway);

            resourceManagerHeartbeatManager.monitorTarget(
                    resourceManagerResourceId,
                    new ResourceManagerHeartbeatReceiver(resourceManagerGateway));
        } else {
            log.debug(
                    "Ignoring resource manager connection to {} because it's duplicated or outdated.",
                    resourceManagerId);
        }
    }

    private void closeResourceManagerConnection(Exception cause) {
        if (establishedResourceManagerConnection != null) {
            dissolveResourceManagerConnection(establishedResourceManagerConnection, cause);
            establishedResourceManagerConnection = null;
        }

        if (resourceManagerConnection != null) {
            // stop a potentially ongoing registration process
            resourceManagerConnection.close();
            resourceManagerConnection = null;
        }
    }

    private void dissolveResourceManagerConnection(
            EstablishedResourceManagerConnection establishedResourceManagerConnection,
            Exception cause) {
        final ResourceID resourceManagerResourceID =
                establishedResourceManagerConnection.getResourceManagerResourceID();

        if (log.isDebugEnabled()) {
            log.debug(
                    "Close ResourceManager connection {}.",
                    resourceManagerResourceID.getStringWithMetadata(),
                    cause);
        } else {
            log.info(
                    "Close ResourceManager connection {}: {}",
                    resourceManagerResourceID.getStringWithMetadata(),
                    cause.getMessage());
        }

        resourceManagerHeartbeatManager.unmonitorTarget(resourceManagerResourceID);

        ResourceManagerGateway resourceManagerGateway =
                establishedResourceManagerConnection.getResourceManagerGateway();
        resourceManagerGateway.disconnectJobManager(
                jobGraph.getJobID(), schedulerNG.requestJobStatus(), cause);
        blocklistHandler.deregisterBlocklistListener(resourceManagerGateway);
        slotPoolService.disconnectResourceManager();
    }

    private String getNodeIdOfTaskManager(ResourceID taskManagerId) {
        checkState(registeredTaskManagers.containsKey(taskManagerId));
        return registeredTaskManagers.get(taskManagerId).getTaskManagerLocation().getNodeId();
    }

    // ----------------------------------------------------------------------------------------------
    // Service methods
    // ----------------------------------------------------------------------------------------------

    @Override
    public JobMasterGateway getGateway() {
        return getSelfGateway(JobMasterGateway.class);
    }

    // ----------------------------------------------------------------------------------------------
    // Utility classes
    // ----------------------------------------------------------------------------------------------

    private static final class TaskExecutorHeartbeatSender
            extends HeartbeatSender<AllocatedSlotReport> {
        private final TaskExecutorGateway taskExecutorGateway;

        private TaskExecutorHeartbeatSender(TaskExecutorGateway taskExecutorGateway) {
            this.taskExecutorGateway = taskExecutorGateway;
        }

        @Override
        public CompletableFuture<Void> requestHeartbeat(
                ResourceID resourceID, AllocatedSlotReport allocatedSlotReport) {
            return taskExecutorGateway.heartbeatFromJobManager(resourceID, allocatedSlotReport);
        }
    }

    private static final class ResourceManagerHeartbeatReceiver extends HeartbeatReceiver<Void> {
        private final ResourceManagerGateway resourceManagerGateway;

        private ResourceManagerHeartbeatReceiver(ResourceManagerGateway resourceManagerGateway) {
            this.resourceManagerGateway = resourceManagerGateway;
        }

        @Override
        public CompletableFuture<Void> receiveHeartbeat(ResourceID resourceID, Void payload) {
            return resourceManagerGateway.heartbeatFromJobManager(resourceID);
        }
    }

    private class ResourceManagerLeaderListener implements LeaderRetrievalListener {

        @Override
        public void notifyLeaderAddress(final String leaderAddress, final UUID leaderSessionID) {
            runAsync(
                    () ->
                            notifyOfNewResourceManagerLeader(
                                    leaderAddress,
                                    ResourceManagerId.fromUuidOrNull(leaderSessionID)));
        }

        @Override
        public void handleError(final Exception exception) {
            handleJobMasterError(
                    new Exception("Fatal error in the ResourceManager leader service", exception));
        }
    }

    // ----------------------------------------------------------------------------------------------

    private class ResourceManagerConnection
            extends RegisteredRpcConnection<
                    ResourceManagerId,
                    ResourceManagerGateway,
                    JobMasterRegistrationSuccess,
                    RegistrationResponse.Rejection> {
        private final JobID jobID;

        private final ResourceID jobManagerResourceID;

        private final String jobManagerRpcAddress;

        private final JobMasterId jobMasterId;

        ResourceManagerConnection(
                final Logger log,
                final JobID jobID,
                final ResourceID jobManagerResourceID,
                final String jobManagerRpcAddress,
                final JobMasterId jobMasterId,
                final String resourceManagerAddress,
                final ResourceManagerId resourceManagerId,
                final Executor executor) {
            super(log, resourceManagerAddress, resourceManagerId, executor);
            this.jobID = checkNotNull(jobID);
            this.jobManagerResourceID = checkNotNull(jobManagerResourceID);
            this.jobManagerRpcAddress = checkNotNull(jobManagerRpcAddress);
            this.jobMasterId = checkNotNull(jobMasterId);
        }

        @Override
        protected RetryingRegistration<
                        ResourceManagerId,
                        ResourceManagerGateway,
                        JobMasterRegistrationSuccess,
                        RegistrationResponse.Rejection>
                generateRegistration() {
            return new RetryingRegistration<
                    ResourceManagerId,
                    ResourceManagerGateway,
                    JobMasterRegistrationSuccess,
                    RegistrationResponse.Rejection>(
                    log,
                    getRpcService(),
                    "ResourceManager",
                    ResourceManagerGateway.class,
                    getTargetAddress(),
                    getTargetLeaderId(),
                    jobMasterConfiguration.getRetryingRegistrationConfiguration()) {

                @Override
                protected CompletableFuture<RegistrationResponse> invokeRegistration(
                        ResourceManagerGateway gateway,
                        ResourceManagerId fencingToken,
                        long timeoutMillis) {
                    Time timeout = Time.milliseconds(timeoutMillis);

                    return gateway.registerJobMaster(
                            jobMasterId,
                            jobManagerResourceID,
                            jobManagerRpcAddress,
                            jobID,
                            timeout);
                }
            };
        }

        @Override
        protected void onRegistrationSuccess(final JobMasterRegistrationSuccess success) {
            runAsync(
                    () -> {
                        // filter out outdated connections
                        //noinspection ObjectEquality
                        if (this == resourceManagerConnection) {
                            establishResourceManagerConnection(success);
                        }
                    });
        }

        @Override
        protected void onRegistrationRejection(RegistrationResponse.Rejection rejection) {
            handleJobMasterError(
                    new IllegalStateException(
                            "The ResourceManager should never reject a JobMaster registration."));
        }

        @Override
        protected void onRegistrationFailure(final Throwable failure) {
            handleJobMasterError(failure);
        }
    }

    // ----------------------------------------------------------------------------------------------

    private class JobManagerJobStatusListener implements JobStatusListener {

        private volatile boolean running = true;

        @Override
        public void jobStatusChanges(
                final JobID jobId, final JobStatus newJobStatus, final long timestamp) {

            if (running) {
                // run in rpc thread to avoid concurrency
                runAsync(() -> jobStatusChanged(newJobStatus));
            }
        }

        private void stop() {
            running = false;
        }
    }

    private class TaskManagerHeartbeatListener
            implements HeartbeatListener<
                    TaskExecutorToJobManagerHeartbeatPayload, AllocatedSlotReport> {

        @Override
        public void notifyHeartbeatTimeout(ResourceID resourceID) {
            final String message =
                    String.format(
                            "Heartbeat of TaskManager with id %s timed out.",
                            resourceID.getStringWithMetadata());

            log.info(message);
            handleTaskManagerConnectionLoss(resourceID, new TimeoutException(message));
        }

        private void handleTaskManagerConnectionLoss(ResourceID resourceID, Exception cause) {
            validateRunsInMainThread();
            disconnectTaskManager(resourceID, cause);
        }

        @Override
        public void notifyTargetUnreachable(ResourceID resourceID) {
            final String message =
                    String.format(
                            "TaskManager with id %s is no longer reachable.",
                            resourceID.getStringWithMetadata());

            log.info(message);
            handleTaskManagerConnectionLoss(resourceID, new JobMasterException(message));
        }

        @Override
        public void reportPayload(
                ResourceID resourceID, TaskExecutorToJobManagerHeartbeatPayload payload) {
            validateRunsInMainThread();
            executionDeploymentReconciler.reconcileExecutionDeployments(
                    resourceID,
                    payload.getExecutionDeploymentReport(),
                    executionDeploymentTracker.getExecutionsOn(resourceID));
            for (AccumulatorSnapshot snapshot :
                    payload.getAccumulatorReport().getAccumulatorSnapshots()) {
                schedulerNG.updateAccumulators(snapshot);
            }
        }

        @Override
        public AllocatedSlotReport retrievePayload(ResourceID resourceID) {
            validateRunsInMainThread();
            return slotPoolService.createAllocatedSlotReport(resourceID);
        }
    }

    private class ResourceManagerHeartbeatListener implements HeartbeatListener<Void, Void> {

        @Override
        public void notifyHeartbeatTimeout(final ResourceID resourceId) {
            final String message =
                    String.format(
                            "The heartbeat of ResourceManager with id %s timed out.",
                            resourceId.getStringWithMetadata());
            log.info(message);

            handleResourceManagerConnectionLoss(resourceId, new TimeoutException(message));
        }

        private void handleResourceManagerConnectionLoss(ResourceID resourceId, Exception cause) {
            validateRunsInMainThread();
            if (establishedResourceManagerConnection != null
                    && establishedResourceManagerConnection
                            .getResourceManagerResourceID()
                            .equals(resourceId)) {
                reconnectToResourceManager(cause);
            }
        }

        @Override
        public void notifyTargetUnreachable(ResourceID resourceID) {
            final String message =
                    String.format(
                            "ResourceManager with id %s is no longer reachable.",
                            resourceID.getStringWithMetadata());
            log.info(message);

            handleResourceManagerConnectionLoss(resourceID, new JobMasterException(message));
        }

        @Override
        public void reportPayload(ResourceID resourceID, Void payload) {
            // nothing to do since the payload is of type Void
        }

        @Override
        public Void retrievePayload(ResourceID resourceID) {
            return null;
        }
    }

    private class JobMasterBlocklistContext implements BlocklistContext {

        @Override
        public void blockResources(Collection<BlockedNode> blockedNodes) {
            Set<String> blockedNodeIds =
                    blockedNodes.stream().map(BlockedNode::getNodeId).collect(Collectors.toSet());

            Collection<ResourceID> blockedTaskMangers =
                    registeredTaskManagers.keySet().stream()
                            .filter(
                                    taskManagerId ->
                                            blockedNodeIds.contains(
                                                    getNodeIdOfTaskManager(taskManagerId)))
                            .collect(Collectors.toList());

            blockedTaskMangers.forEach(
                    taskManagerId -> {
                        Exception cause =
                                new FlinkRuntimeException(
                                        String.format(
                                                "TaskManager %s is blocked.",
                                                taskManagerId.getStringWithMetadata()));
                        slotPoolService.releaseFreeSlotsOnTaskManager(taskManagerId, cause);
                    });
        }

        @Override
        public void unblockResources(Collection<BlockedNode> unblockedNodes) {}
    }
}
