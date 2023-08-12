/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.AllocatedSlotReport;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Optional;

/** Service used by the {@link JobMaster} to manage a slot pool. */
// 1、SlotPoolService 用于为 JM 管理 slot pool
// 2、slot pool 为当前作业的 slot 请求而服务，它会向 RM 请求 slot 资源
// 3、slot pool 会维护请求到的 slot 列表信息，即使 RM 挂掉了，也可以使用当前作业空闲的 slot 资源进行分配
// 4、而如果一个 slot 不再使用的话，即使作业在运行，也是可以释放掉的（所有的 slot 都是通过 AllocationID 来区分的）
public interface SlotPoolService extends AutoCloseable {

    /**
     * Tries to cast this slot pool service into the given clazz.
     *
     * @param clazz to cast the slot pool service into
     * @param <T> type of clazz
     * @return {@link Optional#of} the target type if it can be cast; otherwise {@link
     *     Optional#empty()}
     */
    default <T> Optional<T> castInto(Class<T> clazz) {
        if (clazz.isAssignableFrom(this.getClass())) {
            return Optional.of(clazz.cast(this));
        } else {
            return Optional.empty();
        }
    }

    /**
     * Start the encapsulated slot pool implementation.
     *
     * @param jobMasterId jobMasterId to start the service with
     * @param address address of the owner
     * @param mainThreadExecutor mainThreadExecutor to run actions in the main thread
     * @throws Exception if the service cannot be started
     */
    void start(
            JobMasterId jobMasterId, String address, ComponentMainThreadExecutor mainThreadExecutor)
            throws Exception;

    /** Close the slot pool service. */
    void close();

    /**
     * Offers multiple slots to the {@link SlotPoolService}. The slot offerings can be individually
     * accepted or rejected by returning the collection of accepted slot offers.
     *
     * @param taskManagerLocation from which the slot offers originate
     * @param taskManagerGateway to talk to the slot offerer
     * @param offers slot offers which are offered to the {@link SlotPoolService}
     * @return A collection of accepted slot offers. The remaining slot offers are implicitly
     *     rejected.
     */
    Collection<SlotOffer> offerSlots(
            TaskManagerLocation taskManagerLocation,
            TaskManagerGateway taskManagerGateway,
            Collection<SlotOffer> offers);

    /**
     * Fails the allocation with the given allocationId.
     *
     * @param taskManagerId taskManagerId is non-null if the signal comes from a TaskManager; if the
     *     signal comes from the ResourceManager, then it is null
     * @param allocationId allocationId identifies which allocation to fail
     * @param cause cause why the allocation failed
     * @return Optional task executor if it has no more slots registered
     */
    // 分配失败，并释放相应的 slot，这种情况可能是请求超时由 JM 触发或者 TM 分配失败
    Optional<ResourceID> failAllocation(
            @Nullable ResourceID taskManagerId, AllocationID allocationId, Exception cause);

    /**
     * Registers a TaskExecutor with the given {@link ResourceID} at {@link SlotPoolService}.
     *
     * @param taskManagerId identifying the TaskExecutor to register
     * @return true iff a new resource id was registered
     */
    // 注册 TM，此处会记录注册过来的 TM，只能向注册过的 TM 分配 slot
    boolean registerTaskManager(ResourceID taskManagerId);

    /**
     * Releases a TaskExecutor with the given {@link ResourceID} from the {@link SlotPoolService}.
     *
     * @param taskManagerId identifying the TaskExecutor which shall be released from the SlotPool
     * @param cause for the releasing of the TaskManager
     * @return true iff a given registered resource id was removed
     */
    // 注销 TM，该 TM 相关的 slot 都会被释放，task 会被取消，slot pool 会通知相应 TM 释放掉 slot
    boolean releaseTaskManager(ResourceID taskManagerId, Exception cause);

    /**
     * Releases all free slots belonging to the owning TaskExecutor if it has been registered.
     *
     * @param taskManagerId identifying the TaskExecutor
     * @param cause cause for failing the slots
     */
    void releaseFreeSlotsOnTaskManager(ResourceID taskManagerId, Exception cause);

    /**
     * Connects the SlotPool to the given ResourceManager. After this method is called, the SlotPool
     * will be able to request resources from the given ResourceManager.
     *
     * @param resourceManagerGateway The RPC gateway for the resource manager.
     */
    // slot pool 与 RM 建立连接，之后 slot pool 就可以向 RM 请求 slot 资源了
    void connectToResourceManager(ResourceManagerGateway resourceManagerGateway);

    /**
     * Disconnects the slot pool from its current Resource Manager. After this call, the pool will
     * not be able to request further slots from the Resource Manager, and all currently pending
     * requests to the resource manager will be canceled.
     *
     * <p>The slot pool will still be able to serve slots from its internal pool.
     */
    void disconnectResourceManager();

    /**
     * Create report about the allocated slots belonging to the specified task manager.
     *
     * @param taskManagerId identifies the task manager
     * @return the allocated slots on the task manager
     */
    // 汇报指定 TM 上的 slot 分配情况
    AllocatedSlotReport createAllocatedSlotReport(ResourceID taskManagerId);

    /**
     * Notifies that not enough resources are available to fulfill the resource requirements.
     *
     * @param acquiredResources the resources that have been acquired
     */
    default void notifyNotEnoughResourcesAvailable(
            Collection<ResourceRequirement> acquiredResources) {}
}
