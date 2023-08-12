/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.util;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobmaster.slotpool.LocationPreferenceSlotSelectionStrategy;
import org.apache.flink.runtime.jobmaster.slotpool.PreviousAllocationSlotSelectionStrategy;
import org.apache.flink.runtime.jobmaster.slotpool.SlotSelectionStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class for selecting {@link SlotSelectionStrategy}. */
public class SlotSelectionStrategyUtils {

    private static final Logger LOG = LoggerFactory.getLogger(SlotSelectionStrategyUtils.class);

    // 选择 slot 分配策略
    public static SlotSelectionStrategy selectSlotSelectionStrategy(
            final JobType jobType, final Configuration configuration) {
        final boolean evenlySpreadOutSlots =
                configuration.getBoolean(ClusterOptions.EVENLY_SPREAD_OUT_SLOTS_STRATEGY);

        final SlotSelectionStrategy locationPreferenceSlotSelectionStrategy;

        locationPreferenceSlotSelectionStrategy =
                evenlySpreadOutSlots
                        ? LocationPreferenceSlotSelectionStrategy.createEvenlySpreadOut()
                        : LocationPreferenceSlotSelectionStrategy.createDefault();

        // 根据 state.backend.local-recover 配置选择
        final boolean isLocalRecoveryEnabled =
                configuration.getBoolean(CheckpointingOptions.LOCAL_RECOVERY);
        if (isLocalRecoveryEnabled) {
            // 流处理任务
            if (jobType == JobType.STREAMING) {
                // PreviousAllocationSlotSelectionStrategy 策略会根据上次的分配记录，如果这个位置刚好在 slot pool 的可用列表里，就会直接选择这个 slot，
                // 否则就会进入 locationPreferenceSlotSelectionStrategy 的处理逻辑
                return PreviousAllocationSlotSelectionStrategy.create(
                        locationPreferenceSlotSelectionStrategy);
            } else {
                LOG.warn(
                        "Batch job does not support local recovery. Falling back to use "
                                + locationPreferenceSlotSelectionStrategy.getClass());
                return locationPreferenceSlotSelectionStrategy;
            }
        } else {
            // LocationPreferenceSlotSelectionStrategy 策略会对可用的 slot 列表打分，选择分数最高的（分数相同取第一个）
            return locationPreferenceSlotSelectionStrategy;
        }
    }

    /** Private default constructor to avoid being instantiated. */
    private SlotSelectionStrategyUtils() {}
}
