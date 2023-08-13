/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.util.Preconditions;

import static java.util.Objects.requireNonNull;

/**
 * Default implementation of KeyedStateStore that currently forwards state registration to a {@link
 * RuntimeContext}.
 */
// KeyedStateStore 的默认实现
public class DefaultKeyedStateStore implements KeyedStateStore {

    // DefaultKeyedStateStore 持有 KeyedStateBackend 的引用，
    // 所有的状态获取的方法实际上都由 KeyedStateBackend 来完成
    protected final KeyedStateBackend<?> keyedStateBackend;
    protected final ExecutionConfig executionConfig;

    public DefaultKeyedStateStore(
            KeyedStateBackend<?> keyedStateBackend, ExecutionConfig executionConfig) {
        this.keyedStateBackend = Preconditions.checkNotNull(keyedStateBackend);
        this.executionConfig = Preconditions.checkNotNull(executionConfig);
    }

    @Override
    public <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties) {
        requireNonNull(stateProperties, "The state properties must not be null");
        try {
            stateProperties.initializeSerializerUnlessSet(executionConfig);
            // 获取状态
            return getPartitionedState(stateProperties);
        } catch (Exception e) {
            throw new RuntimeException("Error while getting state", e);
        }
    }

    @Override
    public <T> ListState<T> getListState(ListStateDescriptor<T> stateProperties) {
        requireNonNull(stateProperties, "The state properties must not be null");
        try {
            stateProperties.initializeSerializerUnlessSet(executionConfig);
            ListState<T> originalState = getPartitionedState(stateProperties);
            return new UserFacingListState<>(originalState);
        } catch (Exception e) {
            throw new RuntimeException("Error while getting state", e);
        }
    }

    @Override
    public <T> ReducingState<T> getReducingState(ReducingStateDescriptor<T> stateProperties) {
        requireNonNull(stateProperties, "The state properties must not be null");
        try {
            stateProperties.initializeSerializerUnlessSet(executionConfig);
            return getPartitionedState(stateProperties);
        } catch (Exception e) {
            throw new RuntimeException("Error while getting state", e);
        }
    }

    @Override
    public <IN, ACC, OUT> AggregatingState<IN, OUT> getAggregatingState(
            AggregatingStateDescriptor<IN, ACC, OUT> stateProperties) {
        requireNonNull(stateProperties, "The state properties must not be null");
        try {
            stateProperties.initializeSerializerUnlessSet(executionConfig);
            return getPartitionedState(stateProperties);
        } catch (Exception e) {
            throw new RuntimeException("Error while getting state", e);
        }
    }

    @Override
    public <UK, UV> MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV> stateProperties) {
        requireNonNull(stateProperties, "The state properties must not be null");
        try {
            stateProperties.initializeSerializerUnlessSet(executionConfig);
            MapState<UK, UV> originalState = getPartitionedState(stateProperties);
            return new UserFacingMapState<>(originalState);
        } catch (Exception e) {
            throw new RuntimeException("Error while getting state", e);
        }
    }

    protected <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor)
            throws Exception {
        // 注意方法中前两个和 namespace 有关的参数，
        // 通过引入 namespace，Flink 可以确保在不同的 namespace 下存在相同名称的状态，但它们的值不用相同。
        // 也就是说，状态实际上是和（namespace, name）这两个值相对应的。
        // namespace 的主要应用场景是在窗口中，比如说，假如我需要在窗口中使用状态，这个状态是和具体的窗口相关联的，
        // 假如没有 namespace 的存在，我们要如何获取窗口间互相独立的状态呢？
        // 有了 namespace，把窗口作为 namespace，这个问题自然迎刃而解了。注意，只有无法合并的窗口才可以这样使用，
        // 如果窗口可以合并（如session window），则无法保证 namespace 的不变性，自然不能这样使用。
        return keyedStateBackend.getPartitionedState(
                VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescriptor);
    }
}
