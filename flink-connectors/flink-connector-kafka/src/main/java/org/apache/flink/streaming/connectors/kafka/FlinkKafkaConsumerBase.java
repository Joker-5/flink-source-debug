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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.RuntimeContextInitializationContextAdapters;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.config.OffsetCommitMode;
import org.apache.flink.streaming.connectors.kafka.config.OffsetCommitModes;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractPartitionDiscoverer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaCommitCallback;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionAssigner;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionStateSentinel;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicsDescriptor;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPunctuatedWatermarksAdapter;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.SerializedValue;

import org.apache.commons.collections.map.LinkedMap;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import static org.apache.flink.streaming.connectors.kafka.internals.metrics.KafkaConsumerMetricConstants.COMMITS_FAILED_METRICS_COUNTER;
import static org.apache.flink.streaming.connectors.kafka.internals.metrics.KafkaConsumerMetricConstants.COMMITS_SUCCEEDED_METRICS_COUNTER;
import static org.apache.flink.streaming.connectors.kafka.internals.metrics.KafkaConsumerMetricConstants.KAFKA_CONSUMER_METRICS_GROUP;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class of all Flink Kafka Consumer data sources. This implements the common behavior across
 * all Kafka versions.
 *
 * <p>The Kafka version specific behavior is defined mainly in the specific subclasses of the {@link
 * AbstractFetcher}.
 *
 * @param <T> The type of records produced by this data source
 */
@Internal
public abstract class FlinkKafkaConsumerBase<T> extends RichParallelSourceFunction<T>
        implements CheckpointListener, ResultTypeQueryable<T>, CheckpointedFunction {

    private static final long serialVersionUID = -6272159445203409112L;

    protected static final Logger LOG = LoggerFactory.getLogger(FlinkKafkaConsumerBase.class);

    /** The maximum number of pending non-committed checkpoints to track, to avoid memory leaks. */
    public static final int MAX_NUM_PENDING_CHECKPOINTS = 100;

    /**
     * The default interval to execute partition discovery, in milliseconds ({@code Long.MIN_VALUE},
     * i.e. disabled by default).
     */
    public static final long PARTITION_DISCOVERY_DISABLED = Long.MIN_VALUE;

    /** Boolean configuration key to disable metrics tracking. * */
    public static final String KEY_DISABLE_METRICS = "flink.disable-metrics";

    /** Configuration key to define the consumer's partition discovery interval, in milliseconds. */
    public static final String KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS =
            "flink.partition-discovery.interval-millis";

    /** State name of the consumer's partition offset states. */
    private static final String OFFSETS_STATE_NAME = "topic-partition-offset-states";

    // ------------------------------------------------------------------------
    //  configuration state, set on the client relevant for all subtasks
    // ------------------------------------------------------------------------

    /** Describes whether we are discovering partitions for fixed topics or a topic pattern. */
    private final KafkaTopicsDescriptor topicsDescriptor;

    /** The schema to convert between Kafka's byte messages, and Flink's objects. */
    protected final KafkaDeserializationSchema<T> deserializer;

    /**
     * The set of topic partitions that the source will read, with their initial offsets to start
     * reading from.
     */
    // 保存订阅 Topic 的所有 partition 以及初始消费的 offset
    private Map<KafkaTopicPartition, Long> subscribedPartitionsToStartOffsets;

    /**
     * Optional watermark strategy that will be run per Kafka partition, to exploit per-partition
     * timestamp characteristics. The watermark strategy is kept in serialized form, to deserialize
     * it into multiple copies.
     */
    private SerializedValue<WatermarkStrategy<T>> watermarkStrategy;

    /**
     * User-set flag determining whether or not to commit on checkpoints. Note: this flag does not
     * represent the final offset commit mode.
     */
    private boolean enableCommitOnCheckpoints = true;

    /** User-set flag to disable filtering restored partitions with current topics descriptor. */
    private boolean filterRestoredPartitionsWithCurrentTopicsDescriptor = true;

    /**
     * The offset commit mode for the consumer. The value of this can only be determined in {@link
     * FlinkKafkaConsumerBase#open(Configuration)} since it depends on whether or not checkpointing
     * is enabled for the job.
     */
    private OffsetCommitMode offsetCommitMode;

    /** User configured value for discovery interval, in milliseconds. */
    private final long discoveryIntervalMillis;

    /** The startup mode for the consumer (default is {@link StartupMode#GROUP_OFFSETS}). */
    private StartupMode startupMode = StartupMode.GROUP_OFFSETS;

    /**
     * Specific startup offsets; only relevant when startup mode is {@link
     * StartupMode#SPECIFIC_OFFSETS}.
     */
    private Map<KafkaTopicPartition, Long> specificStartupOffsets;

    /**
     * Timestamp to determine startup offsets; only relevant when startup mode is {@link
     * StartupMode#TIMESTAMP}.
     */
    private Long startupOffsetsTimestamp;

    // ------------------------------------------------------------------------
    //  runtime state (used individually by each parallel subtask)
    // ------------------------------------------------------------------------

    /** Data for pending but uncommitted offsets. */
    private final LinkedMap pendingOffsetsToCommit = new LinkedMap();

    /** The fetcher implements the connections to the Kafka brokers. */
    private transient volatile AbstractFetcher<T, ?> kafkaFetcher;

    /** The partition discoverer, used to find new partitions. */
    private transient volatile AbstractPartitionDiscoverer partitionDiscoverer;

    /**
     * The offsets to restore to, if the consumer restores state from a checkpoint.
     *
     * <p>This map will be populated by the {@link #initializeState(FunctionInitializationContext)}
     * method.
     *
     * <p>Using a sorted map as the ordering is important when using restored state to seed the
     * partition discoverer.
     */
    // 如果指定了恢复的 checkpoint 路径，启动时候将会读取 restoredState 变量里面的内容来获取起始 offset，
    // 而不再是使用 StartupMode 中的枚举值作为初始的 offset
    private transient volatile TreeMap<KafkaTopicPartition, Long> restoredState;

    /** Accessor for state in the operator state backend. */
    // 保存了 checkpoint 要持久化存储的内容，例如每个 partition 已经消费的 offset 等信息
    private transient ListState<Tuple2<KafkaTopicPartition, Long>> unionOffsetStates;

    /** Discovery loop, executed in a separate thread. */
    private transient volatile Thread discoveryLoopThread;

    /** Flag indicating whether the consumer is still running. */
    private volatile boolean running = true;

    // ------------------------------------------------------------------------
    //  internal metrics
    // ------------------------------------------------------------------------

    /**
     * Flag indicating whether or not metrics should be exposed. If {@code true}, offset metrics
     * (e.g. current offset, committed offset) and Kafka-shipped metrics will be registered.
     */
    private final boolean useMetrics;

    /** Counter for successful Kafka offset commits. */
    private transient Counter successfulCommits;

    /** Counter for failed Kafka offset commits. */
    private transient Counter failedCommits;

    /**
     * Callback interface that will be invoked upon async Kafka commit completion. Please be aware
     * that default callback implementation in base class does not provide any guarantees on
     * thread-safety. This is sufficient for now because current supported Kafka connectors
     * guarantee no more than 1 concurrent async pending offset commit.
     */
    private transient KafkaCommitCallback offsetCommitCallback;

    // ------------------------------------------------------------------------

    /**
     * Base constructor.
     *
     * @param topics fixed list of topics to subscribe to (null, if using topic pattern)
     * @param topicPattern the topic pattern to subscribe to (null, if using fixed topics)
     * @param deserializer The deserializer to turn raw byte messages into Java/Scala objects.
     * @param discoveryIntervalMillis the topic / partition discovery interval, in milliseconds (0
     *     if discovery is disabled).
     */
    public FlinkKafkaConsumerBase(
            List<String> topics,
            Pattern topicPattern,
            KafkaDeserializationSchema<T> deserializer,
            long discoveryIntervalMillis,
            boolean useMetrics) {
        this.topicsDescriptor = new KafkaTopicsDescriptor(topics, topicPattern);
        this.deserializer = checkNotNull(deserializer, "valueDeserializer");

        checkArgument(
                discoveryIntervalMillis == PARTITION_DISCOVERY_DISABLED
                        || discoveryIntervalMillis >= 0,
                "Cannot define a negative value for the topic / partition discovery interval.");
        this.discoveryIntervalMillis = discoveryIntervalMillis;

        this.useMetrics = useMetrics;
    }

    /**
     * Make sure that auto commit is disabled when our offset commit mode is ON_CHECKPOINTS. This
     * overwrites whatever setting the user configured in the properties.
     *
     * @param properties - Kafka configuration properties to be adjusted
     * @param offsetCommitMode offset commit mode
     */
    protected static void adjustAutoCommitConfig(
            Properties properties, OffsetCommitMode offsetCommitMode) {
        // 确保在 Flink 开启 Checkpoint 时，关闭 Kafka 消费 offset 自动提交的配置
        if (offsetCommitMode == OffsetCommitMode.ON_CHECKPOINTS
                || offsetCommitMode == OffsetCommitMode.DISABLED) {
            properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        }
    }
    // ------------------------------------------------------------------------
    //  Configuration
    // ------------------------------------------------------------------------

    /**
     * Sets the given {@link WatermarkStrategy} on this consumer. These will be used to assign
     * timestamps to records and generates watermarks to signal event time progress.
     *
     * <p>Running timestamp extractors / watermark generators directly inside the Kafka source
     * (which you can do by using this method), per Kafka partition, allows users to let them
     * exploit the per-partition characteristics.
     *
     * <p>When a subtask of a FlinkKafkaConsumer source reads multiple Kafka partitions, the streams
     * from the partitions are unioned in a "first come first serve" fashion. Per-partition
     * characteristics are usually lost that way. For example, if the timestamps are strictly
     * ascending per Kafka partition, they will not be strictly ascending in the resulting Flink
     * DataStream, if the parallel source subtask reads more than one partition.
     *
     * <p>Common watermark generation patterns can be found as static methods in the {@link
     * org.apache.flink.api.common.eventtime.WatermarkStrategy} class.
     *
     * @return The consumer object, to allow function chaining.
     */
    public FlinkKafkaConsumerBase<T> assignTimestampsAndWatermarks(
            WatermarkStrategy<T> watermarkStrategy) {
        checkNotNull(watermarkStrategy);

        try {
            ClosureCleaner.clean(
                    watermarkStrategy, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
            this.watermarkStrategy = new SerializedValue<>(watermarkStrategy);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "The given WatermarkStrategy is not serializable", e);
        }

        return this;
    }

    /**
     * Specifies an {@link AssignerWithPunctuatedWatermarks} to emit watermarks in a punctuated
     * manner. The watermark extractor will run per Kafka partition, watermarks will be merged
     * across partitions in the same way as in the Flink runtime, when streams are merged.
     *
     * <p>When a subtask of a FlinkKafkaConsumer source reads multiple Kafka partitions, the streams
     * from the partitions are unioned in a "first come first serve" fashion. Per-partition
     * characteristics are usually lost that way. For example, if the timestamps are strictly
     * ascending per Kafka partition, they will not be strictly ascending in the resulting Flink
     * DataStream, if the parallel source subtask reads more than one partition.
     *
     * <p>Running timestamp extractors / watermark generators directly inside the Kafka source, per
     * Kafka partition, allows users to let them exploit the per-partition characteristics.
     *
     * <p>Note: One can use either an {@link AssignerWithPunctuatedWatermarks} or an {@link
     * AssignerWithPeriodicWatermarks}, not both at the same time.
     *
     * <p>This method uses the deprecated watermark generator interfaces. Please switch to {@link
     * #assignTimestampsAndWatermarks(WatermarkStrategy)} to use the new interfaces instead. The new
     * interfaces support watermark idleness and no longer need to differentiate between "periodic"
     * and "punctuated" watermarks.
     *
     * @deprecated Please use {@link #assignTimestampsAndWatermarks(WatermarkStrategy)} instead.
     * @param assigner The timestamp assigner / watermark generator to use.
     * @return The consumer object, to allow function chaining.
     */
    @Deprecated
    public FlinkKafkaConsumerBase<T> assignTimestampsAndWatermarks(
            AssignerWithPunctuatedWatermarks<T> assigner) {
        checkNotNull(assigner);

        if (this.watermarkStrategy != null) {
            throw new IllegalStateException("Some watermark strategy has already been set.");
        }

        try {
            ClosureCleaner.clean(assigner, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
            final WatermarkStrategy<T> wms =
                    new AssignerWithPunctuatedWatermarksAdapter.Strategy<>(assigner);

            return assignTimestampsAndWatermarks(wms);
        } catch (Exception e) {
            throw new IllegalArgumentException("The given assigner is not serializable", e);
        }
    }

    /**
     * Specifies an {@link AssignerWithPunctuatedWatermarks} to emit watermarks in a punctuated
     * manner. The watermark extractor will run per Kafka partition, watermarks will be merged
     * across partitions in the same way as in the Flink runtime, when streams are merged.
     *
     * <p>When a subtask of a FlinkKafkaConsumer source reads multiple Kafka partitions, the streams
     * from the partitions are unioned in a "first come first serve" fashion. Per-partition
     * characteristics are usually lost that way. For example, if the timestamps are strictly
     * ascending per Kafka partition, they will not be strictly ascending in the resulting Flink
     * DataStream, if the parallel source subtask reads more that one partition.
     *
     * <p>Running timestamp extractors / watermark generators directly inside the Kafka source, per
     * Kafka partition, allows users to let them exploit the per-partition characteristics.
     *
     * <p>Note: One can use either an {@link AssignerWithPunctuatedWatermarks} or an {@link
     * AssignerWithPeriodicWatermarks}, not both at the same time.
     *
     * <p>This method uses the deprecated watermark generator interfaces. Please switch to {@link
     * #assignTimestampsAndWatermarks(WatermarkStrategy)} to use the new interfaces instead. The new
     * interfaces support watermark idleness and no longer need to differentiate between "periodic"
     * and "punctuated" watermarks.
     *
     * @deprecated Please use {@link #assignTimestampsAndWatermarks(WatermarkStrategy)} instead.
     * @param assigner The timestamp assigner / watermark generator to use.
     * @return The consumer object, to allow function chaining.
     */
    @Deprecated
    public FlinkKafkaConsumerBase<T> assignTimestampsAndWatermarks(
            AssignerWithPeriodicWatermarks<T> assigner) {
        checkNotNull(assigner);

        if (this.watermarkStrategy != null) {
            throw new IllegalStateException("Some watermark strategy has already been set.");
        }

        try {
            ClosureCleaner.clean(assigner, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
            final WatermarkStrategy<T> wms =
                    new AssignerWithPeriodicWatermarksAdapter.Strategy<>(assigner);

            return assignTimestampsAndWatermarks(wms);
        } catch (Exception e) {
            throw new IllegalArgumentException("The given assigner is not serializable", e);
        }
    }

    /**
     * Specifies whether or not the consumer should commit offsets back to Kafka on checkpoints.
     *
     * <p>This setting will only have effect if checkpointing is enabled for the job. If
     * checkpointing isn't enabled, only the "auto.commit.enable" (for 0.8) / "enable.auto.commit"
     * (for 0.9+) property settings will be used.
     *
     * @return The consumer object, to allow function chaining.
     */
    public FlinkKafkaConsumerBase<T> setCommitOffsetsOnCheckpoints(boolean commitOnCheckpoints) {
        this.enableCommitOnCheckpoints = commitOnCheckpoints;
        return this;
    }

    /**
     * Specifies the consumer to start reading from the earliest offset for all partitions. This
     * lets the consumer ignore any committed group offsets in Zookeeper / Kafka brokers.
     *
     * <p>This method does not affect where partitions are read from when the consumer is restored
     * from a checkpoint or savepoint. When the consumer is restored from a checkpoint or savepoint,
     * only the offsets in the restored state will be used.
     *
     * @return The consumer object, to allow function chaining.
     */
    public FlinkKafkaConsumerBase<T> setStartFromEarliest() {
        this.startupMode = StartupMode.EARLIEST;
        this.startupOffsetsTimestamp = null;
        this.specificStartupOffsets = null;
        return this;
    }

    /**
     * Specifies the consumer to start reading from the latest offset for all partitions. This lets
     * the consumer ignore any committed group offsets in Zookeeper / Kafka brokers.
     *
     * <p>This method does not affect where partitions are read from when the consumer is restored
     * from a checkpoint or savepoint. When the consumer is restored from a checkpoint or savepoint,
     * only the offsets in the restored state will be used.
     *
     * @return The consumer object, to allow function chaining.
     */
    public FlinkKafkaConsumerBase<T> setStartFromLatest() {
        this.startupMode = StartupMode.LATEST;
        this.startupOffsetsTimestamp = null;
        this.specificStartupOffsets = null;
        return this;
    }

    /**
     * Specifies the consumer to start reading partitions from a specified timestamp. The specified
     * timestamp must be before the current timestamp. This lets the consumer ignore any committed
     * group offsets in Zookeeper / Kafka brokers.
     *
     * <p>The consumer will look up the earliest offset whose timestamp is greater than or equal to
     * the specific timestamp from Kafka. If there's no such offset, the consumer will use the
     * latest offset to read data from kafka.
     *
     * <p>This method does not affect where partitions are read from when the consumer is restored
     * from a checkpoint or savepoint. When the consumer is restored from a checkpoint or savepoint,
     * only the offsets in the restored state will be used.
     *
     * @param startupOffsetsTimestamp timestamp for the startup offsets, as milliseconds from epoch.
     * @return The consumer object, to allow function chaining.
     */
    public FlinkKafkaConsumerBase<T> setStartFromTimestamp(long startupOffsetsTimestamp) {
        checkArgument(
                startupOffsetsTimestamp >= 0,
                "The provided value for the startup offsets timestamp is invalid.");

        long currentTimestamp = System.currentTimeMillis();
        checkArgument(
                startupOffsetsTimestamp <= currentTimestamp,
                "Startup time[%s] must be before current time[%s].",
                startupOffsetsTimestamp,
                currentTimestamp);

        this.startupMode = StartupMode.TIMESTAMP;
        this.startupOffsetsTimestamp = startupOffsetsTimestamp;
        this.specificStartupOffsets = null;
        return this;
    }

    /**
     * Specifies the consumer to start reading from any committed group offsets found in Zookeeper /
     * Kafka brokers. The "group.id" property must be set in the configuration properties. If no
     * offset can be found for a partition, the behaviour in "auto.offset.reset" set in the
     * configuration properties will be used for the partition.
     *
     * <p>This method does not affect where partitions are read from when the consumer is restored
     * from a checkpoint or savepoint. When the consumer is restored from a checkpoint or savepoint,
     * only the offsets in the restored state will be used.
     *
     * @return The consumer object, to allow function chaining.
     */
    public FlinkKafkaConsumerBase<T> setStartFromGroupOffsets() {
        this.startupMode = StartupMode.GROUP_OFFSETS;
        this.startupOffsetsTimestamp = null;
        this.specificStartupOffsets = null;
        return this;
    }

    /**
     * Specifies the consumer to start reading partitions from specific offsets, set independently
     * for each partition. The specified offset should be the offset of the next record that will be
     * read from partitions. This lets the consumer ignore any committed group offsets in Zookeeper
     * / Kafka brokers.
     *
     * <p>If the provided map of offsets contains entries whose {@link KafkaTopicPartition} is not
     * subscribed by the consumer, the entry will be ignored. If the consumer subscribes to a
     * partition that does not exist in the provided map of offsets, the consumer will fallback to
     * the default group offset behaviour (see {@link
     * FlinkKafkaConsumerBase#setStartFromGroupOffsets()}) for that particular partition.
     *
     * <p>If the specified offset for a partition is invalid, or the behaviour for that partition is
     * defaulted to group offsets but still no group offset could be found for it, then the
     * "auto.offset.reset" behaviour set in the configuration properties will be used for the
     * partition
     *
     * <p>This method does not affect where partitions are read from when the consumer is restored
     * from a checkpoint or savepoint. When the consumer is restored from a checkpoint or savepoint,
     * only the offsets in the restored state will be used.
     *
     * @return The consumer object, to allow function chaining.
     */
    public FlinkKafkaConsumerBase<T> setStartFromSpecificOffsets(
            Map<KafkaTopicPartition, Long> specificStartupOffsets) {
        this.startupMode = StartupMode.SPECIFIC_OFFSETS;
        this.startupOffsetsTimestamp = null;
        this.specificStartupOffsets = checkNotNull(specificStartupOffsets);
        return this;
    }

    /**
     * By default, when restoring from a checkpoint / savepoint, the consumer always ignores
     * restored partitions that are no longer associated with the current specified topics or topic
     * pattern to subscribe to.
     *
     * <p>This method configures the consumer to not filter the restored partitions, therefore
     * always attempting to consume whatever partition was present in the previous execution
     * regardless of the specified topics to subscribe to in the current execution.
     *
     * @return The consumer object, to allow function chaining.
     */
    public FlinkKafkaConsumerBase<T> disableFilterRestoredPartitionsWithSubscribedTopics() {
        this.filterRestoredPartitionsWithCurrentTopicsDescriptor = false;
        return this;
    }

    // ------------------------------------------------------------------------
    //  Work methods
    // ------------------------------------------------------------------------

    @Override
    public void open(Configuration configuration) throws Exception {
        // determine the offset commit mode
        this.offsetCommitMode =
                OffsetCommitModes.fromConfiguration(
                        getIsAutoCommitEnabled(),
                        enableCommitOnCheckpoints,
                        ((StreamingRuntimeContext) getRuntimeContext()).isCheckpointingEnabled());

        // create the partition discoverer
        this.partitionDiscoverer =
                createPartitionDiscoverer(
                        topicsDescriptor,
                        getRuntimeContext().getIndexOfThisSubtask(),
                        getRuntimeContext().getNumberOfParallelSubtasks());
        this.partitionDiscoverer.open();

        subscribedPartitionsToStartOffsets = new HashMap<>();
        final List<KafkaTopicPartition> allPartitions = partitionDiscoverer.discoverPartitions();
        if (restoredState != null) {
            for (KafkaTopicPartition partition : allPartitions) {
                if (!restoredState.containsKey(partition)) {
                    restoredState.put(partition, KafkaTopicPartitionStateSentinel.EARLIEST_OFFSET);
                }
            }

            for (Map.Entry<KafkaTopicPartition, Long> restoredStateEntry :
                    restoredState.entrySet()) {
                // seed the partition discoverer with the union state while filtering out
                // restored partitions that should not be subscribed by this subtask
                if (KafkaTopicPartitionAssigner.assign(
                                restoredStateEntry.getKey(),
                                getRuntimeContext().getNumberOfParallelSubtasks())
                        == getRuntimeContext().getIndexOfThisSubtask()) {
                    subscribedPartitionsToStartOffsets.put(
                            restoredStateEntry.getKey(), restoredStateEntry.getValue());
                }
            }

            if (filterRestoredPartitionsWithCurrentTopicsDescriptor) {
                subscribedPartitionsToStartOffsets
                        .entrySet()
                        .removeIf(
                                entry -> {
                                    if (!topicsDescriptor.isMatchingTopic(
                                            entry.getKey().getTopic())) {
                                        LOG.warn(
                                                "{} is removed from subscribed partitions since it is no longer associated with topics descriptor of current execution.",
                                                entry.getKey());
                                        return true;
                                    }
                                    return false;
                                });
            }

            LOG.info(
                    "Consumer subtask {} will start reading {} partitions with offsets in restored state: {}",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    subscribedPartitionsToStartOffsets.size(),
                    subscribedPartitionsToStartOffsets);
        } else {
            // use the partition discoverer to fetch the initial seed partitions,
            // and set their initial offsets depending on the startup mode.
            // for SPECIFIC_OFFSETS and TIMESTAMP modes, we set the specific offsets now;
            // for other modes (EARLIEST, LATEST, and GROUP_OFFSETS), the offset is lazily
            // determined
            // when the partition is actually read.
            switch (startupMode) {
                case SPECIFIC_OFFSETS:
                    if (specificStartupOffsets == null) {
                        throw new IllegalStateException(
                                "Startup mode for the consumer set to "
                                        + StartupMode.SPECIFIC_OFFSETS
                                        + ", but no specific offsets were specified.");
                    }

                    for (KafkaTopicPartition seedPartition : allPartitions) {
                        Long specificOffset = specificStartupOffsets.get(seedPartition);
                        if (specificOffset != null) {
                            // since the specified offsets represent the next record to read, we
                            // subtract
                            // it by one so that the initial state of the consumer will be correct
                            subscribedPartitionsToStartOffsets.put(
                                    seedPartition, specificOffset - 1);
                        } else {
                            // default to group offset behaviour if the user-provided specific
                            // offsets
                            // do not contain a value for this partition
                            subscribedPartitionsToStartOffsets.put(
                                    seedPartition, KafkaTopicPartitionStateSentinel.GROUP_OFFSET);
                        }
                    }

                    break;
                case TIMESTAMP:
                    if (startupOffsetsTimestamp == null) {
                        throw new IllegalStateException(
                                "Startup mode for the consumer set to "
                                        + StartupMode.TIMESTAMP
                                        + ", but no startup timestamp was specified.");
                    }

                    for (Map.Entry<KafkaTopicPartition, Long> partitionToOffset :
                            fetchOffsetsWithTimestamp(allPartitions, startupOffsetsTimestamp)
                                    .entrySet()) {
                        subscribedPartitionsToStartOffsets.put(
                                partitionToOffset.getKey(),
                                (partitionToOffset.getValue() == null)
                                        // if an offset cannot be retrieved for a partition with the
                                        // given timestamp,
                                        // we default to using the latest offset for the partition
                                        ? KafkaTopicPartitionStateSentinel.LATEST_OFFSET
                                        // since the specified offsets represent the next record to
                                        // read, we subtract
                                        // it by one so that the initial state of the consumer will
                                        // be correct
                                        : partitionToOffset.getValue() - 1);
                    }

                    break;
                default:
                    // 将所有 partition 的起始 offset 初始化为 StartupMode 中的指定值，
                    // 比如 -915623761773L 这个值就表示了当前为 GROUP_OFFSETS 模式。
                    for (KafkaTopicPartition seedPartition : allPartitions) {
                        subscribedPartitionsToStartOffsets.put(
                                seedPartition, startupMode.getStateSentinel());
                    }
            }

            if (!subscribedPartitionsToStartOffsets.isEmpty()) {
                switch (startupMode) {
                    case EARLIEST:
                        LOG.info(
                                "Consumer subtask {} will start reading the following {} partitions from the earliest offsets: {}",
                                getRuntimeContext().getIndexOfThisSubtask(),
                                subscribedPartitionsToStartOffsets.size(),
                                subscribedPartitionsToStartOffsets.keySet());
                        break;
                    case LATEST:
                        LOG.info(
                                "Consumer subtask {} will start reading the following {} partitions from the latest offsets: {}",
                                getRuntimeContext().getIndexOfThisSubtask(),
                                subscribedPartitionsToStartOffsets.size(),
                                subscribedPartitionsToStartOffsets.keySet());
                        break;
                    case TIMESTAMP:
                        LOG.info(
                                "Consumer subtask {} will start reading the following {} partitions from timestamp {}: {}",
                                getRuntimeContext().getIndexOfThisSubtask(),
                                subscribedPartitionsToStartOffsets.size(),
                                startupOffsetsTimestamp,
                                subscribedPartitionsToStartOffsets.keySet());
                        break;
                    case SPECIFIC_OFFSETS:
                        LOG.info(
                                "Consumer subtask {} will start reading the following {} partitions from the specified startup offsets {}: {}",
                                getRuntimeContext().getIndexOfThisSubtask(),
                                subscribedPartitionsToStartOffsets.size(),
                                specificStartupOffsets,
                                subscribedPartitionsToStartOffsets.keySet());

                        List<KafkaTopicPartition> partitionsDefaultedToGroupOffsets =
                                new ArrayList<>(subscribedPartitionsToStartOffsets.size());
                        for (Map.Entry<KafkaTopicPartition, Long> subscribedPartition :
                                subscribedPartitionsToStartOffsets.entrySet()) {
                            if (subscribedPartition.getValue()
                                    == KafkaTopicPartitionStateSentinel.GROUP_OFFSET) {
                                partitionsDefaultedToGroupOffsets.add(subscribedPartition.getKey());
                            }
                        }

                        if (partitionsDefaultedToGroupOffsets.size() > 0) {
                            LOG.warn(
                                    "Consumer subtask {} cannot find offsets for the following {} partitions in the specified startup offsets: {}"
                                            + "; their startup offsets will be defaulted to their committed group offsets in Kafka.",
                                    getRuntimeContext().getIndexOfThisSubtask(),
                                    partitionsDefaultedToGroupOffsets.size(),
                                    partitionsDefaultedToGroupOffsets);
                        }
                        break;
                    case GROUP_OFFSETS:
                        LOG.info(
                                "Consumer subtask {} will start reading the following {} partitions from the committed group offsets in Kafka: {}",
                                getRuntimeContext().getIndexOfThisSubtask(),
                                subscribedPartitionsToStartOffsets.size(),
                                subscribedPartitionsToStartOffsets.keySet());
                }
            } else {
                LOG.info(
                        "Consumer subtask {} initially has no partitions to read from.",
                        getRuntimeContext().getIndexOfThisSubtask());
            }
        }

        this.deserializer.open(
                RuntimeContextInitializationContextAdapters.deserializationAdapter(
                        getRuntimeContext(), metricGroup -> metricGroup.addGroup("user")));
    }

    @Override
    // FlinkKafkaConsumer 中的 run 方法的具体实现
    public void run(SourceContext<T> sourceContext) throws Exception {
        if (subscribedPartitionsToStartOffsets == null) {
            throw new Exception("The partitions were not set for the consumer");
        }

        // initialize commit metrics and default offset callback method
        this.successfulCommits =
                this.getRuntimeContext()
                        .getMetricGroup()
                        .counter(COMMITS_SUCCEEDED_METRICS_COUNTER);
        this.failedCommits =
                this.getRuntimeContext().getMetricGroup().counter(COMMITS_FAILED_METRICS_COUNTER);
        final int subtaskIndex = this.getRuntimeContext().getIndexOfThisSubtask();

        this.offsetCommitCallback =
                new KafkaCommitCallback() {
                    @Override
                    public void onSuccess() {
                        successfulCommits.inc();
                    }

                    @Override
                    public void onException(Throwable cause) {
                        LOG.warn(
                                String.format(
                                        "Consumer subtask %d failed async Kafka commit.",
                                        subtaskIndex),
                                cause);
                        failedCommits.inc();
                    }
                };

        // mark the subtask as temporarily idle if there are no initial seed partitions;
        // once this subtask discovers some partitions and starts collecting records, the subtask's
        // status will automatically be triggered back to be active.
        if (subscribedPartitionsToStartOffsets.isEmpty()) {
            sourceContext.markAsTemporarilyIdle();
        }

        LOG.info(
                "Consumer subtask {} creating fetcher with offsets {}.",
                getRuntimeContext().getIndexOfThisSubtask(),
                subscribedPartitionsToStartOffsets);
        // from this point forward:
        //   - 'snapshotState' will draw offsets from the fetcher,
        //     instead of being built from `subscribedPartitionsToStartOffsets`
        //   - 'notifyCheckpointComplete' will start to do work (i.e. commit offsets to
        //     Kafka through the fetcher, if configured to do so)
        // 创建一个 Fetcher 对象，
        this.kafkaFetcher =
                createFetcher(
                        sourceContext,
                        subscribedPartitionsToStartOffsets,
                        watermarkStrategy,
                        (StreamingRuntimeContext) getRuntimeContext(),
                        offsetCommitMode,
                        getRuntimeContext().getMetricGroup().addGroup(KAFKA_CONSUMER_METRICS_GROUP),
                        useMetrics);

        if (!running) {
            return;
        }

        // depending on whether we were restored with the current state version (1.3),
        // remaining logic branches off into 2 paths:
        //  1) New state - partition discovery loop executed as separate thread, with this
        //                 thread running the main fetcher loop
        //  2) Old state - partition discovery is disabled and only the main fetcher loop is
        // executed
        if (discoveryIntervalMillis == PARTITION_DISCOVERY_DISABLED) {
            // 拉取 Kafka 消息
            kafkaFetcher.runFetchLoop();
        } else {
            runWithPartitionDiscovery();
        }
    }

    private void runWithPartitionDiscovery() throws Exception {
        final AtomicReference<Exception> discoveryLoopErrorRef = new AtomicReference<>();
        createAndStartDiscoveryLoop(discoveryLoopErrorRef);

        kafkaFetcher.runFetchLoop();

        // make sure that the partition discoverer is waked up so that
        // the discoveryLoopThread exits
        partitionDiscoverer.wakeup();
        joinDiscoveryLoopThread();

        // rethrow any fetcher errors
        final Exception discoveryLoopError = discoveryLoopErrorRef.get();
        if (discoveryLoopError != null) {
            throw new RuntimeException(discoveryLoopError);
        }
    }

    @VisibleForTesting
    void joinDiscoveryLoopThread() throws InterruptedException {
        if (discoveryLoopThread != null) {
            discoveryLoopThread.join();
        }
    }

    private void createAndStartDiscoveryLoop(AtomicReference<Exception> discoveryLoopErrorRef) {
        discoveryLoopThread =
                new Thread(
                        () -> {
                            try {
                                // --------------------- partition discovery loop
                                // ---------------------

                                // throughout the loop, we always eagerly check if we are still
                                // running before
                                // performing the next operation, so that we can escape the loop as
                                // soon as possible

                                while (running) {
                                    if (LOG.isDebugEnabled()) {
                                        LOG.debug(
                                                "Consumer subtask {} is trying to discover new partitions ...",
                                                getRuntimeContext().getIndexOfThisSubtask());
                                    }

                                    final List<KafkaTopicPartition> discoveredPartitions;
                                    try {
                                        discoveredPartitions =
                                                partitionDiscoverer.discoverPartitions();
                                    } catch (AbstractPartitionDiscoverer.WakeupException
                                            | AbstractPartitionDiscoverer.ClosedException e) {
                                        // the partition discoverer may have been closed or woken up
                                        // before or during the discovery;
                                        // this would only happen if the consumer was canceled;
                                        // simply escape the loop
                                        break;
                                    }

                                    // no need to add the discovered partitions if we were closed
                                    // during the meantime
                                    if (running && !discoveredPartitions.isEmpty()) {
                                        kafkaFetcher.addDiscoveredPartitions(discoveredPartitions);
                                    }

                                    // do not waste any time sleeping if we're not running anymore
                                    if (running && discoveryIntervalMillis != 0) {
                                        try {
                                            Thread.sleep(discoveryIntervalMillis);
                                        } catch (InterruptedException iex) {
                                            // may be interrupted if the consumer was canceled
                                            // midway; simply escape the loop
                                            break;
                                        }
                                    }
                                }
                            } catch (Exception e) {
                                discoveryLoopErrorRef.set(e);
                            } finally {
                                // calling cancel will also let the fetcher loop escape
                                // (if not running, cancel() was already called)
                                if (running) {
                                    cancel();
                                }
                            }
                        },
                        "Kafka Partition Discovery for "
                                + getRuntimeContext().getTaskNameWithSubtasks());

        discoveryLoopThread.start();
    }

    @Override
    public void cancel() {
        // set ourselves as not running;
        // this would let the main discovery loop escape as soon as possible
        running = false;

        if (discoveryLoopThread != null) {

            if (partitionDiscoverer != null) {
                // we cannot close the discoverer here, as it is error-prone to concurrent access;
                // only wakeup the discoverer, the discovery loop will clean itself up after it
                // escapes
                partitionDiscoverer.wakeup();
            }

            // the discovery loop may currently be sleeping in-between
            // consecutive discoveries; interrupt to shutdown faster
            discoveryLoopThread.interrupt();
        }

        // abort the fetcher, if there is one
        if (kafkaFetcher != null) {
            kafkaFetcher.cancel();
        }
    }

    @Override
    public void close() throws Exception {
        cancel();

        joinDiscoveryLoopThread();

        Exception exception = null;
        if (partitionDiscoverer != null) {
            try {
                partitionDiscoverer.close();
            } catch (Exception e) {
                exception = e;
            }
        }

        try {
            super.close();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        if (exception != null) {
            throw exception;
        }
    }

    // ------------------------------------------------------------------------
    //  Checkpoint and restore
    // ------------------------------------------------------------------------

    @Override
    public final void initializeState(FunctionInitializationContext context) throws Exception {

        OperatorStateStore stateStore = context.getOperatorStateStore();

        this.unionOffsetStates =
                stateStore.getUnionListState(
                        new ListStateDescriptor<>(
                                OFFSETS_STATE_NAME,
                                createStateSerializer(getRuntimeContext().getExecutionConfig())));

        if (context.isRestored()) {
            restoredState = new TreeMap<>(new KafkaTopicPartition.Comparator());

            // populate actual holder for restored state
            for (Tuple2<KafkaTopicPartition, Long> kafkaOffset : unionOffsetStates.get()) {
                restoredState.put(kafkaOffset.f0, kafkaOffset.f1);
            }

            LOG.info(
                    "Consumer subtask {} restored state: {}.",
                    getRuntimeContext().getIndexOfThisSubtask(),
                    restoredState);
        } else {
            LOG.info(
                    "Consumer subtask {} has no restore state.",
                    getRuntimeContext().getIndexOfThisSubtask());
        }
    }

    @Override
    public final void snapshotState(FunctionSnapshotContext context) throws Exception {
        if (!running) {
            LOG.debug("snapshotState() called on closed source");
        } else {
            // 清空状态，这是一个算子状态
            unionOffsetStates.clear();

            final AbstractFetcher<?, ?> fetcher = this.kafkaFetcher;
            if (fetcher == null) {
                // the fetcher has not yet been initialized, which means we need to return the
                // originally restored offsets or the assigned partitions
                for (Map.Entry<KafkaTopicPartition, Long> subscribedPartition :
                        subscribedPartitionsToStartOffsets.entrySet()) {
                    unionOffsetStates.add(
                            Tuple2.of(
                                    subscribedPartition.getKey(), subscribedPartition.getValue()));
                }

                if (offsetCommitMode == OffsetCommitMode.ON_CHECKPOINTS) {
                    // the map cannot be asynchronously updated, because only one checkpoint call
                    // can happen
                    // on this function at a time: either snapshotState() or
                    // notifyCheckpointComplete()
                    pendingOffsetsToCommit.put(context.getCheckpointId(), restoredState);
                }
            } else {
                HashMap<KafkaTopicPartition, Long> currentOffsets = fetcher.snapshotCurrentState();

                if (offsetCommitMode == OffsetCommitMode.ON_CHECKPOINTS) {
                    // the map cannot be asynchronously updated, because only one checkpoint call
                    // can happen
                    // on this function at a time: either snapshotState() or
                    // notifyCheckpointComplete()
                    // 保存要提交的 offset
                    pendingOffsetsToCommit.put(context.getCheckpointId(), currentOffsets);
                }

                for (Map.Entry<KafkaTopicPartition, Long> kafkaTopicPartitionLongEntry :
                        currentOffsets.entrySet()) {
                    // 在状态中保存 offset
                    unionOffsetStates.add(
                            Tuple2.of(
                                    kafkaTopicPartitionLongEntry.getKey(),
                                    kafkaTopicPartitionLongEntry.getValue()));
                }
            }

            if (offsetCommitMode == OffsetCommitMode.ON_CHECKPOINTS) {
                // truncate the map of pending offsets to commit, to prevent infinite growth
                while (pendingOffsetsToCommit.size() > MAX_NUM_PENDING_CHECKPOINTS) {
                    pendingOffsetsToCommit.remove(0);
                }
            }
        }
    }

    @Override
    public final void notifyCheckpointComplete(long checkpointId) throws Exception {
        if (!running) {
            LOG.debug("notifyCheckpointComplete() called on closed source");
            return;
        }

        final AbstractFetcher<?, ?> fetcher = this.kafkaFetcher;
        if (fetcher == null) {
            LOG.debug("notifyCheckpointComplete() called on uninitialized source");
            return;
        }

        if (offsetCommitMode == OffsetCommitMode.ON_CHECKPOINTS) {
            // only one commit operation must be in progress
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Consumer subtask {} committing offsets to Kafka/ZooKeeper for checkpoint {}.",
                        getRuntimeContext().getIndexOfThisSubtask(),
                        checkpointId);
            }

            try {
                final int posInMap = pendingOffsetsToCommit.indexOf(checkpointId);
                if (posInMap == -1) {
                    LOG.warn(
                            "Consumer subtask {} received confirmation for unknown checkpoint id {}",
                            getRuntimeContext().getIndexOfThisSubtask(),
                            checkpointId);
                    return;
                }

                @SuppressWarnings("unchecked")
                Map<KafkaTopicPartition, Long> offsets =
                        (Map<KafkaTopicPartition, Long>) pendingOffsetsToCommit.remove(posInMap);

                // remove older checkpoints in map
                for (int i = 0; i < posInMap; i++) {
                    pendingOffsetsToCommit.remove(0);
                }

                if (offsets == null || offsets.size() == 0) {
                    LOG.debug(
                            "Consumer subtask {} has empty checkpoint state.",
                            getRuntimeContext().getIndexOfThisSubtask());
                    return;
                }

                fetcher.commitInternalOffsetsToKafka(offsets, offsetCommitCallback);
            } catch (Exception e) {
                if (running) {
                    throw e;
                }
                // else ignore exception if we are no longer running
            }
        }
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {}

    // ------------------------------------------------------------------------
    //  Kafka Consumer specific methods
    // ------------------------------------------------------------------------

    /**
     * Creates the fetcher that connect to the Kafka brokers, pulls data, deserialized the data, and
     * emits it into the data streams.
     *
     * @param sourceContext The source context to emit data to.
     * @param subscribedPartitionsToStartOffsets The set of partitions that this subtask should
     *     handle, with their start offsets.
     * @param watermarkStrategy Optional, a serialized WatermarkStrategy.
     * @param runtimeContext The task's runtime context.
     * @return The instantiated fetcher
     * @throws Exception The method should forward exceptions
     */
    protected abstract AbstractFetcher<T, ?> createFetcher(
            SourceContext<T> sourceContext,
            Map<KafkaTopicPartition, Long> subscribedPartitionsToStartOffsets,
            SerializedValue<WatermarkStrategy<T>> watermarkStrategy,
            StreamingRuntimeContext runtimeContext,
            OffsetCommitMode offsetCommitMode,
            MetricGroup kafkaMetricGroup,
            boolean useMetrics)
            throws Exception;

    /**
     * Creates the partition discoverer that is used to find new partitions for this subtask.
     *
     * @param topicsDescriptor Descriptor that describes whether we are discovering partitions for
     *     fixed topics or a topic pattern.
     * @param indexOfThisSubtask The index of this consumer subtask.
     * @param numParallelSubtasks The total number of parallel consumer subtasks.
     * @return The instantiated partition discoverer
     */
    protected abstract AbstractPartitionDiscoverer createPartitionDiscoverer(
            KafkaTopicsDescriptor topicsDescriptor,
            int indexOfThisSubtask,
            int numParallelSubtasks);

    protected abstract boolean getIsAutoCommitEnabled();

    protected abstract Map<KafkaTopicPartition, Long> fetchOffsetsWithTimestamp(
            Collection<KafkaTopicPartition> partitions, long timestamp);

    // ------------------------------------------------------------------------
    //  ResultTypeQueryable methods
    // ------------------------------------------------------------------------

    @Override
    public TypeInformation<T> getProducedType() {
        return deserializer.getProducedType();
    }

    // ------------------------------------------------------------------------
    //  Test utilities
    // ------------------------------------------------------------------------

    @VisibleForTesting
    Map<KafkaTopicPartition, Long> getSubscribedPartitionsToStartOffsets() {
        return subscribedPartitionsToStartOffsets;
    }

    @VisibleForTesting
    TreeMap<KafkaTopicPartition, Long> getRestoredState() {
        return restoredState;
    }

    @VisibleForTesting
    OffsetCommitMode getOffsetCommitMode() {
        return offsetCommitMode;
    }

    @VisibleForTesting
    LinkedMap getPendingOffsetsToCommit() {
        return pendingOffsetsToCommit;
    }

    @VisibleForTesting
    public boolean getEnableCommitOnCheckpoints() {
        return enableCommitOnCheckpoints;
    }

    /**
     * Creates state serializer for kafka topic partition to offset tuple. Using of the explicit
     * state serializer with KryoSerializer is needed because otherwise users cannot use
     * 'disableGenericTypes' properties with KafkaConsumer.
     */
    @VisibleForTesting
    static TupleSerializer<Tuple2<KafkaTopicPartition, Long>> createStateSerializer(
            ExecutionConfig executionConfig) {
        // explicit serializer will keep the compatibility with GenericTypeInformation and allow to
        // disableGenericTypes for users
        TypeSerializer<?>[] fieldSerializers =
                new TypeSerializer<?>[] {
                    new KryoSerializer<>(KafkaTopicPartition.class, executionConfig),
                    LongSerializer.INSTANCE
                };
        @SuppressWarnings("unchecked")
        Class<Tuple2<KafkaTopicPartition, Long>> tupleClass =
                (Class<Tuple2<KafkaTopicPartition, Long>>) (Class<?>) Tuple2.class;
        return new TupleSerializer<>(tupleClass, fieldSerializers);
    }
}
