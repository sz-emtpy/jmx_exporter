package io.prometheus.jmx;

import static java.util.logging.Level.SEVERE;

import io.prometheus.jmx.kafka.AbstractKafkaService;
import io.prometheus.jmx.kafka.KafkaClientHolder;
import io.prometheus.jmx.kafka.KafkaConfig;
import io.prometheus.jmx.logger.Logger;
import io.prometheus.jmx.logger.LoggerFactory;
import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaCollector {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaCollector.class);

    JmxCollector.Config config;

    private PrometheusRegistry prometheusRegistry;
    private KafkaClientHolder kafkaClientHolder;

    public KafkaCollector(JmxCollector.Config config, PrometheusRegistry prometheusRegistry) {
        this.config = config;
        this.prometheusRegistry = prometheusRegistry;
        KafkaConfig kafkaConfig =
                new KafkaConfig(
                        config.kafkaBootServer,
                        config.kafkaRequestTimeoutMs,
                        config.kafkaSecurityProtocol,
                        config.kafkaSaslMechanism,
                        config.kafkaSaslJaasConfig,
                        config.kafkaEnableAcl);
        kafkaClientHolder = new KafkaClientHolder(kafkaConfig);
    }

    private Gauge kafka_brokers;
    private Gauge kafka_broker_info;
    private Gauge kafka_topic_partitions;
    private Gauge kafka_topic_partition_current_offset;
    private Gauge kafka_topic_partition_oldest_offset;
    private Gauge kafka_topic_partition_in_sync_replica;
    private Gauge kafka_topic_partition_leader;
    private Gauge kafka_topic_partition_leader_is_preferred;
    private Gauge kafka_topic_partition_replicas;
    private Gauge kafka_topic_partition_under_replicated_partition;
    private Gauge kafka_consumergroup_current_offset;
    private Gauge kafka_consumergroup_lag;
    private Gauge kafka_consumergroupzookeeper_lag_zookeeper;

    public void register() {
        kafka_broker_info =
                Gauge.builder()
                        .name("kafka_broker_labels")
                        .labelNames("clusterId", "brokerId", "host", "port")
                        .help("Information about the Kafka Broker.")
                        .register(prometheusRegistry);

        kafka_brokers =
                Gauge.builder()
                        .name("kafka_brokers")
                        .help("Number of Brokers in the Kafka Cluster.")
                        .register(prometheusRegistry);

        kafka_topic_partitions =
                Gauge.builder()
                        .name("kafka_topic_partitions")
                        .labelNames("topic")
                        .help("Number of partitions for this Topic")
                        .register(prometheusRegistry);

        kafka_topic_partition_current_offset =
                Gauge.builder()
                        .name("kafka_topic_partition_current_offset")
                        .labelNames("topic", "partition")
                        .help("Current Offset of a Broker at Topic/Partition")
                        .register(prometheusRegistry);

        kafka_topic_partition_oldest_offset =
                Gauge.builder()
                        .name("kafka_topic_partition_oldest_offset")
                        .labelNames("topic", "partition")
                        .help("Oldest Offset of a Broker at Topic/Partition")
                        .register(prometheusRegistry);

        kafka_topic_partition_in_sync_replica =
                Gauge.builder()
                        .name("kafka_topic_partition_in_sync_replica")
                        .labelNames("topic", "partition")
                        .help("Number of In-Sync Replicas for this Topic/Partition")
                        .register(prometheusRegistry);

        kafka_topic_partition_leader =
                Gauge.builder()
                        .name("kafka_topic_partition_leader")
                        .labelNames("topic", "partition")
                        .help("Leader Broker ID of this Topic/Partition")
                        .register(prometheusRegistry);

        kafka_topic_partition_leader_is_preferred =
                Gauge.builder()
                        .name("kafka_topic_partition_leader_is_preferred")
                        .labelNames("topic", "partition")
                        .help("1 if Topic/Partition is using the Preferred Broker")
                        .register(prometheusRegistry);

        kafka_topic_partition_replicas =
                Gauge.builder()
                        .name("kafka_topic_partition_replicas")
                        .labelNames("topic", "partition")
                        .help("Number of Replicas for this Topic/Partition")
                        .register(prometheusRegistry);

        kafka_topic_partition_under_replicated_partition =
                Gauge.builder()
                        .name("kafka_topic_partition_under_replicated_partition")
                        .labelNames("topic", "partition")
                        .help("1 if Topic/Partition is under Replicated")
                        .register(prometheusRegistry);

        kafka_consumergroup_current_offset =
                Gauge.builder()
                        .name("kafka_consumergroup_current_offset")
                        .labelNames("topic", "partition", "consumergroup")
                        .help("Current Offset of a ConsumerGroup at Topic/Partition")
                        .register(prometheusRegistry);

        kafka_consumergroup_lag =
                Gauge.builder()
                        .name("kafka_consumergroup_lag")
                        .labelNames("topic", "partition", "consumergroup")
                        .help("Current Approximate Lag of a ConsumerGroup at Topic/Partition")
                        .register(prometheusRegistry);

        kafka_consumergroupzookeeper_lag_zookeeper =
                Gauge.builder()
                        .name("kafka_consumergroupzookeeper_lag_zookeeper")
                        .help(
                                "Current Approximate Lag(zookeeper) of a ConsumerGroup at"
                                        + " Topic/Partition")
                        .register(prometheusRegistry);
    }

    public KafkaCollector collect() {
        try {
            // 查询集群信息
            DescribeClusterResult clusterInfo =
                    kafkaClientHolder.getAdminClient().describeCluster();
            kafka_brokers.set(clusterInfo.nodes().get().size());
            // 获取 broker 节点信息
            for (Node broker : clusterInfo.nodes().get()) {
                kafka_broker_info
                        .labelValues(
                                clusterInfo.clusterId().get(),
                                String.valueOf(broker.id()),
                                broker.host(),
                                String.valueOf(broker.port()))
                        .set(1);
            }
        } catch (ExecutionException | InterruptedException e) {

        }

        try {
            Set<String> strings =
                    kafkaClientHolder
                            .getAdminClient()
                            .listTopics(kafkaClientHolder.timeoutMs(new ListTopicsOptions()))
                            .names()
                            .get();
            for (String topic : strings) {
                Map<String, TopicDescription> topicDescriptions =
                        kafkaClientHolder
                                .getAdminClient()
                                .describeTopics(java.util.Collections.singletonList(topic))
                                .all()
                                .get();
                // 遍历并打印 topic 分区信息
                for (Map.Entry<String, TopicDescription> entry : topicDescriptions.entrySet()) {
                    //                    System.out.println("Topic: " + entry.getKey());
                    kafka_topic_partitions
                            .labelValues(entry.getKey())
                            .set(entry.getValue().partitions().size());
                    for (TopicPartitionInfo partitionInfo : entry.getValue().partitions()) {
                        //                        System.out.println("Partition: " +
                        // partitionInfo.partition());
                        //                        System.out.println("Leader: " +
                        // partitionInfo.leader());
                        //                        System.out.println("Replicas: " +
                        // partitionInfo.replicas());
                        //                        System.out.println("ISR: " + partitionInfo.isr());
                        kafka_topic_partition_leader
                                .labelValues(
                                        entry.getKey(), String.valueOf(partitionInfo.partition()))
                                .set(partitionInfo.leader().id());
                        kafka_topic_partition_replicas
                                .labelValues(
                                        entry.getKey(), String.valueOf(partitionInfo.partition()))
                                .set(partitionInfo.replicas().size());
                        kafka_topic_partition_in_sync_replica
                                .labelValues(
                                        entry.getKey(), String.valueOf(partitionInfo.partition()))
                                .set(partitionInfo.isr().size());

                        if (partitionInfo.replicas() != null
                                && partitionInfo.replicas().size() > 0
                                && partitionInfo.leader().id()
                                        == partitionInfo.replicas().get(0).id()) {
                            kafka_topic_partition_leader_is_preferred
                                    .labelValues(
                                            entry.getKey(),
                                            String.valueOf(partitionInfo.partition()))
                                    .set(1);
                        } else {
                            kafka_topic_partition_leader_is_preferred
                                    .labelValues(
                                            entry.getKey(),
                                            String.valueOf(partitionInfo.partition()))
                                    .set(0);
                        }

                        if (partitionInfo.replicas() != null
                                && partitionInfo.isr() != null
                                && partitionInfo.isr().size() < partitionInfo.replicas().size()) {
                            kafka_topic_partition_under_replicated_partition
                                    .labelValues(
                                            entry.getKey(),
                                            String.valueOf(partitionInfo.partition()))
                                    .set(1);
                        } else {
                            kafka_topic_partition_under_replicated_partition
                                    .labelValues(
                                            entry.getKey(),
                                            String.valueOf(partitionInfo.partition()))
                                    .set(0);
                        }
                    }
                }
            }
        } catch (ExecutionException | InterruptedException e) {

        }

        Map<TopicPartition, Long> beginOffsetMap = getBeginOffset(null);
        for (Map.Entry<TopicPartition, Long> entry : beginOffsetMap.entrySet()) {
            kafka_topic_partition_oldest_offset
                    .labelValues(entry.getKey().topic(), String.valueOf(entry.getKey().partition()))
                    .set(entry.getValue());
        }

        Map<TopicPartition, Long> endOffsetMap = getEndOffset(null);

        for (Map.Entry<TopicPartition, Long> entry : endOffsetMap.entrySet()) {
            kafka_topic_partition_current_offset
                    .labelValues(entry.getKey().topic(), String.valueOf(entry.getKey().partition()))
                    .set(entry.getValue());
        }

        for (String groupId : getGroupList()) {
            Map<TopicPartition, Long> committedOffsetMap = getCommittedOffset(groupId);
            committedOffsetMap.forEach(
                    (topicPartition, committedOffset) -> {
                        long endOffset = endOffsetMap.get(topicPartition);
                        long lag = endOffset - committedOffset;
                        // 上报消费积压信息
                        kafka_consumergroup_current_offset
                                .labelValues(
                                        topicPartition.topic(),
                                        String.valueOf(topicPartition.partition()),
                                        groupId)
                                .set(Double.valueOf(committedOffset));
                        // 上报消费位点，采集消费位点是为了变相的计算消费端的tps，因为目前还不能直接获取消费端的消费tps相关指标
                        // 所以通过间接的方式，计算消费位点的平均增长速率来估算消费端的消费tps，如果出现消费tps猛增的话（可能是积压太多，突然消费，如：出现读取历史数据），可以考虑预警，作相关处理
                        // 不使用rate等增长速率计算方式，因为无法得到负值。取平均估算，如果出现负值，可以认为出现了位点重置操作
                        // 消费位点不能使用Counter类型，因为可以重置，所以不能保证一定是时刻增长
                        kafka_consumergroup_lag
                                .labelValues(
                                        topicPartition.topic(),
                                        String.valueOf(topicPartition.partition()),
                                        groupId)
                                .set(Double.valueOf(lag));
                    });
        }
        return null;
    }

    public List<String> getGroupList() {
        ListConsumerGroupsResult result = kafkaClientHolder.getAdminClient().listConsumerGroups();
        try {
            return result.all().get().stream()
                    .map(ConsumerGroupListing::groupId)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            LOGGER.log(SEVERE, "getGroupList error", e);
            return Collections.EMPTY_LIST;
        }
    }

    public Map<TopicPartition, Long> getConsumerLag(String groupId) {
        ListConsumerGroupOffsetsResult consumerGroupOffsets =
                kafkaClientHolder.getAdminClient().listConsumerGroupOffsets(groupId);
        try {
            Map<TopicPartition, OffsetAndMetadata> consumeOffsetMap =
                    consumerGroupOffsets.partitionsToOffsetAndMetadata().get(3, TimeUnit.SECONDS);

            Properties props = kafkaClientHolder.getProperties();
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            props.put(
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());
            props.put(
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());

            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props); ) {
                Map<TopicPartition, Long> endOffsetMap =
                        consumer.endOffsets(consumeOffsetMap.keySet());

                Map<TopicPartition, Long> result = new HashMap<>();
                consumeOffsetMap.forEach(
                        (k, v) -> {
                            if (endOffsetMap.containsKey(k)) {
                                result.put(k, endOffsetMap.get(k) - v.offset());
                            }
                        });

                return result;
            }

        } catch (Exception e) {
            LOGGER.log(SEVERE, "getConsumerLag error", e);
        }

        return Collections.emptyMap();
    }

    public Map<TopicPartition, Long> getCommittedOffset(String groupId) {
        Map<TopicPartition, Long> res = new HashMap<>();

        try {
            Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap =
                    kafkaClientHolder
                            .getAdminClient()
                            .listConsumerGroupOffsets(
                                    groupId,
                                    kafkaClientHolder.timeoutMs(
                                            new ListConsumerGroupOffsetsOptions()))
                            .partitionsToOffsetAndMetadata()
                            .get();
            offsetAndMetadataMap.forEach((t, o) -> res.put(t, o.offset()));
        } catch (Exception e) {
            LOGGER.log(SEVERE, "listConsumerGroupOffsets error", e);
            return Collections.emptyMap();
        }

        return res;
    }

    // topic
    public List<TopicPartition> getTopicPartitionList(Collection<String> topics) {
        Collection<String> searchTopics = new HashSet<>();
        if (CollectionUtils.isEmpty(topics)) {
            try {
                Set<String> strings =
                        kafkaClientHolder
                                .getAdminClient()
                                .listTopics(kafkaClientHolder.timeoutMs(new ListTopicsOptions()))
                                .names()
                                .get();
                searchTopics.addAll(strings);
            } catch (Exception e) {
                LOGGER.log(SEVERE, "listTopics error.", e);
                return Collections.emptyList();
            }
        } else {
            searchTopics.addAll(topics);
        }

        try {
            Map<String, TopicDescription> topicDescriptionMap =
                    kafkaClientHolder
                            .getAdminClient()
                            .describeTopics(
                                    searchTopics,
                                    kafkaClientHolder.timeoutMs(new DescribeTopicsOptions()))
                            .all()
                            .get();
            return topicDescriptionMap.values().stream()
                    .flatMap(
                            description ->
                                    description.partitions().stream()
                                            .map(
                                                    p ->
                                                            new TopicPartition(
                                                                    description.name(),
                                                                    p.partition())))
                    .collect(Collectors.toList());
        } catch (Exception e) {
            LOGGER.log(SEVERE, "describeTopics error.", e);
            return Collections.emptyList();
        }
    }

    public Map<TopicPartition, Long> getEndOffset(Collection<TopicPartition> topicPartitions) {

        if (CollectionUtils.isEmpty(topicPartitions)) {
            topicPartitions = getTopicPartitionList(null);
        }

        Properties props = kafkaClientHolder.getProperties();
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, AbstractKafkaService.INNER_CONSUMER);

        try (KafkaConsumer consumer =
                new KafkaConsumer(
                        props, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
            return consumer.endOffsets(topicPartitions);
        }
    }

    public Map<TopicPartition, Long> getBeginOffset(Collection<TopicPartition> topicPartitions) {

        if (CollectionUtils.isEmpty(topicPartitions)) {
            topicPartitions = getTopicPartitionList(null);
        }

        Properties props = kafkaClientHolder.getProperties();
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, AbstractKafkaService.INNER_CONSUMER);

        try (KafkaConsumer consumer =
                new KafkaConsumer(
                        props, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
            return consumer.beginningOffsets(topicPartitions);
        }
    }
}
