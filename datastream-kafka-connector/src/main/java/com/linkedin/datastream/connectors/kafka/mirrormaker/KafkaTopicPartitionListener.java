/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka.mirrormaker;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.connectors.kafka.GroupIdConstructor;
import com.linkedin.datastream.connectors.kafka.KafkaBrokerAddress;
import com.linkedin.datastream.connectors.kafka.KafkaConnectionString;
import com.linkedin.datastream.connectors.kafka.KafkaConsumerFactory;
import com.linkedin.datastream.server.PartitionListener;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.collections.ListUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * doc
 */
public class KafkaTopicPartitionListener extends Thread implements PartitionListener {
  private final Logger _log = LoggerFactory.getLogger(KafkaTopicPartitionListener.class.getName());

  private static final String DEST_CONSUMER_GROUP_ID_SUFFIX = "-topic-partition-listener";
  private static final long FETCH_PARTITION_INTERVAL_MS = 30000;

  private final KafkaConsumerFactory<?, ?> _kafkaConsumerFactory;

  private Properties _consumerProperties;
  private Consumer<?, ?> _consumer;
  private Datastream _datastream;
  private GroupIdConstructor _groupIdConstructor;
  private List<String> _subscribedPartitions = new ArrayList<>();
  private Runnable _callback;
  private KafkaConnectionString _sourceConnection;
  private Pattern _topicPattern;


  /**
   * doc
   */
  public KafkaTopicPartitionListener(KafkaConsumerFactory<?, ?> consumerFactory,
      GroupIdConstructor groupIdConstructor, Properties consumerProperties) {
    _consumerProperties = consumerProperties;
    _kafkaConsumerFactory = consumerFactory;
    _groupIdConstructor = groupIdConstructor;
    _sourceConnection = null;
    _topicPattern = null;
  }

  //TODO: how do you perform an update to a datastream
  @Override
  public void start(Datastream datastream, Runnable callback) {
    _datastream = datastream;
    _sourceConnection = KafkaConnectionString.valueOf(datastream.getSource().getConnectionString());
    String bootstrapValue = String.join(KafkaConnectionString.BROKER_LIST_DELIMITER,
        _sourceConnection.getBrokers().stream().map(KafkaBrokerAddress::toString).collect(Collectors.toList()));
    _consumer = createConsumer(_consumerProperties, bootstrapValue,
        _groupIdConstructor.constructGroupId(_datastream) + DEST_CONSUMER_GROUP_ID_SUFFIX);
    _topicPattern = Pattern.compile(_sourceConnection.getTopicName());
    _callback = callback;
    this.start();
  }

  @Override
  public void shutdown() {
    this.interrupt();
    if (_consumer != null) {
      _consumer.close();
    }
    _consumer = null;
  }

  @Override
  public List<String> getSubscribedPartitions() {
    return _subscribedPartitions;
  }

  private List<String> getPartitionsInfo() {
    Map<String, List<PartitionInfo>> sourceTopics = _consumer.listTopics();
    List<TopicPartition> topicPartitions = sourceTopics.keySet().stream()
        .filter(t1 -> _topicPattern.matcher(t1).matches()).flatMap(t2 ->
            sourceTopics.get(t2).stream().map(partitionInfo ->
            new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))).collect(Collectors.toList());

    return topicPartitions.stream().map(TopicPartition::toString).sorted().collect(Collectors.toList());
  }

  private Consumer<?, ?> createConsumer(Properties consumerProps, String bootstrapServers, String groupId) {
    Properties properties = new Properties();
    properties.putAll(consumerProps);
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        ByteArrayDeserializer.class.getCanonicalName());
    properties.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        ByteArrayDeserializer.class.getCanonicalName());
    return _kafkaConsumerFactory.createConsumer(properties);
  }

  /**
   * doc
   */
  //TODO fault recovery
  public void run() {
    _log.info("Fetch thread for {} started", _datastream.getName());
    while (!isInterrupted()) {
      try {
        // If partition is changed
        List<String> newPartitionInfo = getPartitionsInfo();
        _log.debug("Fetch partition info for {}, oldPartitionInfo: {}, new Partition info: {}"
            , _datastream.getName(), _subscribedPartitions, newPartitionInfo);

        if (!ListUtils.isEqualList(newPartitionInfo, _subscribedPartitions)) {
          _log.info("get updated partition info for {}, oldPartitionInfo: {}, new Partition info: {}"
              , _datastream.getName(), _subscribedPartitions, newPartitionInfo);
          _subscribedPartitions = Collections.synchronizedList(newPartitionInfo);
          _callback.run();
        }
        Thread.sleep(FETCH_PARTITION_INTERVAL_MS);
      } catch (Throwable t) {
        _log.error("detect error for thread " + _datastream.getName() + ", ex: ", t);
      }
    }
  }
}
