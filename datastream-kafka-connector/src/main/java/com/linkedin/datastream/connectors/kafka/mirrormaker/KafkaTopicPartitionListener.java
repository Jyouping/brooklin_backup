/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka.mirrormaker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.connectors.kafka.GroupIdConstructor;
import com.linkedin.datastream.connectors.kafka.KafkaBrokerAddress;
import com.linkedin.datastream.connectors.kafka.KafkaConnectionString;
import com.linkedin.datastream.connectors.kafka.KafkaConsumerFactory;
import com.linkedin.datastream.server.DatastreamGroup;
import com.linkedin.datastream.server.PartitionListener;

/**
 * A partition listener to listen change from Kafka
 */
public class KafkaTopicPartitionListener implements PartitionListener {
  private final Logger _log = LoggerFactory.getLogger(KafkaTopicPartitionListener.class.getName());

  private static final String DEST_CONSUMER_GROUP_ID_SUFFIX = "-topic-partition-listener";
  private static final long FETCH_PARTITION_INTERVAL_MS = 30000;

  private final KafkaConsumerFactory<?, ?> _kafkaConsumerFactory;

  private Properties _consumerProperties;
  private GroupIdConstructor _groupIdConstructor;
  private boolean _shutdown;

  private Map<String, PartitionDiscoveryThread> _partitionDiscoveryThreadMap = new HashMap<>();
  private java.util.function.Consumer<String> _partitionChangeCallback;

  /**
   * Constructor for KafkaTopicPartitionListener
   */
  public KafkaTopicPartitionListener(KafkaConsumerFactory<?, ?> consumerFactory,
      GroupIdConstructor groupIdConstructor, Properties consumerProperties) {
    _consumerProperties = consumerProperties;
    _kafkaConsumerFactory = consumerFactory;
    _groupIdConstructor = groupIdConstructor;
    _shutdown = false;
  }

  @Override
  public void onPartitionChange(java.util.function.Consumer<String> callback) {
    _partitionChangeCallback = callback;
  }

  @Override
  public void shutdown() {
    _shutdown = true;
    _partitionDiscoveryThreadMap.values().forEach(Thread::interrupt);
  }

  public Optional<List<String>> getPartitions(String datastreamGroupName) {
    if (_partitionDiscoveryThreadMap.containsKey(datastreamGroupName)) {
      if (_partitionDiscoveryThreadMap.get(datastreamGroupName)._initialized) {
        return Optional.of(Collections.unmodifiableList(_partitionDiscoveryThreadMap.get(datastreamGroupName)._subscribedPartitions));
      }
    }
    return Optional.empty();
  }


  @Override
  public void deregister(String datastreamGroupName) {
    _log.info("attempted to deregister datastream group {}", datastreamGroupName);

    Optional.ofNullable(_partitionDiscoveryThreadMap.remove(datastreamGroupName)).ifPresent(Thread::interrupt);
  }

  public List<String> getRegisteredDatastreamGroups() {
    return new ArrayList<>(_partitionDiscoveryThreadMap.keySet());
  }


  @Override
  public void register(DatastreamGroup datastreamGroup) {
    String datastreamGroupName = datastreamGroup.getTaskPrefix();
    if (_partitionDiscoveryThreadMap.containsKey(datastreamGroupName)) {
      _partitionDiscoveryThreadMap.get(datastreamGroupName).setDatastream(datastreamGroup.getDatastreams().get(0));
    } else {
      PartitionDiscoveryThread partitionDiscoveryThread =
          new PartitionDiscoveryThread(datastreamGroup.getTaskPrefix(), datastreamGroup.getDatastreams().get(0));
      partitionDiscoveryThread.start();
      _partitionDiscoveryThreadMap.put(datastreamGroupName, partitionDiscoveryThread);
        _log.info("PartitionListener for {} registered", datastreamGroupName);
    }
    _log.info("initial subscribed partitions {}", getPartitions(datastreamGroupName));
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

  class PartitionDiscoveryThread extends Thread {
    private Consumer<?, ?> _consumer;
    private Datastream _datastream;
    private String _datastreamGroupName;
    private List<String> _subscribedPartitions = new ArrayList<>();
    private Pattern _topicPattern;
    private boolean _initialized;


    private PartitionDiscoveryThread(String datastreamGroupName, Datastream datastream) {
      _datastream  = datastream;
      _datastreamGroupName = datastreamGroupName;
      _topicPattern = Pattern.compile(
          KafkaConnectionString.valueOf(_datastream.getSource().getConnectionString()).getTopicName());
      _initialized = false;
    }

    public void setDatastream(Datastream datastream) {
      _datastream = datastream;
    }

    private List<String> getPartitionsInfo() {
      Map<String, List<PartitionInfo>> sourceTopics = _consumer.listTopics();
      List<TopicPartition> topicPartitions = sourceTopics.keySet().stream()
          .filter(t1 -> _topicPattern.matcher(t1).matches()).flatMap(t2 ->
              sourceTopics.get(t2).stream().map(partitionInfo ->
                  new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))).collect(Collectors.toList());

      return topicPartitions.stream().map(TopicPartition::toString).sorted().collect(Collectors.toList());
    }

    @Override
    public void run() {
      String bootstrapValue = String.join(KafkaConnectionString.BROKER_LIST_DELIMITER,
          KafkaConnectionString.valueOf(_datastream.getSource().getConnectionString())
              .getBrokers().stream().map(KafkaBrokerAddress::toString).collect(Collectors.toList()));
      _consumer = createConsumer(_consumerProperties, bootstrapValue,
          _groupIdConstructor.constructGroupId(_datastream) + DEST_CONSUMER_GROUP_ID_SUFFIX);

      _log.info("Fetch thread for {} started", _datastream.getName());
      while (!isInterrupted() && !_shutdown) {
        try {
          // If partition is changed
          List<String> newPartitionInfo = getPartitionsInfo();
          _log.info("Fetch partition info for {}, oldPartitionInfo: {}, new Partition info: {}"
              , _datastream.getName(), _subscribedPartitions, newPartitionInfo);

          if (!ListUtils.isEqualList(newPartitionInfo, _subscribedPartitions)) {
            _log.info("get updated partition info for {}, oldPartitionInfo: {}, new Partition info: {}"
                , _datastream.getName(), _subscribedPartitions, newPartitionInfo);

            _subscribedPartitions = Collections.synchronizedList(newPartitionInfo);
            _initialized = true;
            _partitionChangeCallback.accept(_datastreamGroupName);
          }
          Thread.sleep(FETCH_PARTITION_INTERVAL_MS);
        } catch (Throwable t) {
          _log.error("detect error for thread " + _datastream.getName() + ", ex: ", t);
        }
      }
      if (_consumer != null) {
        _consumer.close();
      }
      _consumer = null;
      _log.info("Fetch thread for {} stopped", _datastream.getName());
    }
  }
}
