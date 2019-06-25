/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka.mirrormaker;

import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.connectors.kafka.GroupIdConstructor;
import com.linkedin.datastream.connectors.kafka.KafkaConsumerFactoryImpl;
import com.linkedin.datastream.server.PartitionListenerFactory;
import com.linkedin.datastream.server.PartitionListener;
import java.util.Properties;

/**
 * A Partition listener implementation for Kafka based connector
 */
public class KafkaTopicPartitionListenerFactory implements PartitionListenerFactory {
  public static final String DOMAIN_KAFKA_CONSUMER = "consumer";

  /**
   * create a Kafka partition listener instance
   */
  public PartitionListener createPartitionListener(String clusterName, Properties config) {
    GroupIdConstructor groupIdConstructor = new KafkaMirrorMakerGroupIdConstructor(false, clusterName);
    VerifiableProperties verifiableProperties = new VerifiableProperties(config);
    Properties consumerProperties = verifiableProperties.getDomainProperties(DOMAIN_KAFKA_CONSUMER);
    return new KafkaTopicPartitionListener(new KafkaConsumerFactoryImpl(), groupIdConstructor, consumerProperties);
  }
}
