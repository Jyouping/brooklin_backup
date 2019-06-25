/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;


/**
 * Partition Listener listens the topic partitions change for all registered datastream groups. It is required to
 * implement this interface if the connector wants to manage the assignment
 */
public interface PartitionListener {

  /**
   * register the consumer function which is triggered when a partition change is detected for
   * a registered datastream.
   *
   * The callback function is a lamda consumer takes the name of datastream group as a consumer. It is expected to
   * be issued in the same thread that detecting the partition changed. Thus we expect this callback function to
   * be finished very quickly
   *
   * @param callback a lamda consumer which takes the name of datastream group
   */
  void onPartitionChange(Consumer<String> callback);

  /**
   * Register a datastream group into the partition listener, partition listener will create and maintain a
   * dedicated listening thread for that datastream group. This needs to be called if the datastream is created/resumed
   */
  void register(DatastreamGroup datastreamGroup);

  /**
   *  deregister a datastream group from partition listener, partition listener will close the topic listening thread
   *  for that datastream group. This needs to be called if the datastream is paused or deleted
   */
  void deregister(String datastreamGroupName);


  /**
   * Get the partitions for a given datastream group. Return Optional.empty() if the datastreamGroup is not found or
   * if the partitions has not yet fetched through the listening thread
   */
  Optional<List<String>> getPartitions(String datastreamGroupName);

  /**
   * Get the list of registered datastream group which has a topic listening thread running
   */
  List<String> getRegisteredDatastreamGroups();

  /**
   * Shut down the partition listener
   */
  void shutdown();
}
