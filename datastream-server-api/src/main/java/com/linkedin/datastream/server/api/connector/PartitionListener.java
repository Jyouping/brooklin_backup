/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.api.connector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import com.linkedin.datastream.common.DatastreamPartitionsMetadata;
import com.linkedin.datastream.server.DatastreamGroup;


/**
 * Partition Listener define the interface requires for listening and management the partition changes. It is required to
 * implement this interface if the connector wants to manage the assignment
 */
public interface PartitionListener {

  /**
   * register a consumer function which will be triggered when a partition change is detected within this connector
   *
   * The callback function is a lambda consumer takes the name of datastream group as a consumer. It is expected to
   * be issued in the same thread that detecting the partition changed. Thus we expect this callback function to
   * be finished very quickly
   *
   * @param callback a lamda consumer which takes the name of datastream group
   */
  default void subscribePartitionChange(Consumer<String> callback) {

  }

  /**
   * callback when the datastreamGroups assigned to this connector instance has been changed
   */
  default void onDatastreamChange(List<DatastreamGroup> datastreamGroups) {

  }

  /**
   * Get the partitions for all datastream group. Return Optional.empty() for that datastreamGroup if it has not fetch
   * the partition info yet
   */
  default Map<String, Optional<DatastreamPartitionsMetadata>> getDatastreamPartitions() {
    return new HashMap<String, Optional<DatastreamPartitionsMetadata>>();
  }
}
