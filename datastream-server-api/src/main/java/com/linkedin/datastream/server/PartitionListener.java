/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.util.List;
import java.util.function.BiConsumer;


/**
 * doc
 */
public interface PartitionListener {

  /**
   * doc
   */
  void start(BiConsumer<String, List<String>> changeCallback);

  /**
   * doc
   */
  List<String> getSubscribedPartitions(String datastreamGroupName);

  /**
   * doc
   */
  List<String> popFreshPartitions(String datastreamGroupName);



  void shutdown();

  /**
   *
   */
  void register(DatastreamGroup datastreamGroup);

  /**
   *
   */
  void unregister(String datastreamGroupName);

  /**
   *
   */
  List<String> getRegisteredDatastreamGroups();
}
