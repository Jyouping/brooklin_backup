/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.util.List;
import com.linkedin.datastream.common.Datastream;
import java.util.Set;


/**
 * doc
 */
public interface PartitionListener {

  /**
   * doc
   */
  void start(Datastream datastream, java.util.function.Consumer<List<String>> callback);

  /**
   * doc
   */
  void shutdown();

  /**
   * doc
   */
  List<String> getSubscribedPartitions();
}
