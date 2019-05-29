/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.api.strategy;

import com.linkedin.datastream.server.DatastreamTask;
import java.util.List;


/**
 * doc
 * doc
 */
public interface PartitionAssignmentStrategy {
  /**
   * doc
   */
  void assign(List<DatastreamTask> assignedTask, List<String> pendingPartition, List<String> subscribedPartitions);
}
