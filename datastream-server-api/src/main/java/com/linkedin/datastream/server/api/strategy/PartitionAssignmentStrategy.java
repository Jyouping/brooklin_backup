/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.api.strategy;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.server.DatastreamGroup;
import com.linkedin.datastream.server.DatastreamTask;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * doc
 * doc
 */
public interface PartitionAssignmentStrategy {
  /**
   * doc
   */
  Map<String, Set<DatastreamTask>> assign(DatastreamGroup datastreamGroup, Map<String, Set<DatastreamTask>> currentAssignment,
      Map<String, Set<String>> suggestAssignment, List<String> subscribedPartitions);
}
