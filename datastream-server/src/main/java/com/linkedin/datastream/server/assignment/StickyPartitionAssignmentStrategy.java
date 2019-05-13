/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.assignment;

import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.api.strategy.PartitionAssignmentStrategy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 */
public class StickyPartitionAssignmentStrategy implements PartitionAssignmentStrategy  {
  private static final Logger LOG = LoggerFactory.getLogger(StickyPartitionAssignmentStrategy.class.getName());

  @Override
  public void assign(List<DatastreamTask> assignedTask, List<String> partitions) {
    //TODO examine the change in assignedTask during leader partition assignment

    int maxPartitionPerTask = (int) Math.ceil((double) partitions.size() / (double) assignedTask.size());
    // STEP1: drop the removed partition, and put the non-existing partition in assignedPartition
    List<String> toAssignPartitions = new ArrayList<>(partitions);

    assignedTask.stream().forEach(t -> {
      List<String> toRemovedPartition = t.getPartitionsV2().stream().filter(p -> !partitions.contains(p)).collect(
          Collectors.toList());
      t.getPartitionsV2().removeAll(toRemovedPartition);
      toAssignPartitions.removeAll(t.getPartitionsV2());
    });

    Collections.sort(toAssignPartitions);
    // Step2: assign the remaining partitions
    int i = 0;
    while (toAssignPartitions.size() > 0) {
      List<String> taskOwnedPartitions = assignedTask.get(i % assignedTask.size()).getPartitionsV2();
      if (taskOwnedPartitions.size() < maxPartitionPerTask) {
        taskOwnedPartitions.add(toAssignPartitions.remove(toAssignPartitions.size() - 1));
      }
      ++i;
    }

    //TODO: check if there is an imbalance and move the imbalance paritions around
    sanityChecks(assignedTask, partitions);
  }

  private void sanityChecks(List<DatastreamTask> assignedTask, List<String> partitions) {
    int total = 0;
    List<String> toCheckPartitions = new ArrayList<>(partitions);

    for (DatastreamTask task : assignedTask) {
      total += task.getPartitionsV2().size();
      toCheckPartitions.removeAll(task.getPartitionsV2());
    }
    if (total != partitions.size()) {
      throw new DatastreamRuntimeException(String.format("Validation failed after assignment, assigned partitions "
          + "size: {} is not equal to new partitions size: {}", total, partitions.size()));
    }
    if (toCheckPartitions.size() > 0) {
      throw new DatastreamRuntimeException(String.format("Validation failed after assignment, unassigned partition: {}", toCheckPartitions));
    }
  }
}
