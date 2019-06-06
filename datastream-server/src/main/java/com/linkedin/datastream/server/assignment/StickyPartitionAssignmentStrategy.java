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
 * Partition assignment strategy follow with a Sticky round robin assignment
 * The original assignment suffers a minimal disruption unless there is a huge imbalance
 * At the same time, any new assigned partition will be shuffled so that we can generate
 * a slightly different assignment if there is a need to rebalance all
 * It follows three steps
 * 1) Removed the partitions that no longer need to be unassigned
 * 2) move some partitions from heavily imbalanced task to toAssignPartitions
 * 3) Assign toAssigned according to a round robin assignment
 */
public class StickyPartitionAssignmentStrategy implements PartitionAssignmentStrategy  {
  private static final Logger LOG = LoggerFactory.getLogger(StickyPartitionAssignmentStrategy.class.getName());
  private static final int MAX_ALLOW_INBALANCE_THRESHOLD = 2;

  @Override
  public void assign(List<DatastreamTask> assignedTask, List<String> pendingPartition, List<String> subscribedPartitions) {
    // Assignment logic 1) filter toReassignPartition from subscribedPartitions
    // 2) assign toReassignPartition

    LOG.info("assignment info, task: {}, partitions: {}, subscribedOne: {}",
        assignedTask, pendingPartition, subscribedPartitions);
    List<String> toRevokedPartitions = new ArrayList<>();

    //Step 1: revoke the partitions that no longer can be assigned
    assignedTask.stream().forEach(t -> {
      List<String> toRemovedPartition = t.getPartitionsV2().stream().filter(p -> !subscribedPartitions.contains(p)).collect(
          Collectors.toList());
      t.getPartitionsV2().removeAll(toRemovedPartition);
      toRevokedPartitions.addAll(toRemovedPartition);
    });


    // Step2: drop the partitions that which is no longer subscribed from to Assigned partitions
    List<String> toAssignPartitions = new ArrayList<>(pendingPartition.stream()
        .filter(p -> subscribedPartitions.contains(p)).collect(Collectors.toList()));


    // Step 2: invoke heavily imbalanced partition

    int assignedPartitionSize = assignedTask.stream().map(t -> t.getPartitionsV2().size())
        .mapToInt(Integer::intValue).sum();

    int maxPartitionPerTask = (int) Math.ceil((double) (assignedPartitionSize + toAssignPartitions.size()) / (double) assignedTask.size());

    assignedTask.stream().forEach(task -> {
      while (task.getPartitionsV2().size() > maxPartitionPerTask + MAX_ALLOW_INBALANCE_THRESHOLD) {
        toRevokedPartitions.add(task.getPartitionsV2().remove(task.getPartitionsV2().size() - 1));
      }
    });

    // Step 3: assign the remaining partitions
    Collections.shuffle(toAssignPartitions);

    int i = 0;
    while (toAssignPartitions.size() > 0) {
      List<String> taskOwnedPartitions = assignedTask.get(i % assignedTask.size()).getPartitionsV2();
      if (taskOwnedPartitions.size() < maxPartitionPerTask) {
        taskOwnedPartitions.add(toAssignPartitions.remove(toAssignPartitions.size() - 1));
      }
      ++i;
    }

    if (toRevokedPartitions.size() > 0) {
      LOG.info("To revoke partitions {}", toRevokedPartitions);
    }
    //TODO fix
    //sanityChecks(assignedTask, subscribedPartitions);
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
      throw new DatastreamRuntimeException(String.format("Validation failed after assignment, "
          + "unassigned partition: {}", toCheckPartitions));
    }
  }
}
