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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 */
public class StickyPartitionAssignmentStrategy implements PartitionAssignmentStrategy  {
  private static final Logger LOG = LoggerFactory.getLogger(StickyPartitionAssignmentStrategy.class.getName());
  private static final int MAX_ALLOW_INBALANCE_THRESHOLD = 2;

  @Override
  public void assign(List<DatastreamTask> assignedTask, List<String> pendingPartition, List<String> freshPartitions,
      List<String> subscribedPartitions) {
    // Assignment logic 1) filter toReassignPartition from subscribedPartitions
    // 2) assign toReassignPartition

    LOG.info("assignment info, task: {}, pendinPartitions: {}, freshPartitions: {}, subscribedOne: {}",
        assignedTask, pendingPartition, freshPartitions, subscribedPartitions);

    List<String> previousSubscribedPartitions = new ArrayList<>();
    //Step 1: revoke the partitions which is removed from subscribed partition
    assignedTask.stream().forEach(t -> {
      List<String> toRemovedPartition = t.getPartitionsV2().stream().filter(p -> !subscribedPartitions.contains(p)).collect(
          Collectors.toList());
      if (!toRemovedPartition.isEmpty()) {
        t.revokePartitions(toRemovedPartition);
        LOG.info("Partitions {} is removed as it was no longer subscribed", toRemovedPartition);
      }
      previousSubscribedPartitions.addAll(t.getPartitionsV2());
    });


    // Step2: drop the pending partitions that which is no longer subscribed from to Assigned partitions
    List<String> toAssignPendingPartitions = new ArrayList<>(pendingPartition.stream()
        .filter(p -> subscribedPartitions.contains(p)).collect(Collectors.toList()));


    // Step 3: Calculated the fresh partitions, we cannot directly get fresh partitions from subscribedPartitions
    // as it will have a race condition that the partition is free from the task but has't been put into partition pool
    // The fresh partition must been get from subscriptions
    freshPartitions.removeAll(toAssignPendingPartitions);
    freshPartitions.removeAll(previousSubscribedPartitions);

    // Step 4: assign the fresh and pending partitions from partition pool
    Collections.shuffle(toAssignPendingPartitions);
    Collections.shuffle(freshPartitions);

    int size = freshPartitions.size() + toAssignPendingPartitions.size() + previousSubscribedPartitions.size();

    int maxPartitionPerTask = (int) Math.ceil((double) size / (double) assignedTask.size());

    int i = 0;
    while (toAssignPendingPartitions.size() > 0) {
      DatastreamTask task = assignedTask.get(i % assignedTask.size());

      if (task.getPartitionsV2().size() < maxPartitionPerTask) {
        task.assignPartitions(Collections.singletonList(toAssignPendingPartitions.remove(toAssignPendingPartitions.size() - 1)), false);
      }
      ++i;
    }

    while (freshPartitions.size() > 0) {
      DatastreamTask task = assignedTask.get(i % assignedTask.size());
      if (task.getPartitionsV2().size() < maxPartitionPerTask) {
        task.assignPartitions(Collections.singletonList(freshPartitions.remove(freshPartitions.size() - 1)), true);
      }
      ++i;
    }


    //Step 4: revoke heavily imbalanced task, put into partition pool
    List<String> toMovePartitions = new ArrayList<>();

    assignedTask.stream().forEach(task -> {
      while (task.getPartitionsV2().size() > maxPartitionPerTask + MAX_ALLOW_INBALANCE_THRESHOLD) {
        String toMovePartition = task.getPartitionsV2().get(task.getPartitionsV2().size() - 1);
        task.revokePartitions(Collections.singletonList(toMovePartition));
        toMovePartitions.add(toMovePartition);
      }
    });


    if (toMovePartitions.size() > 0) {
      LOG.info("To move partitions {}", toMovePartitions);
    }

    LOG.info("Assignment info {}", assignedTask);
   // sanityChecks(assignedTask, toMovePartitions, subscribedPartitions);
  }

  private void sanityChecks(List<DatastreamTask> assignedTask, List<String> toMovePartitions, List<String> allPartitions) {
    int total = 0;
    List<String> toCheckPartitions = new ArrayList<>(allPartitions);
    toCheckPartitions.removeAll(toMovePartitions);

    for (DatastreamTask task : assignedTask) {
      total += task.getPartitionsV2().size();
      toCheckPartitions.removeAll(task.getPartitionsV2());
    }
    if (total != allPartitions.size()) {
      throw new DatastreamRuntimeException(String.format("Validation failed after assignment, assigned partitions "
          + "size: {} is not equal to new partitions size: {}", total, allPartitions.size()));
    }
    if (toCheckPartitions.size() > 0) {
      throw new DatastreamRuntimeException(String.format("Validation failed after assignment, "
          + "unassigned partition: {}", toCheckPartitions));
    }
  }
}
