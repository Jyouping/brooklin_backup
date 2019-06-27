/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.assignment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.server.DatastreamGroup;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.DatastreamTaskImpl;

/**
 * An partition assignment strategy. This StickyPartitionAssignmentStrategy creates new tasks and remove old tasks
 * to accommodate the change in partition assignment. The total number of tasks is unchanged during this process.
 * The strategy is also "Sticky", i.e., it minimize the potential partitions change between new tasks/old tasks
 */
public class StickyPartitionAssignmentStrategy {
  private static final Logger LOG = LoggerFactory.getLogger(StickyPartitionAssignmentStrategy.class.getName());

  /**
   * assign partitions to a particular datastream group
   *
   * @param dg datastream group which needs the partition assignment
   * @param currentAssignment the old assignment
   * @param allPartitions the subscribed partitions received from partition listener
   * @return new assignment mapping
   */
  public Map<String, Set<DatastreamTask>> assignPartitions(DatastreamGroup dg, Map<String,
      Set<DatastreamTask>> currentAssignment, List<String> allPartitions) {

    LOG.info("old partition assignment info, assignment: {}", currentAssignment);

    List<String> assignedPartitions = new ArrayList<>();
    int totalTaskCount = 0;
    for (Set<DatastreamTask> tasks : currentAssignment.values()) {
      Set<DatastreamTask> dgTask = tasks.stream().filter(dg::belongsTo).collect(Collectors.toSet());
      dgTask.stream().forEach(t -> assignedPartitions.addAll(t.getPartitionsV2()));
      totalTaskCount += dgTask.size();
    }

    List<String> unassignedPartitions = new ArrayList<>(allPartitions);
    unassignedPartitions.removeAll(assignedPartitions);

    int maxPartitionPerTask = allPartitions.size() / totalTaskCount;
    final AtomicInteger remainder = new AtomicInteger(allPartitions.size() % totalTaskCount);
    LOG.info("maxPartitionPerTask {}, task count {}", maxPartitionPerTask, totalTaskCount);

    Collections.shuffle(unassignedPartitions);

    Map<String, Set<DatastreamTask>> newAssignment = new HashMap<>();

    currentAssignment.keySet().stream().forEach(instance -> {
      Set<DatastreamTask> tasks = currentAssignment.get(instance);
      Set<DatastreamTask> newAssignedTask = tasks.stream().map(task -> {
        if (!dg.belongsTo(task)) {
          return task;
        } else {
          Set<String> partitions = new HashSet<>(task.getPartitionsV2());
          partitions.retainAll(allPartitions);

          //We need to create new task if the partition is changed
          boolean partitionChanged = partitions.size() != task.getPartitionsV2().size();

          int allowedPartitions = remainder.get() > 0 ? maxPartitionPerTask + 1 : maxPartitionPerTask;

          while (partitions.size() < allowedPartitions && unassignedPartitions.size() > 0) {
            partitions.add(unassignedPartitions.remove(unassignedPartitions.size() - 1));
            partitionChanged = true;
          }

          if (remainder.get() > 0) {
            remainder.decrementAndGet();
          }

          if (partitionChanged) {
            return new DatastreamTaskImpl((DatastreamTaskImpl) task, partitions);
          } else {
            return task;
          }
        }
      }).collect(Collectors.toSet());
      newAssignment.put(instance, newAssignedTask);
    });
    LOG.info("new assignment info, assignment: {}, all partitions: {}", newAssignment, allPartitions);

    sanityChecks(dg.getTaskPrefix(), newAssignment, allPartitions);
    return newAssignment;
  }

  /**
   * Move a partition for a datastream group according to the suggestAssignment
   *
   * @param dg datastream group which needs the partition movement
   * @param currentAssignment the old assignment
   * @param targetAssignment the target assignment retrieved from Zookeeper
   * @param allPartitions the subscribed partitions received from partition listener
   * @return new assignment
   */
  public Map<String, Set<DatastreamTask>> movePartitions(DatastreamGroup dg,
      Map<String, Set<DatastreamTask>> currentAssignment,
      Map<String, Set<String>> targetAssignment, List<String> allPartitions) {

    LOG.info("Try to move partition, task: {}, target assignment: {}, all partitions: {}", currentAssignment,
        targetAssignment, allPartitions);

    Map<String, Set<DatastreamTask>> newAssignment = new HashMap<>(currentAssignment);

    Set<String> toReassignPartitions = new HashSet<>();
    targetAssignment.values().stream().forEach(toReassignPartitions::addAll);
    toReassignPartitions.retainAll(allPartitions);

    //construct a map to store moved partition, key: partition name, value: source task name
    Map<String, String> partitionMovementSourceMap = new HashMap<>();

    //Release a partition into the map
    newAssignment.keySet().stream().forEach(instance -> {
      Set<DatastreamTask> prevTasks = currentAssignment.get(instance);
      Set<DatastreamTask> newTasks = prevTasks.stream().map(task -> {
        if (!dg.belongsTo(task)) {
          return task;
        }
        Set<String> movedPartitions = new HashSet<>(task.getPartitionsV2());
        movedPartitions.retainAll(toReassignPartitions);
        if (!movedPartitions.isEmpty()) {
          movedPartitions.stream().forEach(p -> partitionMovementSourceMap.put(p, task.getDatastreamTaskName()));
          List<String> partitions = new ArrayList<>(task.getPartitionsV2());
          partitions.removeAll(movedPartitions);
          return new DatastreamTaskImpl((DatastreamTaskImpl) task, partitions);
        } else {
          return task;
        }
      }).filter(t -> t != null).collect(Collectors.toSet());
      newAssignment.put(instance, newTasks);
    });

    //Assign the movement Info
    targetAssignment.forEach((inst, partitions) -> {
      Set<String> confirmedPartitions = partitions.stream().filter(partitionMovementSourceMap::containsKey)
          .collect(Collectors.toSet());

      //find a task with small number of partitions on that instance

      Optional<DatastreamTask> toAssignTask;
      if (newAssignment.containsKey(inst)) {
        Set<DatastreamTask> dgTasks = newAssignment.get(inst).stream().filter(dg::belongsTo)
            .collect(Collectors.toSet());
        toAssignTask = dgTasks.stream().reduce((task1, task2) ->
            task1.getPartitionsV2().size() < task2.getPartitionsV2().size() ? task1 : task2);
      } else {
        toAssignTask = Optional.empty();
      }

      DatastreamTask task = toAssignTask.orElseThrow(() ->
          new DatastreamRuntimeException("No task is allocated in instance " + inst));
      newAssignment.get(inst).remove(task);
      List<String> newPartitions = new ArrayList<>(task.getPartitionsV2());
      newPartitions.addAll(confirmedPartitions);
      DatastreamTaskImpl newTask = new DatastreamTaskImpl((DatastreamTaskImpl) task, newPartitions);
      confirmedPartitions.stream().forEach(p -> newTask.addDependentTask(partitionMovementSourceMap.get(p)));
      newAssignment.get(inst).add(newTask);
    });

    sanityChecks(dg.getTaskPrefix(), newAssignment, allPartitions);
    LOG.info("assignment info, task: {}", newAssignment);
    return newAssignment;
  }

  /**
   * check if the computed assignment have all the partitions
   */
  private void sanityChecks(String datastreamGroupName, Map<String, Set<DatastreamTask>> assignedTasks, List<String> allPartitions) {
    int total = 0;

    List<String> unassignedPartitions = new ArrayList<>(allPartitions);

    for (Set<DatastreamTask> tasksSet : assignedTasks.values()) {
      for (DatastreamTask task : tasksSet) {
        if (datastreamGroupName.equals(task.getTaskPrefix())) {
          total += task.getPartitionsV2().size();
          unassignedPartitions.removeAll(task.getPartitionsV2());
        }
      }
    }
    if (total != allPartitions.size()) {
      throw new DatastreamRuntimeException(String.format("Validation failed after assignment, assigned partitions "
          + "size: {} is not equal to all partitions size: {}", total, allPartitions.size()));
    }
    if (unassignedPartitions.size() > 0) {
      throw new DatastreamRuntimeException(String.format("Validation failed after assignment, "
          + "unassigned partition: {}", unassignedPartitions));
    }
  }
}
