/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.assignment;

import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.server.DatastreamGroup;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.DatastreamTaskImpl;
import com.linkedin.datastream.server.api.strategy.PartitionAssignmentStrategy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 */
public class StickyPartitionAssignmentStrategy implements PartitionAssignmentStrategy  {
  private static final Logger LOG = LoggerFactory.getLogger(StickyPartitionAssignmentStrategy.class.getName());

  public Map<String, Set<DatastreamTask>> assignSubscribedPartitions(DatastreamGroup dg, Map<String,
      Set<DatastreamTask>> currentAssignment, List<String> subscribedPartitions) {
    //Assign
    LOG.info("partition assignment info, task: {}, subscribedOne: {}", currentAssignment, subscribedPartitions);

    List<String> assignedPartitions = new ArrayList<>();
    int totalTaskCount = 0;
    for (Set<DatastreamTask> tasks : currentAssignment.values()) {
      Set<DatastreamTask> dgTask = tasks.stream().filter(dg::belongsTo).collect(Collectors.toSet());
      dgTask.stream().forEach(t -> assignedPartitions.addAll(t.getPartitionsV2()));
      totalTaskCount += tasks.size();
    }

    List<String> unassignedPartitions = new ArrayList<>(subscribedPartitions);
    unassignedPartitions.removeAll(assignedPartitions);

    int maxPartitionPerTask = (int) Math.ceil((double) subscribedPartitions.size() / (double) totalTaskCount);
    LOG.info("maxPartitionPerTask {}, task count {}", maxPartitionPerTask, totalTaskCount);

    Collections.shuffle(unassignedPartitions);

    //update the assignment
    Map<String, Set<DatastreamTask>> newAssignment = new HashMap<>();

    currentAssignment.keySet().stream().forEach(instance -> {
      Set<DatastreamTask> tasks = currentAssignment.get(instance);
      Set<DatastreamTask> newAssignedTask = tasks.stream().map(task -> {
        if (!dg.belongsTo(task)) {
          return task;
        } else {
          Set<String> partitions = new HashSet<>(task.getPartitionsV2());
          partitions.retainAll(subscribedPartitions);

          //We need to create new task if the partition is changed
          boolean partitionChanged = partitions.size() != task.getPartitionsV2().size();

          while(partitions.size() < maxPartitionPerTask && unassignedPartitions.size() > 0) {
            partitions.add(unassignedPartitions.remove(unassignedPartitions.size() - 1));
            partitionChanged = true;
          }

          if (partitionChanged) {
            return new DatastreamTaskImpl((DatastreamTaskImpl)task, partitions);
          } else {
            return task;
          }
        }
      }).collect(Collectors.toSet());
      newAssignment.put(instance, newAssignedTask);
    });
    LOG.info("new assignment info, task: {}, subscribedOne: {}", newAssignment, subscribedPartitions);

    return newAssignment;
  }

  public Map<String, Set<DatastreamTask>> movePartitions(DatastreamGroup dg,
      Map<String, Set<DatastreamTask>> currentAssignment,
      Map<String, Set<String>> suggestAssignment, List<String> subscribedPartitions) {

    LOG.info("Try to move partition, task: {}, suggested assignment: {}, subscribedOne: {}", currentAssignment,
        suggestAssignment, subscribedPartitions);

    Map<String, Set<DatastreamTask>> newAssignment = new HashMap<>(currentAssignment);

    Set<String> toReassignPartitions = new HashSet<>();
    suggestAssignment.values().stream().forEach(toReassignPartitions::addAll);
    toReassignPartitions.retainAll(subscribedPartitions);

    //construct a map for partition movement, key: partition name, value: source task name
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
          return new DatastreamTaskImpl((DatastreamTaskImpl)task, partitions);
        } else {
          return task;
        }
      }).filter(t -> t!= null).collect(Collectors.toSet());
      newAssignment.put(instance, newTasks);
    });
    //TODO: check the broken parts

    //Assign the movement Info
    suggestAssignment.forEach((inst, partitions) -> {
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
      DatastreamTaskImpl newTask = new DatastreamTaskImpl((DatastreamTaskImpl)task, newPartitions);
      confirmedPartitions.stream().forEach(p -> newTask.addPreviousTask(partitionMovementSourceMap.get(p)));
      newAssignment.get(inst).add(newTask);
    });


    LOG.info("assignment info, task: {}", newAssignment);
    return newAssignment;
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
