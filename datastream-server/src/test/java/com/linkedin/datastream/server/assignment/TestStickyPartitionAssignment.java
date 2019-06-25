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
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.connectors.DummyConnector;
import com.linkedin.datastream.server.DatastreamGroup;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.DatastreamTaskImpl;
import com.linkedin.datastream.server.zk.ZkAdapter;
import com.linkedin.datastream.testutil.DatastreamTestUtils;

import static org.mockito.Mockito.mock;


/**
 * Tests for {@link TestStickyPartitionAssignment}
 */
public class TestStickyPartitionAssignment {

  private static final Logger LOG = LoggerFactory.getLogger(TestStickyPartitionAssignment.class.getName());

  @Test
  public void testCreateAssignmentAcrossAllTasks() {
    StickyPartitionAssignmentStrategy strategy = new StickyPartitionAssignmentStrategy();
    Set<DatastreamTask> taskSet = new HashSet<>();
    List<DatastreamGroup> datastreams = generateDatastreams("ds", 1);
    Map<String, Set<DatastreamTask>> assignment = generateEmptyAssignment(datastreams, 1, 3);
    assignment.put("instance1", taskSet);

    List<String> partitions = ImmutableList.of("t-0", "t-1", "t1-0");

    assignment = strategy.assignPartitions(datastreams.get(0), assignment, partitions);

    for (DatastreamTask task : assignment.get("instance1")) {
      Assert.assertEquals(task.getPartitionsV2().size(), 1);
    }
  }

  @Test
  public void testMovePartition() {
    StickyPartitionAssignmentStrategy strategy = new StickyPartitionAssignmentStrategy();
    List<DatastreamGroup> datastreams = generateDatastreams("ds", 1);
    Map<String, Set<DatastreamTask>> assignment = generateEmptyAssignment(datastreams, 3, 2);
    List<String> partitions = ImmutableList.of("t-0", "t-1", "t-2", "t-3", "t-4");

    // Generate partition assignment
    assignment = strategy.assignPartitions(datastreams.get(0), assignment, partitions);

    Map<String, Set<String>> targetAssignment = new HashMap<>();
    targetAssignment.put("instance2", ImmutableSet.of("t-3", "t-2", "t-1", "t-5"));
    targetAssignment.put("instance1", ImmutableSet.of("t-0"));

    assignment = strategy.movePartitions(datastreams.get(0), assignment, targetAssignment, partitions);

    Assert.assertTrue(getPartitionsFromTask(assignment.get("instance2")).contains("t-1"));
    Assert.assertTrue(getPartitionsFromTask(assignment.get("instance2")).contains("t-2"));
    Assert.assertTrue(getPartitionsFromTask(assignment.get("instance2")).contains("t-3"));


    Assert.assertEquals(getTotalPartitions(assignment), 5);
  }


  @Test
  public void testRemovePartitions() {
    StickyPartitionAssignmentStrategy strategy = new StickyPartitionAssignmentStrategy();
    List<DatastreamGroup> datastreams = generateDatastreams("ds", 1);
    Map<String, Set<DatastreamTask>> assignment = generateEmptyAssignment(datastreams, 3, 2);

    List<String> partitions = ImmutableList.of("t-0", "t-1", "t-2", "t-3", "t-4", "t-5", "t-6");

    // Generate partition assignment
    assignment = strategy.assignPartitions(datastreams.get(0), assignment, partitions);

    List<String> newPartitions = ImmutableList.of("t-1", "t-3", "t-4", "t-6");

    assignment = strategy.assignPartitions(datastreams.get(0), assignment, newPartitions);

    List<String> remainingPartitions = new ArrayList<>();
    for(String instance : assignment.keySet()) {
      for (DatastreamTask task : assignment.get(instance)) {
        remainingPartitions.addAll(task.getPartitionsV2());
      }
    }

    Assert.assertEquals(new HashSet<String>(newPartitions), new HashSet<String>(remainingPartitions));
  }

  private  Map<String, Set<DatastreamTask>> generateEmptyAssignment(List<DatastreamGroup> datastreams,
      int instanceNum, int taskNum) {
    Map<String, Set<DatastreamTask>> assignment = new HashMap<>();
    for (int i = 0; i < instanceNum; ++i) {
      Set<DatastreamTask> set = new HashSet<>();
      for (int j = 0; j < taskNum; ++j) {
        DatastreamTaskImpl task = new DatastreamTaskImpl(datastreams.get(0).getDatastreams());
        task.setZkAdapter(mock(ZkAdapter.class));
        set.add(task);
      }
      assignment.put("instance" + i, set);
    }
    return assignment;
  }

  private Set<String> getPartitionsFromTask(Set<DatastreamTask> tasks) {
    Set<String> partitions = new HashSet<>();
    tasks.stream().forEach(t -> partitions.addAll(t.getPartitionsV2()));
    return partitions;
  }


  private int getTotalPartitions(Map<String, Set<DatastreamTask>> assignment) {
    int count = 0;
    for (Set<DatastreamTask> tasks : assignment.values()) {
      count += tasks.stream().map(t -> t.getPartitionsV2().size()).mapToInt(Integer::intValue).sum();
    }
    return count;
  }

  private List<DatastreamGroup> generateDatastreams(String namePrefix, int numberOfDatastreams) {
    List<DatastreamGroup> datastreams = new ArrayList<>();
    String type = DummyConnector.CONNECTOR_TYPE;
    for (int index = 0; index < numberOfDatastreams; index++) {
      Datastream ds = DatastreamTestUtils.createDatastream(type, namePrefix + index, "DummySource");
      ds.getMetadata().put(DatastreamMetadataConstants.OWNER_KEY, "person_" + index);
      ds.getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, DatastreamTaskImpl.getTaskPrefix(ds));
      datastreams.add(new DatastreamGroup(Collections.singletonList(ds)));
    }
    return datastreams;
  }

}