/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.assignment;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.connectors.DummyConnector;
import com.linkedin.datastream.server.DatastreamGroup;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.DatastreamTaskImpl;
import com.linkedin.datastream.server.api.strategy.PartitionAssignmentStrategy;
import com.linkedin.datastream.server.zk.ZkAdapter;
import com.linkedin.datastream.testutil.DatastreamTestUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


/**
 * Tests for {@link TestStickyPartitionAssignment}
 */
public class TestStickyPartitionAssignment {

  private static final Logger LOG = LoggerFactory.getLogger(TestStickyPartitionAssignment.class.getName());

  @Test
  public void testCreateAssignmentAcrossAllTasks() {
    PartitionAssignmentStrategy strategy = new StickyPartitionAssignmentStrategy();
    Set<DatastreamTask> taskSet = new HashSet<>();
    List<DatastreamGroup> datastreams = generateDatastreams("ds", 1);
    for (int i = 0; i < 3; ++i) {
      DatastreamTaskImpl task = new DatastreamTaskImpl(datastreams.get(0).getDatastreams());
      task.setZkAdapter(mock(ZkAdapter.class));
      taskSet.add(task);
    }
    Map<String, Set<DatastreamTask>> assignment = new HashMap<>();
    assignment.put("instance1", taskSet);

    List<String> partitions = ImmutableList.of("t-0", "t-1", "t1-0");

    assignment = strategy.assignSubscribedPartitions(datastreams.get(0), assignment, partitions);

    for (DatastreamTask task : assignment.get("instance1")) {
      Assert.assertEquals(task.getPartitionsV2().size(), 1);
    }
  }

  @Test
  public void testMovePartition() {
    PartitionAssignmentStrategy strategy = new StickyPartitionAssignmentStrategy();
    Set<DatastreamTask> taskSet = new HashSet<>();
    List<DatastreamGroup> datastreams = generateDatastreams("ds", 1);
    Map<String, Set<DatastreamTask>> assignment = new HashMap<>();

    for (int i = 0; i < 3; ++i) {
      DatastreamTaskImpl task1 = new DatastreamTaskImpl(datastreams.get(0).getDatastreams());
      task1.setZkAdapter(mock(ZkAdapter.class));
      DatastreamTaskImpl task2 = new DatastreamTaskImpl(datastreams.get(0).getDatastreams());
      task2.setZkAdapter(mock(ZkAdapter.class));

      assignment.put("instance" + i, ImmutableSet.of(task1, task2));
    }

    List<String> partitions = ImmutableList.of("t-0", "t-1", "t-2", "t-3", "t-4");

    // Generate partition assignment
    assignment = strategy.assignSubscribedPartitions(datastreams.get(0), assignment, partitions);

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
  public void testDeleteAssignment() {
    /*
    PartitionAssignmentStrategy strategy = new StickyPartitionAssignmentStrategy();
    List<DatastreamTask> taskList = new ArrayList<>();
    List<DatastreamGroup> datastreams = generateDatastreams("ds", 1);
    taskList.add(new DatastreamTaskImpl(datastreams.get(0).getDatastreams()));
    taskList.add(new DatastreamTaskImpl(datastreams.get(0).getDatastreams()));
    taskList.add(new DatastreamTaskImpl(datastreams.get(0).getDatastreams()));

    List<String> partitions = ImmutableList.of("t-0", "t-1", "t1-0");

    strategy.movePartitions(taskList, partitions);

    String expectedTaskName = null;
    for (DatastreamTask task : taskList) {
      if (task.getPartitionsV2().get(0).equals("t1-0")) {
        expectedTaskName = task.getDatastreamTaskName();
      }
    }
    strategy.movePartitions(taskList, ImmutableList.of("t1-0"));

    for (DatastreamTask task : taskList) {
      if (task.getDatastreamTaskName().equals(expectedTaskName)) {
        Assert.assertEquals(task.getPartitionsV2().get(0), "t1-0");
      } else {
        Assert.assertEquals(task.getPartitionsV2().size(), 0);
      }
    }
    */
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
