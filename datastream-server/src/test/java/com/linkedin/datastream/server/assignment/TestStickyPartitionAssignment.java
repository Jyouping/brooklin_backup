/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.assignment;

import com.google.common.collect.ImmutableList;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.connectors.DummyConnector;
import com.linkedin.datastream.server.DatastreamGroup;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.DatastreamTaskImpl;
import com.linkedin.datastream.server.api.strategy.PartitionAssignmentStrategy;
import com.linkedin.datastream.testutil.DatastreamTestUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;



/**
 * Tests for {@link TestStickyPartitionAssignment}
 */
public class TestStickyPartitionAssignment {

  private static final Logger LOG = LoggerFactory.getLogger(TestStickyPartitionAssignment.class.getName());

  @Test
  public void testCreateAssignmentAcrossAllInstances() {
    PartitionAssignmentStrategy strategy = new StickyPartitionAssignmentStrategy();
    List<DatastreamTask> taskList = new ArrayList<>();
    List<DatastreamGroup> datastreams = generateDatastreams("ds", 1);
    taskList.add(new DatastreamTaskImpl(datastreams.get(0).getDatastreams()));
    taskList.add(new DatastreamTaskImpl(datastreams.get(0).getDatastreams()));
    taskList.add(new DatastreamTaskImpl(datastreams.get(0).getDatastreams()));

    List<String> partitions = ImmutableList.of("t-0", "t-1", "t1-0");

    strategy.assign(taskList, partitions);
    for (DatastreamTask task : taskList) {
      Assert.assertEquals(task.getPartitionsV2().size(), 1);
    }
  }

  @Test
  public void testDeleteAssignment() {
    PartitionAssignmentStrategy strategy = new StickyPartitionAssignmentStrategy();
    List<DatastreamTask> taskList = new ArrayList<>();
    List<DatastreamGroup> datastreams = generateDatastreams("ds", 1);
    taskList.add(new DatastreamTaskImpl(datastreams.get(0).getDatastreams()));
    taskList.add(new DatastreamTaskImpl(datastreams.get(0).getDatastreams()));
    taskList.add(new DatastreamTaskImpl(datastreams.get(0).getDatastreams()));

    List<String> partitions = ImmutableList.of("t-0", "t-1", "t1-0");

    strategy.assign(taskList, partitions);

    String expectedTaskName = null;
    for (DatastreamTask task : taskList) {
      if (task.getPartitionsV2().get(0).equals("t1-0")) {
        expectedTaskName = task.getDatastreamTaskName();
      }
    }
    strategy.assign(taskList, ImmutableList.of("t1-0"));

    for (DatastreamTask task : taskList) {
      if (task.getDatastreamTaskName().equals(expectedTaskName)) {
        Assert.assertEquals(task.getPartitionsV2().get(0), "t1-0");
      } else {
        Assert.assertEquals(task.getPartitionsV2().size(), 0);
      }
    }
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
