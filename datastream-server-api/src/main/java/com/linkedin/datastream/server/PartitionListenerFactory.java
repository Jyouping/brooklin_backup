/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.util.Properties;

/**
 * Factory to create partition listener
 */
public interface PartitionListenerFactory {

  /**
   * create a partition listener instance
   * @param clusterName name of the cluster
   * @param config the config associated to this partition listener
   * @return partition listener instance
   */
  PartitionListener createPartitionListener(String clusterName, Properties config);
}
