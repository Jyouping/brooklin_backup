package com.linkedin.datastream.server;

import com.linkedin.datastream.common.JsonUtils;
import java.util.List;


public class TargetAssignment {
  private List<String> _partitionNames;
  private String _targetHost;

  public String getTargetHost() {
    return _targetHost;
  }

  public TargetAssignment(List<String> partitionName, String targetHost) {
    _partitionNames = partitionName;
    _targetHost = targetHost;
  }

  //Used for json constructor
  public TargetAssignment() {

  }


  public static TargetAssignment fromJson(String json) {
    TargetAssignment assignment = JsonUtils.fromJson(json, TargetAssignment.class);
    return assignment;
  }
  public String toJson() {
    return JsonUtils.toJson(this);
  }

  public List<String> getPartitionNames() {
    return _partitionNames;
  }

  public void setPartitionNames(List<String> partitionNames) {
    _partitionNames = partitionNames;
  }

  public void setTargetHost(String targetHost) {
    _targetHost = targetHost;
  }
}
