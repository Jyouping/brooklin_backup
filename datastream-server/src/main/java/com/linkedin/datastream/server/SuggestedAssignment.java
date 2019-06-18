package com.linkedin.datastream.server;

import com.linkedin.datastream.common.JsonUtils;
import java.util.List;


public class SuggestedAssignment {
  private List<String> _partitionNames;
  private String _targetInstance;

  public String getTargetInstance() {
    return _targetInstance;
  }

  public SuggestedAssignment(List<String> partitionName, String targetInstance) {
    _partitionNames = partitionName;
    _targetInstance = targetInstance;
  }

  //Used for json constructor
  public SuggestedAssignment() {

  }


  public static SuggestedAssignment fromJson(String json) {
    SuggestedAssignment assignment = JsonUtils.fromJson(json, SuggestedAssignment.class);
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

  public void setTargetInstance(String targetInstance) {
    _targetInstance = targetInstance;
  }
}
