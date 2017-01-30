/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package co.cask.hydrator.plugin.batch.split;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Splits generator class for DynamoDBBatchSource.
 */
public class DynamoDBSplitGenerator {

  public List<InputSplit> generateSplits(int maxClusterMapTasks, int numSegments, Configuration conf) {
    int numMappers = Math.min(maxClusterMapTasks, numSegments);
    List<List<Integer>> segmentsPerSplit = new ArrayList<List<Integer>>(numMappers);
    for (int i = 0; i < numMappers; i++) {
      segmentsPerSplit.add(new ArrayList<Integer>());
    }
    // Round-robin which split gets which segment id.
    int mapper = 0;
    for (int i = 0; i < numSegments; i++) {
      segmentsPerSplit.get(mapper).add(i);
      mapper = (mapper + 1) % numMappers;
    }

    InputSplit[] splits = new InputSplit[numMappers];
    for (int i = 0; i < numMappers; i++) {
      splits[i] = createDynamoDBSplit(i, segmentsPerSplit.get(i), numSegments);
    }
    return Arrays.asList(splits);
  }

  private DynamoDBSegmentsSplit createDynamoDBSplit(int splitId, List<Integer> segments, int totalSegments) {
    return new DynamoDBSegmentsSplit(splitId, segments, totalSegments);
  }
}
