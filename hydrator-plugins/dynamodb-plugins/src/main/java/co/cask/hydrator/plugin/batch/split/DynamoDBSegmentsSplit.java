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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * InputSplit class for DynamoDBBatchSource.
 */
public class DynamoDBSegmentsSplit extends InputSplit implements Writable {
  private int splitId;
  private List<Integer> segments;
  private int totalSegments;

  public DynamoDBSegmentsSplit() {
    this.segments = new ArrayList<>();
  }

  public DynamoDBSegmentsSplit(int splitId, List<Integer> segments, int totalSegments) {
    this.segments = segments;
    this.splitId = splitId;
    this.totalSegments = totalSegments;
  }

  @Override
  public long getLength() {
    return 0;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    splitId = in.readInt();
    int numSegments = in.readInt();
    this.segments = new ArrayList<>(numSegments);
    for (int i = 0; i < numSegments; i++) {
      this.segments.add(in.readInt());
    }
    totalSegments = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(splitId);
    out.writeInt(segments.size());
    for (Integer segment : segments) {
      out.writeInt(segment);
    }
    out.writeInt(totalSegments);
  }

  @Override
  public String[] getLocations() throws IOException {
    return new String[0];
  }
}
