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
package co.cask.hydrator.plugin;

import co.cask.cdap.etl.mock.common.MockPipelineConfigurer;
import co.cask.hydrator.plugin.batch.DynamoDBBatchSource;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit Tests for DynamoDBConfig.
 */
public class DynamoDBBatchSourceConfigTest {

  @Test
  public void testInvalidTableNameLength() throws Exception {
    DynamoDBBatchSource.DynamoDBConfig config = new
      DynamoDBBatchSource.DynamoDBConfig("Referencename", "testKey", "testKey", "us-east-1", "", "tm", "Id = :v_iD",
                                         "", "", ":v_iD|120", ":v_iD|int", "", "");

    MockPipelineConfigurer configurer = new MockPipelineConfigurer(null);
    try {
      new DynamoDBBatchSource(config).configurePipeline(configurer);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Table name 'tm' does not follow the DynamoDB naming rules. Table name must be between 3 " +
                            "and 255 characters long.", e.getMessage());
    }
  }

  @Test
  public void testInvalidTableName() throws Exception {
    DynamoDBBatchSource.DynamoDBConfig config = new
      DynamoDBBatchSource.DynamoDBConfig("Referencename", "testKey", "testKey", "us-east-1", "", "wrong%^table",
                                         "Id = :v_iD", "", "", ":v_iD|120", ":v_iD|int", "", "");
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(null);
    try {
      new DynamoDBBatchSource(config).configurePipeline(configurer);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Table name 'wrong%^table' does not follow the DynamoDB naming rules. Table names can " +
                            "contain only the following characters: 'a-z, A-Z, 0-9, underscore(_), dot(.) and dash(-)" +
                            "'.", e.getMessage());
    }
  }
}
