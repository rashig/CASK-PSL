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
package co.cask.hydrator.plugin.batch;

import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.hydrator.common.KeyValueListParser;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableCollection;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * DynamoDBRecordReader - Instantiates a record reader that will read the items from DynamoDB table.
 */
public class DynamoDBRecordReader extends RecordReader<LongWritable, Item> {
  private ItemCollection<QueryOutcome> items;
  private Configuration conf;
  private DynamoDB dynamoDB;
  // Map key that represents the item index.
  private LongWritable key;
  // Map value that represents an item.
  private Item value;
  private Iterator<Item> iterator;
  private long itemIdx;
  private boolean flag = false;


  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
    throws IOException, InterruptedException {
    conf = taskAttemptContext.getConfiguration();
    dynamoDB = getDynamoDbClient(conf);

    //Calling testTableExists() before each mapper, to test whether table exists or not, for reading the items.
    testTableExists(dynamoDB);
    if (!flag) {
      throw new IllegalArgumentException(
        String.format("Table '%s' does not exist.", conf.get(DynamoDBConstants.TABLE_NAME)));
    }

    Table table = dynamoDB.getTable(conf.get(DynamoDBConstants.TABLE_NAME));
    QuerySpec spec = new QuerySpec()
      .withKeyConditionExpression(conf.get(DynamoDBConstants.QUERY))
      .withValueMap(getValueMap());

    if (!Strings.isNullOrEmpty(conf.get(DynamoDBConstants.NAME_MAPPINGS))) {
      NameMap nameMap = getNameMap();
      spec.withNameMap(nameMap);
    }

    if (!Strings.isNullOrEmpty(conf.get(DynamoDBConstants.FILTER_QUERY))) {
      spec.withFilterExpression(conf.get(DynamoDBConstants.FILTER_QUERY));
    }

    items = table.query(spec);
    iterator = items.iterator();
    itemIdx = 0;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (!iterator.hasNext()) {
      return false;
    }
    Item item = iterator.next();
    key = new LongWritable(itemIdx);
    itemIdx++;
    value = item;
    return true;
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public Item getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return 0;
  }

  @Override
  public void close() throws IOException {
    dynamoDB.shutdown();
  }

  /**
   * Creates and returns the DynamoDB client for establishing the connection and reading the items from DynamoDb table.
   *
   * @return DynamoDB client
   */
  private DynamoDB getDynamoDbClient(Configuration conf) {
    AmazonDynamoDBClient dynamoDBClient;
    BasicAWSCredentials credentials = new BasicAWSCredentials(conf.get(DynamoDBConstants.ACCESS_KEY), conf.get
      (DynamoDBConstants.SECRET_ACCESS_KEY));

    if (Strings.isNullOrEmpty(conf.get(DynamoDBConstants.REGION_ID))) {
      dynamoDBClient = new AmazonDynamoDBClient(credentials).withRegion(Regions.DEFAULT_REGION);
    } else {
      dynamoDBClient = new AmazonDynamoDBClient(credentials).withRegion(RegionUtils.getRegion(
        conf.get(DynamoDBConstants.REGION_ID)));
    }

    if (!Strings.isNullOrEmpty(conf.get(DynamoDBConstants.ENDPOINT_URL))) {
      dynamoDBClient.setEndpoint("http://" + conf.get(DynamoDBConstants.ENDPOINT_URL));
    }
    return new DynamoDB(dynamoDBClient);
  }

  /**
   * Creates and returns the NameMap as per the 'attribute name mappings', that will be used while querying to the
   * DynamoDB table.
   *
   * @return NameMap
   */
  private NameMap getNameMap() {
    NameMap nameMap = new NameMap();
    KeyValueListParser kvParser = new KeyValueListParser("\\s*,\\s*", "\\|");
    for (KeyValue<String, String> keyVal : kvParser.parse(conf.get(DynamoDBConstants.NAME_MAPPINGS))) {
      String key = keyVal.getKey();
      String val = keyVal.getValue();
      nameMap.with(key, val);
    }
    return nameMap;
  }

  /**
   * Creates and returns the ValueMap as per the 'attribute value mappings', that will be used while querying the
   * DynamoDb table.
   *
   * @return ValueMap
   */
  private ValueMap getValueMap() {
    ValueMap valueMap = new ValueMap();
    Map<String, String> placeholdersTypeMap = getPlaceholderTypeMap();
    KeyValueListParser kvParser = new KeyValueListParser("\\s*,\\s*", "\\|");

    if (!Strings.isNullOrEmpty(conf.get(DynamoDBConstants.VALUE_MAPPINGS))) {
      for (KeyValue<String, String> keyVal : kvParser.parse(conf.get(DynamoDBConstants.VALUE_MAPPINGS))) {
        String key = keyVal.getKey();
        String val = keyVal.getValue();

        if (placeholdersTypeMap.containsKey(key)) {
          String type = placeholdersTypeMap.get(key);
          switch (type) {
            case "boolean":
              valueMap.with(key, Boolean.parseBoolean(val));
              break;
            case "int":
              valueMap.with(key, Integer.parseInt(val));
              break;
            case "long":
              valueMap.with(key, Long.parseLong(val));
              break;
            case "float":
              valueMap.with(key, Float.parseFloat(val));
              break;
            case "double":
              valueMap.with(key, Double.parseDouble(val));
              break;
            case "string":
              valueMap.with(key, val);
              break;
            default:
              throw new IllegalArgumentException(String.format("Unsupported data type '%s' that can be used for " +
                                                                 "querying. Supported data types are: 'boolean, int, " +
                                                                 "long, float, double and string'.", type));
          }
        }
      }
    }
    return valueMap;
  }

  /**
   * Creates and returns a map of placeholder tokens and its type, that will be used while querying to the DynamoDB
   * table.
   *
   * @return map of placeholders and its type
   */
  private Map<String, String> getPlaceholderTypeMap() {
    Map<String, String> placeholderTypeMap = new HashMap<>();
    KeyValueListParser kvParser = new KeyValueListParser("\\s*,\\s*", "\\|");
    if (!Strings.isNullOrEmpty(conf.get(DynamoDBConstants.PLACEHOLDERS_TYPE))) {
      for (KeyValue<String, String> keyVal : kvParser.parse(conf.get(DynamoDBConstants.PLACEHOLDERS_TYPE))) {
        String key = keyVal.getKey();
        String val = keyVal.getValue();
        placeholderTypeMap.put(key, val);
      }
    }
    return placeholderTypeMap;
  }

  /**
   * Verifies whether the requested table exists or not.
   *
   * @param dynamoDB
   */
  private void testTableExists(DynamoDB dynamoDB) {
    TableCollection<ListTablesResult> tables = dynamoDB.listTables();
    Iterator<Table> iterator = tables.iterator();

    while (iterator.hasNext()) {
      Table table = iterator.next();
      if (table.getTableName().equals(conf.get(DynamoDBConstants.TABLE_NAME))) {
        flag = true;
        break;
      }
    }
  }
}
