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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.batch.InputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.hydrator.common.ReferencePluginConfig;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.google.common.base.Strings;
import org.apache.hadoop.io.LongWritable;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Batch DynamoDB Source Plugin - Reads the data from AWS DynamoDB table.
 */
@Plugin(type = "batchsource")
@Name("DynamoDB")
@Description("DynamoDB Batch Source that will read the data items from AWS DynamoDB table and emit each item as a " +
  "JSON string, in the body field of the StructuredRecord, that can be further processed downstream in the pipeline.")
public class DynamoDBBatchSource extends BatchSource<LongWritable, Item, StructuredRecord> {
  private static final Schema OUTPUT_SCHEMA = Schema.recordOf(
    "output", Schema.Field.of("body", Schema.of(Schema.Type.STRING)));
  private final DynamoDBConfig config;

  public DynamoDBBatchSource(DynamoDBConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    config.validateTableName();
    pipelineConfigurer.getStageConfigurer().setOutputSchema(OUTPUT_SCHEMA);
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    context.setInput(Input.of(config.referenceName, new DynamoDBInputFormatProvider(config)));
  }

  @Override
  public void transform(KeyValue<LongWritable, Item> input, Emitter<StructuredRecord> emitter) throws Exception {
    StructuredRecord.Builder builder = StructuredRecord.builder(OUTPUT_SCHEMA);
    builder.set("body", input.getValue().toJSONPretty());
    emitter.emit(builder.build());
  }

  /**
   * Config class for DynamoDBBatchSource.
   */
  public static class DynamoDBConfig extends ReferencePluginConfig {

    @Description("Access key for AWS DynamoDB to connect to. (Macro Enabled)")
    @Macro
    private String accessKey;
    @Description("Secret access key for AWS DynamoDB to connect to. (Macro Enabled)")
    @Macro
    private String secretAccessKey;
    @Description("The region for AWS DynamoDB to connect to. Default is us_west_2 i.e. US West (Oregon).")
    @Nullable
    private String regionId;
    @Description("The hostname and port for AWS DynamoDB instance to connect to, separated by a colon. For example, " +
      "localhost:8000.")
    @Nullable
    private String endpointUrl;
    @Description("The DynamoDB table to read the data from.")
    private String tableName;
    @Description("Query to read the items from DynamoDB table. Query must include a partition key value and an " +
      "equality condition and it must be specified in the following format: 'hashAttributeName = :hashval'. For " +
      "example, ID = :v_id or ID = :v_id AND Title = :v_title, if sort key condition is used to read the data from " +
      "table. (Macro Enabled)")
    @Macro
    private String query;
    @Description("Filter query to return only the desired items that satisfy the condition. All other items are " +
      "discarded. It must be specified in the similar format like main query. For example, rating = :v_rating. " +
      "(Macro Enabled)")
    @Nullable
    @Macro
    private String filterQuery;
    @Description("List of the placeholder tokens used as the attribute name in the 'Query or FilterQuery' along with " +
      "their actual attribute names. This is a comma-separated list of key-value pairs, where each pair is separated " +
      "by a pipe sign '|' and specifies the tokens and actual attribute names. For example, '#yr|year', if the query " +
      "is like: '#yr = :yyyy'. This might be necessary if an attribute name conflicts with a DynamoDB reserved word. " +
      "(Macro Enabled)")
    @Nullable
    @Macro
    private String nameMappings;
    @Description("List of the placeholder tokens used as the attribute values in the 'Query or FilterQuery' along " +
      "with their actual values. This is a comma-separated list of key-value pairs, where each pair is separated by a" +
      " pipe sign '|' and specifies the tokens and actual values. For example, ':v_id|256,:v_title|B', if the query " +
      "is like: 'ID = :v_id AND Title = :v_title'. (Macro Enabled)")
    @Macro
    private String valueMappings;
    @Description("List of the placeholder tokens used as the attribute values in the 'Query or FilterQuery' along " +
      "with their data types. This is a comma-separated list of key-value pairs, where each pair is separated by a " +
      "pipe sign '|' and specifies the tokens and its type. For example, ':v_id|int,:v_title|string', if the query is" +
      " like: 'ID = :v_id AND Title = :v_title'. Supported types are: 'boolean, int, long, float, double and string'." +
      " (Macro Enabled)")
    @Macro
    private String placeholderType;

    @Description("Read Throughput for AWS DynamoDB table to connect to, in double. Default is 1. (Macro Enabled)")
    @Nullable
    @Macro
    private String readThroughput;

    @Description("Read Throughput Percentage for AWS DynamoDB table to connect to. Default is 0.5. (Macro Enabled)")
    @Nullable
    @Macro
    private String readThroughputPercentage;

    public DynamoDBConfig(String referenceName, String accessKey, String secretAccessKey, @Nullable String regionId,
                          @Nullable String endpointUrl, String tableName, String query, @Nullable String filterQuery,
                          @Nullable String nameMappings, String valueMappings, String placeholderType,
                          @Nullable String readThroughput, @Nullable String readThroughputPercentage) {
      super(referenceName);
      this.accessKey = accessKey;
      this.secretAccessKey = secretAccessKey;
      this.regionId = regionId;
      this.endpointUrl = endpointUrl;
      this.tableName = tableName;
      this.query = query;
      this.filterQuery = filterQuery;
      this.nameMappings = nameMappings;
      this.valueMappings = valueMappings;
      this.placeholderType = placeholderType;
      this.readThroughput = readThroughput;
      this.readThroughputPercentage = readThroughputPercentage;
    }

    /**
     * Validates whether the table name follows the DynamoDB naming rules and conventions or not.
     */
    private void validateTableName() {
      int tableNameLength = tableName.length();
      if (tableNameLength < 3 || tableNameLength > 255) {
        throw new IllegalArgumentException(
          String.format("Table name '%s' does not follow the DynamoDB naming rules. Table name must be between 3 and " +
                          "255 characters long.", tableName));

      }

      String pattern = "^[a-zA-Z0-9_.-]+$";
      Pattern patternObj = Pattern.compile(pattern);

      if (!Strings.isNullOrEmpty(tableName)) {
        Matcher matcher = patternObj.matcher(tableName);
        if (!matcher.find()) {
          throw new IllegalArgumentException(
            String.format("Table name '%s' does not follow the DynamoDB naming rules. Table names can contain only " +
                            "the following characters: 'a-z, A-Z, 0-9, underscore(_), dot(.) and dash(-)'.",
                          tableName));
        }
      }
    }
  }

  /**
   * Input format provider for DynamoDBBatchSource.
   */
  private static class DynamoDBInputFormatProvider implements InputFormatProvider {
    private Map<String, String> conf;

    DynamoDBInputFormatProvider(DynamoDBConfig dynamoDBConfig) {
      this.conf = new HashMap<>();
      conf.put(DynamoDBConstants.ACCESS_KEY, dynamoDBConfig.accessKey);
      conf.put(DynamoDBConstants.SECRET_ACCESS_KEY, dynamoDBConfig.secretAccessKey);
      conf.put(DynamoDBConstants.REGION_ID, dynamoDBConfig.regionId == null ? "" : dynamoDBConfig.regionId);
      conf.put(DynamoDBConstants.ENDPOINT_URL, dynamoDBConfig.endpointUrl == null ? "" : dynamoDBConfig.endpointUrl);
      conf.put(DynamoDBConstants.TABLE_NAME, dynamoDBConfig.tableName);
      conf.put(DynamoDBConstants.QUERY, dynamoDBConfig.query);
      conf.put(DynamoDBConstants.FILTER_QUERY, dynamoDBConfig.filterQuery == null ? "" : dynamoDBConfig.filterQuery);
      conf.put(DynamoDBConstants.NAME_MAPPINGS, dynamoDBConfig.nameMappings == null ? "" : dynamoDBConfig.nameMappings);
      conf.put(DynamoDBConstants.VALUE_MAPPINGS, dynamoDBConfig.valueMappings);
      conf.put(DynamoDBConstants.PLACEHOLDERS_TYPE, dynamoDBConfig.placeholderType);
      conf.put(DynamoDBConstants.READ_THROUGHPUT, dynamoDBConfig.readThroughput == null ? "" :
        dynamoDBConfig.readThroughput);
      conf.put(DynamoDBConstants.READ_THROUGHPUT_PERCENT, dynamoDBConfig.readThroughputPercentage == null ? "" :
        dynamoDBConfig.readThroughputPercentage);
    }

    @Override
    public String getInputFormatClassName() {
      return DynamoDBInputFormat.class.getName();
    }

    @Override
    public Map<String, String> getInputFormatConfiguration() {
      return conf;
    }
  }
}
