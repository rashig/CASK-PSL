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

/**
 * An interface that contains constants to be used for DynamoDBBatchSource.
 */
public interface DynamoDBConstants {
  String ACCESS_KEY = "dynamoDB.access.key";
  String SECRET_ACCESS_KEY = "dynamoDB.secret.access.key";
  String REGION_ID = "dynamoDB.region.id";
  String ENDPOINT_URL = "dynamoDB.endpoint.url";
  String TABLE_NAME = "dynamoDB.table.name";
  String QUERY = "dynamoDB.query";
  String FILTER_QUERY = "dynamoDB.filter.query";
  String NAME_MAPPINGS = "dynamoDB.query.name.mappings";
  String VALUE_MAPPINGS = "dynamoDB.query.value.mappings";
  String PLACEHOLDERS_TYPE = "dynamoDB.placeholders.type";
  String READ_THROUGHPUT = "dynamoDB.throughput.read";
  String READ_THROUGHPUT_PERCENT = "dynamoDB.read.throughput.percent";
  String MAX_MAP_TASKS = "mapreduce.local.map.tasks.maximum";

  String DEFAULT_THROUGHPUT_PERCENTAGE = "0.5";
  String DEFAULT_READ_THROUGHPUT = "1";
  int MAX_SCAN_SEGMENTS = 1000000;
  int MIN_SCAN_SEGMENTS = 1;
  double MIN_IO_PER_SEGMENT = 100.0;
  int MIN_READ_THROUGHPUT_PER_MAP = 100;
}
