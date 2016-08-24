/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.TimePartitionDetail;
import co.cask.cdap.api.dataset.lib.TimePartitionOutput;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.batch.ETLWorkflow;
import co.cask.cdap.etl.mock.batch.MockSource;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.WorkflowManager;
import co.cask.hydrator.plugin.common.Properties;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.tephra.Transaction;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionManager;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.Test;
import parquet.avro.AvroParquetWriter;
import parquet.hadoop.ParquetWriter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ETLTPFSTestRun extends ETLBatchTestBase {

  @Test
  public void testPartitionOffsetAndCleanup() throws Exception {
    Schema recordSchema = Schema.recordOf("record",
                                          Schema.Field.of("i", Schema.of(Schema.Type.INT)),
                                          Schema.Field.of("l", Schema.of(Schema.Type.LONG))
    );

    ETLPlugin sourceConfig = new ETLPlugin(
      "TPFSAvro",
      BatchSource.PLUGIN_TYPE,
      ImmutableMap.of(
        Properties.TimePartitionedFileSetDataset.SCHEMA, recordSchema.toString(),
        Properties.TimePartitionedFileSetDataset.TPFS_NAME, "cleanupInput",
        Properties.TimePartitionedFileSetDataset.DELAY, "0d",
        Properties.TimePartitionedFileSetDataset.DURATION, "1h"),
      null);
    ETLPlugin sinkConfig = new ETLPlugin(
      "TPFSAvro",
      BatchSink.PLUGIN_TYPE,
      ImmutableMap.of(
        Properties.TimePartitionedFileSetDataset.SCHEMA, recordSchema.toString(),
        Properties.TimePartitionedFileSetDataset.TPFS_NAME, "cleanupOutput",
        "partitionOffset", "1h",
        "cleanPartitionsOlderThan", "30d"),
      null);

    ETLStage source = new ETLStage("source", sourceConfig);
    ETLStage sink = new ETLStage("sink", sinkConfig);

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "offsetCleanupTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // write input to read
    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(recordSchema.toString());
    GenericRecord record = new GenericRecordBuilder(avroSchema)
      .set("i", Integer.MAX_VALUE)
      .set("l", Long.MAX_VALUE)
      .build();

    DataSetManager<TimePartitionedFileSet> inputManager = getDataset("cleanupInput");
    DataSetManager<TimePartitionedFileSet> outputManager = getDataset("cleanupOutput");

    long runtime = 1451606400000L;
    // add a partition from a 30 days and a minute ago to the output. This should get cleaned up
    long oldOutputTime = runtime - TimeUnit.DAYS.toMillis(30) - TimeUnit.MINUTES.toMillis(1);
    outputManager.get().getPartitionOutput(oldOutputTime).addPartition();
    // also add a partition from a 30 days to the output. This should not get cleaned up.
    long borderlineOutputTime = runtime - TimeUnit.DAYS.toMillis(30);
    outputManager.get().getPartitionOutput(borderlineOutputTime).addPartition();
    outputManager.flush();

    // add data to a partition from 30 minutes before the runtime
    long inputTime = runtime - TimeUnit.MINUTES.toMillis(30);
    TimePartitionOutput timePartitionOutput = inputManager.get().getPartitionOutput(inputTime);
    // Append the file name to the location representing the partition to which the schema would be written.
    Location location = timePartitionOutput.getLocation().append("0.avro");
    DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(avroSchema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
    dataFileWriter.create(avroSchema, location.getOutputStream());
    dataFileWriter.append(record);
    dataFileWriter.close();
    timePartitionOutput.addPartition();
    inputManager.flush();

    // run the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(ETLWorkflow.NAME);
    workflowManager.start(ImmutableMap.of("logical.start.time", String.valueOf(runtime)));
    workflowManager.waitForFinish(4, TimeUnit.MINUTES);

    outputManager.flush();
    // check old partition was cleaned up
    Assert.assertNull(outputManager.get().getPartitionByTime(oldOutputTime));
    // check borderline partition was not cleaned up
    Assert.assertNotNull(outputManager.get().getPartitionByTime(borderlineOutputTime));

    // check data is written to output from 1 hour before
    long outputTime = runtime - TimeUnit.HOURS.toMillis(1);
    TimePartitionDetail outputPartition = outputManager.get().getPartitionByTime(outputTime);
    Assert.assertNotNull(outputPartition);

    List<GenericRecord> outputRecords = readOutput(outputPartition.getLocation(), recordSchema);
    Assert.assertEquals(1, outputRecords.size());
    Assert.assertEquals(Integer.MAX_VALUE, outputRecords.get(0).get("i"));
    Assert.assertEquals(Long.MAX_VALUE, outputRecords.get(0).get("l"));
  }


  @Test
  public void testOrc() throws Exception {
    Schema recordSchema2 = Schema.recordOf("record",
                                           Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                                           Schema.Field.of("floatTest", Schema.of(Schema.Type.FLOAT)),
                                           Schema.Field.of("doubleTest", Schema.of(Schema.Type.DOUBLE)),
                                           Schema.Field.of("boolTest", Schema.of(Schema.Type.BOOLEAN)),
                                           Schema.Field.of("longTest", Schema.of(Schema.Type.LONG)),
                                           Schema.Field.of("byteTest", Schema.of(Schema.Type.BYTES)),
                                           Schema.Field.of("intTest", Schema.of(Schema.Type.INT)),
                                           Schema.Field.of("unionStrTest",
                                                           Schema.unionOf(Schema.of(Schema.Type.STRING),
                                                                          Schema.of(Schema.Type.NULL))),
                                           Schema.Field.of("unionStrNullTest",
                                                           Schema.unionOf(Schema.of(Schema.Type.STRING),
                                                                          Schema.of(Schema.Type.NULL))),
                                           Schema.Field.of("unionIntTest",
                                                           Schema.unionOf(Schema.of(Schema.Type.INT),
                                                                          Schema.of(Schema.Type.NULL))),
                                           Schema.Field.of("unionIntNullTest",
                                                           Schema.unionOf(Schema.of(Schema.Type.INT),
                                                                          Schema.of(Schema.Type.NULL))),
                                           Schema.Field.of("unionFloatTest",
                                                           Schema.unionOf(Schema.of(Schema.Type.FLOAT),
                                                                          Schema.of(Schema.Type.NULL))),
                                           Schema.Field.of("unionFloatNullTest",
                                                           Schema.unionOf(Schema.of(Schema.Type.FLOAT),
                                                                          Schema.of(Schema.Type.NULL))),
                                           Schema.Field.of("unionDoublTest",
                                                           Schema.unionOf(Schema.of(Schema.Type.DOUBLE),
                                                                          Schema.of(Schema.Type.NULL))),
                                           Schema.Field.of("unionDoubleNullTest",
                                                           Schema.unionOf(Schema.of(Schema.Type.DOUBLE),
                                                                          Schema.of(Schema.Type.NULL)))
                                           //TODO test nullable of long and Bytes CDAP-7074
    );

    ETLPlugin sinkConfig = new ETLPlugin("TPFSOrc",
                                         BatchSink.PLUGIN_TYPE,
                                         ImmutableMap.of(
                                           "schema", recordSchema2.toString(),
                                           "name", "outputOrc"),
                                         null);

    ETLStage sink = new ETLStage("sink", sinkConfig);
    String inputDatasetName = "input-batchsinktest";
    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputDatasetName));
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(DATAPIPELINE_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "TPFSOrcSinkTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);
    DataSetManager<Table> inputManager = getDataset(inputDatasetName);

    // write input data
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(recordSchema2)
        .set("name", "a")
        .set("floatTest", 3.6f)
        .set("doubleTest", 4.2)
        .set("boolTest", true)
        .set("longTest", 23456789)
        .set("intTest", 12)
        .set("byteTest", Bytes.toBytes("abcd"))
        .set("unionStrTest", "testUnion")
        .set("unionStrNullTest", null)
        .set("unionIntTest", 12)
        .set("unionIntNullTest", null)
        .set("unionFloatTest", 3.6f)
        .set("unionFloatNullTest", null)
        .set("unionDoublTest", 4.2)
        .set("unionDoubleNullTest", null)
        .build()
    );
    MockSource.writeInput(inputManager, input);

    // run the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForFinish(4, TimeUnit.MINUTES);

    Connection connection = getQueryClient();
    ResultSet results = connection.prepareStatement("select * from dataset_outputOrc").executeQuery();
    results.next();

    Assert.assertEquals("a", results.getString(1));
    Assert.assertEquals(3.6f, results.getFloat(2), 0.1);
    Assert.assertEquals(4.2, results.getDouble(3), 0.1);
    Assert.assertEquals(true, results.getBoolean(4));
    Assert.assertEquals(23456789, results.getLong(5));
    Assert.assertArrayEquals(Bytes.toBytes("abcd"), results.getBytes(6));
    Assert.assertEquals(12, results.getLong(7));
    Assert.assertEquals("testUnion", results.getString(8));
    Assert.assertNull(results.getString(9));
    Assert.assertEquals(12, results.getLong(10));
    Assert.assertEquals(0, results.getLong(11));
    Assert.assertEquals(3.6f, results.getFloat(12), 0.1);
    Assert.assertEquals(0.0, results.getFloat(13), 0.1);
    Assert.assertEquals(4.2, results.getDouble(14), 0.1);
    Assert.assertEquals(0, results.getDouble(15), 0.1);
  }


  @Test
  public void testAvroSourceConversionToAvroSink() throws Exception {

    Schema eventSchema = Schema.recordOf(
      "record",
      Schema.Field.of("int", Schema.of(Schema.Type.INT)));

    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(eventSchema.toString());

    GenericRecord record = new GenericRecordBuilder(avroSchema)
      .set("int", Integer.MAX_VALUE)
      .build();

    String filesetName = "tpfs";
    addDatasetInstance(TimePartitionedFileSet.class.getName(), filesetName, FileSetProperties.builder()
      .setInputFormat(AvroKeyInputFormat.class)
      .setOutputFormat(AvroKeyOutputFormat.class)
      .setInputProperty("schema", avroSchema.toString())
      .setOutputProperty("schema", avroSchema.toString())
      .setEnableExploreOnCreate(true)
      .setSerDe("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
      .setExploreInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
      .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
      .setTableProperty("avro.schema.literal", (avroSchema.toString()))
      .build());
    DataSetManager<TimePartitionedFileSet> fileSetManager = getDataset(filesetName);
    TimePartitionedFileSet tpfs = fileSetManager.get();

    TransactionManager txService = getTxService();
    Transaction tx1 = txService.startShort(100);
    TransactionAware txTpfs = (TransactionAware) tpfs;
    txTpfs.startTx(tx1);

    long timeInMillis = System.currentTimeMillis();
    fileSetManager.get().addPartition(timeInMillis, "directory", ImmutableMap.of("key1", "value1"));
    Location location = fileSetManager.get().getPartitionByTime(timeInMillis).getLocation();
    location = location.append("file.avro");
    FSDataOutputStream outputStream = new FSDataOutputStream(location.getOutputStream(), null);
    DataFileWriter dataFileWriter = new DataFileWriter<>(new GenericDatumWriter<GenericRecord>(avroSchema));
    dataFileWriter.create(avroSchema, outputStream);
    dataFileWriter.append(record);
    dataFileWriter.flush();

    txTpfs.commitTx();
    txService.canCommit(tx1, txTpfs.getTxChanges());
    txService.commit(tx1);
    txTpfs.postTxCommit();

    String newFilesetName = filesetName + "_op";
    ETLBatchConfig etlBatchConfig = constructTPFSETLConfig(filesetName, newFilesetName, eventSchema);

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlBatchConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "sconversion1");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    WorkflowManager workflowManager = appManager.getWorkflowManager(ETLWorkflow.NAME);
    // add a minute to the end time to make sure the newly added partition is included in the run.
    workflowManager.start(ImmutableMap.of("logical.start.time", String.valueOf(timeInMillis + 60 * 1000)));
    workflowManager.waitForFinish(4, TimeUnit.MINUTES);

    DataSetManager<TimePartitionedFileSet> newFileSetManager = getDataset(newFilesetName);
    TimePartitionedFileSet newFileSet = newFileSetManager.get();

    List<GenericRecord> newRecords = readOutput(newFileSet, eventSchema);
    Assert.assertEquals(1, newRecords.size());
    Assert.assertEquals(Integer.MAX_VALUE, newRecords.get(0).get("int"));
  }

  @Test
  public void testParquet() throws Exception {
    Schema recordSchema = Schema.recordOf("record",
                                          Schema.Field.of("i", Schema.of(Schema.Type.INT)),
                                          Schema.Field.of("l", Schema.of(Schema.Type.LONG))
    );

    ETLPlugin sourceConfig = new ETLPlugin("TPFSParquet",
                                           BatchSource.PLUGIN_TYPE,
                                           ImmutableMap.of(
                                             "schema", recordSchema.toString(),
                                             "name", "inputParquet",
                                             "delay", "0d",
                                             "duration", "1h"),
                                           null);
    ETLPlugin sinkConfig = new ETLPlugin("TPFSParquet",
                                         BatchSink.PLUGIN_TYPE,
                                         ImmutableMap.of(
                                           "schema", recordSchema.toString(),
                                           "name", "outputParquet"),
                                         null);

    ETLStage source = new ETLStage("source", sourceConfig);
    ETLStage sink = new ETLStage("sink", sinkConfig);

    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addConnection(source.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(ETLBATCH_ARTIFACT, etlConfig);
    Id.Application appId = Id.Application.from(Id.Namespace.DEFAULT, "parquetTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    // write input to read
    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(recordSchema.toString());
    GenericRecord record = new GenericRecordBuilder(avroSchema)
      .set("i", Integer.MAX_VALUE)
      .set("l", Long.MAX_VALUE)
      .build();
    DataSetManager<TimePartitionedFileSet> inputManager = getDataset("inputParquet");

    long timeInMillis = System.currentTimeMillis();
    inputManager.get().addPartition(timeInMillis, "directory");
    inputManager.flush();
    Location location = inputManager.get().getPartitionByTime(timeInMillis).getLocation();
    location = location.append("file.parquet");
    ParquetWriter<GenericRecord> parquetWriter =
      new AvroParquetWriter<>(new Path(location.toURI()), avroSchema);
    parquetWriter.write(record);
    parquetWriter.close();
    inputManager.flush();

    // run the pipeline
    WorkflowManager workflowManager = appManager.getWorkflowManager(ETLWorkflow.NAME);
    // add a minute to the end time to make sure the newly added partition is included in the run.
    workflowManager.start(ImmutableMap.of("logical.start.time", String.valueOf(timeInMillis + 60 * 1000)));
    workflowManager.waitForFinish(4, TimeUnit.MINUTES);

    DataSetManager<TimePartitionedFileSet> outputManager = getDataset("outputParquet");
    TimePartitionedFileSet newFileSet = outputManager.get();

    List<GenericRecord> newRecords = readOutput(newFileSet, recordSchema);
    Assert.assertEquals(1, newRecords.size());
    Assert.assertEquals(Integer.MAX_VALUE, newRecords.get(0).get("i"));
    Assert.assertEquals(Long.MAX_VALUE, newRecords.get(0).get("l"));
  }

  private ETLBatchConfig constructTPFSETLConfig(String filesetName, String newFilesetName, Schema eventSchema) {
    ETLStage source = new ETLStage(
      "source",
      new ETLPlugin("TPFSAvro",
                    BatchSource.PLUGIN_TYPE,
                    ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA,
                                    eventSchema.toString(),
                                    Properties.TimePartitionedFileSetDataset.TPFS_NAME, filesetName,
                                    Properties.TimePartitionedFileSetDataset.DELAY, "0d",
                                    Properties.TimePartitionedFileSetDataset.DURATION, "2m"),
                    null));
    ETLStage sink = new ETLStage(
      "sink",
      new ETLPlugin("TPFSAvro",
                    BatchSink.PLUGIN_TYPE,
                    ImmutableMap.of(Properties.TimePartitionedFileSetDataset.SCHEMA, eventSchema.toString(),
                                    Properties.TimePartitionedFileSetDataset.TPFS_NAME, newFilesetName),
                    null));

    ETLStage transform = new ETLStage("transform", new ETLPlugin("Projection", Transform.PLUGIN_TYPE,
                                                                 ImmutableMap.<String, String>of(), null));

    return ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(sink)
      .addStage(transform)
      .addConnection(source.getName(), transform.getName())
      .addConnection(transform.getName(), sink.getName())
      .build();
  }
}
