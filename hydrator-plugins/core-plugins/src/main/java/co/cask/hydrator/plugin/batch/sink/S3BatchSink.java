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

package co.cask.hydrator.plugin.batch.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.hydrator.common.ReferenceBatchSink;
import co.cask.hydrator.common.ReferencePluginConfig;
import co.cask.hydrator.common.batch.sink.SinkOutputFormatProvider;
import co.cask.hydrator.plugin.common.Properties;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * {@link S3BatchSink} that stores the data of the latest run of an adapter in S3.
 * @param <KEY_OUT> the type of key the sink outputs
 * @param <VAL_OUT> the type of value the sink outputs
 */
public abstract class S3BatchSink<KEY_OUT, VAL_OUT> extends ReferenceBatchSink<StructuredRecord, KEY_OUT, VAL_OUT> {

  public static final String PATH_DESC = "The S3 path where the data is stored. Example: 's3n://logs'.";
  private static final String ACCESS_ID_DESCRIPTION = "Access ID of the Amazon S3 instance to connect to.";
  private static final String ACCESS_KEY_DESCRIPTION = "Access Key of the Amazon S3 instance to connect to.";
  private static final String PATH_FORMAT_DESCRIPTION = "The format for the path that will be suffixed to the " +
    "basePath; for example: the format 'yyyy-MM-dd-HH-mm' will create a file path ending in '2015-01-01-20-42'. " +
    "Default format used is 'yyyy-MM-dd-HH-mm'.";
  private static final String FILESYSTEM_PROPERTIES_DESCRIPTION = "A JSON string representing a map of properties " +
    "needed for the distributed file system.";
  private static final String DEFAULT_PATH_FORMAT = "yyyy-MM-dd-HH-mm";
  private static final Gson GSON = new Gson();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  private final S3BatchSinkConfig config;
  protected S3BatchSink(S3BatchSinkConfig config) {
    super(config);
    this.config = config;
    // update fileSystemProperties to include accessID and accessKey, so prepareRun can only set fileSystemProperties
    // in configuration, and not deal with accessID and accessKey separately
    // do not create file system properties if macros were provided unless in a test case
    if (!this.config.containsMacro("fileSystemProperties") && !this.config.containsMacro("accessID") &&
        !this.config.containsMacro("accessKey")) {
      this.config.fileSystemProperties = updateFileSystemProperties(this.config.fileSystemProperties,
                                                                    this.config.accessID, this.config.accessKey);
    }
  }

  @Override
  public final void prepareRun(BatchSinkContext context) {
    OutputFormatProvider outputFormatProvider = createOutputFormatProvider(context);
    Map<String, String> outputConfig = new HashMap<>(outputFormatProvider.getOutputFormatConfiguration());

    if (config.fileSystemProperties != null) {
      Map<String, String> properties = GSON.fromJson(config.fileSystemProperties, MAP_STRING_STRING_TYPE);
      outputConfig.putAll(properties);
    }
    context.addOutput(Output.of(config.referenceName, new SinkOutputFormatProvider(
      outputFormatProvider.getOutputFormatClassName(), outputConfig)));
  }

  /**
   * Returns a {@link OutputFormatProvider} to be used for output.
   */
  protected abstract OutputFormatProvider createOutputFormatProvider(BatchSinkContext context);

  private static String updateFileSystemProperties(@Nullable String fileSystemProperties, String accessID,
                                                   String accessKey) {
    Map<String, String> providedProperties;
    if (fileSystemProperties == null) {
      providedProperties = new HashMap<>();
    } else {
      providedProperties = GSON.fromJson(fileSystemProperties, MAP_STRING_STRING_TYPE);
    }
    providedProperties.put("fs.s3n.awsAccessKeyId", accessID);
    providedProperties.put("fs.s3n.awsSecretAccessKey", accessKey);
    return GSON.toJson(providedProperties);
  }

  @VisibleForTesting
  S3BatchSinkConfig getConfig() {
    return config;
  }

  /**
   * S3 Sink configuration.
   */
  public static class S3BatchSinkConfig extends ReferencePluginConfig {

    @Name(Properties.S3BatchSink.BASE_PATH)
    @Description(PATH_DESC)
    @Macro
    protected String basePath;

    @Name(Properties.S3.ACCESS_ID)
    @Description(ACCESS_ID_DESCRIPTION)
    @Macro
    protected String accessID;

    @Name(Properties.S3.ACCESS_KEY)
    @Description(ACCESS_KEY_DESCRIPTION)
    @Macro
    protected String accessKey;

    @Name(Properties.S3BatchSink.PATH_FORMAT)
    @Description(PATH_FORMAT_DESCRIPTION)
    @Nullable
    @Macro
    protected String pathFormat;

    @Description(FILESYSTEM_PROPERTIES_DESCRIPTION)
    @Nullable
    @Macro
    protected String fileSystemProperties;

    public S3BatchSinkConfig() {
      // Set default value for Nullable properties.
      super("");
      this.pathFormat = DEFAULT_PATH_FORMAT;
      this.fileSystemProperties = updateFileSystemProperties(null, accessID, accessKey);
    }

    public S3BatchSinkConfig(String referenceName, String basePath, String accessID, String accessKey,
                             @Nullable String pathFormat, @Nullable String fileSystemProperties) {
      super(referenceName);
      this.basePath = basePath;
      this.pathFormat = pathFormat == null || pathFormat.isEmpty() ? DEFAULT_PATH_FORMAT : pathFormat;
      this.accessID = accessID;
      this.accessKey = accessKey;
      this.fileSystemProperties = updateFileSystemProperties(fileSystemProperties, accessID, accessKey);
    }
  }
}
