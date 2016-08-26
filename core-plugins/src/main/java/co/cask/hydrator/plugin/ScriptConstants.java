/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.hydrator.plugin.transform.JavaTypeConverters;

/**
 * Constants for transforms using JavaScript.
 */
public class ScriptConstants {
  public static final String HELPER_NAME = "CDAP_ETL_SCRIPT_HELPER";
  /**
   * The implementation here must match the {@link JavaTypeConverters} interface.
   */
  public static final String HELPER_DEFINITION = "var " + HELPER_NAME + " = new Object();" +
    HELPER_NAME + ".mapToJSObject = function(map) { " +
    "var result = {}; var it = map.entrySet().iterator(); " +
    "while (it.hasNext()) { var entry = it.next(); result[entry.getKey()] = entry.getValue(); } " +
    "return result; }";
}
