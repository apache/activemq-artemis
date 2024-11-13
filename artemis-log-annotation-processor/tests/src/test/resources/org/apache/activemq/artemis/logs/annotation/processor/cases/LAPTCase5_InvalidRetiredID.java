/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.logs.annotation.processor.cases;

import org.apache.activemq.artemis.logs.annotation.LogBundle;
import org.apache.activemq.artemis.logs.annotation.Message;

// Test class used by LogAnnotationProcessorTest to check failure modes.

// Regex allows IDs 0-9, use higher value for a retiredIDs entry to check it fails.
@LogBundle(projectCode = "LAPTCase5", regexID="[0-9]{1}", retiredIDs = { 2, 4, 10})
public interface LAPTCase5_InvalidRetiredID {

   @Message(id = 1, value = "ignoreMe")
   String ignoreMe();

   @Message(id = 3, value = "ignoreMeAlso")
   String ignoreMeAlso();
}
