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
package org.apache.activemq.artemis.tests.integration.cluster.util;

import org.apache.activemq.artemis.core.config.Configuration;

/**
 * A RemoteServerConfiguration.
 * <br>
 * These classes are initialized through their class name through {@link Class#newInstance()}.
 * Therefore they must have a no argument constructor, and if they are inner classes they must be
 * static.
 */
public abstract class RemoteServerConfiguration {

   public RemoteServerConfiguration() {
   }

   public abstract Configuration getConfiguration();
}
