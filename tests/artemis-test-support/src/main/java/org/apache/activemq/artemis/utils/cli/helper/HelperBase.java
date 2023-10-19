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

package org.apache.activemq.artemis.utils.cli.helper;

import java.io.File;
import java.lang.invoke.MethodHandles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HelperBase {

   private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   public static final String ARTEMIS_HOME_PROPERTY = "artemis.distribution.output";

   File artemisHome;
   File artemisInstance;

   HelperBase(String homeProperty) {
      String propertyHome = System.getProperty(homeProperty);
      if (propertyHome == null) {
         throw new IllegalArgumentException("System property " + propertyHome + " not defined");
      }
      if (propertyHome != null) {
         artemisHome = new File(propertyHome);
      }
      logger.debug("using artemisHome as {}", artemisHome);
      if (!artemisHome.exists()) {
         throw new IllegalArgumentException(artemisHome + " folder does not exist in the file system");
      }
      if (!new File(artemisHome, "/bin").exists() || !new File(artemisHome, "/bin/artemis").exists()) {
         throw new IllegalArgumentException("invalid bin folder");
      }
   }

   public File getArtemisHome() {
      return artemisHome;
   }

   public HelperBase setArtemisHome(File artemisHome) {
      this.artemisHome = artemisHome;
      return this;
   }

   public File getArtemisInstance() {
      return artemisInstance;
   }

   public HelperBase setArtemisInstance(File artemisInstance) {
      this.artemisInstance = artemisInstance;
      return this;
   }

   public String[] getArgs() {
      return args;
   }

   public HelperBase setArgs(String... args) {
      this.args = args;
      return this;
   }

   String[] args = new String[0];
}
