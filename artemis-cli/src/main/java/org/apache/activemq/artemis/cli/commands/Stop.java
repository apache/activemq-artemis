/**
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
package org.apache.activemq.artemis.cli.commands;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import org.apache.activemq.artemis.dto.BrokerDTO;
import org.apache.activemq.artemis.factory.BrokerFactory;

import java.io.File;
import java.net.URI;

@Command(name = "stop", description = "stops the broker instance")
public class Stop implements Action
{
   @Arguments(description = "Broker Configuration URI, default 'xml:${ARTEMIS_INSTANCE}/etc/bootstrap.xml'")
   String configuration;

   @Override
   public Object execute(ActionContext context) throws Exception
   {
      /* We use File URI for locating files.  The ARTEMIS_HOME variable is used to determine file paths.  For Windows
      the ARTEMIS_HOME variable will include back slashes (An invalid file URI character path separator).  For this
      reason we overwrite the ARTEMIS_HOME variable with backslashes replaced with forward slashes. */
      String activemqHome = System.getProperty("artemis.instance").replace("\\", "/");
      System.setProperty("artemis.instance", activemqHome);

      if (configuration == null)
      {
         configuration = "xml:" + activemqHome + "/etc/bootstrap.xml";
      }

      // To support Windows paths as explained above.
      configuration = configuration.replace("\\", "/");

      BrokerDTO broker = BrokerFactory.createBrokerConfiguration(configuration);

      String fileName = new URI(broker.server.configuration).getSchemeSpecificPart();

      File file = new File(fileName).getParentFile();

      File stopFile = new File(file, "STOP_ME");

      stopFile.createNewFile();

      return null;
   }
}
