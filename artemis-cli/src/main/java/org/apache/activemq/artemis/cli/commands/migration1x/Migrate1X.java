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
package org.apache.activemq.artemis.cli.commands.migration1x;

import java.io.File;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import org.apache.activemq.artemis.cli.commands.ActionAbstract;
import org.apache.activemq.artemis.cli.commands.ActionContext;
import org.apache.activemq.artemis.tools.migrate.config.Main;

@Command(name = "migrate1x", description = "Migrates the configuration of a 1.x Artemis Broker")
public class Migrate1X extends ActionAbstract {

   @Arguments(description = "The instance directory to hold the broker's configuration and data.  Path must be writable.", required = true)
   File directory;

   @Override
   public Object execute(ActionContext context) throws Exception {
      if (!directory.exists()) {
         throw new RuntimeException(String.format("The path '%s' does not exist.", directory));
      }
      super.execute(context);
      return run(context);
   }

   public Object run(ActionContext context) throws Exception {
      Main.scanAndTransform(directory);
      return null;
   }

}
