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
package org.apache.activemq.artemis.server;

/**
 *         This class will be spawned in a new vm and will call the bootstrap
 */
public class SpawnedActiveMQBootstrap
{
   public static void main(final String[] args)
   {
      ActiveMQBootstrap bootstrap;
      try
      {
         bootstrap = new ActiveMQBootstrap(args);
         bootstrap.execute();
         System.out.println("STARTED::");
      }
      catch (Throwable e)
      {
         System.out.println("FAILED::" + e.getMessage());
      }
   }
}
