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
package org.apache.activemq.tools;

import java.io.File;

import org.apache.activemq.core.persistence.impl.journal.DescribeJournal;
import org.apache.activemq.core.server.impl.FileLockNodeManager;

/**
 * Writes a human-readable interpretation of the contents of a ActiveMQ {@link org.apache.activemq.core.journal.Journal}.
 * <p>
 * To run this class with Maven, use:
 *
 * <pre>
 * cd activemq-server
 * mvn -q exec:java -Dexec.args="/foo/activemq/bindings /foo/activemq/journal" -Dexec.mainClass="org.apache.activemq.tools.PrintData"
 * </pre>
 * @author clebertsuconic
 */
public class PrintData // NO_UCD (unused code)
{

   protected static void printData(String bindingsDirectory, String messagesDirectory)
   {
      File serverLockFile = new File(messagesDirectory, "server.lock");

      if (serverLockFile.isFile())
      {
         try
         {
            FileLockNodeManager fileLock = new FileLockNodeManager(messagesDirectory, false);
            fileLock.start();
            System.out.println("********************************************");
            System.out.println("Server's ID=" + fileLock.getNodeId().toString());
            System.out.println("********************************************");
            fileLock.stop();
         }
         catch (Exception e)
         {
            e.printStackTrace();
         }
      }

      System.out.println("********************************************");
      System.out.println("B I N D I N G S  J O U R N A L");
      System.out.println("********************************************");

      try
      {
         DescribeJournal.describeBindingsJournal(bindingsDirectory);
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }

      System.out.println("********************************************");
      System.out.println("M E S S A G E S   J O U R N A L");
      System.out.println("********************************************");

      try
      {
         DescribeJournal.describeMessagesJournal(messagesDirectory);
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }
}
