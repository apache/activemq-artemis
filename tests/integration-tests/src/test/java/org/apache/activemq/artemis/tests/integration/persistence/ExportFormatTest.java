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
package org.apache.activemq.artemis.tests.integration.persistence;

import java.io.StringReader;

import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.cli.commands.tools.DecodeJournal;
import org.apache.activemq.artemis.cli.commands.tools.EncodeJournal;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.tests.util.ActiveMQTestBase;
import org.junit.Ignore;
import org.junit.Test;

public class ExportFormatTest extends ActiveMQTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Case the format was changed, and the change was agreed, use _testCreateFormat to recreate this field
   String bindingsFile = "#File,JournalFileImpl: (activemq-bindings-1.bindings id = 1, recordID = 1)\n" +
      "operation@AddRecord,id@1,userRecordType@24,length@8,isUpdate@false,compactCount@0,data@AAAAAH____8=\n" +
      "operation@AddRecordTX,txID@2,id@3,userRecordType@21,length@43,isUpdate@false,compactCount@0,data@AAAABEEAMQAAAAAEQQAxAAABAAAAFHUAcwBlAHIAPQBuAHUAbABsADsAAA==\n" +
      "operation@Commit,txID@2,numberOfRecords@1\n" +
      "operation@AddRecord,id@20,userRecordType@24,length@8,isUpdate@false,compactCount@0,data@AAAAAAAAABQ=\n" +
      "#File,JournalFileImpl: (activemq-bindings-2.bindings id = 2, recordID = 2)";

   // Case the format was changed, and the change was agreed, use _testCreateFormat to recreate this field
   String journalFile = "#File,JournalFileImpl: (activemq-data-1.amq id = 1, recordID = 1)\n" +
      "operation@AddRecordTX,txID@0,id@5,userRecordType@31,length@65,isUpdate@false,compactCount@0,data@AAAAEQAAAE4AAAAAAAAABQEAAAAEQQAxAAAA_wAAAAAAAAAAAAABTh2ZufQEAQAAAAEAAAAGawBlAHkABgAAAAA=\n" +
      "operation@UpdateTX,txID@0,id@5,userRecordType@32,length@8,isUpdate@true,compactCount@0,data@AAAAAAAAAAM=\n" +
      "operation@AddRecordTX,txID@0,id@6,userRecordType@31,length@65,isUpdate@false,compactCount@0,data@AAAAEQAAAE4AAAAAAAAABgEAAAAEQQAxAAAA_wAAAAAAAAAAAAABTh2ZufcEAQAAAAEAAAAGawBlAHkABgAAAAE=\n" +
      "operation@UpdateTX,txID@0,id@6,userRecordType@32,length@8,isUpdate@true,compactCount@0,data@AAAAAAAAAAM=\n" +
      "operation@AddRecordTX,txID@0,id@7,userRecordType@31,length@65,isUpdate@false,compactCount@0,data@AAAAEQAAAE4AAAAAAAAABwEAAAAEQQAxAAAA_wAAAAAAAAAAAAABTh2ZufcEAQAAAAEAAAAGawBlAHkABgAAAAI=\n" +
      "operation@UpdateTX,txID@0,id@7,userRecordType@32,length@8,isUpdate@true,compactCount@0,data@AAAAAAAAAAM=\n" +
      "operation@AddRecordTX,txID@0,id@8,userRecordType@31,length@65,isUpdate@false,compactCount@0,data@AAAAEQAAAE4AAAAAAAAACAEAAAAEQQAxAAAA_wAAAAAAAAAAAAABTh2ZufgEAQAAAAEAAAAGawBlAHkABgAAAAM=\n" +
      "operation@UpdateTX,txID@0,id@8,userRecordType@32,length@8,isUpdate@true,compactCount@0,data@AAAAAAAAAAM=\n" +
      "operation@AddRecordTX,txID@0,id@9,userRecordType@31,length@65,isUpdate@false,compactCount@0,data@AAAAEQAAAE4AAAAAAAAACQEAAAAEQQAxAAAA_wAAAAAAAAAAAAABTh2ZufgEAQAAAAEAAAAGawBlAHkABgAAAAQ=\n" +
      "operation@UpdateTX,txID@0,id@9,userRecordType@32,length@8,isUpdate@true,compactCount@0,data@AAAAAAAAAAM=\n" +
      "operation@Commit,txID@0,numberOfRecords@10\n" +
      "operation@AddRecord,id@13,userRecordType@31,length@65,isUpdate@false,compactCount@0,data@AAAAEQAAAE4AAAAAAAAADQEAAAAEQQAxAAAA_wAAAAAAAAAAAAABTh2ZuhQEAQAAAAEAAAAGawBlAHkABgAAAAU=\n" +
      "operation@Update,id@13,userRecordType@32,length@8,isUpdate@true,compactCount@0,data@AAAAAAAAAAM=\n" +
      "operation@AddRecord,id@14,userRecordType@31,length@65,isUpdate@false,compactCount@0,data@AAAAEQAAAE4AAAAAAAAADgEAAAAEQQAxAAAA_wAAAAAAAAAAAAABTh2ZuhcEAQAAAAEAAAAGawBlAHkABgAAAAY=\n" +
      "operation@Update,id@14,userRecordType@32,length@8,isUpdate@true,compactCount@0,data@AAAAAAAAAAM=\n" +
      "operation@AddRecord,id@15,userRecordType@31,length@65,isUpdate@false,compactCount@0,data@AAAAEQAAAE4AAAAAAAAADwEAAAAEQQAxAAAA_wAAAAAAAAAAAAABTh2ZuhsEAQAAAAEAAAAGawBlAHkABgAAAAc=\n" +
      "operation@Update,id@15,userRecordType@32,length@8,isUpdate@true,compactCount@0,data@AAAAAAAAAAM=\n" +
      "operation@AddRecord,id@16,userRecordType@31,length@65,isUpdate@false,compactCount@0,data@AAAAEQAAAE4AAAAAAAAAEAEAAAAEQQAxAAAA_wAAAAAAAAAAAAABTh2Zuh4EAQAAAAEAAAAGawBlAHkABgAAAAg=\n" +
      "operation@Update,id@16,userRecordType@32,length@8,isUpdate@true,compactCount@0,data@AAAAAAAAAAM=\n" +
      "operation@AddRecord,id@17,userRecordType@31,length@65,isUpdate@false,compactCount@0,data@AAAAEQAAAE4AAAAAAAAAEQEAAAAEQQAxAAAA_wAAAAAAAAAAAAABTh2ZuiIEAQAAAAEAAAAGawBlAHkABgAAAAk=\n" +
      "operation@Update,id@17,userRecordType@32,length@8,isUpdate@true,compactCount@0,data@AAAAAAAAAAM=\n" +
      "#File,JournalFileImpl: (activemq-data-2.amq id = 2, recordID = 2)";

   @Test
   @Ignore // Used to update the format, if you need to use this it means the data format was broken, Be careful on updating the format!
   public void testCreateFormat() throws Exception
   {
      ActiveMQServer server = createServer(true);
      server.start();

      ServerLocator locator = createInVMNonHALocator();

      ClientSessionFactory factory = createSessionFactory(locator);
      ClientSession session = factory.createSession(false, false, false);
      session.createQueue("A1", "A1", true);

      ClientProducer producer = session.createProducer("A1");
      for (int i = 0; i < 5; i++)
      {
         ClientMessage msg = session.createMessage(true);
         msg.putIntProperty("key", i);
         producer.send(msg);
      }
      session.commit();

      session.close();

      session = factory.createSession(false, true, true);

      producer = session.createProducer("A1");

      for (int i = 5; i < 10; i++)
      {
         ClientMessage msg = session.createMessage(true);
         msg.putIntProperty("key", i);
         producer.send(msg);
      }

      locator.close();
      server.stop();

      System.out.println();
      System.out.println("copy & paste the following as bindingsFile:");

      EncodeJournal.exportJournal(server.getConfiguration().getBindingsLocation().getAbsolutePath(), "activemq-bindings", "bindings", 2, 1048576, System.out);

      System.out.println();
      System.out.println("copy & paste the following as dataFile:");

      EncodeJournal.exportJournal(server.getConfiguration().getJournalLocation().getAbsolutePath(), "activemq-data", "amq", 2, 102400, System.out);
   }

   @Test
   public void testConsumeFromFormat() throws Exception
   {
      ActiveMQServer server = createServer(true);

      DecodeJournal.importJournal(server.getConfiguration().getJournalLocation().getAbsolutePath(), "activemq-data", "amq", 2, 102400, new StringReader(journalFile));
      DecodeJournal.importJournal(server.getConfiguration().getBindingsLocation().getAbsolutePath(),
                                  "activemq-bindings",
                                  "bindings",
                                  2,
                                  1048576,
                                  new StringReader(bindingsFile));
      server.start();

      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory factory = createSessionFactory(locator);
      ClientSession session = factory.createSession();
      session.start();

      ClientConsumer consumer = session.createConsumer("A1");
      for (int i = 0; i < 10; i++)
      {
         ClientMessage msg = consumer.receive(5000);
         assertNotNull(msg);
         msg.acknowledge();
         assertEquals(i, msg.getIntProperty("key").intValue());
      }

      session.commit();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
