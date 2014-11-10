/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.hornetq.tests.stress.journal;

import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.junit.Test;

import org.hornetq.tools.XmlDataExporter;
import org.hornetq.tools.XmlDataImporter;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.ServiceTestBase;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

public class XmlImportExportStressTest extends ServiceTestBase
{
   public static final int CONSUMER_TIMEOUT = 5000;

   @Test
   public void testHighVolume() throws Exception
   {
      final String FILE_NAME = getTestDir() + "/export.out";

      final String QUEUE_NAME = "A1";
      HornetQServer server = createServer(true);
      server.start();
      ServerLocator locator = createInVMNonHALocator();
      ClientSessionFactory factory = locator.createSessionFactory();
      ClientSession session = factory.createSession(false, false, false);

      session.createQueue(QUEUE_NAME, QUEUE_NAME, true);

      ClientProducer producer = session.createProducer(QUEUE_NAME);

      ClientMessage msg = session.createMessage(true);
      final int SIZE = 10240;
      final int COUNT = 20000;
      byte[] bodyTst = new byte[SIZE];
      for (int i = 0; i < SIZE; i++)
      {
         bodyTst[i] = (byte) (i + 1);
      }

      msg.getBodyBuffer().writeBytes(bodyTst);
      assertEquals(bodyTst.length, msg.getBodySize());

      for (int i = 0; i < COUNT; i++)
      {
         producer.send(msg);
         if (i % 500 == 0)
         {
            System.out.println("Sent " + i);
            session.commit();
         }
      }

      session.commit();

      session.close();
      locator.close();
      server.stop();

      System.out.println("Writing XML...");
      FileOutputStream xmlOutputStream = new FileOutputStream(FILE_NAME);
      BufferedOutputStream bufferOut = new BufferedOutputStream(xmlOutputStream);
      XmlDataExporter xmlDataExporter = new XmlDataExporter(bufferOut, getBindingsDir(), getJournalDir(), getPageDir(), getLargeMessagesDir());
      xmlDataExporter.writeXMLData();
      bufferOut.close();
      System.out.println("Done writing XML.");

      deleteDirectory(new File(getJournalDir()));
      deleteDirectory(new File(getBindingsDir()));
      deleteDirectory(new File(getPageDir()));
      deleteDirectory(new File(getLargeMessagesDir()));
      server.start();
      locator = createInVMNonHALocator();
      factory = locator.createSessionFactory();
      session = factory.createSession(false, false, true);
      ClientSession managementSession = factory.createSession(false, true, true);

      System.out.println("Reading XML...");
      FileInputStream xmlInputStream = new FileInputStream(FILE_NAME);
      XmlDataImporter xmlDataImporter = new XmlDataImporter(xmlInputStream, session, managementSession);
      xmlDataImporter.processXml();
      xmlInputStream.close();
      System.out.println("Done reading XML.");

      ClientConsumer consumer = session.createConsumer(QUEUE_NAME);
      session.start();

      for (int i = 0; i < COUNT; i++)
      {
         msg = consumer.receive(CONSUMER_TIMEOUT);
         assertNotNull(msg);

         msg.acknowledge();
         if (i % 500 == 0)
         {
            System.out.println("Received " + i);
            session.commit();
         }

         assertEquals(msg.getBodySize(), bodyTst.length);
         byte[] bodyRead = new byte[bodyTst.length];
         msg.getBodyBuffer().readBytes(bodyRead);
         assertEqualsByteArrays(bodyTst, bodyRead);
      }

      session.close();
      locator.close();
      server.stop();
      File temp = new File(FILE_NAME);
      temp.delete();
   }
}
