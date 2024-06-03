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
package org.apache.activemq.artemis.core.config.impl;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import org.apache.activemq.artemis.core.deployers.impl.FileConfigurationParser;
import org.apache.activemq.artemis.logs.AssertionLoggerHandler;
import org.apache.activemq.artemis.tests.util.ServerTestBase;
import org.junit.jupiter.api.Test;

/**
 * When running this test from an IDE add this to the test command line so that the AssertionLoggerHandler works properly:
 * -Dlog4j2.configurationFile=file:<path_to_source>/tests/config/log4j2-tests-config.properties
 */
public class WrongRoleFileConfigurationParserTest extends ServerTestBase {

   @Test
   public void testParsingDefaultServerConfig() throws Exception {
      try (AssertionLoggerHandler loggerHandler = new AssertionLoggerHandler()) {
         FileConfigurationParser parser = new FileConfigurationParser();
         ByteArrayInputStream input = new ByteArrayInputStream(configuration.getBytes(StandardCharsets.UTF_8));
         parser.parseMainConfig(input);

         // Using the code only because I don't want a test failing just for someone editing Log text
         assertTrue(loggerHandler.findText("AMQ222177", "create-durable-queue"));
         assertTrue(loggerHandler.findText("AMQ222177", "delete-durable-queue"));
      }
   }

   private static final String configuration = "<configuration xmlns=\"urn:activemq\"\n" +
      "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" +
      "xsi:schemaLocation=\"urn:activemq /schema/artemis-configuration.xsd\">\n" +
      "<name>ActiveMQ.main.config</name>" + "\n" +
      "<log-delegate-factory-class-name>org.apache.activemq.artemis.integration.logging.Log4jLogDelegateFactory</log-delegate-factory-class-name>" + "\n" +
      "<bindings-directory>${jboss.server.data.dir}/activemq/bindings</bindings-directory>" + "\n" +
      "<journal-directory>${jboss.server.data.dir}/activemq/journal</journal-directory>" + "\n" +
      "<journal-min-files>10</journal-min-files>" + "\n" +
      "<large-messages-directory>${jboss.server.data.dir}/activemq/largemessages</large-messages-directory>" + "\n" +
      "<paging-directory>${jboss.server.data.dir}/activemq/paging</paging-directory>" + "\n" +
      "<connectors>" + "\n" +
      "<connector name=\"netty\">tcp://localhost:61616</connector>" + "\n" +
      "<connector name=\"netty-throughput\">tcp://localhost:5545</connector>" + "\n" +
      "<connector name=\"in-vm\">vm://0</connector>" + "\n" +
      "</connectors>" + "\n" +
      "<acceptors>" + "\n" +
      "<acceptor name=\"netty\">tcp://localhost:5545</acceptor>" + "\n" +
      "<acceptor name=\"netty-throughput\">tcp://localhost:5545</acceptor>" + "\n" +
      "<acceptor name=\"in-vm\">vm://0</acceptor>" + "\n" +
      "</acceptors>" + "\n" +
      "<security-settings>" + "\n" +
      "<security-setting match=\"#\">" + "\n" +
      "<permission type=\"createNonDurableQueue\" roles=\"guest\"/>" + "\n" +
      "<permission type=\"deleteNonDurableQueue\" roles=\"guest\"/>" + "\n" +
      "<permission type=\"create-durable-queue\" roles=\"guest\"/>" + "\n" +
      "<permission type=\"delete-durable-queue\" roles=\"guest\"/>" + "\n" +
      "<permission type=\"consume\" roles=\"guest\"/>" + "\n" +
      "<permission type=\"send\" roles=\"guest\"/>" + "\n" +
      "</security-setting>" + "\n" +
      "</security-settings>" + "\n" +
      "<address-settings>" + "\n" +
      "<address-setting match=\"#\">" + "\n" + "<dead-letter-address>DLQ\n</dead-letter-address>" + "\n" + "<expiry-address>ExpiryQueue\n</expiry-address>" + "\n" + "<redelivery-delay>0\n</redelivery-delay>" + "\n" + "<max-size-bytes>10485760\n</max-size-bytes>" + "\n" + "<message-counter-history-day-limit>10</message-counter-history-day-limit>" + "\n" + "<address-full-policy>BLOCK</address-full-policy>" + "\n" +
      "</address-setting>" + "\n" +
      "</address-settings>" + "\n" +
      "</configuration>";
}
