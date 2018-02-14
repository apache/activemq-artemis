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

package org.apache.activemq.cli.test;

import java.io.InputStream;

import org.apache.activemq.artemis.cli.commands.Create;
import org.junit.Assert;
import org.junit.Test;

public class StreamClassPathTest {

   /**
    * Validate if all the known resources are available on the classpath for the jar
    */
   @Test
   public void testFindStreams() throws Exception {
      openStream(Create.BIN_ARTEMIS_CMD);
      openStream(Create.BIN_ARTEMIS_SERVICE_EXE);
      openStream(Create.BIN_ARTEMIS_SERVICE_XML);
      openStream("etc/" + Create.ETC_ARTEMIS_PROFILE_CMD);
      openStream(Create.BIN_ARTEMIS);
      openStream(Create.BIN_ARTEMIS_SERVICE);
      openStream("etc/" + Create.ETC_ARTEMIS_PROFILE);
      openStream("etc/" + Create.ETC_LOGGING_PROPERTIES);
      openStream("etc/" + Create.ETC_BOOTSTRAP_XML);
      openStream("etc/" + Create.ETC_MANAGEMENT_XML);
      openStream("etc/" + Create.ETC_BROKER_XML);
      openStream("etc/" + Create.ETC_ARTEMIS_ROLES_PROPERTIES);
      openStream("etc/" + Create.ETC_ARTEMIS_USERS_PROPERTIES);
      openStream(Create.ETC_REPLICATED_SETTINGS_TXT);
      openStream(Create.ETC_REPLICATED_SETTINGS_TXT);
      openStream(Create.ETC_SHARED_STORE_SETTINGS_TXT);
      openStream(Create.ETC_CLUSTER_SECURITY_SETTINGS_TXT);
      openStream(Create.ETC_CLUSTER_SETTINGS_TXT);
      openStream(Create.ETC_CONNECTOR_SETTINGS_TXT);
      openStream(Create.ETC_BOOTSTRAP_WEB_SETTINGS_TXT);
      openStream(Create.ETC_JOURNAL_BUFFER_SETTINGS);
      openStream(Create.ETC_AMQP_ACCEPTOR_TXT);
      openStream(Create.ETC_MQTT_ACCEPTOR_TXT);
      openStream(Create.ETC_HORNETQ_ACCEPTOR_TXT);
      openStream(Create.ETC_STOMP_ACCEPTOR_TXT);
      openStream(Create.ETC_PING_TXT);
      openStream(Create.ETC_COMMENTED_PING_TXT);
      openStream(Create.ETC_GLOBAL_MAX_SPECIFIED_TXT);
      openStream(Create.ETC_GLOBAL_MAX_DEFAULT_TXT);
      openStream("etc/" + Create.ETC_JOLOKIA_ACCESS_XML);
      openStream(Create.ETC_DATABASE_STORE_TXT);

   }

   private void openStream(String source) throws Exception {
      Create create = new Create();
      InputStream in = create.openStream(source);
      Assert.assertNotNull(source + " not found", in);
      in.close();
   }
}
