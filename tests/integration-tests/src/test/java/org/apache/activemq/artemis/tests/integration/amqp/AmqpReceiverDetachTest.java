/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.tests.integration.amqp;

import org.apache.activemq.transport.amqp.client.AmqpClient;
import org.apache.activemq.transport.amqp.client.AmqpConnection;
import org.apache.activemq.transport.amqp.client.AmqpFrameValidator;
import org.apache.activemq.transport.amqp.client.AmqpReceiver;
import org.apache.activemq.transport.amqp.client.AmqpSession;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.transport.Detach;
import org.junit.Test;

public class AmqpReceiverDetachTest extends AmqpClientTestSupport {

   private static final long TIMEOUT_MS = 60000;

   @Test(timeout = TIMEOUT_MS)
   public void testNoRemoteDetachLinkOnCloseSession() throws Exception {
      final String address = getTestName();
      final AmqpClient client = createAmqpClient();
      final AmqpConnection connection = client.connect();
      final AmqpSession session = connection.createSession();
      final AmqpReceiver receiver = session.createReceiver(address);
      final AmqpFrameValidator receivedFrameInspector = new AmqpFrameValidator() {

         @Override
         public void inspectDetach(Detach detach, Binary encoded) {
            markAsInvalid("can't receive any remote detach after a session close");
         }
      };
      connection.setReceivedFrameInspector(receivedFrameInspector);
      session.close();
      connection.close();
      receivedFrameInspector.assertValid();
   }
}
