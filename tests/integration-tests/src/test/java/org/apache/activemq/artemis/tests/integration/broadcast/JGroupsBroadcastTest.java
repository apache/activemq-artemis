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
package org.apache.activemq.artemis.tests.integration.broadcast;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.activemq.artemis.api.core.BroadcastEndpoint;
import org.apache.activemq.artemis.api.core.BroadcastEndpointFactory;
import org.apache.activemq.artemis.api.core.ChannelBroadcastEndpointFactory;
import org.apache.activemq.artemis.api.core.jgroups.JChannelManager;
import org.apache.activemq.artemis.tests.util.ArtemisTestCase;
import org.jgroups.JChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class JGroupsBroadcastTest extends ArtemisTestCase {

   @AfterEach
   public void cleanupJChannel() {
      JChannelManager.getInstance().clear();
   }

   @BeforeEach
   public void prepareJChannel() {
      JChannelManager.getInstance().setLoopbackMessages(true);
   }

   @Test
   public void testRefCount() throws Exception {
      JChannel channel = null;
      JChannel newChannel = null;

      try {

         channel = new JChannel("udp.xml");

         String channelName1 = "channel1";

         BroadcastEndpointFactory jgroupsBroadcastCfg1 = new ChannelBroadcastEndpointFactory(channel, channelName1);

         BroadcastEndpoint channelEndpoint1 = jgroupsBroadcastCfg1.createBroadcastEndpoint();

         BroadcastEndpoint channelEndpoint2 = jgroupsBroadcastCfg1.createBroadcastEndpoint();

         BroadcastEndpoint channelEndpoint3 = jgroupsBroadcastCfg1.createBroadcastEndpoint();

         channelEndpoint1.close(true);

         assertTrue(channel.isOpen());

         channelEndpoint2.close(true);

         assertTrue(channel.isOpen());

         channelEndpoint3.close(true);

         assertTrue(channel.isOpen());

         channel.close();

         //after we close the last endpoint reference counting will close the channel so once we create a new one the
         // channel wrapper is recreated
         try {
            channelEndpoint2.openClient();
            fail("this should be closed");
         } catch (Exception e) {
         }

         newChannel = new JChannel("udp.xml");

         jgroupsBroadcastCfg1 = new ChannelBroadcastEndpointFactory(newChannel, channelName1);

         channelEndpoint1 = jgroupsBroadcastCfg1.createBroadcastEndpoint();

         channelEndpoint1.openClient();

      } catch (Exception e) {
         e.printStackTrace();
         throw e;
      } finally {
         try {
            channel.close();
         } catch (Throwable ignored) {

         }
         try {
            newChannel.close();
         } catch (Throwable ignored) {

         }
      }
   }

}
