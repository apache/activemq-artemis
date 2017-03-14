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
package org.apache.activemq.artemis.tests.integration.jms.server.management;

import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.management.AddressControl;
import org.apache.activemq.artemis.tests.integration.management.ManagementControlHelper;
import org.apache.activemq.artemis.tests.util.JMSClusteredTestBase;
import org.apache.activemq.artemis.tests.util.Wait;
import org.junit.Test;

public class TopicControlClusterTest extends JMSClusteredTestBase {

   @Test
   public void testClusteredSubscriptionCount() throws Exception {
      final String topicName = "t1";
      final SimpleString simpleTopicName = SimpleString.toSimpleString(topicName);

      try (Connection conn1 = cf1.createConnection(); Connection conn2 = cf2.createConnection()) {
         conn1.setClientID("someClient1");
         conn2.setClientID("someClient2");

         Topic topic1 = createTopic(topicName);

         Topic topic2 = (Topic) context2.lookup("/topic/" + topicName);

         Session session1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         session1.createDurableSubscriber(topic1, "sub1_1");
         session1.createDurableSubscriber(topic1, "sub1_2");

         Session session2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         session2.createDurableSubscriber(topic2, "sub2");

         AddressControl topicControl1 = ManagementControlHelper.createAddressControl(simpleTopicName, mBeanServer1);
         AddressControl topicControl2 = ManagementControlHelper.createAddressControl(simpleTopicName, mBeanServer2);

         assertTrue("There should be 3 subscriptions on the topic, 2 local and 1 remote.",
                    Wait.waitFor(() -> topicControl1.getQueueNames().length == 3, 2000));

         assertTrue("There should be 3 subscriptions on the topic, 1 local and 2 remote.",
                    Wait.waitFor(() -> topicControl2.getQueueNames().length == 3, 2000));
      }

      jmsServer1.destroyTopic("t1");
      jmsServer2.destroyTopic("t1");
   }
}
