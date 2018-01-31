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

package org.apache.activemq.artemis.uri;

import java.util.List;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.ConfigurationUtils;
import org.junit.Assert;
import org.junit.Test;

public class AcceptorParserTest {

   @Test
   public void testAcceptor() throws Exception {
      List<TransportConfiguration> configs = ConfigurationUtils.parseAcceptorURI("test", "tcp://localhost:8080?tcpSendBufferSize=1048576&tcpReceiveBufferSize=1048576&protocols=openwire&banana=x");

      for (TransportConfiguration config : configs) {
         System.out.println("config:" + config);
         Assert.assertTrue(config.getExtraParams().get("banana").equals("x"));
      }
   }

   @Test
   public void testAcceptorWithQueryParamEscapes() throws Exception {
      List<TransportConfiguration> configs = ConfigurationUtils.parseAcceptorURI("test", "tcp://0.0.0.0:5672?tcpSendBufferSize=1048576;tcpReceiveBufferSize=1048576;virtualTopicConsumerWildcards=Consumer.*.%3E%3B2");

      for (TransportConfiguration config : configs) {
         System.out.println("config:" + config);
         System.out.println(config.getExtraParams().get("virtualTopicConsumerWildcards"));
         Assert.assertTrue(config.getExtraParams().get("virtualTopicConsumerWildcards").equals("Consumer.*.>;2"));
      }
   }
}
