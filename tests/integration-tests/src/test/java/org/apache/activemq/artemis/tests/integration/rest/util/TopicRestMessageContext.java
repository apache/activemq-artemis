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
package org.apache.activemq.artemis.tests.integration.rest.util;

import java.io.IOException;

import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;

public class TopicRestMessageContext extends RestMessageContext {

   public static final String PREFIX_TOPIC = "/topics/";

   private boolean durableSub;

   public TopicRestMessageContext(RestAMQConnection restAMQConnection,
                                  String topic,
                                  boolean durable) throws IOException {
      super(restAMQConnection, topic);
      this.durableSub = durable;
   }

   @Override
   protected String getDestLink() {
      return PREFIX_TOPIC + destination;
   }

   @Override
   protected String getPullConsumerUri() {
      return getDestLink() + "/pull-subscriptions";
   }

   @Override
   public void initPullConsumers() throws IOException {
      String pullUri = getPullConsumerUri();
      CloseableHttpResponse response = null;
      if (this.durableSub || !this.autoAck) {
         String extraOpt = "durable=" + this.durableSub + "&autoAck=" + this.autoAck;
         response = connection.post(pullUri, "application/x-www-form-urlencoded", extraOpt);
      } else {
         response = connection.post(pullUri);
      }

      int code = ResponseUtil.getHttpCode(response);

      try {
         if (code == 201) {
            Header header = response.getFirstHeader("Location");
            contextMap.put(KEY_PULL_CONSUMERS_LOC, header.getValue());
            header = response.getFirstHeader(KEY_MSG_CONSUME_NEXT);
            contextMap.put(KEY_MSG_CONSUME_NEXT, header.getValue());
            header = response.getFirstHeader(KEY_MSG_ACK_NEXT);
            if (header != null) {
               contextMap.put(KEY_MSG_ACK_NEXT, header.getValue());
            }
         } else {
            throw new IllegalStateException("Failed to init pull consumer " + ResponseUtil.getDetails(response));
         }
      } finally {
         response.close();
      }
   }

   @Override
   protected String getPushLink(String pushTarget) {
      return PREFIX_TOPIC + pushTarget;
   }

}
