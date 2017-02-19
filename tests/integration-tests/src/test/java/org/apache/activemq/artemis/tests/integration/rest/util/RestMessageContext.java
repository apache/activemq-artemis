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

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.util.EntityUtils;

public abstract class RestMessageContext implements Closeable {

   public static final String KEY_MSG_CREATE = "msg-create";
   public static final String KEY_MSG_CREATE_ID = "msg-create-with-id";
   public static final String KEY_MSG_PULL = "msg-pull-consumers";
   public static final String KEY_MSG_PUSH = "msg-push-consumers";

   public static final String KEY_MSG_PULL_SUB = "msg-pull-subscriptions";
   public static final String KEY_MSG_PUSH_SUB = "msg-push-subscriptions";

   public static final String KEY_MSG_CREATE_NEXT = "msg-create-next";

   public static final String KEY_PULL_CONSUMERS_LOC = "pull-consumers-location";
   public static final String KEY_MSG_CONSUME_NEXT = "msg-consume-next";

   public static final String KEY_MSG_CONSUMER = "msg-consumer";
   public static final String KEY_MSG_ACK_NEXT = "msg-acknowledge-next";
   public static final String KEY_MSG_ACK = "msg-acknowledgement";

   protected RestAMQConnection connection;
   protected String destination;
   protected Map<String, String> contextMap = new HashMap<>();

   // consumer options
   protected boolean autoAck;
   protected boolean pushConsumer;

   public RestMessageContext(RestAMQConnection restAMQConnection, String dest) throws IOException {
      this(restAMQConnection, dest, true, false);
   }

   public RestMessageContext(RestAMQConnection restAMQConnection,
                             String dest,
                             boolean isAutoAck,
                             boolean isPush) throws IOException {
      this.connection = restAMQConnection;
      this.destination = dest;
      this.autoAck = isAutoAck;
      this.pushConsumer = isPush;
      prepareSelf();
   }

   private void prepareSelf() throws IOException {
      String destLink = getDestLink();
      CloseableHttpResponse response = connection.request(destLink);
      int code = ResponseUtil.getHttpCode(response);
      if (code != 200) {
         System.out.println("failed to init " + destLink);
         System.out.println("reason: " + ResponseUtil.getDetails(response));
      }
      try {
         Header header = response.getFirstHeader(KEY_MSG_CREATE);
         contextMap.put(KEY_MSG_CREATE, header.getValue());
         header = response.getFirstHeader(KEY_MSG_CREATE_ID);
         contextMap.put(KEY_MSG_CREATE_ID, header.getValue());

         header = response.getFirstHeader(KEY_MSG_PULL);
         if (header != null) {
            contextMap.put(KEY_MSG_PULL, header.getValue());
         }
         header = response.getFirstHeader(KEY_MSG_PUSH);
         if (header != null) {
            contextMap.put(KEY_MSG_PUSH, header.getValue());
         }
         header = response.getFirstHeader(KEY_MSG_PULL_SUB);
         if (header != null) {
            contextMap.put(KEY_MSG_PULL_SUB, header.getValue());
         }
         header = response.getFirstHeader(KEY_MSG_PUSH_SUB);
         if (header != null) {
            contextMap.put(KEY_MSG_PUSH_SUB, header.getValue());
         }
      } finally {
         response.close();
      }
   }

   protected abstract String getDestLink();

   protected abstract String getPullConsumerUri();

   protected abstract String getPushLink(String pushTarget);

   public int postMessage(String content, String type) throws IOException {
      String postUri;
      String nextMsgUri = contextMap.get(KEY_MSG_CREATE_NEXT);
      if (nextMsgUri == null) {
         postUri = contextMap.get(KEY_MSG_CREATE);
      } else {
         postUri = nextMsgUri;
      }
      CloseableHttpResponse response = connection.post(postUri, type, content);
      int code = -1;
      try {
         code = ResponseUtil.getHttpCode(response);
         // check redirection
         if (code == 307) {
            Header redirLoc = response.getFirstHeader("Location");
            contextMap.put(KEY_MSG_CREATE_NEXT, redirLoc.getValue());
            code = postMessage(content, type);// do it again.
         } else if (code == 201) {
            Header header = response.getFirstHeader(KEY_MSG_CREATE_NEXT);
            contextMap.put(KEY_MSG_CREATE_NEXT, header.getValue());
         }
      } finally {
         response.close();
      }
      return code;
   }

   public abstract void initPullConsumers() throws IOException;

   public boolean acknowledgement(boolean ackValue) throws IOException {
      String ackUri = contextMap.get(KEY_MSG_ACK);
      if (ackUri != null) {
         CloseableHttpResponse response = connection.post(ackUri, "application/x-www-form-urlencoded", "acknowledge=" + ackValue);
         int code = ResponseUtil.getHttpCode(response);
         if (code == 200) {
            contextMap.put(KEY_MSG_ACK_NEXT, response.getFirstHeader(KEY_MSG_ACK_NEXT).getValue());
         }
         return true;
      }
      return false;
   }

   public String pullMessage() throws IOException {
      String message = null;
      String msgPullUri = null;
      if (autoAck) {
         msgPullUri = contextMap.get(KEY_MSG_CONSUME_NEXT);
         if (msgPullUri == null) {
            initPullConsumers();
            msgPullUri = contextMap.get(KEY_MSG_CONSUME_NEXT);
         }
      } else {
         msgPullUri = contextMap.get(KEY_MSG_ACK_NEXT);
         if (msgPullUri == null) {
            initPullConsumers();
            msgPullUri = contextMap.get(KEY_MSG_ACK_NEXT);
         }
      }

      CloseableHttpResponse response = connection.post(msgPullUri);
      int code = ResponseUtil.getHttpCode(response);
      try {
         if (code == 200) {
            // success
            HttpEntity entity = response.getEntity();
            long len = entity.getContentLength();

            if (len != -1 && len < 1024000) {
               message = EntityUtils.toString(entity);
            } else {
               // drop message
               System.err.println("Message too large, drop it " + len);
            }

            Header header = response.getFirstHeader(KEY_MSG_CONSUMER);
            contextMap.put(KEY_MSG_CONSUMER, header.getValue());

            if (!autoAck) {
               header = response.getFirstHeader(KEY_MSG_ACK);
               contextMap.put(KEY_MSG_ACK, header.getValue());
            } else {
               header = response.getFirstHeader(KEY_MSG_CONSUME_NEXT);
               contextMap.put(KEY_MSG_CONSUME_NEXT, header.getValue());
            }
         } else if (code == 503) {
            if (autoAck) {
               contextMap.put(KEY_MSG_CONSUME_NEXT, response.getFirstHeader(KEY_MSG_CONSUME_NEXT).getValue());
            } else {
               contextMap.put(KEY_MSG_ACK_NEXT, response.getFirstHeader(KEY_MSG_ACK_NEXT).getValue());
            }

            Header header = response.getFirstHeader("Retry-After");
            if (header != null) {
               long retryDelay = Long.valueOf(response.getFirstHeader("Retry-After").getValue());
               try {
                  Thread.sleep(retryDelay);
               } catch (InterruptedException e) {
                  e.printStackTrace();
               }
               message = pullMessage();
            }
         } else {
            throw new IllegalStateException("error: " + ResponseUtil.getDetails(response));
         }
      } finally {
         response.close();
      }

      return message;
   }

   @Override
   public void close() {
      String consumerUri = contextMap.get(KEY_MSG_CONSUMER);
      if (consumerUri != null) {
         try {
            connection.delete(consumerUri);
            contextMap.remove(KEY_MSG_CONSUMER);
         } catch (ClientProtocolException e) {
            e.printStackTrace();
         } catch (IOException e) {
            e.printStackTrace();
         }
      }
   }

   public void setUpPush(String pushTarget) throws Exception {
      String pushLink = this.contextMap.get(KEY_MSG_PUSH);
      String pushRegXml = "<push-registration>" +
         "<link rel=\"destination\" href=\"" +
         this.connection.getTargetUri() +
         this.getPushLink(pushTarget) + "\"/>" +
         "</push-registration>";

      CloseableHttpResponse response = connection.post(pushLink, "application/xml", pushRegXml);
      int code = ResponseUtil.getHttpCode(response);
      try {
         if (code != 201) {
            System.out.println("Failed to push " + pushRegXml);
            System.out.println("Location: " + pushLink);
            throw new Exception("Failed to register push " + ResponseUtil.getDetails(response));
         }
      } finally {
         response.close();
      }
   }
}
