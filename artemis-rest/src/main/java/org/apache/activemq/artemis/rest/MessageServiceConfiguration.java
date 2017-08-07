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
package org.apache.activemq.artemis.rest;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "rest-messaging")
public class MessageServiceConfiguration {

   private int producerSessionPoolSize = 10;
   private long producerTimeToLive = -1;
   private int timeoutTaskInterval = 1;
   private int consumerSessionTimeoutSeconds = 300;
   @Deprecated
   private int consumerWindowSize = -1;
   private boolean defaultDurableSend = false;
   private boolean dupsOk = true;
   private String topicPushStoreDirectory = "topic-push-store";
   private String queuePushStoreDirectory = "queue-push-store";
   @Deprecated
   private String inVmId = "0";
   private String url = "vm://0";
   private boolean useLinkHeaders = false;

   private String deserializationWhiteList;
   private String deserializationBlackList;

   @XmlElement(name = "server-in-vm-id")
   public String getInVmId() {
      return inVmId;
   }

   public void setInVmId(String inVmId) {
      ActiveMQRestLogger.LOGGER.deprecatedConfiguration("server-in-vm-id", "url");
      this.inVmId = inVmId;
   }

   @XmlElement(name = "use-link-headers")
   public boolean isUseLinkHeaders() {
      return useLinkHeaders;
   }

   public void setUseLinkHeaders(boolean useLinkHeaders) {
      this.useLinkHeaders = useLinkHeaders;
   }

   @XmlElement(name = "default-durable-send")
   public boolean isDefaultDurableSend() {
      return defaultDurableSend;
   }

   public void setDefaultDurableSend(boolean defaultDurableSend) {
      this.defaultDurableSend = defaultDurableSend;
   }

   @XmlElement(name = "dups-ok")
   public boolean isDupsOk() {
      return dupsOk;
   }

   public void setDupsOk(boolean dupsOk) {
      this.dupsOk = dupsOk;
   }

   @XmlElement(name = "topic-push-store-dir")
   public String getTopicPushStoreDirectory() {
      return topicPushStoreDirectory;
   }

   public void setTopicPushStoreDirectory(String topicPushStoreDirectory) {
      this.topicPushStoreDirectory = topicPushStoreDirectory;
   }

   @XmlElement(name = "queue-push-store-dir")
   public String getQueuePushStoreDirectory() {
      return queuePushStoreDirectory;
   }

   public void setQueuePushStoreDirectory(String queuePushStoreDirectory) {
      this.queuePushStoreDirectory = queuePushStoreDirectory;
   }

   @XmlElement(name = "producer-time-to-live")
   public long getProducerTimeToLive() {
      return producerTimeToLive;
   }

   public void setProducerTimeToLive(long producerTimeToLive) {
      this.producerTimeToLive = producerTimeToLive;
   }

   @XmlElement(name = "producer-session-pool-size")
   public int getProducerSessionPoolSize() {
      return producerSessionPoolSize;
   }

   public void setProducerSessionPoolSize(int producerSessionPoolSize) {
      this.producerSessionPoolSize = producerSessionPoolSize;
   }

   @XmlElement(name = "session-timeout-task-interval")
   public int getTimeoutTaskInterval() {
      return timeoutTaskInterval;
   }

   public void setTimeoutTaskInterval(int timeoutTaskInterval) {
      this.timeoutTaskInterval = timeoutTaskInterval;
   }

   @XmlElement(name = "consumer-session-timeout-seconds")
   public int getConsumerSessionTimeoutSeconds() {
      return consumerSessionTimeoutSeconds;
   }

   public void setConsumerSessionTimeoutSeconds(int consumerSessionTimeoutSeconds) {
      this.consumerSessionTimeoutSeconds = consumerSessionTimeoutSeconds;
   }

   @XmlElement(name = "consumer-window-size")
   public int getConsumerWindowSize() {
      return consumerWindowSize;
   }

   public void setConsumerWindowSize(int consumerWindowSize) {
      ActiveMQRestLogger.LOGGER.deprecatedConfiguration("consumer-window-size", "url");
      this.consumerWindowSize = consumerWindowSize;
   }

   @XmlElement(name = "url")
   public String getUrl() {
      return url;
   }

   public void setUrl(String url) {
      this.url = url;
   }

   public String getDeserializationWhiteList() {
      return deserializationWhiteList;
   }

   public void setDeserializationWhiteList(String deserializationWhiteList) {
      this.deserializationWhiteList = deserializationWhiteList;
   }

   public String getDeserializationBlackList() {
      return deserializationBlackList;
   }

   public void setDeserializationBlackList(String deserializationBlackList) {
      this.deserializationBlackList = deserializationBlackList;
   }
}
