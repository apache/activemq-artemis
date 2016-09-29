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
package org.apache.activemq.artemis.jms.example;

import java.io.Serializable;

/**
 * Class that holds the parameters used in the performance examples
 */
public class PerfParams implements Serializable {

   private static final long serialVersionUID = -4336539641012356002L;

   enum ClientLibraryType {
      core, openwire, amqp
   }

   private ClientLibraryType libraryType;

   private int noOfMessagesToSend = 1000;

   private int noOfWarmupMessages;

   private int messageSize = 1024; // in bytes

   private boolean durable = false;

   private boolean isSessionTransacted = false;

   private int batchSize = 5000;

   private boolean drainQueue = true;

   private String destinationName;

   private int throttleRate;

   private boolean disableMessageID;

   private boolean disableTimestamp;

   private boolean dupsOK;

   private String uri;

   public String getUri() {
      return uri;
   }

   public void setUri(String uri) {
      this.uri = uri;
   }

   public synchronized int getNoOfMessagesToSend() {
      return noOfMessagesToSend;
   }

   public synchronized void setNoOfMessagesToSend(final int noOfMessagesToSend) {
      this.noOfMessagesToSend = noOfMessagesToSend;
   }

   public synchronized int getNoOfWarmupMessages() {
      return noOfWarmupMessages;
   }

   public synchronized void setNoOfWarmupMessages(final int noOfWarmupMessages) {
      this.noOfWarmupMessages = noOfWarmupMessages;
   }

   public synchronized int getMessageSize() {
      return messageSize;
   }

   public synchronized void setMessageSize(final int messageSize) {
      this.messageSize = messageSize;
   }

   public synchronized boolean isDurable() {
      return durable;
   }

   public synchronized void setDurable(final boolean durable) {
      this.durable = durable;
   }

   public synchronized boolean isSessionTransacted() {
      return isSessionTransacted;
   }

   public synchronized void setSessionTransacted(final boolean isSessionTransacted) {
      this.isSessionTransacted = isSessionTransacted;
   }

   public synchronized int getBatchSize() {
      return batchSize;
   }

   public synchronized void setBatchSize(final int batchSize) {
      this.batchSize = batchSize;
   }

   public String getDestinationName() {
      return destinationName;
   }

   public void setDestinationName(String destinationName) {
      this.destinationName = destinationName;
   }

   public synchronized boolean isDrainQueue() {
      return drainQueue;
   }

   public synchronized void setDrainQueue(final boolean drainQueue) {
      this.drainQueue = drainQueue;
   }

   public synchronized int getThrottleRate() {
      return throttleRate;
   }

   public synchronized void setThrottleRate(final int throttleRate) {
      this.throttleRate = throttleRate;
   }

   public synchronized boolean isDisableMessageID() {
      return disableMessageID;
   }

   public synchronized void setDisableMessageID(final boolean disableMessageID) {
      this.disableMessageID = disableMessageID;
   }

   public synchronized boolean isDisableTimestamp() {
      return disableTimestamp;
   }

   public synchronized void setDisableTimestamp(final boolean disableTimestamp) {
      this.disableTimestamp = disableTimestamp;
   }

   public synchronized boolean isDupsOK() {
      return dupsOK;
   }

   public synchronized void setDupsOK(final boolean dupsOK) {
      this.dupsOK = dupsOK;
   }

   public boolean isOpenwire() {
      return libraryType.equals(ClientLibraryType.openwire);
   }

   public boolean isAMQP() {
      return libraryType.equals(ClientLibraryType.amqp);
   }

   public boolean isCore() {
      return libraryType.equals(ClientLibraryType.core);
   }

   public ClientLibraryType getLibraryType() {
      return libraryType;
   }

   public void setLibraryType(ClientLibraryType libraryType) {
      this.libraryType = libraryType;
   }

   public void setLibraryType(String libraryType) {
      this.libraryType = ClientLibraryType.valueOf(libraryType);
   }

}
