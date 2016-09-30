/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.junit;

import java.util.Map;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.ActiveMQClient;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractActiveMQClientResource extends ExternalResource {

   Logger log = LoggerFactory.getLogger(this.getClass());

   boolean autoCreateQueue = true;

   ServerLocator serverLocator;
   ClientSessionFactory sessionFactory;
   ClientSession session;

   public AbstractActiveMQClientResource(String url) {
      if (url == null) {
         throw new IllegalArgumentException(String.format("Error creating {} - url cannot be null", this.getClass().getSimpleName()));
      }

      try {
         this.serverLocator = ActiveMQClient.createServerLocator(url);
      } catch (Exception ex) {
         throw new RuntimeException(String.format("Error creating {} - createServerLocator( {} ) failed", this.getClass().getSimpleName(), url.toString()), ex);
      }
   }

   public AbstractActiveMQClientResource(ServerLocator serverLocator) {
      if (serverLocator == null) {
         throw new IllegalArgumentException(String.format("Error creating {} - ServerLocator cannot be null", this.getClass().getSimpleName()));
      }

      this.serverLocator = serverLocator;
   }

   /**
    * Adds properties to a ClientMessage
    *
    * @param message
    * @param properties
    */
   public static void addMessageProperties(ClientMessage message, Map<String, Object> properties) {
      if (properties != null && properties.size() > 0) {
         for (Map.Entry<String, Object> property : properties.entrySet()) {
            message.putObjectProperty(property.getKey(), property.getValue());
         }
      }
   }

   @Override
   protected void before() throws Throwable {
      super.before();
      start();
   }

   @Override
   protected void after() {
      stop();
      super.after();
   }

   void start() {
      log.info("Starting {}", this.getClass().getSimpleName());
      try {
         sessionFactory = serverLocator.createSessionFactory();
         session = sessionFactory.createSession();
      } catch (RuntimeException runtimeEx) {
         throw runtimeEx;
      } catch (Exception ex) {
         throw new ActiveMQClientResourceException(String.format("%s initialisation failure", this.getClass().getSimpleName()), ex);
      }

      createClient();

      try {
         session.start();
      } catch (ActiveMQException amqEx) {
         throw new ActiveMQClientResourceException(String.format("%s startup failure", this.getClass().getSimpleName()), amqEx);
      }
   }

   void stop() {
      stopClient();
      if (session != null) {
         try {
            session.close();
         } catch (ActiveMQException amqEx) {
            log.warn("ActiveMQException encountered closing InternalClient ClientSession - ignoring", amqEx);
         } finally {
            session = null;
         }
      }
      if (sessionFactory != null) {
         sessionFactory.close();
         sessionFactory = null;
      }
      if (serverLocator != null) {
         serverLocator.close();
         serverLocator = null;
      }

   }

   protected abstract void createClient();

   protected abstract void stopClient();

   public boolean isAutoCreateQueue() {
      return autoCreateQueue;
   }

   public void setAutoCreateQueue(boolean autoCreateQueue) {
      this.autoCreateQueue = autoCreateQueue;
   }

   public static class ActiveMQClientResourceException extends RuntimeException {

      public ActiveMQClientResourceException(String message) {
         super(message);
      }

      public ActiveMQClientResourceException(String message, Exception cause) {
         super(message, cause);
      }
   }
}
