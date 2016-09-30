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
package org.apache.activemq.artemis.common.testjndi;

import javax.jms.ConnectionFactory;
import javax.jms.Queue;
import javax.jms.Topic;
import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.jndi.LazyCreateContext;
import org.apache.activemq.artemis.uri.ConnectionFactoryParser;

/**
 * A factory of the ActiveMQ Artemis InitialContext which contains
 * {@link ConnectionFactory} instances as well as a child context called
 * <i>destinations</i> which contain all of the current active destinations, in
 * child context depending on the QoS such as transient or durable and queue or
 * topic.
 */
public class TestContextFactory implements InitialContextFactory {

   public static final String REFRESH_TIMEOUT = "refreshTimeout";
   public static final String DISCOVERY_INITIAL_WAIT_TIMEOUT = "discoveryInitialWaitTimeout";
   public static final String DYNAMIC_QUEUE_CONTEXT = "dynamicQueues";
   public static final String DYNAMIC_TOPIC_CONTEXT = "dynamicTopics";
   private String connectionFactoryPrefix = "connectionFactory.";
   private String queuePrefix = "queue.";
   private String topicPrefix = "topic.";

   @Override
   public Context getInitialContext(Hashtable<?, ?> environment) throws NamingException {
      // lets create a factory
      Map<String, Object> data = new ConcurrentHashMap<>();
      for (Map.Entry<?, ?> entry : environment.entrySet()) {
         String key = entry.getKey().toString();
         if (key.startsWith(connectionFactoryPrefix)) {
            String jndiName = key.substring(connectionFactoryPrefix.length());
            try {
               ConnectionFactory factory = createConnectionFactory((String) environment.get(key), jndiName);
               data.put(jndiName, factory);
            } catch (Exception e) {
               e.printStackTrace();
               throw new NamingException("Invalid broker URL");
            }
         }
      }

      createQueues(data, environment);
      createTopics(data, environment);

      data.put(DYNAMIC_QUEUE_CONTEXT, new LazyCreateContext() {
         private static final long serialVersionUID = 6503881346214855588L;

         @Override
         protected Object createEntry(String name) {
            return ActiveMQJMSClient.createQueue(name);
         }
      });
      data.put(DYNAMIC_TOPIC_CONTEXT, new LazyCreateContext() {
         private static final long serialVersionUID = 2019166796234979615L;

         @Override
         protected Object createEntry(String name) {
            return ActiveMQJMSClient.createTopic(name);
         }
      });

      return createContext(environment, data);
   }

   // Properties
   // -------------------------------------------------------------------------
   public String getTopicPrefix() {
      return topicPrefix;
   }

   public void setTopicPrefix(String topicPrefix) {
      this.topicPrefix = topicPrefix;
   }

   public String getQueuePrefix() {
      return queuePrefix;
   }

   public void setQueuePrefix(String queuePrefix) {
      this.queuePrefix = queuePrefix;
   }

   // Implementation methods
   // -------------------------------------------------------------------------

   protected Context createContext(Hashtable<?, ?> environment, Map<String, Object> data) {
      return new TestContext(environment, data);
   }

   protected void createQueues(Map<String, Object> data, Hashtable<?, ?> environment) {
      for (Map.Entry<?, ?> entry : environment.entrySet()) {
         String key = entry.getKey().toString();
         if (key.startsWith(queuePrefix)) {
            String jndiName = key.substring(queuePrefix.length());
            data.put(jndiName, createQueue(entry.getValue().toString()));
         }
      }
   }

   protected void createTopics(Map<String, Object> data, Hashtable<?, ?> environment) {
      for (Map.Entry<?, ?> entry : environment.entrySet()) {
         String key = entry.getKey().toString();
         if (key.startsWith(topicPrefix)) {
            String jndiName = key.substring(topicPrefix.length());
            data.put(jndiName, createTopic(entry.getValue().toString()));
         }
      }
   }

   /**
    * Factory method to create new Queue instances
    */
   protected Queue createQueue(String name) {
      return ActiveMQJMSClient.createQueue(name);
   }

   /**
    * Factory method to create new Topic instances
    */
   protected Topic createTopic(String name) {
      return ActiveMQJMSClient.createTopic(name);
   }

   /**
    * Factory method to create a new connection factory from the given environment
    */
   protected ConnectionFactory createConnectionFactory(String uri, String name) throws Exception {
      ConnectionFactoryParser parser = new ConnectionFactoryParser();
      return parser.newObject(parser.expandURI(uri), name);
   }
}
