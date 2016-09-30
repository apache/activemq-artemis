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

import javax.xml.bind.JAXBContext;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.net.URL;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.apache.activemq.artemis.core.client.impl.ServerLocatorImpl;
import org.apache.activemq.artemis.core.remoting.impl.invm.InVMConnectorFactory;
import org.apache.activemq.artemis.core.remoting.impl.invm.TransportConstants;
import org.apache.activemq.artemis.jms.client.ConnectionFactoryOptions;
import org.apache.activemq.artemis.rest.queue.DestinationSettings;
import org.apache.activemq.artemis.rest.queue.QueueServiceManager;
import org.apache.activemq.artemis.rest.topic.TopicServiceManager;
import org.apache.activemq.artemis.rest.util.CustomHeaderLinkStrategy;
import org.apache.activemq.artemis.rest.util.LinkHeaderLinkStrategy;
import org.apache.activemq.artemis.rest.util.LinkStrategy;
import org.apache.activemq.artemis.rest.util.TimeoutTask;
import org.apache.activemq.artemis.spi.core.naming.BindingRegistry;
import org.apache.activemq.artemis.utils.ActiveMQThreadFactory;
import org.apache.activemq.artemis.utils.XMLUtil;

public class MessageServiceManager {

   protected ExecutorService threadPool;
   protected QueueServiceManager queueManager;
   protected TopicServiceManager topicManager;
   protected TimeoutTask timeoutTask;
   protected int timeoutTaskInterval = 1;
   protected MessageServiceConfiguration configuration = new MessageServiceConfiguration();
   protected boolean configSet = false;
   protected String configResourcePath;
   protected BindingRegistry registry;

   private ClientSessionFactory consumerSessionFactory;

   public MessageServiceManager(ConnectionFactoryOptions jmsOptions) {
      queueManager = new QueueServiceManager(jmsOptions);
      topicManager = new TopicServiceManager(jmsOptions);
   }

   public BindingRegistry getRegistry() {
      return registry;
   }

   public void setRegistry(BindingRegistry registry) {
      this.registry = registry;
   }

   public int getTimeoutTaskInterval() {
      return timeoutTaskInterval;
   }

   public void setTimeoutTaskInterval(int timeoutTaskInterval) {
      this.timeoutTaskInterval = timeoutTaskInterval;
      if (timeoutTask != null) {
         timeoutTask.setInterval(timeoutTaskInterval);
      }
   }

   public ExecutorService getThreadPool() {
      return threadPool;
   }

   public void setThreadPool(ExecutorService threadPool) {
      this.threadPool = threadPool;
   }

   public QueueServiceManager getQueueManager() {
      return queueManager;
   }

   public TopicServiceManager getTopicManager() {
      return topicManager;
   }

   public MessageServiceConfiguration getConfiguration() {
      return configuration;
   }

   public String getConfigResourcePath() {
      return configResourcePath;
   }

   public void setConfigResourcePath(String configResourcePath) {
      this.configResourcePath = configResourcePath;
   }

   public void setConfiguration(MessageServiceConfiguration configuration) {
      this.configuration = configuration;
      this.configSet = true;
   }

   public void start() throws Exception {
      if (configuration == null || configSet == false) {
         if (configResourcePath == null) {
            configuration = new MessageServiceConfiguration();
         } else {
            URL url = getClass().getClassLoader().getResource(configResourcePath);

            if (url == null) {
               // The URL is outside of the classloader. Trying a pure url now
               url = new URL(configResourcePath);
            }
            JAXBContext jaxb = JAXBContext.newInstance(MessageServiceConfiguration.class);
            Reader reader = new InputStreamReader(url.openStream());
            String xml = XMLUtil.readerToString(reader);
            xml = XMLUtil.replaceSystemProps(xml);
            configuration = (MessageServiceConfiguration) jaxb.createUnmarshaller().unmarshal(new StringReader(xml));
         }
      }
      if (threadPool == null)
         threadPool = Executors.newCachedThreadPool(ActiveMQThreadFactory.defaultThreadFactory());
      timeoutTaskInterval = configuration.getTimeoutTaskInterval();
      timeoutTask = new TimeoutTask(timeoutTaskInterval);
      threadPool.execute(timeoutTask);

      DestinationSettings defaultSettings = new DestinationSettings();
      defaultSettings.setConsumerSessionTimeoutSeconds(configuration.getConsumerSessionTimeoutSeconds());
      defaultSettings.setDuplicatesAllowed(configuration.isDupsOk());
      defaultSettings.setDurableSend(configuration.isDefaultDurableSend());

      HashMap<String, Object> transportConfig = new HashMap<>();
      transportConfig.put(TransportConstants.SERVER_ID_PROP_NAME, configuration.getInVmId());

      ServerLocator consumerLocator = new ServerLocatorImpl(false, new TransportConfiguration(InVMConnectorFactory.class.getName(), transportConfig));
      ActiveMQRestLogger.LOGGER.debug("Created ServerLocator: " + consumerLocator);

      if (configuration.getConsumerWindowSize() != -1) {
         consumerLocator.setConsumerWindowSize(configuration.getConsumerWindowSize());
      }

      consumerSessionFactory = consumerLocator.createSessionFactory();
      ActiveMQRestLogger.LOGGER.debug("Created ClientSessionFactory: " + consumerSessionFactory);

      ServerLocator defaultLocator = new ServerLocatorImpl(false, new TransportConfiguration(InVMConnectorFactory.class.getName(), transportConfig));

      ClientSessionFactory sessionFactory = defaultLocator.createSessionFactory();

      LinkStrategy linkStrategy = new LinkHeaderLinkStrategy();
      if (configuration.isUseLinkHeaders()) {
         linkStrategy = new LinkHeaderLinkStrategy();
      } else {
         linkStrategy = new CustomHeaderLinkStrategy();
      }

      queueManager.setServerLocator(defaultLocator);
      queueManager.setSessionFactory(sessionFactory);
      queueManager.setTimeoutTask(timeoutTask);
      queueManager.setConsumerServerLocator(consumerLocator);
      queueManager.setConsumerSessionFactory(consumerSessionFactory);
      queueManager.setDefaultSettings(defaultSettings);
      queueManager.setPushStoreFile(configuration.getQueuePushStoreDirectory());
      queueManager.setProducerPoolSize(configuration.getProducerSessionPoolSize());
      queueManager.setProducerTimeToLive(configuration.getProducerTimeToLive());
      queueManager.setLinkStrategy(linkStrategy);
      queueManager.setRegistry(registry);

      topicManager.setServerLocator(defaultLocator);
      topicManager.setSessionFactory(sessionFactory);
      topicManager.setTimeoutTask(timeoutTask);
      topicManager.setConsumerServerLocator(consumerLocator);
      topicManager.setConsumerSessionFactory(consumerSessionFactory);
      topicManager.setDefaultSettings(defaultSettings);
      topicManager.setPushStoreFile(configuration.getTopicPushStoreDirectory());
      topicManager.setProducerPoolSize(configuration.getProducerSessionPoolSize());
      queueManager.setProducerTimeToLive(configuration.getProducerTimeToLive());
      topicManager.setLinkStrategy(linkStrategy);
      topicManager.setRegistry(registry);

      queueManager.start();
      topicManager.start();
   }

   public void stop() {
      if (queueManager != null)
         queueManager.stop();
      queueManager = null;
      if (topicManager != null)
         topicManager.stop();
      topicManager = null;
      this.timeoutTask.stop();
      threadPool.shutdown();
      try {
         threadPool.awaitTermination(5000, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
      }
      this.consumerSessionFactory.close();
   }
}
