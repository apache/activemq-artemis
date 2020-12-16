/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.artemis.cli.commands.messages;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.JMSSecurityException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;

import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.activemq.artemis.api.core.Pair;
import org.apache.activemq.artemis.cli.commands.ActionContext;
import org.apache.activemq.artemis.cli.commands.InputAbstract;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.qpid.jms.JmsConnectionFactory;

@Command(name = "transfer", description = "Moves Messages from one destination towards another destination")
public class Transfer extends InputAbstract {

   private static final String DEFAULT_BROKER_URL = "tcp://localhost:61616";

   @Option(name = "--source-url", description = "URL towards the broker. (default: Read from current broker.xml or tcp://localhost:61616 if the default cannot be parsed)")
   protected String sourceURL = DEFAULT_BROKER_URL;

   @Option(name = "--source-user", description = "User used to connect")
   protected String sourceUser;

   @Option(name = "--source-password", description = "Password used to connect")
   protected String sourcePassword;

   @Option(name = "--target-url", description = "URL towards the broker. (default: Read from current broker.xml or tcp://localhost:61616 if the default cannot be parsed)")
   protected String targetURL = DEFAULT_BROKER_URL;

   @Option(name = "--target-user", description = "User used to connect")
   protected String targetUser;

   @Option(name = "--target-password", description = "Password used to connect")
   protected String targetPassword;

   @Option(name = "--receive-timeout", description = "Amount of time (in milliseconds) to wait before giving up the loop. 0 means receiveNoWait, -1 means consumer.receive() waiting forever. (default=5000)")
   int receiveTimeout = 5000;

   @Option(name = "--source-client-id", description = "ClientID to be associated with connection")
   String sourceClientID;

   @Option(name = "--source-protocol", description = "Protocol used. Valid values are amqp or core. Default=core.")
   String sourceProtocol = "core";

   @Option(name = "--source-queue", description = "JMS Queue to be used.")
   String sourceQueue;

   @Option(name = "--shared-durable-subscription", description = "Name of a shared subscription name to be used on the input topic")
   String sharedDurableSubscription;

   @Option(name = "--shared-subscription", description = "Name of a shared subscription name to be used on the input topic")
   String sharedSubscription;

   @Option(name = "--durable-consumer", description = "Name of a durable consumer to be used on the input topic")
   String durableConsumer;

   @Option(name = "--no-Local", description = "Use noLocal when applicable on topic operation")
   boolean noLocal;

   @Option(name = "--source-topic", description = "Destination to be used. It can be prefixed with queue:// or topic:// and can be an FQQN in the form of <address>::<queue>. (Default: queue://TEST)")
   String sourceTopic;

   @Option(name = "--source-filter", description = "filter to be used with the consumer")
   String filter;

   @Option(name = "--target-protocol", description = "Protocol used. Valid values are amqp or core. Default=core.")
   String targetProtocol = "core";

   @Option(name = "--commit-interval", description = "Destination to be used. It can be prefixed with queue:// or topic:// and can be an FQQN in the form of <address>::<queue>. (Default: queue://TEST)")
   int commitInterval = 1000;

   @Option(name = "--copy", description = "If this option is chosen we will perform a copy of the queue by rolling back the original TX on the source.")
   boolean copy;

   @Option(name = "--target-queue", description = "JMS Queue to be used.")
   String targetQueue;

   @Option(name = "--target-topic", description = "Destination to be used. It can be prefixed with queue:// or topic:// and can be an FQQN in the form of <address>::<queue>. (Default: queue://TEST)")
   String targetTopic;

   boolean isCopy() {
      return copy;
   }

   @SuppressWarnings("StringEquality")
   @Override
   public Object execute(ActionContext context) throws Exception {
      super.execute(context);

      // it is intentional to make a comparison on the String object here
      // this is to test if the original option was switched or not.
      // we don't care about being .equals at all.
      // as a matter of fact if you pass brokerURL in a way it's equals to DEFAULT_BROKER_URL,
      // we should not the broker URL Instance
      // and still honor the one passed by parameter.
      // SupressWarnings was added to this method to supress the false positive here from error-prone.
      if (sourceURL == DEFAULT_BROKER_URL) {
         String brokerURLInstance = getBrokerURLInstance();

         if (brokerURLInstance != null) {
            sourceURL = brokerURLInstance;
         }
      }

      System.out.println("Connection brokerURL = " + sourceURL);

      ConnectionFactory sourceConnectionFactory = createConnectionFactory("source", sourceProtocol, sourceURL, sourceUser, sourcePassword, sourceClientID);
      Connection sourceConnection = sourceConnectionFactory.createConnection();

      Session sourceSession = sourceConnection.createSession(Session.SESSION_TRANSACTED);
      Destination sourceDestination = createDestination("source", sourceSession, sourceQueue, sourceTopic);
      MessageConsumer consumer = null;
      if (sourceDestination instanceof Queue) {
         if (filter != null) {
            consumer = sourceSession.createConsumer(sourceDestination, filter);
         } else {
            consumer = sourceSession.createConsumer(sourceDestination);
         }
      } else if (sourceDestination instanceof Topic) {

         Topic topic = (Topic) sourceDestination;

         if (durableConsumer != null) {
            if (filter != null) {
               consumer = sourceSession.createDurableConsumer(topic, durableConsumer);
            } else {
               consumer = sourceSession.createDurableConsumer(topic, durableConsumer, filter, noLocal);
            }
         } else if (sharedDurableSubscription != null) {
            if (filter != null) {
               consumer = sourceSession.createSharedDurableConsumer(topic, sharedDurableSubscription, filter);
            } else {
               consumer = sourceSession.createSharedDurableConsumer(topic, sharedDurableSubscription);
            }
         } else if (sharedSubscription != null) {
            if (filter != null) {
               consumer = sourceSession.createSharedConsumer(topic, sharedSubscription, filter);
            } else {
               consumer = sourceSession.createSharedConsumer(topic, sharedSubscription);
            }
         } else {
            throw new IllegalArgumentException("you have to specify --durable-consumer, --shared-durable-subscription or --shared-subscription with a topic");
         }
      }

      ConnectionFactory targetConnectionFactory = createConnectionFactory("target", targetProtocol, targetURL, targetUser, targetPassword, null);
      Connection targetConnection = targetConnectionFactory.createConnection();
      Session targetSession = targetConnection.createSession(Session.SESSION_TRANSACTED);
      Destination targetDestination = createDestination("target", targetSession, targetQueue, targetTopic);
      MessageProducer producer = targetSession.createProducer(targetDestination);

      if (sourceURL.equals(targetURL) && sourceDestination.equals(targetDestination)) {
         System.out.println("You cannot transfer between " + sourceURL + "/" + sourceDestination + " and " + targetURL + "/" + targetDestination + ".\n" + "That would create an infinite recursion.");
         throw new IllegalArgumentException("cannot use " + sourceDestination + " == " + targetDestination);
      }

      sourceConnection.start();
      int pending = 0, total = 0;
      while (true) {

         Message receivedMessage;
         if (receiveTimeout < 0) {
            receivedMessage = consumer.receive();
         } else if (receiveTimeout == 0) {
            receivedMessage = consumer.receiveNoWait();
         } else {
            receivedMessage = consumer.receive(receiveTimeout);
         }

         if (receivedMessage == null) {
            if (isVerbose()) {
               System.out.println("could not receive any more messages");
            }
            break;
         }
         producer.send(receivedMessage);
         pending++;
         total++;

         if (isVerbose()) {
            System.out.println("Received message " + total + " with " + pending + " messages pending to be commited");
         }
         if (pending > commitInterval) {
            System.out.println("Transferred " + pending + " messages of " + total);
            pending = 0;
            targetSession.commit();
            if (!isCopy()) {
               sourceSession.commit();
            }
         }
      }

      System.out.println("Transferred a total of " + total + " messages");

      if (pending != 0) {
         targetSession.commit();
         if (isCopy()) {
            sourceSession.rollback();
         } else {
            sourceSession.commit();
         }
      }

      sourceConnection.close();
      targetConnection.close();

      return null;
   }

   Destination createDestination(String role, Session session, String queue, String topic) throws Exception {
      if (queue != null && topic != null) {
         throw new IllegalArgumentException("Cannot have topic and queue passed as " + role);
      }

      if (queue != null) {
         return session.createQueue(queue);
      }

      if (topic != null) {
         return session.createTopic(topic);
      }

      throw new IllegalArgumentException("You need to pass either a topic or a queue as " + role);
   }

   protected ConnectionFactory createConnectionFactory(String role,
                                                       String protocol,
                                                       String brokerURL,
                                                       String user,
                                                       String password,
                                                       String clientID) throws Exception {
      if (protocol.equals("core")) {
         if (isVerbose()) {
            System.out.println("Creating " + role + " CORE Connection towards " + brokerURL);
         }
         return createCoreConnectionFactory(brokerURL, user, password, clientID);
      } else if (protocol.equals("amqp")) {
         if (isVerbose()) {
            System.out.println("Creating " + role + " AMQP Connection towards " + brokerURL);
         }
         return createAMQPConnectionFactory(brokerURL, user, password, clientID);
      } else {
         throw new IllegalStateException("protocol " + protocol + " not supported");
      }
   }

   private ConnectionFactory createAMQPConnectionFactory(String brokerURL,
                                                         String user,
                                                         String password,
                                                         String clientID) {
      if (brokerURL.startsWith("tcp://")) {
         // replacing tcp:// by amqp://
         brokerURL = "amqp" + brokerURL.substring(3);
      }
      JmsConnectionFactory cf = new JmsConnectionFactory(user, password, brokerURL);
      if (clientID != null) {
         cf.setClientID(clientID);
      }

      try {
         Connection connection = cf.createConnection();
         connection.close();
         return cf;
      } catch (JMSSecurityException e) {
         // if a security exception will get the user and password through an input
         context.err.println("Connection failed::" + e.getMessage());
         userPassword(brokerURL);
         cf = new JmsConnectionFactory(user, password, brokerURL);
         if (clientID != null) {
            cf.setClientID(clientID);
         }
         return cf;
      } catch (JMSException e) {
         // if a connection exception will ask for the URL, user and password
         context.err.println("Connection failed::" + e.getMessage());
         brokerURL = input("--url", "Type in the broker URL for a retry (e.g. tcp://localhost:61616)", brokerURL);
         userPassword(brokerURL);
         cf = new JmsConnectionFactory(user, password, brokerURL);
         if (clientID != null) {
            cf.setClientID(clientID);
         }
         return cf;
      }
   }

   protected ActiveMQConnectionFactory createCoreConnectionFactory(String brokerURL,
                                                                   String user,
                                                                   String password,
                                                                   String clientID) {
      ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(brokerURL, user, password);

      if (clientID != null) {
         System.out.println("Consumer:: clientID = " + clientID);
         cf.setClientID(clientID);
      }
      try {
         Connection connection = cf.createConnection();
         connection.close();
         return cf;
      } catch (JMSSecurityException e) {
         // if a security exception will get the user and password through an input
         if (context != null) {
            context.err.println("Connection failed::" + e.getMessage());
         }
         Pair<String, String> userPair = userPassword(brokerURL);
         cf = new ActiveMQConnectionFactory(brokerURL, userPair.getA(), userPair.getB());
         if (clientID != null) {
            cf.setClientID(clientID);
         }
         return cf;
      } catch (JMSException e) {
         // if a connection exception will ask for the URL, user and password
         if (context != null) {
            context.err.println("Connection failed::" + e.getMessage());
         }
         brokerURL = input("--url", "Type in the broker URL for a retry (e.g. tcp://localhost:61616)", brokerURL);
         Pair<String, String> userPair = userPassword(brokerURL);
         cf = new ActiveMQConnectionFactory(brokerURL, userPair.getA(), userPair.getB());
         if (clientID != null) {
            cf.setClientID(clientID);
         }
         return cf;
      }
   }

   Pair<String, String> userPassword(String uri) {
      System.out.println("Type in user/password towards " + uri);
      String user, password;
      user = input("--user", "Type the username for a retry", null);
      password = inputPassword("--password", "Type the password for a retry", null);
      return new Pair<>(user, password);
   }

}
