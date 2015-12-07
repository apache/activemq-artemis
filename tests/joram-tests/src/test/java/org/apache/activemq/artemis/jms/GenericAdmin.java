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
package org.apache.activemq.artemis.jms;

import javax.naming.Context;
import javax.naming.NamingException;

import org.jboss.logging.Logger;
import org.objectweb.jtests.jms.admin.Admin;

/**
 * GenericAdmin.
 *
 * @FIXME delegate to a JBoss defined admin class
 */
public class GenericAdmin implements Admin {

   public static final Logger log = Logger.getLogger(GenericAdmin.class);

   public static Admin delegate = new AbstractAdmin();

   @Override
   public String getName() {
      String name = GenericAdmin.delegate.getName();
      GenericAdmin.log.debug("Using admin '" + name + "' delegate=" + GenericAdmin.delegate);
      return name;
   }

   @Override
   public void start() throws Exception {
   }

   @Override
   public void stop() throws Exception {
   }

   @Override
   public Context createContext() throws NamingException {
      Context ctx = GenericAdmin.delegate.createContext();
      GenericAdmin.log.debug("Using initial context: " + ctx.getEnvironment());
      return ctx;
   }

   @Override
   public void createConnectionFactory(final String name) {
      GenericAdmin.log.debug("createConnectionFactory '" + name + "'");
      GenericAdmin.delegate.createConnectionFactory(name);
   }

   @Override
   public void deleteConnectionFactory(final String name) {
      GenericAdmin.log.debug("deleteConnectionFactory '" + name + "'");
      GenericAdmin.delegate.deleteConnectionFactory(name);
   }

   @Override
   public void createQueue(final String name) {
      GenericAdmin.log.debug("createQueue '" + name + "'");
      GenericAdmin.delegate.createQueue(name);
   }

   @Override
   public void deleteQueue(final String name) {
      GenericAdmin.log.debug("deleteQueue '" + name + "'");
      GenericAdmin.delegate.deleteQueue(name);
   }

   @Override
   public void createQueueConnectionFactory(final String name) {
      GenericAdmin.log.debug("createQueueConnectionFactory '" + name + "'");
      GenericAdmin.delegate.createQueueConnectionFactory(name);
   }

   @Override
   public void deleteQueueConnectionFactory(final String name) {
      GenericAdmin.log.debug("deleteQueueConnectionFactory '" + name + "'");
      GenericAdmin.delegate.deleteQueueConnectionFactory(name);
   }

   @Override
   public void createTopic(final String name) {
      GenericAdmin.log.debug("createTopic '" + name + "'");
      GenericAdmin.delegate.createTopic(name);
   }

   @Override
   public void deleteTopic(final String name) {
      GenericAdmin.log.debug("deleteTopic '" + name + "'");
      GenericAdmin.delegate.deleteTopic(name);
   }

   @Override
   public void createTopicConnectionFactory(final String name) {
      GenericAdmin.log.debug("createTopicConnectionFactory '" + name + "'");
      GenericAdmin.delegate.createTopicConnectionFactory(name);
   }

   @Override
   public void deleteTopicConnectionFactory(final String name) {
      GenericAdmin.log.debug("deleteTopicConnectionFactory '" + name + "'");
      GenericAdmin.delegate.deleteTopicConnectionFactory(name);
   }

   @Override
   public void startServer() throws Exception {
      GenericAdmin.log.debug("startEmbeddedServer");
      GenericAdmin.delegate.startServer();
   }

   @Override
   public void stopServer() throws Exception {
      GenericAdmin.log.debug("stopEmbeddedServer");
      GenericAdmin.delegate.stopServer();
   }
}
