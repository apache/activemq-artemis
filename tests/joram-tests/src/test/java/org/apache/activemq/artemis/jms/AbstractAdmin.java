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

import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.objectweb.jtests.jms.admin.Admin;

/**
 * AbstractAdmin.
 */
public class AbstractAdmin implements Admin {

   @Override
   public String getName() {
      return getClass().getName();
   }

   @Override
   public void start() {
   }

   @Override
   public void stop() throws Exception {

   }

   @Override
   public InitialContext createContext() throws NamingException {
      return new InitialContext();
   }

   @Override
   public void createConnectionFactory(final String name) {
      throw new RuntimeException("FIXME NYI createConnectionFactory");
   }

   @Override
   public void deleteConnectionFactory(final String name) {
      throw new RuntimeException("FIXME NYI deleteConnectionFactory");
   }

   @Override
   public void createQueue(final String name) {
      throw new RuntimeException("FIXME NYI createQueue");
   }

   @Override
   public void deleteQueue(final String name) {
      throw new RuntimeException("FIXME NYI deleteQueue");
   }

   @Override
   public void createQueueConnectionFactory(final String name) {
      createConnectionFactory(name);
   }

   @Override
   public void deleteQueueConnectionFactory(final String name) {
      deleteConnectionFactory(name);
   }

   @Override
   public void createTopic(final String name) {
      throw new RuntimeException("FIXME NYI createTopic");
   }

   @Override
   public void deleteTopic(final String name) {
      throw new RuntimeException("FIXME NYI deleteTopic");
   }

   @Override
   public void createTopicConnectionFactory(final String name) {
      createConnectionFactory(name);
   }

   @Override
   public void deleteTopicConnectionFactory(final String name) {
      deleteConnectionFactory(name);
   }

   @Override
   public void startServer() {
   }

   @Override
   public void stopServer() {
   }
}
