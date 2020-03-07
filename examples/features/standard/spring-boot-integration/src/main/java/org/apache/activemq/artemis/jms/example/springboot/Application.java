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
package org.apache.activemq.artemis.jms.example.springboot;

import org.apache.activemq.artemis.core.config.impl.SecurityConfiguration;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.spi.core.security.jaas.InVMLoginModule;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.jms.annotation.EnableJms;

/**
 * @see <a href="https://spring.io/guides/gs/messaging-jms/">Spring JMS Messaging Guide</a>
 */
@SpringBootApplication
@EnableJms
public class Application {
   public Application() {

   }

   public static void main(String[] args) throws InterruptedException {
      final ConfigurableApplicationContext context = SpringApplication.run(Application.class);
      System.out.println("********************* Sending message...");
      MessageSender sender = context.getBean(MessageSender.class);
      sender.send("Hello Artemis!");
      Thread.sleep(1000);
      context.close();
   }

   @Bean
   public ActiveMQJAASSecurityManager securityManager(@Value("${amq.broker.user}") String user,
                                                      @Value("${amq.broker.password}") String password,
                                                      @Value("${amq.broker.role}") String role) {
      final SecurityConfiguration configuration = new SecurityConfiguration();
      final ActiveMQJAASSecurityManager securityManager =
               new ActiveMQJAASSecurityManager(InVMLoginModule.class.getName(), configuration);
      configuration.addUser(user, password);
      configuration.addRole(user, role);
      configuration.setDefaultUser(user);

      return securityManager;
   }

   @Bean(initMethod = "start", destroyMethod = "stop")
   public EmbeddedActiveMQ embeddedActiveMQ(ActiveMQJAASSecurityManager securityManager) {
      final EmbeddedActiveMQ embeddedActiveMQ = new EmbeddedActiveMQ();
      embeddedActiveMQ.setSecurityManager(securityManager);
      return embeddedActiveMQ;
   }

}
