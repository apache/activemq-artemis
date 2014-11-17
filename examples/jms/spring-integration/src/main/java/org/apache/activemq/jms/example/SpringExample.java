/*
 * Copyright 2005-2014 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.apache.activemq.jms.example;

import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class SpringExample
{
   public static void main(String[] args) throws Exception
   {
      System.out.println("Creating bean factory...");
      ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(new String[] {"spring-jms-beans.xml"});
      MessageSender sender = (MessageSender)context.getBean("MessageSender");
      System.out.println("Sending message...");
      sender.send("Hello world");
      Thread.sleep(100);
      context.destroy();
   }
}
