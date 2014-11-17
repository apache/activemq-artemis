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
package org.apache.activemq6.jms.tests.message;

import javax.jms.JMSException;
import javax.jms.TextMessage;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 *
 */
public class SimpleJMSTextMessage extends SimpleJMSMessage implements TextMessage
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private String text;

   // Constructors --------------------------------------------------

   public SimpleJMSTextMessage()
   {
      this(null);
   }

   public SimpleJMSTextMessage(final String text)
   {
      this.text = text;
   }

   // TextMessage implementation ------------------------------------

   public void setText(final String text) throws JMSException
   {
      this.text = text;
   }

   public String getText() throws JMSException
   {
      return text;
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
